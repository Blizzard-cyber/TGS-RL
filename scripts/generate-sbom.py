#!/usr/bin/env python3
"""Generate a deterministic, lockfile-derived SPDX-lite SBOM."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "compatibility" / "sbom" / "lockfiles.spdx.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def package_id(ecosystem: str, name: str, version: str) -> str:
    value = re.sub(r"[^A-Za-z0-9.-]+", "-", f"{ecosystem}-{name}-{version}")
    return f"SPDXRef-Package-{value.strip('-')}"


def scoped_package_id(ecosystem: str, name: str, version: str, scope: str) -> str:
    base = package_id(ecosystem, name, version)
    normalized = re.sub(r"[^A-Za-z0-9.-]+", "-", scope).strip("-")
    return f"{base}-{normalized}"


def package(
    ecosystem: str,
    name: str,
    version: str,
    *,
    scope: str,
    source: str | None = None,
    integrity: str | None = None,
    license_id: str | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "SPDXID": package_id(ecosystem, name, version),
        "name": name,
        "versionInfo": version,
        "downloadLocation": source or "NOASSERTION",
        "licenseConcluded": "NOASSERTION",
        "licenseDeclared": license_id or "NOASSERTION",
        "supplier": "NOASSERTION",
        "externalRefs": [
            {
                "referenceCategory": "PACKAGE-MANAGER",
                "referenceType": "purl",
                "referenceLocator": f"pkg:{ecosystem}/{name}@{version}",
            }
        ],
        "properties": {"ecosystem": ecosystem, "scope": scope},
    }
    if integrity:
        item["checksums"] = [{"algorithm": "OTHER", "checksumValue": integrity}]
    return item


def go_packages() -> list[dict[str, Any]]:
    content = (ROOT / "go.mod").read_text(encoding="utf-8")
    indirect: set[str] = set()
    versions: dict[str, str] = {}
    for line in content.splitlines():
        match = re.match(r"\s*([^\s]+)\s+v([^\s]+)(\s+// indirect)?$", line)
        if not match:
            continue
        name, version, marker = match.groups()
        versions[name] = version
        if marker:
            indirect.add(name)
    sums: dict[tuple[str, str], str] = {}
    for line in (ROOT / "go.sum").read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) == 3 and not parts[1].endswith("/go.mod"):
            sums[(parts[0], parts[1].removeprefix("v"))] = parts[2]
    return [
        package(
            "golang",
            name,
            version,
            scope="indirect" if name in indirect else "runtime",
            source=f"https://proxy.golang.org/{name}/@v/v{version}.zip",
            integrity=sums.get((name, version)),
        )
        for name, version in sorted(versions.items())
    ]


def python_packages() -> list[dict[str, Any]]:
    lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    result: list[dict[str, Any]] = []
    for item in lock.get("package", []):
        name = item.get("name")
        version = item.get("version")
        if not isinstance(name, str) or not isinstance(version, str):
            continue
        source_data = item.get("source", {})
        source = source_data.get("registry") if isinstance(source_data, dict) else None
        result.append(package("pypi", name, version, scope="locked", source=source))
    return sorted(result, key=lambda value: (value["name"], value["versionInfo"]))


def gpu_workload_packages() -> list[dict[str, Any]]:
    """Read the pip-compile style lock for the isolated GPU workload image."""
    path = ROOT / "configs" / "hardware" / "gpu-requirements.lock"
    result: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([A-Za-z0-9_.-]+)==([^ \\]+)(?: \\)?", line)
        if match is None:
            continue
        name, version = match.groups()
        item = package(
            "pypi",
            name,
            version,
            scope="gpu-workload",
            source=(
                "https://download.pytorch.org/whl/cu130"
                if name.casefold() == "torch" and "+cu130" in version
                else "https://pypi.org/simple"
            ),
        )
        item["SPDXID"] = scoped_package_id("pypi", name, version, "gpu-workload")
        result.append(item)
    if not result:
        raise ValueError(f"GPU workload lock contains no packages: {path}")
    return sorted(result, key=lambda value: (value["name"], value["versionInfo"]))


def node_packages() -> list[dict[str, Any]]:
    lock = json.loads((ROOT / "console" / "package-lock.json").read_text(encoding="utf-8"))
    result: list[dict[str, Any]] = []
    for path, item in lock.get("packages", {}).items():
        if not path.startswith("node_modules/") or not isinstance(item, dict):
            continue
        name = path.rsplit("node_modules/", maxsplit=1)[-1]
        version = item.get("version")
        if not isinstance(version, str):
            continue
        result.append(
            package(
                "npm",
                name,
                version,
                scope="development" if item.get("dev") else "runtime",
                source=item.get("resolved"),
                integrity=item.get("integrity"),
                license_id=item.get("license"),
            )
        )
    return sorted(result, key=lambda value: (value["name"], value["versionInfo"]))


def build_document() -> dict[str, Any]:
    lockfiles = [
        Path("go.mod"),
        Path("go.sum"),
        Path("uv.lock"),
        Path("console/package-lock.json"),
        Path("configs/hardware/gpu-requirements.lock"),
    ]
    packages_by_id = {
        item["SPDXID"]: item
        for item in go_packages() + python_packages() + gpu_workload_packages() + node_packages()
    }
    packages = sorted(packages_by_id.values(), key=lambda item: item["SPDXID"])
    return {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": "TGS-RL-lockfile-sbom",
        "documentNamespace": "https://github.com/Blizzard-cyber/TGS-RL/sbom/lockfiles",
        "creationInfo": {
            "creators": ["Tool: scripts/generate-sbom.py"],
            "comment": "Deterministic lockfile snapshot; generation time intentionally omitted.",
        },
        "documentDescribes": [item["SPDXID"] for item in packages],
        "lockfiles": [
            {"path": path.as_posix(), "sha256": sha256(ROOT / path)} for path in lockfiles
        ],
        "packages": packages,
    }


def render() -> str:
    return json.dumps(build_document(), indent=2, sort_keys=True, ensure_ascii=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if the committed SBOM is stale")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    expected = render()
    try:
        display_output = output.relative_to(ROOT).as_posix()
    except ValueError:
        display_output = str(output)
    if args.check:
        if not output.is_file() or output.read_text(encoding="utf-8") != expected:
            print(
                f"error: {display_output} is stale; run scripts/generate-sbom.py", file=sys.stderr
            )
            return 1
        print(f"SBOM is current: {display_output}")
        return 0
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(expected, encoding="utf-8")
    print(f"wrote {display_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
