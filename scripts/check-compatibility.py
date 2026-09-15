#!/usr/bin/env python3
"""Validate compatibility claims and their repository evidence."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "compatibility" / "matrix.json"
COMPOSE = ROOT / "compose.yaml"
DOCKERFILES = (
    ROOT / "Dockerfile.operator",
    ROOT / "Dockerfile.local",
    ROOT / "Dockerfile.gpu-smoke",
)
PINNED_IMAGE_REFERENCES = (ROOT / "scripts" / "gpu-preflight.sh",)
BOM = ROOT / "compatibility" / "bom" / "runtime.yaml"
SUPPORTED = "supported"
HARDWARE_VERIFIED = "hardware-verified-single-node"
HARDWARE_PENDING = "implemented-hardware-verification-pending"
CONDITIONAL = "conditional"
UNSUPPORTED = "unsupported"
ALLOWED_STATUS = {SUPPORTED, HARDWARE_VERIFIED, HARDWARE_PENDING, CONDITIONAL, UNSUPPORTED}
REAL_COMPONENTS = {"verl", "openrlhf", "ray", "pytorch", "vllm", "sglang", "nvidia"}


def locked_names() -> set[str]:
    names: set[str] = set()
    for path in (
        ROOT / "go.mod",
        ROOT / "uv.lock",
        ROOT / "console" / "package-lock.json",
        ROOT / "configs" / "hardware" / "gpu-requirements.lock",
    ):
        text = path.read_text(encoding="utf-8").lower()
        names.update(re.findall(r'(?:name\s*=\s*|"name"\s*:\s*)"([^"]+)"', text))
        names.update(re.findall(r"^([a-z0-9_.-]+)==", text, flags=re.MULTILINE))
    if "torch" in names:
        names.add("pytorch")
    return names


def validate_path(raw: str, context: str, errors: list[str]) -> None:
    path = Path(raw)
    if path.is_absolute() or ".." in path.parts:
        errors.append(f"{context}: evidence path must be repository-relative: {raw}")
        return
    if not (ROOT / path).exists():
        errors.append(f"{context}: evidence path does not exist: {raw}")


def validate_matrix(data: dict[str, Any], errors: list[str]) -> None:
    combinations = data.get("combinations")
    if not isinstance(combinations, list) or not combinations:
        errors.append("matrix: combinations must be a non-empty list")
        return
    seen: set[str] = set()
    covered: set[str] = set()
    locks = locked_names()
    for item in combinations:
        if not isinstance(item, dict):
            errors.append("matrix: each combination must be an object")
            continue
        combination_id = item.get("id")
        status = item.get("status")
        if not isinstance(combination_id, str) or not combination_id:
            errors.append("matrix: combination id is required")
            continue
        if combination_id in seen:
            errors.append(f"matrix: duplicate combination id: {combination_id}")
        seen.add(combination_id)
        if status not in ALLOWED_STATUS:
            errors.append(f"{combination_id}: invalid status {status!r}")
        components = item.get("components", {})
        if not isinstance(components, dict):
            errors.append(f"{combination_id}: components must be an object")
            continue
        values = {str(value).lower() for value in components.values()}
        covered.update(values & REAL_COMPONENTS)
        evidence = item.get("evidence", [])
        if not isinstance(evidence, list) or not evidence:
            errors.append(f"{combination_id}: evidence must be non-empty")
        else:
            for path in evidence:
                if isinstance(path, str):
                    validate_path(path, combination_id, errors)
                else:
                    errors.append(f"{combination_id}: evidence entries must be strings")
        required = item.get("required_dependencies", [])
        missing = item.get("missing_dependencies", [])
        if not isinstance(required, list) or not all(isinstance(value, str) for value in required):
            errors.append(f"{combination_id}: required_dependencies must be string list")
            continue
        expected_missing = sorted(name for name in required if name.lower() not in locks)
        if (
            status in {HARDWARE_VERIFIED, HARDWARE_PENDING, CONDITIONAL}
            and sorted(missing) != expected_missing
        ):
            errors.append(
                f"{combination_id}: missing_dependencies must equal "
                f"lockfile gaps {expected_missing}"
            )
        if status == SUPPORTED and expected_missing:
            errors.append(
                f"{combination_id}: supported claim has missing dependencies {expected_missing}"
            )
        if status == SUPPORTED and values & REAL_COMPONENTS:
            errors.append(
                f"{combination_id}: real dependency combination cannot be "
                "marked supported by this offline gate"
            )
        if status in {HARDWARE_VERIFIED, HARDWARE_PENDING} and not values & REAL_COMPONENTS:
            errors.append(f"{combination_id}: hardware evidence status requires a real component")
    missing_components = sorted(REAL_COMPONENTS - covered)
    if missing_components:
        errors.append(f"matrix: declared real components are not covered: {missing_components}")


def validate_yaml_evidence(errors: list[str]) -> None:
    quoted_value = re.compile(r'["\']([^"\']+)["\']')
    repository_roots = {
        "adapters",
        "compatibility",
        "configs",
        "console",
        "scheduler-go",
        "scripts",
        "tests",
        "upstream",
    }
    repository_files = {"go.mod", "go.sum", "pyproject.toml", "uv.lock"}
    for yaml_path in sorted((ROOT / "compatibility").rglob("*.yaml")):
        text = yaml_path.read_text(encoding="utf-8")
        status_match = re.search(r'^status:\s*"([^"]+)"', text, flags=re.MULTILINE)
        if (
            yaml_path.parent.name in {"manifests", "profiles"}
            and status_match
            and status_match.group(1)
            not in {
                SUPPORTED,
                "supported-with-limitations",
                HARDWARE_VERIFIED,
                HARDWARE_PENDING,
                CONDITIONAL,
                UNSUPPORTED,
            }
        ):
            errors.append(
                f"{yaml_path.relative_to(ROOT)}: invalid status {status_match.group(1)!r}"
            )
        if status_match and status_match.group(1) == HARDWARE_VERIFIED:
            if 'status: "gpu-single-node"' not in text and (
                'evidence_status: "gpu-single-node"' not in text
            ):
                errors.append(
                    f"{yaml_path.relative_to(ROOT)}: single-node hardware status "
                    "requires gpu-single-node evidence"
                )
            if "docs/validation/" not in text:
                errors.append(
                    f"{yaml_path.relative_to(ROOT)}: single-node hardware status "
                    "requires a validation document"
                )
        for line_number, line in enumerate(text.splitlines(), 1):
            for match in quoted_value.findall(line):
                path = Path(match)
                if match not in repository_files and (
                    not path.parts or path.parts[0] not in repository_roots
                ):
                    continue
                validate_path(match, f"{yaml_path.relative_to(ROOT)}:{line_number}", errors)


def parse_compose_services(text: str) -> dict[str, dict[str, str]]:
    services: dict[str, dict[str, str]] = {}
    in_services = False
    current_service: str | None = None
    service_indent: int | None = None
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 0 and stripped == "services:":
            in_services = True
            current_service = None
            service_indent = None
            continue
        if indent == 0 and in_services:
            break
        if not in_services:
            continue
        service_match = re.match(r"^(\s{2})([a-zA-Z0-9_-]+):\s*$", line)
        if service_match:
            current_service = service_match.group(2)
            services[current_service] = {}
            service_indent = indent
            continue
        if current_service is None or service_indent is None:
            continue
        if indent <= service_indent:
            current_service = None
            service_indent = None
            continue
        image_match = re.match(r'^\s+image:\s*"([^"]+)"\s*$', line)
        if image_match:
            services[current_service]["image"] = image_match.group(1)
        dockerfile_match = re.match(r"^\s+dockerfile:\s*([^\s#]+)\s*$", line)
        if dockerfile_match:
            services[current_service]["dockerfile"] = dockerfile_match.group(1)
    return services


def parse_bom_images(text: str) -> dict[str, str]:
    images: dict[str, str] = {}
    in_development_images = False
    current_image: str | None = None
    current_digest: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if line.startswith("development_images:"):
            in_development_images = True
            current_image = None
            current_digest = None
            continue
        if not in_development_images:
            continue
        if not line.startswith("  "):
            break
        if line.startswith("  - "):
            if current_image and current_digest:
                images[current_image] = current_digest
            current_image = None
            current_digest = None
        image_match = re.match(r'^\s+image:\s*"([^"]+)"\s*$', line)
        if image_match:
            current_image = image_match.group(1)
            continue
        digest_match = re.match(r'^\s+image_digest:\s*"([^"]+)"\s*$', line)
        if digest_match:
            current_digest = digest_match.group(1)
    if current_image and current_digest:
        images[current_image] = current_digest
    return images


def parse_image_reference(image: str) -> tuple[str, str | None]:
    if "@" not in image:
        return image, None
    repository, digest = image.rsplit("@", 1)
    return repository, digest


def parse_dockerfile_images(text: str) -> list[str]:
    images: list[str] = []
    arguments: dict[str, str] = {}
    for line in text.splitlines():
        argument_match = re.match(r"^\s*ARG\s+([A-Za-z_][A-Za-z0-9_]*)=(\S+)\s*$", line)
        if argument_match:
            arguments[argument_match.group(1)] = argument_match.group(2)
            continue
        match = re.match(
            r"^\s*FROM\s+(?:--[A-Za-z0-9_-]+=(?:\"[^\"]+\"|'[^']+'|[^\s]+)\s+)*([^\s]+)",
            line,
            flags=re.IGNORECASE,
        )
        if match:
            image = match.group(1)
            variable = re.fullmatch(
                r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)", image
            )
            if variable:
                image = arguments.get(variable.group(1) or variable.group(2), image)
            images.append(image)
    return images


def validate_pinned_image(
    image: str, *, context: str, bom_images: dict[str, str], errors: list[str]
) -> None:
    repository, digest = parse_image_reference(image)
    if digest is None or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
        errors.append(f"{context}: image must be pinned by an immutable sha256 digest")
        return
    canonical_repository = canonicalize_image_name(repository)
    expected_digest = bom_images.get(canonical_repository)
    if expected_digest is None:
        errors.append(
            f"{context}: image {canonical_repository} is missing from "
            "compatibility/bom/runtime.yaml"
        )
        return
    if expected_digest != digest:
        errors.append(
            f"{context}: image digest {digest} does not match BOM digest {expected_digest}"
        )


def canonicalize_image_name(repository: str) -> str:
    if repository.startswith(("docker.io/", "ghcr.io/", "gcr.io/", "quay.io/")):
        return repository
    path_without_tag = repository
    if "/" in repository:
        last_slash = repository.rfind("/")
        last_colon = repository.rfind(":")
        if last_colon > last_slash:
            path_without_tag = repository[:last_colon]
    elif ":" in repository:
        path_without_tag = repository.split(":", 1)[0]
    first_segment = path_without_tag.split("/", 1)[0]
    if "." in first_segment or first_segment == "localhost":
        return repository
    if ":" in first_segment:
        if "/" not in repository:
            return f"docker.io/library/{repository}"
        return f"docker.io/{repository}"
    if "/" not in repository:
        return f"docker.io/library/{repository}"
    return f"docker.io/{repository}"


def validate_compose_images(errors: list[str]) -> None:
    services = parse_compose_services(COMPOSE.read_text(encoding="utf-8"))
    if not services:
        errors.append("compose: services must be an object")
        return
    bom_images = parse_bom_images(BOM.read_text(encoding="utf-8"))
    if not bom_images:
        errors.append("bom: development_images must declare image and image_digest")
        return
    referenced_dockerfiles: set[str] = set()
    allowed_dockerfiles = {path.name for path in DOCKERFILES}
    for service_name, service in services.items():
        dockerfile = service.get("dockerfile")
        if dockerfile:
            if dockerfile not in allowed_dockerfiles:
                errors.append(f"compose:{service_name}: unvalidated Dockerfile {dockerfile}")
            referenced_dockerfiles.add(dockerfile)
            continue
        image = service.get("image")
        if image is None:
            errors.append(f"compose:{service_name}: image or build.dockerfile is required")
            continue
        validate_pinned_image(
            image, context=f"compose:{service_name}", bom_images=bom_images, errors=errors
        )
    for dockerfile in DOCKERFILES:
        if (
            dockerfile not in {ROOT / "Dockerfile.operator", ROOT / "Dockerfile.gpu-smoke"}
            and dockerfile.name not in referenced_dockerfiles
        ):
            errors.append(f"compose: unreferenced development Dockerfile {dockerfile.name}")
            continue
        if not dockerfile.is_file():
            errors.append(f"{dockerfile.name}: file is missing")
            continue
        for index, image in enumerate(
            parse_dockerfile_images(dockerfile.read_text(encoding="utf-8")), 1
        ):
            validate_pinned_image(
                image,
                context=f"{dockerfile.name}:FROM[{index}]",
                bom_images=bom_images,
                errors=errors,
            )
    for path in PINNED_IMAGE_REFERENCES:
        if not path.is_file():
            errors.append(f"{path.name}: file is missing")
            continue
        matches = re.findall(
            r"([a-z0-9][a-z0-9._-]*(?:/[a-z0-9._-]+)+(?::[A-Za-z0-9._-]+)?@sha256:[0-9a-f]{64})",
            path.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
        if not matches:
            errors.append(f"{path.name}: no pinned image references were found")
        for index, image in enumerate(matches, 1):
            validate_pinned_image(
                image,
                context=f"{path.name}:IMAGE[{index}]",
                bom_images=bom_images,
                errors=errors,
            )


def main() -> int:
    errors: list[str] = []
    if not MATRIX.is_file():
        errors.append("compatibility/matrix.json is missing")
    else:
        try:
            data = json.loads(MATRIX.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            errors.append(f"matrix: invalid JSON: {exc}")
        else:
            validate_matrix(data, errors)
    validate_yaml_evidence(errors)
    validate_compose_images(errors)
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1
    print("compatibility claims and evidence are valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
