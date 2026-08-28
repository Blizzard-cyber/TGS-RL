#!/usr/bin/env python3
"""Validate the machine-readable upstream patch ledger and replayability."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / "upstream" / "patches.json"


def fail(message: str, errors: list[str]) -> None:
    errors.append(message)


def main() -> int:
    errors: list[str] = []
    try:
        data: dict[str, Any] = json.loads(LEDGER.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"error: cannot read patch ledger: {exc}", file=sys.stderr)
        return 1
    patches = data.get("patches")
    if not isinstance(patches, list):
        print("error: patches must be a list", file=sys.stderr)
        return 1
    seen: set[str] = set()
    for entry in patches:
        if not isinstance(entry, dict):
            fail("patch entry must be an object", errors)
            continue
        patch_id = entry.get("patch_id")
        if not isinstance(patch_id, str) or not patch_id:
            fail("patch_id is required", errors)
            continue
        if patch_id in seen:
            fail(f"{patch_id}: duplicate patch id", errors)
        seen.add(patch_id)
        required = (
            "component",
            "baseline_version",
            "baseline_commit",
            "patch_file",
            "content_sha256",
            "owner",
            "exit_condition",
            "tests",
        )
        for field in required:
            if not entry.get(field):
                fail(f"{patch_id}: {field} is required", errors)
        raw_path = entry.get("patch_file")
        if not isinstance(raw_path, str):
            continue
        relative = Path(raw_path)
        if relative.is_absolute() or ".." in relative.parts or relative.suffix != ".patch":
            fail(f"{patch_id}: invalid patch_file {raw_path}", errors)
            continue
        patch_path = ROOT / relative
        if not patch_path.is_file():
            fail(f"{patch_id}: patch file does not exist: {raw_path}", errors)
            continue
        digest = hashlib.sha256(patch_path.read_bytes()).hexdigest()
        if digest != entry.get("content_sha256"):
            fail(f"{patch_id}: content_sha256 mismatch", errors)
        result = subprocess.run(
            ["git", "apply", "--check", "--recount", str(patch_path)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            fail(
                f"{patch_id}: patch is not replayable on the current baseline: "
                f"{result.stderr.strip()}",
                errors,
            )
        for test_path in entry.get("tests", []):
            if not isinstance(test_path, str) or not (ROOT / test_path).exists():
                fail(f"{patch_id}: missing test evidence {test_path!r}", errors)
    patch_files = {
        path.relative_to(ROOT).as_posix() for path in (ROOT / "upstream").rglob("*.patch")
    }
    registered = {entry.get("patch_file") for entry in patches if isinstance(entry, dict)}
    for unregistered in sorted(patch_files - registered):
        fail(f"unregistered patch file: {unregistered}", errors)
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1
    print(f"upstream patch gate passed ({len(patches)} registered patches)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
