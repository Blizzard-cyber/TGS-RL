#!/usr/bin/env python3
"""Verify that an OCI archive contains exactly the required image platforms."""

from __future__ import annotations

import json
import sys
import tarfile
from pathlib import Path
from typing import Any


def _read_json(archive: tarfile.TarFile, name: str) -> dict[str, Any]:
    member = archive.extractfile(name)
    if member is None:
        raise ValueError(f"missing OCI object: {name}")
    value = json.load(member)
    if not isinstance(value, dict):
        raise ValueError(f"OCI object is not a JSON object: {name}")
    return value


def _descriptor_blob(descriptor: dict[str, Any]) -> str:
    digest = descriptor.get("digest")
    if not isinstance(digest, str) or not digest.startswith("sha256:"):
        raise ValueError("OCI descriptor must use a sha256 digest")
    return f"blobs/sha256/{digest.removeprefix('sha256:')}"


def _platforms(archive: tarfile.TarFile, document: dict[str, Any]) -> set[str]:
    discovered: set[str] = set()
    manifests = document.get("manifests", [])
    if not isinstance(manifests, list):
        raise ValueError("OCI manifests must be a list")
    for descriptor in manifests:
        if not isinstance(descriptor, dict):
            raise ValueError("OCI manifest descriptor must be an object")
        platform = descriptor.get("platform")
        if isinstance(platform, dict):
            os_name = platform.get("os")
            architecture = platform.get("architecture")
            if isinstance(os_name, str) and isinstance(architecture, str):
                discovered.add(f"{os_name}/{architecture}")
        media_type = descriptor.get("mediaType")
        if isinstance(media_type, str) and (
            media_type.endswith(".image.index.v1+json")
            or media_type.endswith(".manifest.list.v2+json")
        ):
            discovered.update(
                _platforms(archive, _read_json(archive, _descriptor_blob(descriptor)))
            )
    return discovered


def main() -> int:
    if len(sys.argv) < 3:
        raise SystemExit("usage: check-oci-platforms.py ARCHIVE PLATFORM [PLATFORM ...]")
    archive_path = Path(sys.argv[1])
    expected = set(sys.argv[2:])
    with tarfile.open(archive_path, "r:*") as archive:
        actual = _platforms(archive, _read_json(archive, "index.json"))
    missing = expected - actual
    if missing:
        raise SystemExit(f"OCI archive is missing required platforms: {', '.join(sorted(missing))}")
    print("OCI platforms verified: " + ", ".join(sorted(expected)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
