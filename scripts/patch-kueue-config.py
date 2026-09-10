#!/usr/bin/env python3
"""Idempotently add the TGS-RL DRA class mapping to Kueue Configuration YAML."""

from __future__ import annotations

import argparse
from pathlib import Path

RESOURCE_LINE = "resources:"
MAPPING_LINE = "  deviceClassMappings:"
ENTRY = "    - name: tgsrl.io/gpu\n      deviceClassNames: [gpu.nvidia.com, mig.nvidia.com]\n"


def patch(text: str) -> str:
    if "name: tgsrl.io/gpu" in text:
        return text if text.endswith("\n") else text + "\n"
    lines = text.rstrip().splitlines()
    mapping_index = next((index for index, line in enumerate(lines) if line == MAPPING_LINE), None)
    if mapping_index is not None:
        lines[mapping_index + 1 : mapping_index + 1] = ENTRY.rstrip().splitlines()
        return "\n".join(lines) + "\n"
    resource_index = next(
        (index for index, line in enumerate(lines) if line == RESOURCE_LINE), None
    )
    addition = [MAPPING_LINE, *ENTRY.rstrip().splitlines()]
    if resource_index is None:
        lines.extend([RESOURCE_LINE, *addition])
    else:
        lines[resource_index + 1 : resource_index + 1] = addition
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    original = args.input.read_text(encoding="utf-8")
    args.output.write_text(patch(original), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
