#!/usr/bin/env python3
"""Validate durable links and command references in repository Markdown docs."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
MARKDOWN_LINK = re.compile(r"!?\[[^]\n]*\]\(([^)\n]+)\)")
MAKE_COMMAND = re.compile(r"(?<![A-Za-z0-9_-])make[ \t]+([A-Za-z0-9_.-]+)")
MAKE_TARGET = re.compile(r"^([A-Za-z0-9_.-]+):(?:[ \t]|$)", re.MULTILINE)
CLI_COMMAND = re.compile(
    r"^[ \t]*(?:uv run --frozen )?tgsrl(?:-gateway)?[ \t]+([a-z][a-z-]+)",
    re.MULTILINE,
)
CLI_PARSER = re.compile(r"subparsers\.add_parser\(\s*[\"']([a-z][a-z-]+)[\"']")
INLINE_CODE = re.compile(r"`([^`\n]+)`")
REPOSITORY_PREFIXES = (
    ".github/",
    "adapters/",
    "api/",
    "cmd/",
    "compatibility/",
    "configs/",
    "console/",
    "deploy/",
    "gateway-python/",
    "gen/",
    "internal/",
    "job-controller-go/",
    "operator-go/",
    "proto/",
    "runtime-python/",
    "scheduler-go/",
    "scripts/",
    "storage/",
    "tests/",
    "upstream/",
)
LOCAL_OUTPUT_PATHS = {"configs/hardware/environment.json"}


def repository_markdown() -> list[Path]:
    completed = subprocess.run(
        [
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "*.md",
            "*.mdx",
            "*.markdown",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [ROOT / line for line in completed.stdout.splitlines() if line]


def link_path(raw: str) -> str | None:
    target = raw.strip()
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1]
    target = target.split(maxsplit=1)[0]
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc or target.startswith(("#", "mailto:")):
        return None
    return unquote(parsed.path) or None


def main() -> int:
    errors: list[str] = []
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    make_targets = set(MAKE_TARGET.findall(makefile))
    cli_source = (ROOT / "gateway-python" / "tgsrl_gateway" / "cli.py").read_text(encoding="utf-8")
    cli_commands = set(CLI_PARSER.findall(cli_source))

    documents = repository_markdown()
    for document in documents:
        text = document.read_text(encoding="utf-8")
        relative = document.relative_to(ROOT)
        if sum(line.lstrip().startswith("```") for line in text.splitlines()) % 2:
            errors.append(f"{relative}: unbalanced fenced code blocks")
        for match in MARKDOWN_LINK.finditer(text):
            path_text = link_path(match.group(1))
            if path_text is None:
                continue
            target = (document.parent / path_text).resolve()
            if not target.exists():
                line = text.count("\n", 0, match.start()) + 1
                errors.append(f"{relative}:{line}: local link does not exist: {path_text}")
        for match in MAKE_COMMAND.finditer(text):
            if match.group(1) not in make_targets:
                line = text.count("\n", 0, match.start()) + 1
                errors.append(f"{relative}:{line}: unknown Make target: {match.group(1)}")
        for match in CLI_COMMAND.finditer(text):
            if match.group(1) not in cli_commands:
                line = text.count("\n", 0, match.start()) + 1
                errors.append(f"{relative}:{line}: unknown tgsrl CLI command: {match.group(1)}")
        for match in INLINE_CODE.finditer(text):
            value = match.group(1).strip().rstrip(".,;:").split("#", 1)[0]
            if (
                not value.startswith(REPOSITORY_PREFIXES)
                or any(character in value for character in " <>*{}$")
                or value in LOCAL_OUTPUT_PATHS
                or not (value.endswith("/") or Path(value).suffix)
            ):
                continue
            target = ROOT / value
            if not target.exists():
                line = text.count("\n", 0, match.start()) + 1
                errors.append(f"{relative}:{line}: repository path does not exist: {value}")

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"documentation-ok ({len(documents)} repository Markdown files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
