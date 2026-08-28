from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from .types import ConfigError, Node, Scalar


def _load_yaml_subset(path: str | Path) -> Node:
    text = Path(path).read_text(encoding="utf-8")
    parser = _YAMLSubsetParser(text, str(path))
    return parser.parse()


class _YAMLSubsetParser:
    def __init__(self, text: str, path: str) -> None:
        self.path = path
        self.lines = self._prepare(text, path)
        self.index = 0

    def parse(self) -> Node:
        if not self.lines:
            raise ConfigError(f"{self.path}: empty configuration file")
        value = self._parse_node(self.lines[0].indent)
        if self.index != len(self.lines):
            line = self.lines[self.index]
            raise ConfigError(f"{self.path}:{line.number}: unexpected trailing content")
        return value

    def _parse_node(self, indent: int) -> Node:
        if self.index >= len(self.lines):
            raise ConfigError(f"{self.path}: unexpected end of file")
        line = self.lines[self.index]
        if line.indent != indent:
            raise ConfigError(f"{self.path}:{line.number}: unexpected indentation")
        if line.text.startswith("- "):
            return self._parse_list(indent)
        return self._parse_mapping(indent)

    def _parse_mapping(self, indent: int) -> dict[str, Node]:
        result: dict[str, Node] = {}
        while self.index < len(self.lines):
            line = self.lines[self.index]
            if line.indent < indent:
                break
            if line.indent != indent:
                raise ConfigError(f"{self.path}:{line.number}: invalid indentation in mapping")
            if line.text.startswith("- "):
                break
            key, raw_value = self._split_key_value(line)
            self.index += 1
            if raw_value in {"|", ">", "|-", ">-"}:
                result[key] = self._parse_block_scalar(indent, raw_value)
                continue
            if raw_value == "":
                result[key] = self._parse_nested(indent, line.number)
                continue
            result[key] = _parse_scalar(raw_value)
        return result

    def _parse_list(self, indent: int) -> list[Node]:
        result: list[Node] = []
        while self.index < len(self.lines):
            line = self.lines[self.index]
            if line.indent < indent:
                break
            if line.indent != indent:
                raise ConfigError(f"{self.path}:{line.number}: invalid indentation in list")
            if not line.text.startswith("- "):
                break
            payload = line.text[2:]
            self.index += 1
            if payload == "":
                result.append(self._parse_nested(indent, line.number))
                continue
            if self._looks_like_mapping_item(payload):
                key, raw_value = self._split_key_value_text(payload, line.number)
                item: dict[str, Node] = {}
                if raw_value in {"|", ">", "|-", ">-"}:
                    item[key] = self._parse_block_scalar(indent, raw_value)
                elif raw_value == "":
                    item[key] = self._parse_nested(indent, line.number)
                else:
                    item[key] = _parse_scalar(raw_value)
                if self.index < len(self.lines) and self.lines[self.index].indent > indent:
                    extra = self._parse_node(self.lines[self.index].indent)
                    if not isinstance(extra, dict):
                        line2 = self.lines[self.index - 1]
                        raise ConfigError(
                            f"{self.path}:{line2.number}: list item with inline mapping "
                            "must continue as a mapping"
                        )
                    overlap = set(item) & set(extra)
                    if overlap:
                        raise ConfigError(
                            f"{self.path}:{line.number}: duplicate keys in list item: "
                            f"{sorted(overlap)}"
                        )
                    item.update(extra)
                result.append(item)
                continue
            result.append(_parse_scalar(payload))
        return result

    def _parse_nested(self, parent_indent: int, line_number: int) -> Node:
        if self.index >= len(self.lines):
            raise ConfigError(f"{self.path}:{line_number}: expected nested value")
        next_line = self.lines[self.index]
        if next_line.indent <= parent_indent:
            raise ConfigError(f"{self.path}:{line_number}: expected indented nested value")
        return self._parse_node(next_line.indent)

    def _parse_block_scalar(self, parent_indent: int, style: str) -> str:
        if self.index >= len(self.lines):
            return ""
        collected: list[_PreparedLine] = []
        while self.index < len(self.lines) and self.lines[self.index].indent > parent_indent:
            collected.append(self.lines[self.index])
            self.index += 1
        if not collected:
            return ""
        min_indent = min(line.indent for line in collected if line.text)
        parts = [line.raw[min_indent:] if line.text else "" for line in collected]
        if style in {">", ">-"}:
            return _fold_block_scalar(parts)
        return "\n".join(parts)

    @staticmethod
    def _looks_like_mapping_item(payload: str) -> bool:
        return ":" in payload and not payload.startswith(("http://", "https://"))

    @staticmethod
    def _prepare(text: str, path: str) -> list[_PreparedLine]:
        prepared: list[_PreparedLine] = []
        for number, raw in enumerate(text.splitlines(), start=1):
            stripped = raw.lstrip(" ")
            if not stripped or stripped.startswith("#"):
                continue
            indent = len(raw) - len(stripped)
            if "\t" in raw:
                raise ConfigError(
                    "tabs are not supported in the YAML subset",
                    code="yaml_parse_error",
                    source=path,
                    field=f"line:{number}",
                )
            prepared.append(_PreparedLine(number=number, indent=indent, text=stripped, raw=raw))
        return prepared

    def _split_key_value(self, line: _PreparedLine) -> tuple[str, str]:
        return self._split_key_value_text(line.text, line.number)

    def _split_key_value_text(self, text: str, line_number: int) -> tuple[str, str]:
        if ":" not in text:
            raise ConfigError(f"{self.path}:{line_number}: expected key: value entry")
        key, value = text.split(":", 1)
        key = key.strip()
        if not key:
            raise ConfigError(f"{self.path}:{line_number}: empty keys are not supported")
        return key, value.lstrip(" ")


@dataclass(frozen=True, slots=True)
class _PreparedLine:
    number: int
    indent: int
    text: str
    raw: str


def _fold_block_scalar(lines: list[str]) -> str:
    paragraphs: list[str] = []
    current: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped == "":
            if current:
                paragraphs.append(" ".join(current))
                current = []
            elif not paragraphs:
                paragraphs.append("")
            continue
        current.append(stripped)
    if current:
        paragraphs.append(" ".join(current))
    return "\n\n".join(paragraphs)


def _parse_scalar(value: str) -> Scalar | list[Node] | dict[str, Node]:
    if value == "{}":
        return {}
    if value == "[]":
        return []
    if value in {"null", "~"}:
        return None
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if len(value) >= 2 and value[0] == value[-1] == '"':
        return value[1:-1]
    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1]
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)
    return value
