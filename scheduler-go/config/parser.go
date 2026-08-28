package config

import (
	"fmt"
	"os"
	"strings"
)

func loadYAMLSubset(path string) (any, error) {
	text, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	parser, err := newYAMLSubsetParser(string(text), path)
	if err != nil {
		return nil, err
	}
	return parser.parse()
}

func newYAMLSubsetParser(text string, path string) (*yamlSubsetParser, error) {
	lines := make([]preparedLine, 0)
	for number, raw := range strings.Split(text, "\n") {
		lineNumber := number + 1
		if strings.ContainsRune(raw, '\t') {
			return nil, configError("tabs are not supported in the YAML subset", "yaml_parse_error", path, fmt.Sprintf("line:%d", lineNumber), nil)
		}
		stripped := strings.TrimLeft(raw, " ")
		if stripped == "" || strings.HasPrefix(stripped, "#") {
			continue
		}
		lines = append(lines, preparedLine{
			Number: lineNumber,
			Indent: len(raw) - len(stripped),
			Text:   stripped,
			Raw:    raw,
		})
	}
	if len(lines) == 0 {
		return nil, configError(path+": empty configuration file", "yaml_parse_error", path, "", nil)
	}
	return &yamlSubsetParser{path: path, lines: lines}, nil
}

func (p *yamlSubsetParser) parse() (any, error) {
	value, err := p.parseNode(p.lines[0].Indent)
	if err != nil {
		return nil, err
	}
	if p.index != len(p.lines) {
		line := p.lines[p.index]
		return nil, configError(fmt.Sprintf("%s:%d: unexpected trailing content", p.path, line.Number), "yaml_parse_error", p.path, fmt.Sprintf("line:%d", line.Number), nil)
	}
	return value, nil
}

func (p *yamlSubsetParser) parseNode(indent int) (any, error) {
	if p.index >= len(p.lines) {
		return nil, configError(p.path+": unexpected end of file", "yaml_parse_error", p.path, "", nil)
	}
	line := p.lines[p.index]
	if line.Indent != indent {
		return nil, configError(fmt.Sprintf("%s:%d: unexpected indentation", p.path, line.Number), "yaml_parse_error", p.path, fmt.Sprintf("line:%d", line.Number), nil)
	}
	if strings.HasPrefix(line.Text, "- ") {
		return p.parseList(indent)
	}
	return p.parseMapping(indent)
}

func (p *yamlSubsetParser) parseMapping(indent int) (map[string]any, error) {
	result := make(map[string]any)
	for p.index < len(p.lines) {
		line := p.lines[p.index]
		if line.Indent < indent {
			break
		}
		if line.Indent != indent {
			return nil, configError(fmt.Sprintf("%s:%d: invalid indentation in mapping", p.path, line.Number), "yaml_parse_error", p.path, fmt.Sprintf("line:%d", line.Number), nil)
		}
		if strings.HasPrefix(line.Text, "- ") {
			break
		}
		key, rawValue, err := p.splitKeyValue(line.Text, line.Number)
		if err != nil {
			return nil, err
		}
		p.index++
		switch rawValue {
		case "|", ">", "|-", ">-":
			value, err := p.parseBlockScalar(indent, rawValue)
			if err != nil {
				return nil, err
			}
			result[key] = value
		case "":
			value, err := p.parseNested(indent, line.Number)
			if err != nil {
				return nil, err
			}
			result[key] = value
		default:
			result[key] = parseScalar(rawValue)
		}
	}
	return result, nil
}

func (p *yamlSubsetParser) parseList(indent int) ([]any, error) {
	result := make([]any, 0)
	for p.index < len(p.lines) {
		line := p.lines[p.index]
		if line.Indent < indent {
			break
		}
		if line.Indent != indent {
			return nil, configError(fmt.Sprintf("%s:%d: invalid indentation in list", p.path, line.Number), "yaml_parse_error", p.path, fmt.Sprintf("line:%d", line.Number), nil)
		}
		if !strings.HasPrefix(line.Text, "- ") {
			break
		}
		payload := strings.TrimPrefix(line.Text, "- ")
		p.index++
		if payload == "" {
			value, err := p.parseNested(indent, line.Number)
			if err != nil {
				return nil, err
			}
			result = append(result, value)
			continue
		}
		if looksLikeMappingItem(payload) {
			key, rawValue, err := p.splitKeyValue(payload, line.Number)
			if err != nil {
				return nil, err
			}
			item := map[string]any{}
			switch rawValue {
			case "|", ">", "|-", ">-":
				value, err := p.parseBlockScalar(indent, rawValue)
				if err != nil {
					return nil, err
				}
				item[key] = value
			case "":
				value, err := p.parseNested(indent, line.Number)
				if err != nil {
					return nil, err
				}
				item[key] = value
			default:
				item[key] = parseScalar(rawValue)
			}
			if p.index < len(p.lines) && p.lines[p.index].Indent > indent {
				extra, err := p.parseNode(p.lines[p.index].Indent)
				if err != nil {
					return nil, err
				}
				extraMap, ok := extra.(map[string]any)
				if !ok {
					return nil, configError(fmt.Sprintf("%s:%d: list item with inline mapping must continue as a mapping", p.path, line.Number), "yaml_parse_error", p.path, fmt.Sprintf("line:%d", line.Number), nil)
				}
				for key, value := range extraMap {
					if _, exists := item[key]; exists {
						return nil, configError(fmt.Sprintf("%s:%d: duplicate keys in list item: [%s]", p.path, line.Number, key), "yaml_parse_error", p.path, fmt.Sprintf("line:%d", line.Number), nil)
					}
					item[key] = value
				}
			}
			result = append(result, item)
			continue
		}
		result = append(result, parseScalar(payload))
	}
	return result, nil
}

func (p *yamlSubsetParser) parseNested(parentIndent int, lineNumber int) (any, error) {
	if p.index >= len(p.lines) {
		return nil, configError(fmt.Sprintf("%s:%d: expected nested value", p.path, lineNumber), "yaml_parse_error", p.path, fmt.Sprintf("line:%d", lineNumber), nil)
	}
	nextLine := p.lines[p.index]
	if nextLine.Indent <= parentIndent {
		return nil, configError(fmt.Sprintf("%s:%d: expected indented nested value", p.path, lineNumber), "yaml_parse_error", p.path, fmt.Sprintf("line:%d", lineNumber), nil)
	}
	return p.parseNode(nextLine.Indent)
}

func (p *yamlSubsetParser) parseBlockScalar(parentIndent int, style string) (string, error) {
	collected := make([]preparedLine, 0)
	for p.index < len(p.lines) && p.lines[p.index].Indent > parentIndent {
		collected = append(collected, p.lines[p.index])
		p.index++
	}
	if len(collected) == 0 {
		return "", nil
	}
	minIndent := collected[0].Indent
	for _, line := range collected {
		if line.Text != "" && line.Indent < minIndent {
			minIndent = line.Indent
		}
	}
	parts := make([]string, 0, len(collected))
	for _, line := range collected {
		if len(line.Raw) >= minIndent {
			parts = append(parts, line.Raw[minIndent:])
		} else {
			parts = append(parts, "")
		}
	}
	if style == ">" || style == ">-" {
		return foldBlockScalar(parts), nil
	}
	return strings.Join(parts, "\n"), nil
}

func (p *yamlSubsetParser) splitKeyValue(text string, lineNumber int) (string, string, error) {
	index := strings.Index(text, ":")
	if index < 0 {
		return "", "", configError(fmt.Sprintf("%s:%d: expected key: value entry", p.path, lineNumber), "yaml_parse_error", p.path, fmt.Sprintf("line:%d", lineNumber), nil)
	}
	key := strings.TrimSpace(text[:index])
	if key == "" {
		return "", "", configError(fmt.Sprintf("%s:%d: empty keys are not supported", p.path, lineNumber), "yaml_parse_error", p.path, fmt.Sprintf("line:%d", lineNumber), nil)
	}
	return key, strings.TrimLeft(text[index+1:], " "), nil
}

func looksLikeMappingItem(payload string) bool {
	return strings.Contains(payload, ":") && !strings.HasPrefix(payload, "http://") && !strings.HasPrefix(payload, "https://")
}

func foldBlockScalar(lines []string) string {
	paragraphs := make([]string, 0)
	current := make([]string, 0)
	for _, line := range lines {
		stripped := strings.TrimSpace(line)
		if stripped == "" {
			if len(current) > 0 {
				paragraphs = append(paragraphs, strings.Join(current, " "))
				current = current[:0]
			} else if len(paragraphs) == 0 {
				paragraphs = append(paragraphs, "")
			}
			continue
		}
		current = append(current, stripped)
	}
	if len(current) > 0 {
		paragraphs = append(paragraphs, strings.Join(current, " "))
	}
	return strings.Join(paragraphs, "\n\n")
}
