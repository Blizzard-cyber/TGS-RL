package config

import "strings"

// SchedulerPolicy returns the generated-runtime-neutral policy projection used
// by the authoritative scheduler package. Callers translate this projection to
// concrete policy/protection/preemption types without reparsing YAML.
func (p PolicyBundle) SchedulerPolicy() SchedulerPolicyConfig {
	return SchedulerPolicyConfig{
		PolicyID:      p.PolicyID,
		PolicyVersion: p.PolicyVersion,
		Strategy:      normalizeStrategy(p.Selection.Strategy),
		TopK:          p.Selection.TopK,
		Protection:    p.Protection,
		Preemption:    p.Preemption,
	}
}

func normalizeStrategy(value string) string {
	switch normalizeToken(value) {
	case "stablefirstfit", "scorefirst":
		return "score_first"
	case "traceaware":
		return "trace_aware"
	case "binpack":
		return "binpack"
	case "noop":
		return "noop"
	case "static":
		return "static"
	default:
		return value
	}
}

func normalizeToken(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(strings.TrimSpace(value)), "-", ""), "_", "")
}
