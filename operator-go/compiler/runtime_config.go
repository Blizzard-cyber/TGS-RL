package compiler

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	qualifiedNameRegexp = regexp.MustCompile(`^[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$`)
	dnsLabelRegexp      = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$`)
	dnsSubdomainRegexp  = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?)*$`)
	labelValueRegexp    = regexp.MustCompile(`^(|[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?)$`)
)

const (
	dnsLabelMaxLength     = 63
	dnsSubdomainMaxLength = 253
)

func cloneRuntimeConfig(config RuntimeConfig) RuntimeConfig {
	cloned := config
	cloned.NodeSelector = cloneStringMap(config.NodeSelector)
	return cloned
}

func normalizeRuntimeConfig(config RuntimeConfig) RuntimeConfig {
	normalized := RuntimeConfig{
		RuntimeClass: RuntimeClassConfig{
			Name:    strings.TrimSpace(config.RuntimeClass.Name),
			Handler: strings.TrimSpace(config.RuntimeClass.Handler),
			Create:  config.RuntimeClass.Create,
		},
		NodeSelector: normalizeNodeSelector(config.NodeSelector),
	}
	if normalized.RuntimeClass.Name == "" {
		normalized.RuntimeClass.Handler = ""
		normalized.RuntimeClass.Create = false
	}
	if normalized.RuntimeClass.Handler == "" {
		normalized.RuntimeClass.Create = false
	}
	return normalized
}

func normalizeNodeSelector(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return dst
}

func validateNodeSelectorEntry(key, value string) error {
	if key == "" {
		return fmt.Errorf("node selector key must not be empty")
	}
	if value == "" {
		return fmt.Errorf("node selector value for %q must not be empty", key)
	}
	if err := validateQualifiedName(key); err != nil {
		return fmt.Errorf("node selector key %q is invalid: %w", key, err)
	}
	if !labelValueRegexp.MatchString(value) {
		return fmt.Errorf("node selector value for %q is invalid: must be 63 characters or fewer and match Kubernetes label syntax", key)
	}
	return nil
}

func validateQualifiedName(value string) error {
	parts := strings.Split(value, "/")
	switch len(parts) {
	case 1:
		if !qualifiedNameRegexp.MatchString(parts[0]) {
			return fmt.Errorf("name segment must match Kubernetes qualified name syntax")
		}
		return nil
	case 2:
		prefix, name := parts[0], parts[1]
		if prefix == "" || name == "" {
			return fmt.Errorf("prefix and name must both be non-empty")
		}
		if err := validateDNSSubdomain(prefix); err != nil {
			return fmt.Errorf("prefix must be a valid DNS subdomain: %w", err)
		}
		if !qualifiedNameRegexp.MatchString(name) {
			return fmt.Errorf("name segment must match Kubernetes qualified name syntax")
		}
		return nil
	default:
		return fmt.Errorf("must contain at most one '/' separator")
	}
}

// validateRuntimeClassName mirrors Kubernetes object metadata validation for
// RuntimeClass: names are DNS-1123 subdomains, not label keys.
func validateRuntimeClassName(value string) error {
	if value == "" {
		return fmt.Errorf("runtime class name must not be empty")
	}
	if err := validateDNSSubdomain(value); err != nil {
		return fmt.Errorf("runtime class name %q is invalid: %w", value, err)
	}
	return nil
}

// validateRuntimeClassHandler mirrors Kubernetes RuntimeClass validation. A
// handler is a non-empty lowercase DNS-1123 label (not a qualified label key).
func validateRuntimeClassHandler(value string) error {
	if value == "" {
		return fmt.Errorf("runtime class handler must not be empty")
	}
	if len(value) > dnsLabelMaxLength {
		return fmt.Errorf("runtime class handler %q is invalid: must be %d characters or fewer", value, dnsLabelMaxLength)
	}
	if !dnsLabelRegexp.MatchString(value) {
		return fmt.Errorf("runtime class handler %q is invalid: must be a lowercase DNS-1123 label", value)
	}
	return nil
}

func validateDNSSubdomain(value string) error {
	if len(value) > dnsSubdomainMaxLength {
		return fmt.Errorf("must be %d characters or fewer", dnsSubdomainMaxLength)
	}
	if !dnsSubdomainRegexp.MatchString(value) {
		return fmt.Errorf("must be a lowercase DNS-1123 subdomain")
	}
	return nil
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
