package config

import (
	"fmt"
	"slices"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CapabilityProto projects the loaded capability config into the scheduler's
// wire-level contract so startup can hand providers one consistent capability
// view derived from product configuration.
func (c CapabilitySetConfig) CapabilityProto(now time.Time) (*tgsrlv1.CapabilitySet, error) {
	if strings.TrimSpace(c.Source) == "" {
		return nil, configError("capabilities.source must not be empty", "schema_validation", c.Path, "capabilities.source", nil)
	}
	if c.Revision < 1 {
		return nil, configError("capabilities.revision must be at least 1", "schema_validation", c.Path, "capabilities.revision", nil)
	}
	measuredAt := c.MeasuredAt
	if measuredAt == nil {
		measuredAt = &now
	}
	capabilities := &tgsrlv1.CapabilitySet{
		Names:            slices.Clone(c.Names),
		Attributes:       cloneStringMap(c.Attributes),
		Algorithms:       slices.Clone(c.Algorithms),
		RolloutModes:     slices.Clone(c.RolloutModes),
		Source:           c.Source,
		Revision:         uint64(c.Revision),
		MeasuredAt:       timestamppb.New(measuredAt.UTC()),
		SupportedActions: slices.Clone(c.SupportedActions),
		Limits:           cloneFloatMap(c.Limits),
		Evidence: []*tgsrlv1.CapabilityEvidence{{
			EvidenceId: fmt.Sprintf("%s/%s/%d", strings.TrimSpace(c.Source), strings.TrimSpace(c.CapabilityID), c.Revision),
			Source:     c.Source,
			Revision:   uint64(c.Revision),
			ObservedAt: timestamppb.New(measuredAt.UTC()),
			Collector:  "scheduler-startup-config",
			Detail:     evidenceDetail(c),
			Attributes: evidenceAttributes(c),
		}},
	}
	return capabilities, nil
}

func evidenceDetail(c CapabilitySetConfig) string {
	parts := []string{}
	if strings.TrimSpace(c.CapabilityID) != "" {
		parts = append(parts, "capability_id="+strings.TrimSpace(c.CapabilityID))
	}
	if strings.TrimSpace(c.SchemaVersion) != "" {
		parts = append(parts, "schema="+strings.TrimSpace(c.SchemaVersion))
	}
	if strings.TrimSpace(c.Path) != "" {
		parts = append(parts, "path="+strings.TrimSpace(c.Path))
	}
	return strings.Join(parts, "; ")
}

func evidenceAttributes(c CapabilitySetConfig) map[string]string {
	attributes := cloneStringMap(c.Attributes)
	if attributes == nil {
		attributes = map[string]string{}
	}
	if strings.TrimSpace(c.Semantics) != "" {
		attributes["semantics"] = strings.TrimSpace(c.Semantics)
	}
	if strings.TrimSpace(c.VerificationStatus) != "" {
		attributes["verification_status"] = strings.TrimSpace(c.VerificationStatus)
	}
	attributes["runtime_loader"] = fmt.Sprintf("%t", c.RuntimeLoader)
	attributes["claim_mock_only"] = fmt.Sprintf("%t", c.Claims.MockOnly)
	attributes["claim_live_gpu"] = fmt.Sprintf("%t", c.Claims.LiveGPU)
	attributes["claim_hardware_latency"] = fmt.Sprintf("%t", c.Claims.HardwareLatency)
	attributes["claim_performance"] = fmt.Sprintf("%t", c.Claims.Performance)
	return attributes
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneFloatMap(values map[string]float64) map[string]float64 {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]float64, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
