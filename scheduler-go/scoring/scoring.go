// Package scoring contains stateless primitives shared by the authoritative
// candidate engine. It intentionally does not provide a default scorer: the
// engine owns score weights and resource-accounting semantics.
package scoring

import (
	"math"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

const (
	CapacityHeadroomComponent = "capacity_headroom"
	ShareHeadroomComponent    = "share_headroom"
	ReadyComponent            = "ready"
	TraceAffinityComponent    = "trace_affinity"
)

// Components creates the complete, authoritative four-component score
// surface. Callers remain responsible for deriving and weighting the values.
func Components(capacityHeadroom, shareHeadroom, ready, traceAffinity float64) map[string]float64 {
	return map[string]float64{
		CapacityHeadroomComponent: capacityHeadroom,
		ShareHeadroomComponent:    shareHeadroom,
		ReadyComponent:            ready,
		TraceAffinityComponent:    traceAffinity,
	}
}

// TraceAffinity returns an unweighted affinity in [0, 1]. Trace labels support
// trace, trace_id, and trace-id; job labels support the corresponding variants.
// Exact trace matches outrank contained trace matches, followed by exact and
// contained job matches.
func TraceAffinity(intent *tgsrlv1.SchedulingIntent, device *tgsrlv1.Device) float64 {
	if intent == nil || device == nil {
		return 0
	}
	traceID := normalize(intent.GetTraceId())
	jobID := normalize(intent.GetJobId())
	if traceID == "" && jobID == "" {
		return 0
	}

	labels := device.GetLabels()
	best := matchLabels(traceID, labels, []string{"trace", "trace_id", "trace-id"}, 1, 0.9)
	best = math.Max(best, matchLabels(jobID, labels, []string{"job", "job_id", "job-id"}, 0.6, 0.5))
	return best
}

func matchLabels(expected string, labels map[string]string, keys []string, exact, contains float64) float64 {
	if expected == "" {
		return 0
	}
	best := 0.0
	for _, key := range keys {
		value := normalize(labels[key])
		switch {
		case value == expected:
			return exact
		case value != "" && strings.Contains(value, expected):
			best = contains
		}
	}
	return best
}

func normalize(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
