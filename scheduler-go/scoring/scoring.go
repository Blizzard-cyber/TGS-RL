package scoring

import (
	"math"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/constraints"
)

// Scorer produces named score components for one feasible unit/device pair.
type Scorer interface {
	Name() string
	Score(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, unit *tgsrlv1.PendingUnit, device *tgsrlv1.Device) map[string]float64
}

// Weighted sums several named score components.
type Weighted struct {
	Weights map[string]float64
}

func (w Weighted) Name() string { return "weighted" }

func (w Weighted) Score(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, unit *tgsrlv1.PendingUnit, device *tgsrlv1.Device) map[string]float64 {
	components := map[string]float64{
		"capacity_headroom": constraints.Headroom(device, unit),
		"share_headroom":    constraints.ShareScore(snapshot, device.GetDeviceId()),
		"priority":          priorityScore(unit),
		"trace_affinity":    traceAffinity(intent, device),
	}
	if len(w.Weights) == 0 {
		return components
	}
	out := make(map[string]float64, len(components))
	for name, value := range components {
		weight := w.Weights[name]
		if weight == 0 {
			weight = 1
		}
		out[name] = round(value * weight)
	}
	return out
}

// Default returns the baseline scorer configuration.
func Default() Scorer {
	return Weighted{
		Weights: map[string]float64{
			"capacity_headroom": 1.5,
			"share_headroom":    1.0,
			"priority":          0.4,
			"trace_affinity":    0.3,
		},
	}
}

func priorityScore(unit *tgsrlv1.PendingUnit) float64 {
	if unit == nil {
		return 0
	}
	return round(math.Max(0, float64(unit.GetPriority())) / 100)
}

func traceAffinity(intent *tgsrlv1.SchedulingIntent, device *tgsrlv1.Device) float64 {
	if intent == nil || device == nil || intent.GetTraceId() == "" {
		return 0
	}
	if strings.Contains(strings.ToLower(device.GetLabels()["trace"]), strings.ToLower(intent.GetTraceId())) {
		return 1
	}
	if strings.Contains(strings.ToLower(device.GetLabels()["job"]), strings.ToLower(intent.GetJobId())) {
		return 0.5
	}
	return 0
}

func round(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return math.Round(value*1_000_000) / 1_000_000
}
