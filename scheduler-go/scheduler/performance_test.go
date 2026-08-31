//go:build performance

package scheduler

import (
	"sort"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func TestPerformanceBudgets(t *testing.T) {
	tests := []struct {
		name    string
		devices int
		units   int
		budget  time.Duration
	}{
		{name: "8-devices-100-units", devices: 8, units: 100, budget: 10 * time.Millisecond},
		{name: "1000-devices-1000-units", devices: 1000, units: 1000, budget: 50 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot, intent := simulationFixture(test.devices, test.units)
			evaluator := testScheduler(t, FallbackNoOp)
			for range 3 {
				assertPerformanceDecision(t, evaluator, snapshot, intent, test.units)
			}
			const samples = 21
			latencies := make([]time.Duration, 0, samples)
			for range samples {
				started := time.Now()
				assertPerformanceDecision(t, evaluator, snapshot, intent, test.units)
				latencies = append(latencies, time.Since(started))
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			p95 := latencies[(samples*95+99)/100-1]
			t.Logf("Evaluate(%d devices, %d units) p95=%s, budget=%s", test.devices, test.units, p95, test.budget)
			if p95 >= test.budget {
				t.Fatalf("Evaluate(%d devices, %d units) p95=%s, budget=%s", test.devices, test.units, p95, test.budget)
			}
		})
	}
}

func assertPerformanceDecision(t testing.TB, evaluator *Scheduler, snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, wantBindings int) {
	t.Helper()
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() || len(plan.GetBindings()) != wantBindings {
		t.Fatalf("Evaluate() = (%d bindings, fallback=%v, error=%v)", len(plan.GetBindings()), decision.GetFallback(), err)
	}
}
