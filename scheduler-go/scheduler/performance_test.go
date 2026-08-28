package scheduler

import (
	"sort"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func TestEvaluateP95Budgets(t *testing.T) {
	if raceEnabled {
		t.Skip("wall-clock performance gate is not meaningful under the race detector")
	}
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
		t.Fatalf("Evaluate() correctness failure: bindings=%d want=%d fallback=%v error=%v", len(plan.GetBindings()), wantBindings, decision.GetFallback(), err)
	}
}

func TestEvaluateLargeScaleCorrectness(t *testing.T) {
	if testing.Short() {
		t.Skip("large-scale correctness gate skipped in short mode")
	}
	snapshot, intent := simulationFixture(1000, 1000)
	plan, decision, err := testScheduler(t, FallbackNoOp).Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() || len(plan.GetBindings()) != 1000 {
		t.Fatalf("large-scale Evaluate() = (%d bindings, fallback=%v, error=%v)", len(plan.GetBindings()), decision.GetFallback(), err)
	}
	seen := make(map[string]struct{}, len(plan.GetBindings()))
	for _, binding := range plan.GetBindings() {
		if _, duplicate := seen[binding.GetPendingUnitId()]; duplicate {
			t.Fatalf("pending unit %q was bound twice", binding.GetPendingUnitId())
		}
		seen[binding.GetPendingUnitId()] = struct{}{}
	}
}
