//go:build performance

package scheduler

import (
	"sort"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func TestPerformanceBudgets(t *testing.T) {
	// Hosted runners occasionally pause an individual sample or batch. Use the
	// median of independent batch P95s so isolated pauses do not hide or invent
	// a sustained scheduler regression. These are CI budgets, not latency SLAs.
	tests := []struct {
		name    string
		devices int
		units   int
		budget  time.Duration
	}{
		{name: "8-devices-100-units", devices: 8, units: 100, budget: 20 * time.Millisecond},
		{name: "1000-devices-1000-units", devices: 1000, units: 1000, budget: 50 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot, intent := simulationFixture(test.devices, test.units)
			evaluator := testScheduler(t, FallbackNoOp)
			for range 3 {
				assertPerformanceDecision(t, evaluator, snapshot, intent, test.units)
			}

			const (
				batches         = 5
				samplesPerBatch = 21
			)
			batchP95s := make([]time.Duration, 0, batches)
			for range batches {
				latencies := make([]time.Duration, 0, samplesPerBatch)
				for range samplesPerBatch {
					started := time.Now()
					assertPerformanceDecision(t, evaluator, snapshot, intent, test.units)
					latencies = append(latencies, time.Since(started))
				}
				batchP95s = append(batchP95s, percentile95(latencies))
			}
			sort.Slice(batchP95s, func(i, j int) bool { return batchP95s[i] < batchP95s[j] })
			medianP95 := batchP95s[len(batchP95s)/2]
			t.Logf("Evaluate(%d devices, %d units) batch_p95=%v, median_p95=%s, budget=%s", test.devices, test.units, batchP95s, medianP95, test.budget)
			if medianP95 >= test.budget {
				t.Fatalf("Evaluate(%d devices, %d units) median batch p95=%s, budget=%s", test.devices, test.units, medianP95, test.budget)
			}
		})
	}
}

func percentile95(latencies []time.Duration) time.Duration {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return latencies[(len(latencies)*95+99)/100-1]
}

func assertPerformanceDecision(t testing.TB, evaluator *Scheduler, snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, wantBindings int) {
	t.Helper()
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() || len(plan.GetBindings()) != wantBindings {
		t.Fatalf("Evaluate() = (%d bindings, fallback=%v, error=%v)", len(plan.GetBindings()), decision.GetFallback(), err)
	}
}
