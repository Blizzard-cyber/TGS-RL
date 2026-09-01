//go:build performance

package scheduler

import (
	"crypto/sha256"
	"runtime"
	"sort"
	"syscall"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

const performanceCalibrationReference = 10 * time.Millisecond

var performanceCalibrationSink [sha256.Size]byte

func TestPerformanceBudgets(t *testing.T) {
	// Hosted runners can deschedule the test process for an entire batch. Measure
	// single-P process CPU time so runner contention and host CPU topology do not
	// masquerade as a Scheduler regression. These are deterministic compute
	// budgets, not latency SLAs.
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })
	normalization := processCPUNormalization(t)
	tests := []struct {
		name             string
		devices          int
		units            int
		budget           time.Duration
		allocationBudget float64
	}{
		{name: "8-devices-100-units", devices: 8, units: 100, budget: 20 * time.Millisecond, allocationBudget: 125_000},
		{name: "1000-devices-1000-units", devices: 1000, units: 1000, budget: 50 * time.Millisecond, allocationBudget: 310_000},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot, intent := simulationFixture(test.devices, test.units)
			evaluator := testScheduler(t, FallbackNoOp)
			for range 3 {
				assertPerformanceDecision(t, evaluator, snapshot, intent, test.units)
			}
			allocations := testing.AllocsPerRun(3, func() {
				assertPerformanceDecision(t, evaluator, snapshot, intent, test.units)
			})
			t.Logf("Evaluate(%d devices, %d units) allocations/op=%.0f, budget=%.0f", test.devices, test.units, allocations, test.allocationBudget)
			if allocations > test.allocationBudget {
				t.Fatalf("Evaluate(%d devices, %d units) allocations/op=%.0f exceeds budget %.0f", test.devices, test.units, allocations, test.allocationBudget)
			}

			const (
				batches         = 5
				samplesPerBatch = 21
			)
			batchP95s := make([]time.Duration, 0, batches)
			for range batches {
				latencies := make([]time.Duration, 0, samplesPerBatch)
				for range samplesPerBatch {
					started := processCPUTime(t)
					assertPerformanceDecision(t, evaluator, snapshot, intent, test.units)
					elapsed := processCPUTime(t) - started
					latencies = append(latencies, time.Duration(float64(elapsed)*normalization))
				}
				batchP95s = append(batchP95s, percentile95(latencies))
			}
			sort.Slice(batchP95s, func(i, j int) bool { return batchP95s[i] < batchP95s[j] })
			medianP95 := batchP95s[len(batchP95s)/2]
			t.Logf("Evaluate(%d devices, %d units) batch_cpu_p95=%v, median_cpu_p95=%s, budget=%s", test.devices, test.units, batchP95s, medianP95, test.budget)
			if medianP95 >= test.budget {
				t.Fatalf("Evaluate(%d devices, %d units) median batch CPU p95=%s, budget=%s", test.devices, test.units, medianP95, test.budget)
			}
		})
	}
}

func processCPUNormalization(t testing.TB) float64 {
	t.Helper()
	payload := make([]byte, 1<<20)
	for index := range payload {
		payload[index] = byte(index)
	}
	digest := performanceCalibrationSink
	calibrations := make([]time.Duration, 0, 7)
	for range 7 {
		started := processCPUTime(t)
		for range 32 {
			digest = sha256.Sum256(payload)
			payload[0] ^= digest[0]
		}
		calibrations = append(calibrations, processCPUTime(t)-started)
	}
	performanceCalibrationSink = digest
	sort.Slice(calibrations, func(i, j int) bool { return calibrations[i] < calibrations[j] })
	elapsed := calibrations[len(calibrations)/2]
	if elapsed <= 0 {
		t.Fatalf("CPU calibration returned %s", elapsed)
	}
	t.Logf("CPU calibrations=%v, median=%s, reference=%s", calibrations, elapsed, performanceCalibrationReference)
	return float64(performanceCalibrationReference) / float64(elapsed)
}

func processCPUTime(t testing.TB) time.Duration {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatalf("Getrusage() error = %v", err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
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
