//go:build performance

package nvidia

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"syscall"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// TestPerformanceBudgets guards local provider event publication and replay
// bookkeeping. Single-P process CPU time prevents hosted-runner descheduling
// and host CPU topology from masquerading as a provider regression. It is not
// a production latency SLA.
func TestPerformanceBudgets(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })
	now := time.Date(2026, time.August, 28, 9, 0, 0, 0, time.UTC)
	p, err := New(
		WithNow(func() time.Time { return now }),
		WithEventRetention(256),
		WithDriver(NewFakeDriver([]*tgsrlv1.Device{{
			DeviceId:    "nvidia-0",
			Kind:        tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
			Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:    &tgsrlv1.ResourceVector{AcceleratorUnits: 1, MemoryBytes: 80 << 30},
			Allocatable: &tgsrlv1.ResourceVector{AcceleratorUnits: 1, MemoryBytes: 80 << 30},
			Capabilities: &tgsrlv1.CapabilitySet{
				Names:            []string{CapabilityName},
				Source:           ProviderID,
				Revision:         1,
				SupportedActions: []string{"bind", "release", "pause", "resume", "set_share", "set_priority", "sleep", "offload", "resize", "rebind", "recreate"},
			},
		}}, nil)),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := p.WatchResources(ctx, 0)
	if err != nil {
		t.Fatalf("WatchResources() error = %v", err)
	}
	for range 2 {
		select {
		case <-stream:
		case <-time.After(time.Second):
			t.Fatal("timed out draining bootstrap events")
		}
	}

	const (
		batches         = 5
		samplesPerBatch = 51
	)
	batchP95s := make([]time.Duration, 0, batches)
	iteration := 0
	for range batches {
		latencies := make([]time.Duration, 0, samplesPerBatch)
		for range samplesPerBatch {
			started := providerProcessCPUTime(t)
			_, err := p.ObserveSandbox(context.Background(), &tgsrlv1.SandboxEvent{
				EventId:    fmt.Sprintf("perf-event-%03d", iteration),
				SandboxId:  "sandbox-a",
				Generation: 1,
				State:      performanceSandboxState(iteration),
			})
			latencies = append(latencies, providerProcessCPUTime(t)-started)
			if err != nil {
				t.Fatalf("ObserveSandbox() error = %v", err)
			}
			iteration++
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		batchP95s = append(batchP95s, latencies[(len(latencies)*95+99)/100-1])
	}
	sort.Slice(batchP95s, func(i, j int) bool { return batchP95s[i] < batchP95s[j] })
	medianP95 := batchP95s[len(batchP95s)/2]
	const budget = 25 * time.Millisecond
	t.Logf("ObserveSandbox batch_cpu_p95=%v, median_cpu_p95=%s, budget=%s", batchP95s, medianP95, budget)
	if medianP95 > budget {
		t.Fatalf("provider resource/runtime apply median batch CPU p95=%s exceeds regression budget %s", medianP95, budget)
	}
}

func providerProcessCPUTime(t testing.TB) time.Duration {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatalf("Getrusage() error = %v", err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func performanceSandboxState(iteration int) tgsrlv1.RuntimeState {
	if iteration%2 == 0 {
		return tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING
	}
	return tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED
}
