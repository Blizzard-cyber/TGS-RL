//go:build performance

package nvidia

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// TestPerformanceBudgets guards local provider event publication and replay
// bookkeeping. It is a regression budget, not a production latency SLA.
func TestPerformanceBudgets(t *testing.T) {
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

	const samples = 51
	latencies := make([]time.Duration, 0, samples)
	for iteration := range samples {
		started := time.Now()
		_, err := p.ObserveSandbox(context.Background(), &tgsrlv1.SandboxEvent{
			EventId:    fmt.Sprintf("perf-event-%03d", iteration),
			SandboxId:  "sandbox-a",
			Generation: 1,
			State:      performanceSandboxState(iteration),
		})
		latencies = append(latencies, time.Since(started))
		if err != nil {
			t.Fatalf("ObserveSandbox() error = %v", err)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[(samples*95+99)/100-1]
	const budget = 25 * time.Millisecond
	t.Logf("ObserveSandbox p95=%s, budget=%s", p95, budget)
	if p95 > budget {
		t.Fatalf("provider resource/runtime apply p95=%s exceeds regression budget %s", p95, budget)
	}
}

func performanceSandboxState(iteration int) tgsrlv1.RuntimeState {
	if iteration%2 == 0 {
		return tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING
	}
	return tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED
}
