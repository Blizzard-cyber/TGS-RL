package nvidia

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// TestApplyResourceEventP95Budget is a local regression gate for provider-side
// resource event publication and replay bookkeeping. It is intentionally local
// and conservative rather than a product SLA claim.
func TestApplyResourceEventP95Budget(t *testing.T) {
	if testing.Short() {
		t.Skip("provider latency gate skipped in short mode")
	}
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
	// Drain bootstrap events so the timed loop measures steady-state apply cost.
	for i := 0; i < 2; i++ {
		select {
		case <-stream:
		case <-time.After(time.Second):
			t.Fatal("timed out draining bootstrap events")
		}
	}

	const iterations = 51
	latencies := make([]time.Duration, 0, iterations)
	for iteration := 0; iteration < iterations; iteration++ {
		started := time.Now()
		_, err := p.ObserveSandbox(context.Background(), &tgsrlv1.SandboxEvent{
			EventId:    fmt.Sprintf("perf-event-%03d", iteration),
			SandboxId:  "sandbox-a",
			Generation: 1,
			State:      sandboxRuntimeState(iteration),
		})
		latencies = append(latencies, time.Since(started))
		if err != nil {
			t.Fatalf("ObserveSandbox() error = %v", err)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[(iterations*95+99)/100-1]
	if p95 > 25*time.Millisecond {
		t.Fatalf("provider resource/runtime apply p95=%s exceeds local regression budget 25ms", p95)
	}
}

func sandboxRuntimeState(iteration int) tgsrlv1.RuntimeState {
	if iteration%2 == 0 {
		return tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING
	}
	return tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED
}
