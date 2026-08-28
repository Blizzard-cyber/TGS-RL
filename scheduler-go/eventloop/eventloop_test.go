package eventloop

import (
	"context"
	"sync"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/cache"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEventLoopPublishesImmutableStateAndRuntimeDispatchesTriggers(t *testing.T) {
	recorder := observability.NewInMemoryRecorder()
	now := time.Date(2026, 8, 28, 2, 0, 0, 0, time.UTC)
	clock := cache.ClockFunc(func() time.Time { return now })
	state := cache.NewState(&tgsrlv1.ClusterSnapshot{
		SnapshotId: "snapshot-1",
		Revision:   1,
		Devices: []*tgsrlv1.Device{{
			DeviceId:    "device-1",
			Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
			Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
			Capabilities: &tgsrlv1.CapabilitySet{
				Names: []string{"logical-cpu"},
			},
		}},
	}, clock)
	loop := New(state, clock, recorder)

	intent := &tgsrlv1.SchedulingIntent{
		ExecutionId:      "exec",
		StageId:          "stage",
		Version:          1,
		JobId:            "job",
		PolicyVersion:    "policy-1",
		IdempotencyKey:   "intent-key",
		SubmittedAt:      timestamppb.New(now),
		ValidUntil:       timestamppb.New(now.Add(5 * time.Minute)),
		UnitCount:        1,
		ResourcesPerUnit: &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 128, AcceleratorUnits: 0.25},
		RequiredCapabilities: &tgsrlv1.CapabilitySet{
			Names: []string{"logical-cpu"},
		},
	}
	loop.PublishIntent(intent)
	loop.PublishResourceEvent(&tgsrlv1.ResourceEvent{
		EventId:          "resource-1",
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED,
		Provider:         "mock",
		ProviderRevision: 2,
		Device: &tgsrlv1.Device{
			DeviceId:    "device-1",
			Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
			Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
			Capabilities: &tgsrlv1.CapabilitySet{
				Names: []string{"logical-cpu"},
			},
		},
	})
	loop.PublishSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId:    "sandbox-1",
		SandboxId:  "sb-1",
		Generation: 1,
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		OccurredAt: timestamppb.New(now),
	})

	view := loop.View()
	if got := len(view.Snapshot.GetPendingUnits()); got != 1 {
		t.Fatalf("pending units = %d, want 1", got)
	}
	if sandbox := view.Sandboxes["sb-1"]; sandbox == nil || sandbox.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("sandbox = %+v, want running sandbox state", sandbox)
	}

	runtime := NewRuntime(loop, RuntimeConfig{
		FastInterval:   10 * time.Millisecond,
		MediumInterval: 20 * time.Millisecond,
		SlowInterval:   30 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		mu       sync.Mutex
		drained  []string
		received = make(chan struct{}, 3)
	)
	runtime.SetTrigger(func(_ context.Context, executionID, stageID string) {
		mu.Lock()
		drained = append(drained, executionID+"/"+stageID)
		mu.Unlock()
		select {
		case received <- struct{}{}:
		default:
		}
	})
	runtime.Start(ctx)
	defer runtime.Stop()
	runtime.Enqueue("exec", "stage")
	for range 3 {
		select {
		case <-received:
		case <-time.After(time.Second):
			t.Fatal("runtime did not dispatch all three trigger queues")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if got := len(drained); got != 3 {
		t.Fatalf("trigger count = %d, want 3 queue drains", got)
	}
	for _, key := range drained {
		if key != "exec/stage" {
			t.Fatalf("trigger key = %q, want exec/stage", key)
		}
	}
}

func TestEventLoopPublishesIntentWithInjectedClock(t *testing.T) {
	fixed := time.Date(2026, 8, 27, 15, 0, 0, 0, time.UTC)
	recorder := observability.NewInMemoryRecorder()
	clock := cache.ClockFunc(func() time.Time { return fixed })

	buildLoop := func() *EventLoop {
		state := cache.NewState(&tgsrlv1.ClusterSnapshot{
			SnapshotId: "snapshot-fixed",
			Revision:   9,
			ObservedAt: timestamppb.New(fixed.Add(-time.Minute)),
			Devices: []*tgsrlv1.Device{{
				DeviceId:    "device-1",
				Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
				Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
				Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
				Capabilities: &tgsrlv1.CapabilitySet{
					Names: []string{"logical-cpu"},
				},
			}},
		}, clock)
		return New(state, clock, recorder)
	}

	intent := &tgsrlv1.SchedulingIntent{
		ExecutionId:      "exec",
		StageId:          "stage",
		Version:          1,
		JobId:            "job",
		PolicyVersion:    "policy-1",
		IdempotencyKey:   "intent-key",
		SubmittedAt:      timestamppb.New(fixed),
		ValidUntil:       timestamppb.New(fixed.Add(time.Minute)),
		UnitCount:        1,
		ResourcesPerUnit: &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 128, AcceleratorUnits: 0.25},
		RequiredCapabilities: &tgsrlv1.CapabilitySet{
			Names: []string{"logical-cpu"},
		},
	}

	firstLoop := buildLoop()
	firstLoop.PublishIntent(intent)
	first := firstLoop.View()
	if got := first.Snapshot.GetObservedAt().AsTime(); !got.Equal(fixed) {
		t.Fatalf("ObservedAt = %s, want %s", got, fixed)
	}
	if got := len(first.Snapshot.GetPendingUnits()); got != 1 {
		t.Fatalf("pending units = %d, want 1", got)
	}

	secondLoop := buildLoop()
	secondLoop.PublishIntent(intent)
	second := secondLoop.View()
	if got := second.Snapshot.GetObservedAt().AsTime(); !got.Equal(first.Snapshot.GetObservedAt().AsTime()) {
		t.Fatalf("ObservedAt mismatch: first=%s second=%s", first.Snapshot.GetObservedAt().AsTime(), got)
	}
	if got := second.Snapshot.GetPendingUnits()[0].GetPendingUnitId(); got != first.Snapshot.GetPendingUnits()[0].GetPendingUnitId() {
		t.Fatalf("pending unit mismatch: first=%q second=%q", first.Snapshot.GetPendingUnits()[0].GetPendingUnitId(), got)
	}
}
