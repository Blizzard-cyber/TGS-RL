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
	runtime.SetTrigger(func(_ context.Context, trigger *Trigger) {
		if trigger == nil {
			return
		}
		mu.Lock()
		drained = append(drained, trigger.ExecutionID+"/"+trigger.StageID+":"+trigger.TickKind.String())
		mu.Unlock()
		select {
		case received <- struct{}{}:
		default:
		}
	})
	runtime.Start(ctx)
	defer runtime.Stop()
	runtime.Enqueue(&Trigger{ExecutionID: "exec", StageID: "stage", Cause: "test-enqueue", ObservedRevision: 7})
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
	want := map[string]bool{
		"exec/stage:TICK_KIND_FAST":   false,
		"exec/stage:TICK_KIND_MEDIUM": false,
		"exec/stage:TICK_KIND_SLOW":   false,
	}
	for _, key := range drained {
		if _, ok := want[key]; !ok {
			t.Fatalf("trigger key = %q, want one of fast/medium/slow drains for exec/stage", key)
		}
		want[key] = true
	}
	for key, seen := range want {
		if !seen {
			t.Fatalf("missing trigger drain %q", key)
		}
	}
}

func TestRuntimeQueueKeyKeepsDelimitedIdentitiesDistinct(t *testing.T) {
	runtime := NewRuntime(nil, RuntimeConfig{})
	runtime.Enqueue(&Trigger{ExecutionID: "execution/one", StageID: "stage"})
	runtime.Enqueue(&Trigger{ExecutionID: "execution", StageID: "one/stage"})

	if got := runtime.fastQueue.Len(); got != 2 {
		t.Fatalf("fast queue length = %d, want two distinct identities", got)
	}
}

func TestMergeTriggerUsesNewestObservationAndStableCauses(t *testing.T) {
	older := &Trigger{
		ExecutionID:      "execution",
		StageID:          "stage",
		Cause:            "sandbox|intent",
		ObservedRevision: 4,
		ContractObservation: &tgsrlv1.ContractObservation{
			EventId:    "observation-old",
			ObservedAt: timestamppb.New(time.Unix(10, 0)),
		},
	}
	newer := &Trigger{
		ExecutionID:      "execution",
		StageID:          "stage",
		Cause:            "resource|intent",
		ObservedRevision: 5,
		ContractObservation: &tgsrlv1.ContractObservation{
			EventId:    "observation-new",
			ObservedAt: timestamppb.New(time.Unix(11, 0)),
		},
	}

	merged := MergeTrigger(older, newer)
	if merged.ObservedRevision != 5 {
		t.Fatalf("observed revision = %d, want 5", merged.ObservedRevision)
	}
	if got := merged.ContractObservation.GetEventId(); got != "observation-new" {
		t.Fatalf("observation = %q, want newest observation", got)
	}
	if got := merged.Cause; got != "intent|resource|sandbox" {
		t.Fatalf("cause = %q, want deterministic merged causes", got)
	}
	merged.ContractObservation.EventId = "caller-mutation"
	if newer.ContractObservation.GetEventId() != "observation-new" {
		t.Fatal("MergeTrigger aliased the incoming observation")
	}
}

func TestMergeTriggerUsesNewerObservationWithoutRevisionChange(t *testing.T) {
	current := &Trigger{
		ExecutionID: "execution", StageID: "stage", ObservedRevision: 5,
		ContractObservation: &tgsrlv1.ContractObservation{
			EventId: "old", ObservedAt: timestamppb.New(time.Unix(10, 0)),
		},
	}
	incoming := &Trigger{
		ExecutionID: "execution", StageID: "stage", ObservedRevision: 5,
		ContractObservation: &tgsrlv1.ContractObservation{
			EventId: "new", ObservedAt: timestamppb.New(time.Unix(11, 0)),
		},
	}

	if got := MergeTrigger(current, incoming).ContractObservation.GetEventId(); got != "new" {
		t.Fatalf("observation = %q, want causally newer observation", got)
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
