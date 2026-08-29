package cache

import (
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeClock struct {
	now time.Time
}

func (f fakeClock) Now() time.Time { return f.now }

type mutableClock struct{ now time.Time }

func (f *mutableClock) Now() time.Time { return f.now }

func TestSandboxEventUsesCacheClockForDualTimes(t *testing.T) {
	receivedAt := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	clock := &mutableClock{now: receivedAt}
	state := NewState(&tgsrlv1.ClusterSnapshot{SnapshotId: "seed", Revision: 1}, clock)
	sourceAt := receivedAt.Add(-time.Hour)
	apply := func(id string, revision uint64, runtimeState tgsrlv1.RuntimeState) *tgsrlv1.Sandbox {
		t.Helper()
		view := state.Update(func(next *Snapshot) bool {
			return next.ApplySandboxEvent(&tgsrlv1.SandboxEvent{
				EventId: id, SandboxId: "dual-time", Generation: 1, ProviderRevision: revision,
				State: runtimeState, OccurredAt: timestamppb.New(sourceAt.Add(time.Duration(revision) * time.Second)),
			})
		})
		return view.Sandboxes["dual-time"]
	}

	first := apply("first", 1, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	if !first.GetStateChangedAt().AsTime().Equal(receivedAt) || !first.GetLastConfirmedAt().AsTime().Equal(receivedAt) {
		t.Fatalf("first sandbox times = %+v", first)
	}
	clock.now = clock.now.Add(10 * time.Second)
	confirmed := apply("confirm", 2, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	if !confirmed.GetStateChangedAt().AsTime().Equal(receivedAt) || !confirmed.GetLastConfirmedAt().AsTime().Equal(clock.now) {
		t.Fatalf("same-state confirmation reset transition time: %+v", confirmed)
	}
	clock.now = clock.now.Add(10 * time.Second)
	transitioned := apply("transition", 3, tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED)
	if !transitioned.GetStateChangedAt().AsTime().Equal(clock.now) || !transitioned.GetLastConfirmedAt().AsTime().Equal(clock.now) {
		t.Fatalf("transition times = %+v", transitioned)
	}
}

func TestSandboxDualTimeDescriptorAndRoundTrip(t *testing.T) {
	descriptor := (&tgsrlv1.Sandbox{}).ProtoReflect().Descriptor().Fields()
	if got := descriptor.ByName("observed_at").Number(); got != 12 {
		t.Fatalf("observed_at field number = %d, want 12", got)
	}
	if got := descriptor.ByName("state_changed_at").Number(); got != 19 {
		t.Fatalf("state_changed_at field number = %d, want 19", got)
	}
	if got := descriptor.ByName("last_confirmed_at").Number(); got != 20 {
		t.Fatalf("last_confirmed_at field number = %d, want 20", got)
	}
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	want := &tgsrlv1.Sandbox{
		SandboxId: "round-trip", ObservedAt: timestamppb.New(now.Add(-time.Hour)),
		StateChangedAt: timestamppb.New(now.Add(-time.Minute)), LastConfirmedAt: timestamppb.New(now),
	}
	payload, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	got := &tgsrlv1.Sandbox{}
	if err := proto.Unmarshal(payload, got); err != nil {
		t.Fatalf("proto.Unmarshal() error = %v", err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestStateAppliesDedupAndGenerationFence(t *testing.T) {
	state := NewState(&tgsrlv1.ClusterSnapshot{SnapshotId: "seed", Revision: 7}, fakeClock{now: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)})
	intent := &tgsrlv1.SchedulingIntent{
		ExecutionId:      "exec",
		StageId:          "stage",
		Version:          1,
		UnitCount:        2,
		JobId:            "job",
		Priority:         3,
		ResourcesPerUnit: &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 128},
		SubmittedAt:      timestamppb.New(time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)),
		ValidUntil:       timestamppb.New(time.Date(2026, 8, 27, 12, 5, 0, 0, time.UTC)),
	}
	view := state.Update(func(next *Snapshot) bool {
		changed := next.UpsertIntent(intent)
		next.RebuildPendingUnits(time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC))
		return changed
	})
	if len(view.Snapshot.GetPendingUnits()) != 2 {
		t.Fatalf("pending_units = %d, want 2", len(view.Snapshot.GetPendingUnits()))
	}

	resource := &tgsrlv1.ResourceEvent{
		EventId:          "res-1",
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED,
		Provider:         "mock",
		ProviderRevision: 8,
		Device:           &tgsrlv1.Device{DeviceId: "d1"},
	}
	first := state.Update(func(next *Snapshot) bool { return next.ApplyResourceEvent(resource) })
	second := state.Update(func(next *Snapshot) bool { return next.ApplyResourceEvent(resource) })
	if got, want := len(first.Snapshot.GetDevices()), 1; got != want {
		t.Fatalf("first devices = %d, want %d", got, want)
	}
	if got, want := len(second.Snapshot.GetDevices()), 1; got != want {
		t.Fatalf("second devices = %d, want %d", got, want)
	}

	state.Update(func(next *Snapshot) bool {
		return next.ApplySandboxEvent(&tgsrlv1.SandboxEvent{
			EventId:    "sb-2",
			SandboxId:  "sandbox-1",
			Generation: 2,
			State:      tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		})
	})
	view = state.Update(func(next *Snapshot) bool {
		return next.ApplySandboxEvent(&tgsrlv1.SandboxEvent{
			EventId:    "sb-1",
			SandboxId:  "sandbox-1",
			Generation: 1,
			State:      tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED,
		})
	})
	if got := view.Sandboxes["sandbox-1"].GetGeneration(); got != 2 {
		t.Fatalf("sandbox generation = %d, want 2", got)
	}
	if got := view.Sandboxes["sandbox-1"].GetState(); got != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("sandbox state = %s, want RUNNING", got)
	}
}

func TestSandboxEventMutableFieldsRespectPresence(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	state := NewState(&tgsrlv1.ClusterSnapshot{SnapshotId: "seed", Revision: 1}, fakeClock{now: now})
	share, priority, offloaded := 0.5, int32(7), true
	state.Update(func(next *Snapshot) bool {
		return next.ApplySandboxEvent(&tgsrlv1.SandboxEvent{
			EventId: "sandbox-initial", SandboxId: "sandbox-1", Generation: 1,
			State: tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING, Share: &share, Priority: &priority, Offloaded: &offloaded,
			OccurredAt: timestamppb.New(now),
		})
	})
	view := state.Update(func(next *Snapshot) bool {
		return next.ApplySandboxEvent(&tgsrlv1.SandboxEvent{
			EventId: "sandbox-state-only", SandboxId: "sandbox-1", Generation: 1,
			State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, OccurredAt: timestamppb.New(now.Add(time.Second)),
		})
	})
	sandbox := view.Sandboxes["sandbox-1"]
	if sandbox.GetShare() != share || sandbox.GetPriority() != priority || !sandbox.GetOffloaded() {
		t.Fatalf("state-only legacy event erased mutable fields: %+v", sandbox)
	}
}

func TestResourceAndSandboxEventsUseIndependentCursorRules(t *testing.T) {
	now := time.Date(2026, 8, 27, 13, 0, 0, 0, time.UTC)
	state := NewState(&tgsrlv1.ClusterSnapshot{SnapshotId: "seed", Revision: 1}, fakeClock{now: now})
	firstResource := &tgsrlv1.ResourceEvent{EventId: "resource-a", EventType: tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED, Provider: "mock", ProviderRevision: 7, Device: &tgsrlv1.Device{DeviceId: "device-a"}}
	secondResource := &tgsrlv1.ResourceEvent{EventId: "resource-b", EventType: tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED, Provider: "mock", ProviderRevision: 7, Device: &tgsrlv1.Device{DeviceId: "device-b"}}
	if view := state.Update(func(next *Snapshot) bool { return next.ApplyResourceEvent(firstResource) }); len(view.Snapshot.GetDevices()) != 1 {
		t.Fatalf("first resource event was not applied: %+v", view.Snapshot.GetDevices())
	}
	if view := state.Update(func(next *Snapshot) bool { return next.ApplyResourceEvent(secondResource) }); len(view.Snapshot.GetDevices()) != 2 {
		t.Fatalf("distinct same-revision resource event was rejected: %+v", view.Snapshot.GetDevices())
	}

	share := 0.5
	event := &tgsrlv1.SandboxEvent{EventId: "sandbox-event", IdempotencyKey: "sandbox-key", SandboxId: "sandbox-a", Generation: 1, ProviderRevision: 8, State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, Share: &share, OccurredAt: timestamppb.New(now)}
	view := state.Update(func(next *Snapshot) bool { return next.ApplySandboxEvent(event) })
	if sandbox := view.Sandboxes["sandbox-a"]; sandbox == nil || sandbox.GetShare() != share {
		t.Fatalf("sandbox event was self-rejected after cursor advance: %+v", sandbox)
	}
	beforeRevision := view.Revision
	view = state.Update(func(next *Snapshot) bool { return next.ApplySandboxEvent(event) })
	if view.Revision != beforeRevision {
		t.Fatalf("duplicate sandbox event advanced cache revision from %d to %d", beforeRevision, view.Revision)
	}
}
