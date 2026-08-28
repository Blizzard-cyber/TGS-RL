package cache

import (
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeClock struct {
	now time.Time
}

func (f fakeClock) Now() time.Time { return f.now }

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
