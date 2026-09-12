package statuswatch

import (
	"context"
	"io"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
)

func TestProjectRequiresObservedAdmissionAndClaimAllocation(t *testing.T) {
	if got := Project(&Snapshot{WorkloadAdmitted: true}, true); got.Ready {
		t.Fatal("admission without required claim allocation must not be BOUND")
	}
	bound := Project(&Snapshot{WorkloadAdmitted: true, ResourceClaimsAllocated: true}, true)
	if !bound.Ready || bound.State != tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND {
		t.Fatalf("projection = %+v, want BOUND", bound)
	}
	running := Project(&Snapshot{WorkloadAdmitted: true, ResourceClaimsAllocated: true, JobActive: 1}, true)
	if running.State != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("projection = %+v, want RUNNING", running)
	}
}

func TestProjectTerminalStatePrecedence(t *testing.T) {
	failed := Project(&Snapshot{JobActive: 1, JobFailed: 1}, false)
	if failed.EventType != tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_FAILED || failed.Health != tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED {
		t.Fatalf("failed projection = %+v", failed)
	}
	succeeded := Project(&Snapshot{JobSucceeded: 1}, false)
	if succeeded.EventType != tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_TERMINATED {
		t.Fatalf("succeeded projection = %+v", succeeded)
	}
}

func TestProjectPausedAndDeletedReadback(t *testing.T) {
	initiallySuspended := Project(&Snapshot{WorkloadAdmitted: true, ResourceClaimsAllocated: true, JobPaused: true}, true)
	if !initiallySuspended.Ready || initiallySuspended.State != tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND {
		t.Fatalf("Kueue admission suspension projection = %+v, want BOUND", initiallySuspended)
	}
	paused := Project(&Snapshot{WorkloadAdmitted: true, JobPaused: true, ControlCommitted: true, ControlAction: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE}, false)
	if !paused.Ready || paused.State != tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED {
		t.Fatalf("paused projection = %+v", paused)
	}
	deleted := Project(&Snapshot{JobDeleted: true}, false)
	if !deleted.Ready || deleted.State != tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED {
		t.Fatalf("deleted projection = %+v", deleted)
	}
}

func TestFakeObserverEmitsObservedBoundThenRunning(t *testing.T) {
	fakeBackend := backend.NewFake()
	bundle := &api.Bundle{
		Key: "test/bundle", Namespace: "test", Generation: 7, Fingerprint: "fp",
		ResourceClaim: &api.ResourceClaim{TypeMeta: api.TypeMeta{APIVersion: "resource.k8s.io/v1beta1", Kind: "ResourceClaim"}, ObjectMeta: api.ObjectMeta{Name: "claim", Namespace: "test"}, Spec: api.ResourceClaimSpec{Count: 1}},
		Job:           api.Job{TypeMeta: api.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: api.ObjectMeta{Name: "job", Namespace: "test"}, Spec: api.JobSpec{Parallelism: 2}},
		Workload:      api.Workload{TypeMeta: api.TypeMeta{APIVersion: "kueue.x-k8s.io/v1beta1", Kind: "Workload"}, ObjectMeta: api.ObjectMeta{Name: "workload", Namespace: "test"}},
		RuntimeClass:  &api.RuntimeClass{TypeMeta: api.TypeMeta{APIVersion: "node.k8s.io/v1", Kind: "RuntimeClass"}, ObjectMeta: api.ObjectMeta{Name: "runtime"}},
	}
	if _, err := fakeBackend.Apply(context.Background(), bundle); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	stream, err := NewFake(fakeBackend).Watch(context.Background(), Request{
		Bundle:   bundle,
		Bindings: []*tgsrlv1.Binding{{BindingId: "binding-1"}},
	})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	first, _ := stream.Recv()
	second, _ := stream.Recv()
	if Project(first, true).State != tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND {
		t.Fatalf("first snapshot = %+v, want BOUND", first)
	}
	if Project(second, true).State != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("second snapshot = %+v, want RUNNING", second)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("final Recv() error = %v, want EOF", err)
	}
}
