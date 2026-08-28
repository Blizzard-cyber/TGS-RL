package control

import (
	"context"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestServerAcknowledgesControlWithoutPublishingObservedState(t *testing.T) {
	fakeBackend := backend.NewFake()
	seedBackend(t, fakeBackend)
	server, err := NewServer(fakeBackend)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.ApplyRuntimeControl(context.Background(), controlRequest(tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, "pause-1", 4))
	if err != nil {
		t.Fatal(err)
	}
	if !response.GetAccepted() || response.GetIdempotent() || response.GetBackendRevision() != 1 {
		t.Fatalf("response = %+v", response)
	}
	snapshots, err := fakeBackend.Snapshots(context.Background(), testBundle())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 2 || !snapshots[1].JobPaused {
		t.Fatalf("backend readback = %+v, want paused state for ObservationManager", snapshots)
	}
}

func TestServerIdempotencyAndGenerationFence(t *testing.T) {
	fakeBackend := backend.NewFake()
	seedBackend(t, fakeBackend)
	server, _ := NewServer(fakeBackend)
	request := controlRequest(tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, "pause-1", 4)
	first, err := server.ApplyRuntimeControl(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := server.ApplyRuntimeControl(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.GetIdempotent() || second.GetBackendRevision() != first.GetBackendRevision() {
		t.Fatalf("replay = %+v", second)
	}
	response, err := server.ApplyRuntimeControl(context.Background(), controlRequest(tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME, "resume-1", 3))
	if status.Code(err) != codes.FailedPrecondition || response != nil {
		t.Fatalf("response = %+v code=%s err=%v", response, status.Code(err), err)
	}
}

func TestServerMapsValidationNotFoundConflictAndDeadline(t *testing.T) {
	empty := backend.NewFake()
	server, _ := NewServer(empty)
	invalid := controlRequest(tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, "invalid", 4)
	invalid.Targets[0].SandboxId = ""
	if _, err := server.ApplyRuntimeControl(context.Background(), invalid); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("validation code = %s", status.Code(err))
	}
	if _, err := server.ApplyRuntimeControl(context.Background(), controlRequest(tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, "missing", 4)); status.Code(err) != codes.NotFound {
		t.Fatalf("not found code = %s", status.Code(err))
	}
	seedBackend(t, empty)
	if _, err := server.ApplyRuntimeControl(context.Background(), controlRequest(tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, "same", 4)); err != nil {
		t.Fatal(err)
	}
	if _, err := server.ApplyRuntimeControl(context.Background(), controlRequest(tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME, "same", 4)); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflict code = %s", status.Code(err))
	}
	expired := controlRequest(tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, "expired", 4)
	expired.Deadline = timestamppb.New(time.Now().Add(-time.Second))
	if _, err := server.ApplyRuntimeControl(context.Background(), expired); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline code = %s", status.Code(err))
	}
}

func seedBackend(t *testing.T, fakeBackend *backend.FakeBackend) {
	t.Helper()
	if _, err := fakeBackend.Apply(context.Background(), testBundle()); err != nil {
		t.Fatal(err)
	}
}

func testBundle() *api.Bundle {
	return &api.Bundle{Key: "test/bundle", Namespace: "test", Generation: 4, Fingerprint: "fp-4", SourceRunID: "run-1", SourceJobID: "job-1", SourceTraceID: "trace-1", RuntimeTargets: []api.RuntimeTarget{{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", BindingID: "binding-1", DecisionID: "decision-1", PlanID: "plan-1", ActionID: "action-1", Generation: 4}}, Workload: api.Workload{TypeMeta: api.TypeMeta{APIVersion: "kueue.x-k8s.io/v1beta1", Kind: "Workload"}, ObjectMeta: api.ObjectMeta{Name: "workload", Namespace: "test"}}, Job: api.Job{TypeMeta: api.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: api.ObjectMeta{Name: "job", Namespace: "test"}, Spec: api.JobSpec{Parallelism: 1}}, RuntimeClass: &api.RuntimeClass{TypeMeta: api.TypeMeta{APIVersion: "node.k8s.io/v1", Kind: "RuntimeClass"}, ObjectMeta: api.ObjectMeta{Name: "runtime"}}}
}

func controlRequest(action tgsrlv1.JobCommandType, key string, generation uint64) *tgsrlv1.ApplyRuntimeControlRequest {
	return &tgsrlv1.ApplyRuntimeControlRequest{Action: action, JobId: "job-1", RunId: "run-1", TraceId: "trace-1", RequestId: "request-1", IdempotencyKey: key, Targets: []*tgsrlv1.RuntimeControlTarget{{RuntimeUnitId: "unit-1", SandboxId: "sandbox-1", ExpectedGeneration: generation}}}
}
