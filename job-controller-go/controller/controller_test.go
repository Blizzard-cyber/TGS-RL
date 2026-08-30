package controller

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/compiler"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/runtimeclient"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestControllerLifecycleAndRetry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	driver := runtimeclient.NewFakeDriver()
	engine := testControllerWithDriver(t, now, driver)
	ctx := context.Background()
	job := validJob(now)

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{
		Job:            job,
		RequestId:      "req-create",
		IdempotencyKey: "idem-create",
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if created.GetJob().GetJobId() == "" {
		t.Fatal("CreateJob() returned empty job id")
	}

	admitted, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{
		JobId:          created.GetJob().GetJobId(),
		RequestId:      "req-admit",
		IdempotencyKey: "idem-admit",
		Reason:         "validated",
	})
	if err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	if admitted.GetJob().GetState() != tgsrlv1.JobState_JOB_STATE_PENDING {
		t.Fatalf("AdmitJob() state = %s, want PENDING", admitted.GetJob().GetState())
	}

	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	if len(runs.GetRuns()) != 1 {
		t.Fatalf("ListJobRuns() len = %d, want 1", len(runs.GetRuns()))
	}
	runID := runs.GetRuns()[0].GetRunId()

	started, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		Actor:          "tester",
		RequestId:      "req-start",
		IdempotencyKey: "idem-start",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand(start) error = %v", err)
	}
	if started.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_STARTING {
		t.Fatalf("start run_state = %s, want STARTING", started.GetRun().GetRunState())
	}
	reportObservedStatus(t, engine, ctx, created.GetJob().GetJobId(), runID, tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY, "runtime running", "runtime-observation", 1, now.Add(time.Second), tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, true)

	paused, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE,
		Actor:          "tester",
		RequestId:      "req-pause",
		IdempotencyKey: "idem-pause",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand(pause) error = %v", err)
	}
	if paused.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSING {
		t.Fatalf("pause run_state = %s, want PAUSING", paused.GetRun().GetRunState())
	}
	reportObservedStatus(t, engine, ctx, created.GetJob().GetJobId(), runID, tgsrlv1.ComponentHealth_COMPONENT_HEALTH_DEGRADED, "runtime paused", "runtime-observation", 2, now.Add(2*time.Second), tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, true)

	resumed, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME,
		Actor:          "tester",
		RequestId:      "req-resume",
		IdempotencyKey: "idem-resume",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand(resume) error = %v", err)
	}
	if resumed.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_RESUMING {
		t.Fatalf("resume run_state = %s, want RESUMING", resumed.GetRun().GetRunState())
	}
	reportObservedStatus(t, engine, ctx, created.GetJob().GetJobId(), runID, tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY, "runtime resumed", "runtime-observation", 3, now.Add(3*time.Second), tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, true)

	stopped, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP,
		Actor:          "tester",
		RequestId:      "req-stop",
		IdempotencyKey: "idem-stop",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand(stop) error = %v", err)
	}
	if stopped.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_STOPPING {
		t.Fatalf("stop run_state = %s, want STOPPING", stopped.GetRun().GetRunState())
	}
	reportObservedStatus(t, engine, ctx, created.GetJob().GetJobId(), runID, tgsrlv1.ComponentHealth_COMPONENT_HEALTH_DEGRADED, "runtime terminated", "runtime-observation", 4, now.Add(4*time.Second), tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED, true)

	retried, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RETRY,
		Actor:          "tester",
		RequestId:      "req-retry",
		IdempotencyKey: "idem-retry",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand(retry) error = %v", err)
	}
	if retried.GetRun().GetAttempt() != 2 {
		t.Fatalf("retry attempt = %d, want 2", retried.GetRun().GetAttempt())
	}
	if retried.GetRun().GetRunId() == runID {
		t.Fatal("retry reused run id")
	}
	if retried.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_WAITING {
		t.Fatalf("retry run_state = %s, want WAITING", retried.GetRun().GetRunState())
	}

	listedRuns, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns(post-retry) error = %v", err)
	}
	if len(listedRuns.GetRuns()) != 2 {
		t.Fatalf("ListJobRuns(post-retry) len = %d, want 2", len(listedRuns.GetRuns()))
	}
	if listedRuns.GetRuns()[0].GetAttempt() != 2 || listedRuns.GetRuns()[0].GetRunId() != retried.GetRun().GetRunId() {
		t.Fatalf(
			"ListJobRuns(post-retry) first run = attempt %d id %q, want latest attempt 2 id %q",
			listedRuns.GetRuns()[0].GetAttempt(),
			listedRuns.GetRuns()[0].GetRunId(),
			retried.GetRun().GetRunId(),
		)
	}
	if listedRuns.GetRuns()[1].GetAttempt() != 1 || listedRuns.GetRuns()[1].GetRunId() != runID {
		t.Fatalf(
			"ListJobRuns(post-retry) second run = attempt %d id %q, want original attempt 1 id %q",
			listedRuns.GetRuns()[1].GetAttempt(),
			listedRuns.GetRuns()[1].GetRunId(),
			runID,
		)
	}
}

func TestApplyJobCommandAcceptedRequestDoesNotProjectFinalStateWithoutObservation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 10, 5, 0, 0, time.UTC)
	repository, err := state.NewMemoryRepository(state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	driver := newDesiredOnlyDriver(runtimeclient.NewFakeDriver())
	engine, err := New(Config{
		Repository: repository,
		Runtime:    driver,
		Clock:      state.ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	runID := runs.GetRuns()[0].GetRunId()

	if response, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		RequestId:      "req-start-transient",
		IdempotencyKey: "idem-start-transient",
	}); err != nil {
		t.Fatalf("ApplyJobCommand(start) error = %v", err)
	} else if response.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_STARTING {
		t.Fatalf("start run_state = %s, want STARTING", response.GetRun().GetRunState())
	} else if response.GetOperation().GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
		t.Fatalf("start operation state = %s, want RUNNING", response.GetOperation().GetState())
	}

	if response, err := engine.GetJobRun(ctx, &tgsrlv1.GetJobRunRequest{JobId: created.GetJob().GetJobId(), RunId: runID}); err != nil {
		t.Fatalf("GetJobRun(start) error = %v", err)
	} else if response.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_STARTING {
		t.Fatalf("stored start run_state = %s, want STARTING", response.GetRun().GetRunState())
	}

	reportObservedStatus(t, engine, ctx, created.GetJob().GetJobId(), runID, tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY, "runtime healthy", "runtime-observation", 1, now, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, true)
	if response, err := engine.GetJobRun(ctx, &tgsrlv1.GetJobRunRequest{JobId: created.GetJob().GetJobId(), RunId: runID}); err != nil {
		t.Fatalf("GetJobRun(after start observation) error = %v", err)
	} else if response.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING {
		t.Fatalf("observed start run_state = %s, want RUNNING", response.GetRun().GetRunState())
	}

	if response, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE,
		RequestId:      "req-pause-transient",
		IdempotencyKey: "idem-pause-transient",
	}); err != nil {
		t.Fatalf("ApplyJobCommand(pause) error = %v", err)
	} else if response.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSING {
		t.Fatalf("pause run_state = %s, want PAUSING", response.GetRun().GetRunState())
	} else if response.GetOperation().GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
		t.Fatalf("pause operation state = %s, want RUNNING", response.GetOperation().GetState())
	}

	if response, err := engine.GetJobRun(ctx, &tgsrlv1.GetJobRunRequest{JobId: created.GetJob().GetJobId(), RunId: runID}); err != nil {
		t.Fatalf("GetJobRun(pause) error = %v", err)
	} else if response.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSING {
		t.Fatalf("stored pause run_state = %s, want PAUSING", response.GetRun().GetRunState())
	}

	reportObservedStatus(t, engine, ctx, created.GetJob().GetJobId(), runID, tgsrlv1.ComponentHealth_COMPONENT_HEALTH_DEGRADED, "runtime paused", "runtime-observation", 2, now.Add(time.Minute), tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, true)
	if response, err := engine.GetJobRun(ctx, &tgsrlv1.GetJobRunRequest{JobId: created.GetJob().GetJobId(), RunId: runID}); err != nil {
		t.Fatalf("GetJobRun(after pause observation) error = %v", err)
	} else if response.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSED {
		t.Fatalf("observed pause run_state = %s, want PAUSED", response.GetRun().GetRunState())
	}
}

func TestControllerInvalidTransitionsAndValidation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 10, 15, 0, 0, time.UTC)
	engine := testController(t, now)
	ctx := context.Background()

	invalid := validJob(now)
	invalid.ProtocolVersion = "v0.2"
	validateResponse, err := engine.ValidateJob(ctx, &tgsrlv1.ValidateJobRequest{Job: invalid})
	if err != nil {
		t.Fatalf("ValidateJob() error = %v", err)
	}
	if validateResponse.GetValid() {
		t.Fatal("ValidateJob() valid = true, want false")
	}
	if len(validateResponse.GetDiagnostics()) == 0 {
		t.Fatal("ValidateJob() returned no diagnostics")
	}

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:   created.GetJob().GetJobId(),
		Command: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ApplyJobCommand(start without run) code = %s, want FailedPrecondition", status.Code(err))
	}

	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	runID := runs.GetRuns()[0].GetRunId()

	if _, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:   created.GetJob().GetJobId(),
		RunId:   runID,
		Command: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ApplyJobCommand(resume admitted) code = %s, want FailedPrecondition", status.Code(err))
	}
}

func TestControllerComponentAggregationPreservesObservations(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 10, 30, 0, 0, time.UTC)
	engine := testController(t, now)
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	run := runs.GetRuns()[0]

	first, err := engine.ReportComponentStatus(ctx, &tgsrlv1.ReportComponentStatusRequest{ComponentStatus: &tgsrlv1.ComponentStatus{
		Component:  "runtime",
		Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
		Detail:     "ready",
		Source:     "scheduler",
		Revision:   1,
		ObservedAt: timestamppb.New(now),
		JobId:      created.GetJob().GetJobId(),
		RunId:      run.GetRunId(),
		TraceId:    run.GetTraceId(),
		DataKind:   created.GetJob().GetDataKind(),
	}})
	if err != nil {
		t.Fatalf("ReportComponentStatus(first) error = %v", err)
	}
	if first.GetComponentStatus().GetHealth() != tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY {
		t.Fatalf("first aggregate health = %s, want HEALTHY", first.GetComponentStatus().GetHealth())
	}

	second, err := engine.ReportComponentStatus(ctx, &tgsrlv1.ReportComponentStatusRequest{ComponentStatus: &tgsrlv1.ComponentStatus{
		Component:  "runtime",
		Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED,
		Detail:     "pod crash",
		Source:     "runtime-observation",
		Revision:   2,
		ObservedAt: timestamppb.New(now.Add(time.Minute)),
		Annotations: map[string]string{
			"runtime.state": tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED.String(),
		},
		JobId:    created.GetJob().GetJobId(),
		RunId:    run.GetRunId(),
		TraceId:  run.GetTraceId(),
		DataKind: created.GetJob().GetDataKind(),
	}})
	if err != nil {
		t.Fatalf("ReportComponentStatus(second) error = %v", err)
	}
	aggregate := second.GetComponentStatus()
	if aggregate.GetHealth() != tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED {
		t.Fatalf("aggregate health = %s, want FAILED", aggregate.GetHealth())
	}
	if aggregate.GetAnnotations()["observation.scheduler.detail"] != "ready" {
		t.Fatalf("missing preserved scheduler observation: %+v", aggregate.GetAnnotations())
	}
	if aggregate.GetAnnotations()["observation.runtime-observation.detail"] != "pod crash" {
		t.Fatalf("missing runtime observation: %+v", aggregate.GetAnnotations())
	}
	if aggregate.GetAnnotations()["runtime.state"] != tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED.String() {
		t.Fatalf("aggregate runtime.state = %q, want FAILED", aggregate.GetAnnotations()["runtime.state"])
	}

	gotRun, err := engine.GetJobRun(ctx, &tgsrlv1.GetJobRunRequest{JobId: created.GetJob().GetJobId(), RunId: run.GetRunId()})
	if err != nil {
		t.Fatalf("GetJobRun() error = %v", err)
	}
	if len(gotRun.GetRun().GetComponentStatus()) != 1 {
		t.Fatalf("stored component_status len = %d, want 1 aggregate", len(gotRun.GetRun().GetComponentStatus()))
	}

	gotJob, err := engine.GetJob(ctx, &tgsrlv1.GetJobRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if gotJob.GetJob().GetState() != tgsrlv1.JobState_JOB_STATE_FAILED {
		t.Fatalf("stored job state = %s, want FAILED", gotJob.GetJob().GetState())
	}
}

func TestApplyJobCommandRequiresConvergedTrueForNonFailureCompletion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 9, 10, 0, 0, time.UTC)
	repository, err := state.NewMemoryRepository(state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	driver := newDesiredOnlyDriver(runtimeclient.NewFakeDriver())
	engine, err := New(Config{
		Repository: repository,
		Runtime:    driver,
		Clock:      state.ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	runID := runs.GetRuns()[0].GetRunId()

	if _, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		RequestId:      "req-start-converged",
		IdempotencyKey: "idem-start-converged",
	}); err != nil {
		t.Fatalf("ApplyJobCommand(start) error = %v", err)
	}

	reportObservedStatus(t, engine, ctx, created.GetJob().GetJobId(), runID, tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY, "runtime accepted but not converged", "runtime-observation", 1, now, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, false)
	if response, err := engine.GetJobRun(ctx, &tgsrlv1.GetJobRunRequest{JobId: created.GetJob().GetJobId(), RunId: runID}); err != nil {
		t.Fatalf("GetJobRun(non-converged) error = %v", err)
	} else if response.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_STARTING {
		t.Fatalf("non-converged run_state = %s, want STARTING", response.GetRun().GetRunState())
	}

	reportObservedStatus(t, engine, ctx, created.GetJob().GetJobId(), runID, tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY, "runtime converged", "runtime-observation", 2, now.Add(time.Minute), tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, true)
	if response, err := engine.GetJobRun(ctx, &tgsrlv1.GetJobRunRequest{JobId: created.GetJob().GetJobId(), RunId: runID}); err != nil {
		t.Fatalf("GetJobRun(converged) error = %v", err)
	} else if response.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING {
		t.Fatalf("converged run_state = %s, want RUNNING", response.GetRun().GetRunState())
	}
}

func TestApplyJobCommandIgnoresNonAuthoritativeRuntimeSignalForLifecycle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 9, 20, 0, 0, time.UTC)
	engine := testControllerWithDriver(t, now, newDesiredOnlyDriver(runtimeclient.NewFakeDriver()))
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	runID := runs.GetRuns()[0].GetRunId()

	if _, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		RequestId:      "req-start-non-authoritative",
		IdempotencyKey: "idem-start-non-authoritative",
	}); err != nil {
		t.Fatalf("ApplyJobCommand(start) error = %v", err)
	}

	response := reportObservedStatus(t, engine, ctx, created.GetJob().GetJobId(), runID, tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY, "scheduler says running", "scheduler", 1, now, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, true)
	if got := response.GetComponentStatus().GetAnnotations()["runtime.source"]; got != "" {
		t.Fatalf("runtime.source = %q, want empty for non-authoritative source", got)
	}
	if got := response.GetComponentStatus().GetObservedRuntimeState(); got != tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN {
		t.Fatalf("observed_runtime_state = %s, want UNKNOWN", got)
	}

	if response, err := engine.GetJobRun(ctx, &tgsrlv1.GetJobRunRequest{JobId: created.GetJob().GetJobId(), RunId: runID}); err != nil {
		t.Fatalf("GetJobRun() error = %v", err)
	} else if response.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_STARTING {
		t.Fatalf("run_state = %s, want STARTING", response.GetRun().GetRunState())
	}
}

func TestApplyJobCommandUsesLatestAuthoritativeRuntimeObservationOnly(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 9, 25, 0, 0, time.UTC)
	engine := testControllerWithDriver(t, now, newDesiredOnlyDriver(runtimeclient.NewFakeDriver()))
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	runID := runs.GetRuns()[0].GetRunId()

	if _, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		RequestId:      "req-start-latest-authoritative",
		IdempotencyKey: "idem-start-latest-authoritative",
	}); err != nil {
		t.Fatalf("ApplyJobCommand(start) error = %v", err)
	}

	first := reportObservedStatus(t, engine, ctx, created.GetJob().GetJobId(), runID, tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY, "runtime running", "runtime-observation", 10, now.Add(time.Minute), tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, true)
	if got := first.GetComponentStatus().GetAnnotations()["runtime.source"]; got != "runtime-observation" {
		t.Fatalf("runtime.source = %q, want runtime-observation", got)
	}
	stale := reportObservedStatus(t, engine, ctx, created.GetJob().GetJobId(), runID, tgsrlv1.ComponentHealth_COMPONENT_HEALTH_DEGRADED, "stale paused", "runtime-observation", 9, now, tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, true)
	if got := stale.GetComponentStatus().GetObservedRuntimeState(); got != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("observed_runtime_state = %s, want RUNNING after stale authoritative update", got)
	}
	if got := stale.GetComponentStatus().GetAnnotations()["observation.runtime-observation.detail"]; got != "runtime running" {
		t.Fatalf("runtime-observation detail = %q, want latest authoritative detail kept", got)
	}

	if response, err := engine.GetJobRun(ctx, &tgsrlv1.GetJobRunRequest{JobId: created.GetJob().GetJobId(), RunId: runID}); err != nil {
		t.Fatalf("GetJobRun() error = %v", err)
	} else if response.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING {
		t.Fatalf("run_state = %s, want RUNNING", response.GetRun().GetRunState())
	}
}

func TestApplyJobCommandAuthoritativeRuntimeObservationFailedFailsFast(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 9, 30, 0, 0, time.UTC)
	engine := testControllerWithDriver(t, now, newDesiredOnlyDriver(runtimeclient.NewFakeDriver()))
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	runID := runs.GetRuns()[0].GetRunId()

	started, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		RequestId:      "req-start-fail-fast",
		IdempotencyKey: "idem-start-fail-fast",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand(start) error = %v", err)
	}
	response := reportObservedStatus(t, engine, ctx, created.GetJob().GetJobId(), runID, tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED, "runtime crashed", "runtime-observation", 1, now.Add(time.Minute), tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED, true)
	if response.GetComponentStatus().GetAnnotations()["runtime.source"] != "runtime-observation" {
		t.Fatalf("runtime.source = %q, want runtime-observation", response.GetComponentStatus().GetAnnotations()["runtime.source"])
	}

	gotRun, err := engine.GetJobRun(ctx, &tgsrlv1.GetJobRunRequest{JobId: created.GetJob().GetJobId(), RunId: runID})
	if err != nil {
		t.Fatalf("GetJobRun() error = %v", err)
	}
	if gotRun.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_FAILED {
		t.Fatalf("run_state = %s, want FAILED", gotRun.GetRun().GetRunState())
	}
	if gotRun.GetRun().GetState() != tgsrlv1.JobState_JOB_STATE_FAILED {
		t.Fatalf("run state value = %s, want FAILED", gotRun.GetRun().GetState())
	}
	latest := gotRun.GetRun().GetOperations()[len(gotRun.GetRun().GetOperations())-1]
	if latest.GetOperationId() != started.GetOperation().GetOperationId() {
		t.Fatalf("failed operation id = %q, want %q", latest.GetOperationId(), started.GetOperation().GetOperationId())
	}
	if latest.GetState() != tgsrlv1.OperationState_OPERATION_STATE_FAILED {
		t.Fatalf("operation state = %s, want FAILED", latest.GetState())
	}
	if latest.GetErrorCode() != "RUNTIME_STATE_FAILED" {
		t.Fatalf("operation error_code = %q, want RUNTIME_STATE_FAILED", latest.GetErrorCode())
	}
}

func TestControllerComponentAggregationRetainsMultiSourceAndIgnoresStaleSameSource(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 10, 35, 0, 0, time.UTC)
	engine := testController(t, now)
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	run := runs.GetRuns()[0]

	observations := []*tgsrlv1.ComponentStatus{
		{
			Component:  "runtime",
			Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_PROGRESSING,
			Detail:     "queued",
			Source:     "scheduler",
			Revision:   10,
			ObservedAt: timestamppb.New(now),
			JobId:      created.GetJob().GetJobId(),
			RunId:      run.GetRunId(),
			TraceId:    run.GetTraceId(),
			DataKind:   created.GetJob().GetDataKind(),
		},
		{
			Component:  "runtime",
			Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
			Detail:     "assigned",
			Source:     "provider",
			Revision:   4,
			ObservedAt: timestamppb.New(now.Add(time.Minute)),
			JobId:      created.GetJob().GetJobId(),
			RunId:      run.GetRunId(),
			TraceId:    run.GetTraceId(),
			DataKind:   created.GetJob().GetDataKind(),
		},
		{
			Component:  "runtime",
			Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_DEGRADED,
			Detail:     "node pressure",
			Source:     "operator",
			Revision:   2,
			ObservedAt: timestamppb.New(now.Add(2 * time.Minute)),
			JobId:      created.GetJob().GetJobId(),
			RunId:      run.GetRunId(),
			TraceId:    run.GetTraceId(),
			DataKind:   created.GetJob().GetDataKind(),
		},
		{
			Component:  "runtime",
			Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED,
			Detail:     "worker crashed",
			Source:     "runtime",
			Revision:   8,
			ObservedAt: timestamppb.New(now.Add(3 * time.Minute)),
			JobId:      created.GetJob().GetJobId(),
			RunId:      run.GetRunId(),
			TraceId:    run.GetTraceId(),
			DataKind:   created.GetJob().GetDataKind(),
		},
		{
			Component:  "runtime",
			Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED,
			Detail:     "stale scheduler failure",
			Source:     "scheduler",
			Revision:   9,
			ObservedAt: timestamppb.New(now.Add(-time.Minute)),
			JobId:      created.GetJob().GetJobId(),
			RunId:      run.GetRunId(),
			TraceId:    run.GetTraceId(),
			DataKind:   created.GetJob().GetDataKind(),
		},
	}

	var aggregate *tgsrlv1.ComponentStatus
	for _, observation := range observations {
		response, err := engine.ReportComponentStatus(ctx, &tgsrlv1.ReportComponentStatusRequest{ComponentStatus: observation})
		if err != nil {
			t.Fatalf("ReportComponentStatus(%s) error = %v", observation.GetSource(), err)
		}
		aggregate = response.GetComponentStatus()
	}

	if aggregate.GetHealth() != tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED {
		t.Fatalf("aggregate health = %s, want FAILED", aggregate.GetHealth())
	}
	if aggregate.GetAnnotations()["observation.scheduler.detail"] != "queued" {
		t.Fatalf("scheduler detail = %q, want latest scheduler observation", aggregate.GetAnnotations()["observation.scheduler.detail"])
	}
	if aggregate.GetAnnotations()["observation.provider.detail"] != "assigned" {
		t.Fatalf("provider detail = %q", aggregate.GetAnnotations()["observation.provider.detail"])
	}
	if aggregate.GetAnnotations()["observation.operator.detail"] != "node pressure" {
		t.Fatalf("operator detail = %q", aggregate.GetAnnotations()["observation.operator.detail"])
	}
	if aggregate.GetAnnotations()["observation.runtime.detail"] != "worker crashed" {
		t.Fatalf("runtime detail = %q", aggregate.GetAnnotations()["observation.runtime.detail"])
	}
	if aggregate.GetAnnotations()["observation.sources"] != "operator,provider,runtime,scheduler" {
		t.Fatalf("observation.sources = %q", aggregate.GetAnnotations()["observation.sources"])
	}
}

func TestControllerWatchEventsBacklogAndIdempotency(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 10, 45, 0, 0, time.UTC)
	engine := testController(t, now)
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{
		Job:            validJob(now),
		RequestId:      "req-create",
		IdempotencyKey: "idem-create",
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	duplicate, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{
		Job:            validJob(now),
		RequestId:      "req-create",
		IdempotencyKey: "idem-create",
	})
	if err != nil {
		t.Fatalf("CreateJob(duplicate) error = %v", err)
	}
	if duplicate.GetOperation().GetOperationId() != created.GetOperation().GetOperationId() {
		t.Fatal("duplicate CreateJob() returned different operation")
	}

	ch, cancel, err := engine.WatchJobEvents(created.GetJob().GetJobId(), "", 0, "")
	if err != nil {
		t.Fatalf("WatchJobEvents() error = %v", err)
	}
	defer cancel()

	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	select {
	case event := <-ch:
		if event.GetJobId() != created.GetJob().GetJobId() {
			t.Fatalf("watch event job_id = %q, want %q", event.GetJobId(), created.GetJob().GetJobId())
		}
	case <-time.After(time.Second):
		t.Fatal("WatchJobEvents() did not receive backlog/live event")
	}

	listed, err := engine.ListJobEvents(ctx, &tgsrlv1.ListJobEventsRequest{JobId: created.GetJob().GetJobId(), Limit: 10})
	if err != nil {
		t.Fatalf("ListJobEvents() error = %v", err)
	}
	if len(listed.GetEvents()) < 3 {
		t.Fatalf("ListJobEvents() len = %d, want at least 3", len(listed.GetEvents()))
	}
	firstPage, err := engine.ListJobEvents(ctx, &tgsrlv1.ListJobEventsRequest{JobId: created.GetJob().GetJobId(), Limit: 1})
	if err != nil {
		t.Fatalf("ListJobEvents(first page) error = %v", err)
	}
	if len(firstPage.GetEvents()) != 1 || firstPage.GetNextPageToken() == "" {
		t.Fatalf("ListJobEvents(first page) = %+v, want one event plus next token", firstPage)
	}
	secondPage, err := engine.ListJobEvents(ctx, &tgsrlv1.ListJobEventsRequest{
		JobId:     created.GetJob().GetJobId(),
		Limit:     1,
		PageToken: firstPage.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("ListJobEvents(second page) error = %v", err)
	}
	if len(secondPage.GetEvents()) != 1 {
		t.Fatalf("ListJobEvents(second page) len = %d, want 1", len(secondPage.GetEvents()))
	}
	if firstPage.GetEvents()[0].GetEventId() == secondPage.GetEvents()[0].GetEventId() {
		t.Fatal("ListJobEvents(page_token) returned the same event twice")
	}
	if _, err := engine.ListJobEvents(ctx, &tgsrlv1.ListJobEventsRequest{
		JobId:         created.GetJob().GetJobId(),
		PageToken:     firstPage.GetNextPageToken(),
		AfterSequence: 1,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListJobEvents(page_token+after_sequence) code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestCreateJobIdempotencyIgnoresRequestIDButRejectsPayloadChange(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 11, 0, 0, 0, time.UTC)
	engine := testController(t, now)
	ctx := context.Background()

	first, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{
		Job:            validJob(now),
		RequestId:      "req-create-1",
		IdempotencyKey: "idem-create-semantic",
	})
	if err != nil {
		t.Fatalf("CreateJob(first) error = %v", err)
	}
	second, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{
		Job:            validJob(now),
		RequestId:      "req-create-2",
		IdempotencyKey: "idem-create-semantic",
	})
	if err != nil {
		t.Fatalf("CreateJob(second) error = %v", err)
	}
	if first.GetOperation().GetOperationId() != second.GetOperation().GetOperationId() {
		t.Fatalf("operation replay mismatch: first=%q second=%q", first.GetOperation().GetOperationId(), second.GetOperation().GetOperationId())
	}

	mutated := validJob(now)
	mutated.Queue = "priority"
	if _, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{
		Job:            mutated,
		RequestId:      "req-create-3",
		IdempotencyKey: "idem-create-semantic",
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateJob(payload change) code = %s, want AlreadyExists", status.Code(err))
	}
}

func TestCreateJobIdempotencyIgnoresServerGeneratedTimestamp(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 11, 1, 0, 0, time.UTC)
	repository, err := state.NewMemoryRepository()
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	engine, err := New(Config{
		Repository: repository,
		Runtime:    runtimeclient.NewFakeDriver(),
		Clock:      state.ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	job := validJob(now)
	job.CreatedAt = nil
	first, err := engine.CreateJob(context.Background(), &tgsrlv1.CreateJobRequest{
		Job: job, IdempotencyKey: "idem-generated-time",
	})
	if err != nil {
		t.Fatalf("CreateJob(first) error = %v", err)
	}
	now = now.Add(time.Hour)
	second, err := engine.CreateJob(context.Background(), &tgsrlv1.CreateJobRequest{
		Job: job, IdempotencyKey: "idem-generated-time",
	})
	if err != nil {
		t.Fatalf("CreateJob(retry) error = %v", err)
	}
	if first.GetOperation().GetOperationId() != second.GetOperation().GetOperationId() {
		t.Fatalf("operation replay mismatch: first=%q second=%q", first.GetOperation().GetOperationId(), second.GetOperation().GetOperationId())
	}
}

func TestOperationIdentityIsStableOnlyWithIdempotencyKey(t *testing.T) {
	t.Parallel()

	firstTime := time.Date(2026, 8, 28, 11, 2, 0, 0, time.UTC)
	engine := testController(t, firstTime)
	withoutKey := engine.newOperation(tgsrlv1.OperationType_OPERATION_TYPE_START, "job", "run", "actor", "reason", "request", "", tgsrlv1.DataKind_DATA_KIND_LIVE, nil, firstTime)
	withoutKeyLater := engine.newOperation(tgsrlv1.OperationType_OPERATION_TYPE_START, "job", "run", "actor", "reason", "request", "", tgsrlv1.DataKind_DATA_KIND_LIVE, nil, firstTime)
	if withoutKey.GetOperationId() == withoutKeyLater.GetOperationId() {
		t.Fatal("non-idempotent lifecycle calls reused an operation identity")
	}
	withKey := engine.newOperation(tgsrlv1.OperationType_OPERATION_TYPE_START, "job", "run", "actor", "reason", "request-a", "stable-key", tgsrlv1.DataKind_DATA_KIND_LIVE, nil, firstTime)
	withKeyLater := engine.newOperation(tgsrlv1.OperationType_OPERATION_TYPE_START, "job", "run", "actor", "reason", "request-a", "stable-key", tgsrlv1.DataKind_DATA_KIND_LIVE, nil, firstTime.Add(time.Second))
	if withKey.GetOperationId() != withKeyLater.GetOperationId() {
		t.Fatal("idempotent lifecycle calls changed operation identity")
	}
}

func TestCreateRunByStoredJobIDAndListOperations(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 27, 11, 0, 0, 0, time.UTC)
	engine := testController(t, now)
	ctx := context.Background()
	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	runResponse, err := engine.CreateJobRun(ctx, &tgsrlv1.CreateJobRunRequest{
		JobId: created.GetJob().GetJobId(), IdempotencyKey: "create-run-by-id",
	})
	if err != nil {
		t.Fatalf("CreateJobRun(job_id) error = %v", err)
	}
	if runResponse.GetRun().GetJobId() != created.GetJob().GetJobId() {
		t.Fatalf("run job_id = %q", runResponse.GetRun().GetJobId())
	}
	if err := engine.repository.Update(func(store state.Store) error {
		job, _ := store.GetJob(created.GetJob().GetJobId())
		job.State = tgsrlv1.JobState_JOB_STATE_RUNNING
		store.PutJob(job)
		return nil
	}); err != nil {
		t.Fatalf("update job state: %v", err)
	}
	replayed, err := engine.CreateJobRun(ctx, &tgsrlv1.CreateJobRunRequest{
		JobId: created.GetJob().GetJobId(), IdempotencyKey: "create-run-by-id",
	})
	if err != nil {
		t.Fatalf("CreateJobRun(replay after job state change) error = %v", err)
	}
	if replayed.GetRun().GetRunId() != runResponse.GetRun().GetRunId() {
		t.Fatalf("CreateJobRun(replay) run_id = %q, want %q", replayed.GetRun().GetRunId(), runResponse.GetRun().GetRunId())
	}
	listed, err := engine.ListOperations(ctx, &tgsrlv1.ListOperationsRequest{
		JobId: created.GetJob().GetJobId(), Limit: 1,
	})
	if err != nil {
		t.Fatalf("ListOperations() error = %v", err)
	}
	if len(listed.GetOperations()) != 1 || listed.GetCursor() == "" {
		t.Fatalf("ListOperations() = %+v", listed)
	}
}

func TestCreateRunRejectsMismatchedInlineJobAndJobID(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 27, 11, 2, 0, 0, time.UTC)
	engine := testController(t, now)
	ctx := context.Background()

	requestJob := validJob(now)
	if _, err := engine.CreateJobRun(ctx, &tgsrlv1.CreateJobRunRequest{
		Job:            requestJob,
		JobId:          "another-job",
		IdempotencyKey: "create-run-mismatch",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateJobRun(mismatched job_id) code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestCreateJobRunIdempotencyIgnoresRequestIDButRejectsPayloadChange(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 11, 5, 0, 0, time.UTC)
	engine := testController(t, now)
	ctx := context.Background()

	baseJob := validJob(now)
	first, err := engine.CreateJobRun(ctx, &tgsrlv1.CreateJobRunRequest{
		Job:            baseJob,
		RequestId:      "req-create-run-1",
		IdempotencyKey: "idem-create-run-semantic",
	})
	if err != nil {
		t.Fatalf("CreateJobRun(first) error = %v", err)
	}
	second, err := engine.CreateJobRun(ctx, &tgsrlv1.CreateJobRunRequest{
		Job:            validJob(now),
		RequestId:      "req-create-run-2",
		IdempotencyKey: "idem-create-run-semantic",
	})
	if err != nil {
		t.Fatalf("CreateJobRun(second) error = %v", err)
	}
	if first.GetRun().GetRunId() != second.GetRun().GetRunId() {
		t.Fatalf("run replay mismatch: first=%q second=%q", first.GetRun().GetRunId(), second.GetRun().GetRunId())
	}

	mutated := validJob(now)
	mutated.Priority = 99
	if _, err := engine.CreateJobRun(ctx, &tgsrlv1.CreateJobRunRequest{
		Job:            mutated,
		RequestId:      "req-create-run-3",
		IdempotencyKey: "idem-create-run-semantic",
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateJobRun(payload change) code = %s, want AlreadyExists", status.Code(err))
	}
}

func TestCreateJobRunAttemptUsesLatestRunBeyondDefaultPage(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 16, 30, 0, 0, time.UTC)
	repository, err := state.NewMemoryRepository()
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	job := validJob(now)
	job.JobId = "job-many-runs"
	if err := repository.Update(func(store state.Store) error {
		store.PutJob(job)
		for attempt := uint64(1); attempt <= 101; attempt++ {
			store.PutRun(compiler.NewRun(job, attempt, tgsrlv1.JobRunState_JOB_RUN_STATE_VALIDATING, now))
		}
		return nil
	}); err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	engine, err := New(Config{
		Repository: repository,
		Runtime:    runtimeclient.NewFakeDriver(),
		Clock:      state.ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	response, err := engine.CreateJobRun(context.Background(), &tgsrlv1.CreateJobRunRequest{
		JobId: job.GetJobId(),
	})
	if err != nil {
		t.Fatalf("CreateJobRun() error = %v", err)
	}
	if response.GetRun().GetAttempt() != 102 {
		t.Fatalf("CreateJobRun() attempt = %d, want 102", response.GetRun().GetAttempt())
	}
}

func TestJobAndRunListsHonorAfterIDs(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 16, 45, 0, 0, time.UTC)
	engine := testController(t, now)
	ctx := context.Background()
	firstJob := validJob(now)
	firstJob.JobId = "job-after-a"
	secondJob := validJob(now.Add(time.Second))
	secondJob.JobId = "job-after-b"
	for _, job := range []*tgsrlv1.RLTrainingJob{firstJob, secondJob} {
		if _, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: job}); err != nil {
			t.Fatalf("CreateJob(%s) error = %v", job.GetJobId(), err)
		}
	}
	jobs, err := engine.ListJobs(ctx, &tgsrlv1.ListJobsRequest{AfterJobId: firstJob.GetJobId(), Limit: 1})
	if err != nil {
		t.Fatalf("ListJobs(after) error = %v", err)
	}
	if len(jobs.GetJobs()) != 1 || jobs.GetJobs()[0].GetJobId() != secondJob.GetJobId() {
		t.Fatalf("ListJobs(after) = %+v", jobs.GetJobs())
	}

	firstRun, err := engine.CreateJobRun(ctx, &tgsrlv1.CreateJobRunRequest{JobId: firstJob.GetJobId()})
	if err != nil {
		t.Fatalf("CreateJobRun(first) error = %v", err)
	}
	secondRun, err := engine.CreateJobRun(ctx, &tgsrlv1.CreateJobRunRequest{JobId: firstJob.GetJobId()})
	if err != nil {
		t.Fatalf("CreateJobRun(second) error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: firstJob.GetJobId(), AfterRunId: secondRun.GetRun().GetRunId(), Limit: 1})
	if err != nil {
		t.Fatalf("ListJobRuns(after) error = %v", err)
	}
	if len(runs.GetRuns()) != 1 || runs.GetRuns()[0].GetRunId() != firstRun.GetRun().GetRunId() {
		t.Fatalf("ListJobRuns(after) = %+v", runs.GetRuns())
	}
}

func TestRecordOperationDuplicateDoesNotAppendRunOperationTwice(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 11, 5, 0, 0, time.UTC)
	engine := testController(t, now)
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	runID := runs.GetRuns()[0].GetRunId()

	operation := &tgsrlv1.Operation{
		OperationId: "record-op-1",
		Type:        tgsrlv1.OperationType_OPERATION_TYPE_VALIDATE,
		State:       tgsrlv1.OperationState_OPERATION_STATE_SUCCEEDED,
		CreatedAt:   timestamppb.New(now),
		CompletedAt: timestamppb.New(now),
		JobId:       created.GetJob().GetJobId(),
		RunId:       runID,
		RequestId:   "req-record",
		Cursor:      "record-op-1",
	}
	if _, err := engine.RecordOperation(ctx, &tgsrlv1.RecordOperationRequest{Operation: operation}); err != nil {
		t.Fatalf("RecordOperation(first) error = %v", err)
	}
	if _, err := engine.RecordOperation(ctx, &tgsrlv1.RecordOperationRequest{Operation: cloneOperation(operation)}); err != nil {
		t.Fatalf("RecordOperation(duplicate) error = %v", err)
	}

	gotRun, err := engine.GetJobRun(ctx, &tgsrlv1.GetJobRunRequest{JobId: created.GetJob().GetJobId(), RunId: runID})
	if err != nil {
		t.Fatalf("GetJobRun() error = %v", err)
	}
	count := 0
	for _, current := range gotRun.GetRun().GetOperations() {
		if current.GetOperationId() == operation.GetOperationId() {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("duplicate recorded operation count = %d, want 1", count)
	}
}

func TestAdmitJobIdempotentReplayWaitsForInflightCompletion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 11, 15, 0, 0, time.UTC)
	repository, err := state.NewMemoryRepository(state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	driver := newBlockingDriver(runtimeclient.NewFakeDriver())
	engine, err := New(Config{
		Repository: repository,
		Runtime:    driver,
		Clock:      state.ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx := context.Background()
	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	started := make(chan struct{})
	resultCh := make(chan *tgsrlv1.AdmitJobResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		close(started)
		response, admitErr := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{
			JobId:          created.GetJob().GetJobId(),
			RequestId:      "req-admit",
			IdempotencyKey: "idem-admit",
		})
		if admitErr != nil {
			errCh <- admitErr
			return
		}
		resultCh <- response
	}()
	<-started
	driver.waitUntilPrepareBlocked(t)

	duplicateCh := make(chan *tgsrlv1.AdmitJobResponse, 1)
	duplicateErrCh := make(chan error, 1)
	go func() {
		response, admitErr := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{
			JobId:          created.GetJob().GetJobId(),
			RequestId:      "req-admit",
			IdempotencyKey: "idem-admit",
		})
		if admitErr != nil {
			duplicateErrCh <- admitErr
			return
		}
		duplicateCh <- response
	}()
	select {
	case <-duplicateCh:
		t.Fatal("duplicate AdmitJob() returned before inflight prepare completed")
	case err := <-duplicateErrCh:
		t.Fatalf("duplicate AdmitJob() error = %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	driver.releasePrepare()

	select {
	case err := <-errCh:
		t.Fatalf("first AdmitJob() error = %v", err)
	case response := <-resultCh:
		if response.GetOperation().GetState() != tgsrlv1.OperationState_OPERATION_STATE_SUCCEEDED {
			t.Fatalf("first operation state = %s, want SUCCEEDED", response.GetOperation().GetState())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first AdmitJob() did not complete")
	}
	select {
	case err := <-duplicateErrCh:
		t.Fatalf("duplicate AdmitJob() error = %v", err)
	case duplicate := <-duplicateCh:
		if duplicate.GetOperation().GetState() != tgsrlv1.OperationState_OPERATION_STATE_SUCCEEDED {
			t.Fatalf("duplicate operation state = %s, want SUCCEEDED", duplicate.GetOperation().GetState())
		}
		if duplicate.GetJob().GetState() != tgsrlv1.JobState_JOB_STATE_PENDING {
			t.Fatalf("duplicate job state = %s, want PENDING", duplicate.GetJob().GetState())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("duplicate AdmitJob() did not complete")
	}
	if calls := driver.prepareCallCount(); calls != 1 {
		t.Fatalf("PrepareRuntime calls = %d, want 1", calls)
	}
}

func TestAdmitJobIdempotencyIgnoresRequestIDButRejectsReasonChange(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 11, 10, 0, 0, time.UTC)
	engine := testController(t, now)
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	first, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{
		JobId:          created.GetJob().GetJobId(),
		RequestId:      "req-admit-1",
		IdempotencyKey: "idem-admit-semantic",
		Reason:         "validated",
	})
	if err != nil {
		t.Fatalf("AdmitJob(first) error = %v", err)
	}
	second, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{
		JobId:          created.GetJob().GetJobId(),
		RequestId:      "req-admit-2",
		IdempotencyKey: "idem-admit-semantic",
		Reason:         "validated",
	})
	if err != nil {
		t.Fatalf("AdmitJob(second) error = %v", err)
	}
	if first.GetOperation().GetOperationId() != second.GetOperation().GetOperationId() {
		t.Fatalf("operation replay mismatch: first=%q second=%q", first.GetOperation().GetOperationId(), second.GetOperation().GetOperationId())
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{
		JobId:          created.GetJob().GetJobId(),
		RequestId:      "req-admit-3",
		IdempotencyKey: "idem-admit-semantic",
		Reason:         "different-reason",
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("AdmitJob(reason change) code = %s, want AlreadyExists", status.Code(err))
	}
}

func TestApplyJobCommandRuntimeFailureFinalizesPersistedOperation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 11, 30, 0, 0, time.UTC)
	repository, err := state.NewMemoryRepository(state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	driver := runtimeclient.NewFakeDriver()
	engine, err := New(Config{
		Repository: repository,
		Runtime:    driver,
		Clock:      state.ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx := context.Background()
	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	driver.CommandErr = errors.New("runtime start failed")
	response, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runs.GetRuns()[0].GetRunId(),
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		RequestId:      "req-start",
		IdempotencyKey: "idem-start-fail",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand() error = %v", err)
	}
	if response.GetOperation().GetState() != tgsrlv1.OperationState_OPERATION_STATE_FAILED {
		t.Fatalf("operation state = %s, want FAILED", response.GetOperation().GetState())
	}
	if response.GetOperation().GetErrorCode() != "RUNTIME_COMMAND_FAILED" {
		t.Fatalf("operation error_code = %q", response.GetOperation().GetErrorCode())
	}
	if response.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_WAITING {
		t.Fatalf("run state = %s, want WAITING", response.GetRun().GetRunState())
	}
	if response.GetRun().GetState() != tgsrlv1.JobState_JOB_STATE_PENDING {
		t.Fatalf("run state value = %s, want PENDING", response.GetRun().GetState())
	}

	gotOperation, err := engine.GetOperation(ctx, &tgsrlv1.GetOperationRequest{OperationId: response.GetOperation().GetOperationId()})
	if err != nil {
		t.Fatalf("GetOperation() error = %v", err)
	}
	if gotOperation.GetOperation().GetState() != tgsrlv1.OperationState_OPERATION_STATE_FAILED {
		t.Fatalf("stored operation state = %s, want FAILED", gotOperation.GetOperation().GetState())
	}
}

func TestApplyJobCommandAmbiguousRuntimeErrorKeepsOperationRunning(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 11, 15, 0, 0, time.UTC)
	driver := runtimeclient.NewFakeDriver()
	driver.CommandErr = status.Error(codes.Unavailable, "dispatch result unknown")
	engine := testControllerWithDriver(t, now, driver)
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	response, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runs.GetRuns()[0].GetRunId(),
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		RequestId:      "req-start-ambiguous",
		IdempotencyKey: "idem-start-ambiguous",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand() error = %v", err)
	}
	if response.GetOperation().GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
		t.Fatalf("operation state = %s, want RUNNING", response.GetOperation().GetState())
	}
	if response.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_STARTING {
		t.Fatalf("run state = %s, want STARTING", response.GetRun().GetRunState())
	}
}

func TestApplyJobCommandIdempotentReplayDoesNotReinvokeRuntimeWhileOperationRunning(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 11, 35, 0, 0, time.UTC)
	driver := newDesiredOnlyDriver(runtimeclient.NewFakeDriver())
	engine := testControllerWithDriver(t, now, driver)
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	runID := runs.GetRuns()[0].GetRunId()

	first, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		RequestId:      "req-start-replay",
		IdempotencyKey: "idem-start-replay",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand(first) error = %v", err)
	}
	second, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		RequestId:      "req-start-replay",
		IdempotencyKey: "idem-start-replay",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand(second) error = %v", err)
	}
	if first.GetOperation().GetOperationId() != second.GetOperation().GetOperationId() {
		t.Fatalf("operation id replay mismatch: first=%q second=%q", first.GetOperation().GetOperationId(), second.GetOperation().GetOperationId())
	}
	if driver.startCalls != 1 {
		t.Fatalf("start calls = %d, want 1", driver.startCalls)
	}
}

func TestApplyJobCommandIdempotencyIgnoresRequestIDButRejectsReasonChange(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 11, 20, 0, 0, time.UTC)
	driver := newDesiredOnlyDriver(runtimeclient.NewFakeDriver())
	engine := testControllerWithDriver(t, now, driver)
	ctx := context.Background()

	created, err := engine.CreateJob(ctx, &tgsrlv1.CreateJobRequest{Job: validJob(now)})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if _, err := engine.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{JobId: created.GetJob().GetJobId()}); err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	runs, err := engine.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: created.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	runID := runs.GetRuns()[0].GetRunId()

	first, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		Actor:          "tester",
		Reason:         "same-reason",
		RequestId:      "req-start-semantic-1",
		IdempotencyKey: "idem-start-semantic",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand(first) error = %v", err)
	}
	second, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		Actor:          "tester",
		Reason:         "same-reason",
		RequestId:      "req-start-semantic-2",
		IdempotencyKey: "idem-start-semantic",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand(second) error = %v", err)
	}
	if first.GetOperation().GetOperationId() != second.GetOperation().GetOperationId() {
		t.Fatalf("operation replay mismatch: first=%q second=%q", first.GetOperation().GetOperationId(), second.GetOperation().GetOperationId())
	}
	if _, err := engine.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          created.GetJob().GetJobId(),
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		Actor:          "tester",
		Reason:         "different-reason",
		RequestId:      "req-start-semantic-3",
		IdempotencyKey: "idem-start-semantic",
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("ApplyJobCommand(reason change) code = %s, want AlreadyExists", status.Code(err))
	}
}

func TestApplyJobCommandResumesPersistedRunningOperationAfterRestart(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 11, 25, 0, 0, time.UTC)
	dir := t.TempDir()
	repository, err := state.NewFileRepository(dir, state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewFileRepository() error = %v", err)
	}
	job := validJob(now)
	normalizedJob, diagnostics := compiler.NormalizeJob(job, now)
	if len(diagnostics) != 0 {
		t.Fatalf("NormalizeJob() diagnostics = %v", diagnostics)
	}
	run := compiler.NewRun(normalizedJob, 1, tgsrlv1.JobRunState_JOB_RUN_STATE_STARTING, now)
	run.State = tgsrlv1.JobState_JOB_STATE_PENDING
	operation := &tgsrlv1.Operation{
		OperationId:    "op-command-running",
		Type:           tgsrlv1.OperationType_OPERATION_TYPE_START,
		State:          tgsrlv1.OperationState_OPERATION_STATE_RUNNING,
		CreatedAt:      timestamppb.New(now),
		JobId:          normalizedJob.GetJobId(),
		RunId:          run.GetRunId(),
		TraceId:        run.GetTraceId(),
		DataKind:       normalizedJob.GetDataKind(),
		RequestId:      "req-start-restart",
		IdempotencyKey: "idem-start-restart",
		Cursor:         "cursor-op-command-running",
	}
	run.Operations = []*tgsrlv1.Operation{operation}
	normalizedJob.State = tgsrlv1.JobState_JOB_STATE_PENDING
	if err := repository.Update(func(store state.Store) error {
		store.PutJob(normalizedJob)
		store.PutRun(run)
		store.PutOperation(operation)
		store.PutIdempotency("command", "idem-start-restart", &state.IdempotencyRecord{
			Scope:       "command",
			Key:         "idem-start-restart",
			RequestHash: hashCommandRequest(normalizedJob.GetJobId(), run.GetRunId(), tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START, "tester", ""),
			OperationID: operation.GetOperationId(),
			JobID:       normalizedJob.GetJobId(),
			RunID:       run.GetRunId(),
		})
		return nil
	}); err != nil {
		t.Fatalf("seed Update() error = %v", err)
	}

	recovered, err := state.NewFileRepository(dir, state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewFileRepository(recover) error = %v", err)
	}
	driver := newDesiredOnlyDriver(runtimeclient.NewFakeDriver())
	engine, err := New(Config{
		Repository: recovered,
		Runtime:    driver,
		Clock:      state.ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	response, err := engine.ApplyJobCommand(context.Background(), &tgsrlv1.ApplyJobCommandRequest{
		JobId:          normalizedJob.GetJobId(),
		RunId:          run.GetRunId(),
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		Actor:          "tester",
		RequestId:      "req-start-restart-2",
		IdempotencyKey: "idem-start-restart",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand(restart) error = %v", err)
	}
	if response.GetOperation().GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
		t.Fatalf("operation state = %s, want RUNNING", response.GetOperation().GetState())
	}
	if driver.startCalls != 1 {
		t.Fatalf("start calls = %d, want 1", driver.startCalls)
	}
}

func TestAdmitJobResumesPersistedRunningOperationAfterRestart(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 11, 45, 0, 0, time.UTC)
	dir := t.TempDir()
	repository, err := state.NewFileRepository(dir, state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewFileRepository() error = %v", err)
	}
	job := validJob(now)
	normalizedJob, diagnostics := compiler.NormalizeJob(job, now)
	if len(diagnostics) != 0 {
		t.Fatalf("NormalizeJob() diagnostics = %v", diagnostics)
	}
	run := compiler.NewRun(normalizedJob, 1, tgsrlv1.JobRunState_JOB_RUN_STATE_ADMITTING, now)
	run.State = tgsrlv1.JobState_JOB_STATE_PENDING
	operation := &tgsrlv1.Operation{
		OperationId:    "op-admit-running",
		Type:           tgsrlv1.OperationType_OPERATION_TYPE_ADMIT,
		State:          tgsrlv1.OperationState_OPERATION_STATE_RUNNING,
		CreatedAt:      timestamppb.New(now),
		JobId:          normalizedJob.GetJobId(),
		RunId:          run.GetRunId(),
		TraceId:        run.GetTraceId(),
		DataKind:       normalizedJob.GetDataKind(),
		RequestId:      "req-admit-restart",
		IdempotencyKey: "idem-admit-restart",
		Cursor:         "cursor-op-admit-running",
	}
	run.Operations = []*tgsrlv1.Operation{operation}
	normalizedJob.State = tgsrlv1.JobState_JOB_STATE_PENDING
	if err := repository.Update(func(store state.Store) error {
		store.PutJob(normalizedJob)
		store.PutRun(run)
		store.PutOperation(operation)
		store.PutIdempotency("admit", "idem-admit-restart", &state.IdempotencyRecord{
			Scope:       "admit",
			Key:         "idem-admit-restart",
			RequestHash: hashAdmitRequest(normalizedJob.GetJobId(), ""),
			OperationID: operation.GetOperationId(),
			JobID:       normalizedJob.GetJobId(),
			RunID:       run.GetRunId(),
		})
		return nil
	}); err != nil {
		t.Fatalf("seed Update() error = %v", err)
	}

	recovered, err := state.NewFileRepository(dir, state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewFileRepository(recover) error = %v", err)
	}
	engine, err := New(Config{
		Repository: recovered,
		Runtime:    runtimeclient.NewFakeDriver(),
		Clock:      state.ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	response, err := engine.AdmitJob(context.Background(), &tgsrlv1.AdmitJobRequest{
		JobId:          normalizedJob.GetJobId(),
		RequestId:      "req-admit-restart",
		IdempotencyKey: "idem-admit-restart",
	})
	if err != nil {
		t.Fatalf("AdmitJob(restart) error = %v", err)
	}
	if response.GetOperation().GetState() != tgsrlv1.OperationState_OPERATION_STATE_SUCCEEDED {
		t.Fatalf("operation state = %s, want SUCCEEDED", response.GetOperation().GetState())
	}

	gotRun, err := engine.GetJobRun(context.Background(), &tgsrlv1.GetJobRunRequest{
		JobId: normalizedJob.GetJobId(),
		RunId: run.GetRunId(),
	})
	if err != nil {
		t.Fatalf("GetJobRun() error = %v", err)
	}
	if gotRun.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_WAITING {
		t.Fatalf("run state = %s, want WAITING", gotRun.GetRun().GetRunState())
	}
}

func testController(t *testing.T, now time.Time) *Controller {
	t.Helper()
	return testControllerWithDriver(t, now, runtimeclient.NewFakeDriver())
}

func testControllerWithDriver(t *testing.T, now time.Time, driver runtimeclient.Driver) *Controller {
	t.Helper()
	repository, err := state.NewMemoryRepository(state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	engine, err := New(Config{
		Repository: repository,
		Runtime:    driver,
		Clock:      state.ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return engine
}

func reportObservedStatus(t *testing.T, engine *Controller, ctx context.Context, jobID, runID string, health tgsrlv1.ComponentHealth, detail, source string, revision uint64, observedAt time.Time, runtimeState tgsrlv1.RuntimeState, converged bool) *tgsrlv1.ReportComponentStatusResponse {
	t.Helper()
	annotations := map[string]string{}
	if runtimeState != tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN {
		annotations["runtime.state"] = runtimeState.String()
	}
	if runtimeState != tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN {
		annotations["runtime.converged"] = strconv.FormatBool(converged)
	}
	response, err := engine.ReportComponentStatus(ctx, &tgsrlv1.ReportComponentStatusRequest{ComponentStatus: &tgsrlv1.ComponentStatus{
		Component:            "runtime",
		Health:               health,
		Detail:               detail,
		Source:               source,
		Revision:             revision,
		ObservedAt:           timestamppb.New(observedAt),
		Annotations:          annotations,
		ObservedRuntimeState: runtimeState,
		Converged:            converged,
		JobId:                jobID,
		RunId:                runID,
	}})
	if err != nil {
		t.Fatalf("ReportComponentStatus() error = %v", err)
	}
	return response
}

type blockingDriver struct {
	inner *runtimeclient.FakeDriver

	mu             sync.Mutex
	blockPrepare   bool
	prepareCalls   int
	prepareBlocked chan struct{}
	prepareRelease chan struct{}
}

type desiredOnlyDriver struct {
	inner      *runtimeclient.FakeDriver
	startCalls int
}

func newDesiredOnlyDriver(inner *runtimeclient.FakeDriver) *desiredOnlyDriver {
	return &desiredOnlyDriver{inner: inner}
}

func newBlockingDriver(inner *runtimeclient.FakeDriver) *blockingDriver {
	return &blockingDriver{
		inner:          inner,
		blockPrepare:   true,
		prepareBlocked: make(chan struct{}),
		prepareRelease: make(chan struct{}),
	}
}

func (d *blockingDriver) ValidateRuntime(ctx context.Context, request *tgsrlv1.ValidateRuntimeRequest) (*tgsrlv1.ValidateRuntimeResponse, error) {
	return d.inner.ValidateRuntime(ctx, request)
}

func (d *blockingDriver) CompileRuntime(ctx context.Context, request *tgsrlv1.CompileRuntimeRequest) (*tgsrlv1.CompileRuntimeResponse, error) {
	return d.inner.CompileRuntime(ctx, request)
}

func (d *blockingDriver) PrepareRuntime(ctx context.Context, request *tgsrlv1.PrepareRuntimeRequest) (*tgsrlv1.PrepareRuntimeResponse, error) {
	d.mu.Lock()
	d.prepareCalls++
	block := d.blockPrepare
	blocked := d.prepareBlocked
	if d.blockPrepare {
		d.blockPrepare = false
	}
	release := d.prepareRelease
	d.mu.Unlock()
	if block {
		close(blocked)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return d.inner.PrepareRuntime(ctx, request)
}

func (d *blockingDriver) prepareCallCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.prepareCalls
}

func (d *blockingDriver) StartRuntime(ctx context.Context, request *tgsrlv1.StartRuntimeRequest) (*tgsrlv1.StartRuntimeResponse, error) {
	return d.inner.StartRuntime(ctx, request)
}

func (d *blockingDriver) PauseRuntime(ctx context.Context, request *tgsrlv1.PauseRuntimeRequest) (*tgsrlv1.PauseRuntimeResponse, error) {
	return d.inner.PauseRuntime(ctx, request)
}

func (d *blockingDriver) ResumeRuntime(ctx context.Context, request *tgsrlv1.ResumeRuntimeRequest) (*tgsrlv1.ResumeRuntimeResponse, error) {
	return d.inner.ResumeRuntime(ctx, request)
}

func (d *blockingDriver) StopRuntime(ctx context.Context, request *tgsrlv1.StopRuntimeRequest) (*tgsrlv1.StopRuntimeResponse, error) {
	return d.inner.StopRuntime(ctx, request)
}

func (d *blockingDriver) TerminateRuntime(ctx context.Context, request *tgsrlv1.TerminateRuntimeRequest) (*tgsrlv1.TerminateRuntimeResponse, error) {
	return d.inner.TerminateRuntime(ctx, request)
}

func (d *blockingDriver) GetRuntimeStatus(ctx context.Context, request *tgsrlv1.GetRuntimeStatusRequest) (*tgsrlv1.GetRuntimeStatusResponse, error) {
	return d.inner.GetRuntimeStatus(ctx, request)
}

func (d *blockingDriver) Close() error { return d.inner.Close() }

func (d *desiredOnlyDriver) ValidateRuntime(ctx context.Context, request *tgsrlv1.ValidateRuntimeRequest) (*tgsrlv1.ValidateRuntimeResponse, error) {
	return d.inner.ValidateRuntime(ctx, request)
}

func (d *desiredOnlyDriver) CompileRuntime(ctx context.Context, request *tgsrlv1.CompileRuntimeRequest) (*tgsrlv1.CompileRuntimeResponse, error) {
	return d.inner.CompileRuntime(ctx, request)
}

func (d *desiredOnlyDriver) PrepareRuntime(ctx context.Context, request *tgsrlv1.PrepareRuntimeRequest) (*tgsrlv1.PrepareRuntimeResponse, error) {
	return d.inner.PrepareRuntime(ctx, request)
}

func (d *desiredOnlyDriver) StartRuntime(ctx context.Context, request *tgsrlv1.StartRuntimeRequest) (*tgsrlv1.StartRuntimeResponse, error) {
	d.startCalls++
	if _, err := d.inner.StartRuntime(ctx, request); err != nil {
		return nil, err
	}
	return &tgsrlv1.StartRuntimeResponse{Cursor: "start-desired-only"}, nil
}

func (d *desiredOnlyDriver) PauseRuntime(ctx context.Context, request *tgsrlv1.PauseRuntimeRequest) (*tgsrlv1.PauseRuntimeResponse, error) {
	if _, err := d.inner.PauseRuntime(ctx, request); err != nil {
		return nil, err
	}
	return &tgsrlv1.PauseRuntimeResponse{Cursor: "pause-desired-only"}, nil
}

func (d *desiredOnlyDriver) ResumeRuntime(ctx context.Context, request *tgsrlv1.ResumeRuntimeRequest) (*tgsrlv1.ResumeRuntimeResponse, error) {
	if _, err := d.inner.ResumeRuntime(ctx, request); err != nil {
		return nil, err
	}
	return &tgsrlv1.ResumeRuntimeResponse{Cursor: "resume-desired-only"}, nil
}

func (d *desiredOnlyDriver) StopRuntime(ctx context.Context, request *tgsrlv1.StopRuntimeRequest) (*tgsrlv1.StopRuntimeResponse, error) {
	if _, err := d.inner.StopRuntime(ctx, request); err != nil {
		return nil, err
	}
	return &tgsrlv1.StopRuntimeResponse{Cursor: "stop-desired-only"}, nil
}

func (d *desiredOnlyDriver) TerminateRuntime(ctx context.Context, request *tgsrlv1.TerminateRuntimeRequest) (*tgsrlv1.TerminateRuntimeResponse, error) {
	if _, err := d.inner.TerminateRuntime(ctx, request); err != nil {
		return nil, err
	}
	return &tgsrlv1.TerminateRuntimeResponse{Cursor: "terminate-desired-only"}, nil
}

func (d *desiredOnlyDriver) GetRuntimeStatus(_ context.Context, request *tgsrlv1.GetRuntimeStatusRequest) (*tgsrlv1.GetRuntimeStatusResponse, error) {
	return &tgsrlv1.GetRuntimeStatusResponse{
		RuntimeUnits: []*tgsrlv1.RuntimeUnit{{
			RuntimeUnitId: "unit-" + request.GetRunId(),
			RunId:         request.GetRunId(),
			State:         tgsrlv1.RuntimeState_RUNTIME_STATE_REQUESTED,
			Generation:    1,
		}},
		Cursor: "desired-only-status",
	}, nil
}

func (d *desiredOnlyDriver) Close() error { return d.inner.Close() }

func (d *blockingDriver) waitUntilPrepareBlocked(t *testing.T) {
	t.Helper()
	select {
	case <-d.prepareBlocked:
	case <-time.After(2 * time.Second):
		t.Fatal("PrepareRuntime() did not block")
	}
}

func (d *blockingDriver) releasePrepare() {
	d.mu.Lock()
	release := d.prepareRelease
	d.mu.Unlock()
	close(release)
}

func validJob(now time.Time) *tgsrlv1.RLTrainingJob {
	return &tgsrlv1.RLTrainingJob{
		DisplayName:       "trainer",
		ProtocolVersion:   "v0.3",
		Algorithm:         "ppo",
		Runtime:           validRuntime(),
		ExecutionContract: validContract(),
		ResourcesPerUnit:  &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4 << 30, AcceleratorUnits: 1},
		RequiredCapabilities: &tgsrlv1.CapabilitySet{
			Names:            []string{"gpu"},
			Algorithms:       []string{"ppo"},
			RolloutModes:     []string{"ROLLOUT_MODE_SYNC"},
			SupportedActions: []string{"start", "pause", "resume", "stop", "retry", "terminate"},
		},
		DesiredUnits: 1,
		Priority:     1,
		Queue:        "default",
		CreatedAt:    timestamppb.New(now),
		Labels:       map[string]string{"team": "rl"},
		RolloutMode:  tgsrlv1.RolloutMode_ROLLOUT_MODE_SYNC,
		ModelRef:     "model-1",
		DatasetRef:   "dataset-1",
		PolicyRef:    "policy-1",
		DataKind:     tgsrlv1.DataKind_DATA_KIND_SYNTHETIC,
	}
}

func validRuntime() *tgsrlv1.FrameworkRuntimeSpec {
	return &tgsrlv1.FrameworkRuntimeSpec{
		Framework:               "fake",
		FrameworkVersion:        "1.0.0",
		ExecutionBackend:        "fake",
		ExecutionBackendVersion: "1.0.0",
		Trainer:                 "fake",
		TrainerVersion:          "1.0.0",
		RolloutEngine:           "fake",
		RolloutEngineVersion:    "1.0.0",
		ImageDigest:             "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		PatchSet:                []string{"patch-b", "patch-a"},
	}
}

func validContract() *tgsrlv1.ExecutionContract {
	return &tgsrlv1.ExecutionContract{
		ContractId: "contract-1",
		Version:    "1.0.0",
		PhaseGraph: &tgsrlv1.PhaseGraph{
			Phases: []*tgsrlv1.Phase{{
				PhaseId:     "phase-train",
				DisplayName: "Train",
				Kind:        tgsrlv1.PhaseKind_PHASE_KIND_ACTOR,
				Parallelism: 1,
				MaxAttempts: 1,
				Labels:      map[string]string{"stage": "train"},
			}},
			EntryPhaseIds: []string{"phase-train"},
		},
		CommitPolicy: &tgsrlv1.CommitPolicy{
			Mode:                   tgsrlv1.CommitMode_COMMIT_MODE_ALL_OR_NOTHING,
			MinimumSuccessfulUnits: 1,
			MaxRetries:             1,
			CommitTimeout:          durationpb.New(time.Minute),
			RequireSafePoint:       true,
		},
		BackpressurePolicy: &tgsrlv1.BackpressurePolicy{
			Mode:               tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_BLOCK_PRODUCER,
			LowWatermark:       1,
			HighWatermark:      2,
			MaximumBufferLevel: 4,
			StallTimeout:       durationpb.New(time.Minute),
		},
		SafePointPolicy: &tgsrlv1.SafePointPolicy{
			Enabled:          true,
			Trigger:          tgsrlv1.SafePointTrigger_SAFE_POINT_TRIGGER_EXPLICIT,
			Interval:         durationpb.New(time.Minute),
			MaximumWait:      durationpb.New(time.Minute),
			RequiredPhaseIds: []string{"phase-train"},
		},
		Capabilities: &tgsrlv1.Capabilities{
			DeterministicReplay:  true,
			TransactionalCommits: true,
			CheckpointRestore:    true,
		},
	}
}
