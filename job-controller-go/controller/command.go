package controller

import (
	"context"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ApplyJobCommand executes one lifecycle command.
func (c *Controller) ApplyJobCommand(ctx context.Context, request *tgsrlv1.ApplyJobCommandRequest) (*tgsrlv1.ApplyJobCommandResponse, error) {
	if request == nil || strings.TrimSpace(request.GetJobId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	commandType, eventType, opType, err := mapCommand(request.GetCommand())
	if err != nil {
		return nil, err
	}
	requestHash := hashCommandRequest(request.GetJobId(), request.GetRunId(), commandType, request.GetActor(), request.GetReason())
	if response, ok, err := c.loadCommandIdempotency(request.GetIdempotencyKey(), requestHash); ok || err != nil {
		if err != nil || response == nil || response.GetOperation().GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
			return response, err
		}
		if inflightErr, waited := c.waitInflight(response.GetOperation().GetOperationId()); waited {
			if inflightErr != nil {
				return nil, inflightErr
			}
			reloaded, reloadedOK, reloadErr := c.loadCommandIdempotency(request.GetIdempotencyKey(), requestHash)
			if reloadErr != nil {
				return nil, reloadErr
			}
			if reloadedOK {
				return reloaded, nil
			}
		}
		if commandDispatchAccepted(response.GetOperation()) {
			return response, nil
		}
		return c.resumeCommandPhase(ctx, request, requestHash, response.GetOperation().GetOperationId(), response.GetOperation().GetRunId(), response.GetOperation().GetJobId())
	}
	now := c.clock.Now().UTC()
	plan, response, err := c.persistCommandPhase(request, requestHash, eventType, opType, now)
	if err != nil || plan == nil {
		return response, err
	}
	resultAny, err := c.runInflight(plan.operation.GetOperationId(), func() (any, error) {
		if plan.isRetry {
			result, prepareErr := c.prepareRun(ctx, cloneJob(plan.job), cloneRun(plan.run), cloneOperation(plan.operation))
			if prepareErr != nil {
				return nil, prepareErr
			}
			return c.finalizeRetryPhase(request, requestHash, plan, result)
		}
		runtimeErr := c.invokeRuntime(ctx, plan.command, cloneRun(plan.run), cloneOperation(plan.operation), request)
		return c.finalizeCommandPhase(plan, runtimeErr)
	})
	if err != nil {
		return nil, err
	}
	return resultAny.(*tgsrlv1.ApplyJobCommandResponse), nil
}

func (c *Controller) persistCommandPhase(request *tgsrlv1.ApplyJobCommandRequest, requestHash string, eventType tgsrlv1.JobEventType, opType tgsrlv1.OperationType, now time.Time) (*commandPlan, *tgsrlv1.ApplyJobCommandResponse, error) {
	var (
		plan     *commandPlan
		response *tgsrlv1.ApplyJobCommandResponse
	)
	err := c.repository.Update(func(store state.Store) error {
		job, run, err := c.resolveTargetRun(store, request.GetJobId(), request.GetRunId())
		if err != nil {
			return err
		}
		if run == nil {
			return status.Errorf(codes.FailedPrecondition, "job %q has no run to mutate", request.GetJobId())
		}
		if existing, ok := c.semanticIdempotentOperation(store, job, run, opType); ok {
			if request.GetIdempotencyKey() != "" {
				store.PutIdempotency("command", request.GetIdempotencyKey(), &state.IdempotencyRecord{
					Scope:       "command",
					Key:         request.GetIdempotencyKey(),
					RequestHash: requestHash,
					OperationID: existing.GetOperationId(),
					JobID:       job.GetJobId(),
					RunID:       run.GetRunId(),
				})
			}
			response = &tgsrlv1.ApplyJobCommandResponse{Operation: existing, Run: run}
			return nil
		}

		mutatedJob := cloneJob(job)
		mutatedRun := cloneRun(run)
		isRetry := false
		switch request.GetCommand() {
		case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START:
			if mutatedRun.GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_ADMITTED && mutatedRun.GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_WAITING && mutatedRun.GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_STOPPED {
				return invalidTransition("start", mutatedRun.GetRunState())
			}
			mutatedRun.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_STARTING
			mutatedRun.State = tgsrlv1.JobState_JOB_STATE_PENDING
			mutatedJob.State = tgsrlv1.JobState_JOB_STATE_PENDING
		case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE:
			if mutatedRun.GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING {
				return invalidTransition("pause", mutatedRun.GetRunState())
			}
			mutatedRun.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSING
			mutatedRun.State = tgsrlv1.JobState_JOB_STATE_RUNNING
			mutatedJob.State = tgsrlv1.JobState_JOB_STATE_RUNNING
		case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME:
			if mutatedRun.GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSED {
				return invalidTransition("resume", mutatedRun.GetRunState())
			}
			mutatedRun.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_RESUMING
			mutatedRun.State = tgsrlv1.JobState_JOB_STATE_PAUSED
			mutatedJob.State = tgsrlv1.JobState_JOB_STATE_PAUSED
		case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP:
			if mutatedRun.GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING && mutatedRun.GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSED && mutatedRun.GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_ADMITTED {
				return invalidTransition("stop", mutatedRun.GetRunState())
			}
			mutatedRun.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_STOPPING
			mutatedRun.State = tgsrlv1.JobState_JOB_STATE_RUNNING
			mutatedJob.State = tgsrlv1.JobState_JOB_STATE_RUNNING
		case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE:
			if isTerminalRunState(mutatedRun.GetRunState()) {
				return invalidTransition("terminate", mutatedRun.GetRunState())
			}
			mutatedRun.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_TERMINATING
			mutatedRun.State = tgsrlv1.JobState_JOB_STATE_RUNNING
			mutatedJob.State = tgsrlv1.JobState_JOB_STATE_RUNNING
		case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RETRY:
			if !isTerminalRunState(mutatedRun.GetRunState()) {
				return invalidTransition("retry", mutatedRun.GetRunState())
			}
			mutatedJob.State = tgsrlv1.JobState_JOB_STATE_PENDING
			mutatedRun = c.newRun(job, mutatedRun.GetAttempt()+1, tgsrlv1.JobRunState_JOB_RUN_STATE_RETRYING, now)
			isRetry = true
		default:
			return status.Error(codes.InvalidArgument, "unsupported command")
		}

		operation := c.newOperation(opType, mutatedJob.GetJobId(), mutatedRun.GetRunId(), request.GetActor(), request.GetReason(), request.GetRequestId(), request.GetIdempotencyKey(), mutatedJob.GetDataKind(), nil, now)
		operation.State = tgsrlv1.OperationState_OPERATION_STATE_RUNNING
		operation.CompletedAt = nil
		mutatedRun.Operations = appendOperation(mutatedRun.GetOperations(), operation)

		store.PutJob(mutatedJob)
		store.PutRun(mutatedRun)
		store.PutOperation(operation)
		detail := strings.ToLower(strings.TrimPrefix(eventType.String(), "JOB_EVENT_TYPE_"))
		if isRetry {
			detail = "job retried"
		}
		store.AppendEvent(c.newEvent(eventType, mutatedJob, mutatedRun, operation, nil, detail, now))
		store.AppendEvent(c.newEvent(tgsrlv1.JobEventType_JOB_EVENT_TYPE_OPERATION_RECORDED, mutatedJob, mutatedRun, operation, nil, "operation recorded", now))
		if request.GetIdempotencyKey() != "" {
			store.PutIdempotency("command", request.GetIdempotencyKey(), &state.IdempotencyRecord{
				Scope:       "command",
				Key:         request.GetIdempotencyKey(),
				RequestHash: requestHash,
				OperationID: operation.GetOperationId(),
				JobID:       mutatedJob.GetJobId(),
				RunID:       mutatedRun.GetRunId(),
			})
		}
		plan = &commandPlan{
			job:       cloneJob(mutatedJob),
			run:       cloneRun(mutatedRun),
			operation: cloneOperation(operation),
			eventType: eventType,
			command:   request.GetCommand(),
			isRetry:   isRetry,
			stableJob: cloneJob(job),
			stableRun: stableRunForCommand(run, request.GetCommand()),
		}
		return nil
	})
	return plan, response, err
}

func (c *Controller) finalizeRetryPhase(request *tgsrlv1.ApplyJobCommandRequest, requestHash string, plan *commandPlan, result prepareResult) (*tgsrlv1.ApplyJobCommandResponse, error) {
	now := c.clock.Now().UTC()
	var response *tgsrlv1.ApplyJobCommandResponse
	err := c.repository.Update(func(store state.Store) error {
		job, run, operation, err := c.loadCommittedOperation(store, plan.job.GetJobId(), plan.run.GetRunId(), plan.operation.GetOperationId())
		if err != nil {
			return err
		}
		if operation.GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
			response = &tgsrlv1.ApplyJobCommandResponse{Operation: operation, Run: run}
			return nil
		}
		if result.err != nil {
			c.failOperation(job, run, operation, "RUNTIME_PREPARE_FAILED", result.err.Error(), now)
		} else if len(result.diagnostics) > 0 {
			c.failOperation(job, run, operation, "RUNTIME_PREPARE_FAILED", strings.Join(result.diagnostics, "; "), now)
		} else {
			job.State = tgsrlv1.JobState_JOB_STATE_PENDING
			run.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_WAITING
			run.State = tgsrlv1.JobState_JOB_STATE_PENDING
			operation.State = tgsrlv1.OperationState_OPERATION_STATE_SUCCEEDED
			operation.CompletedAt = timestamppb.New(now)
			if request.GetIdempotencyKey() != "" {
				store.PutIdempotency("command", request.GetIdempotencyKey(), &state.IdempotencyRecord{
					Scope:       "command",
					Key:         request.GetIdempotencyKey(),
					RequestHash: requestHash,
					OperationID: operation.GetOperationId(),
					JobID:       job.GetJobId(),
					RunID:       run.GetRunId(),
				})
			}
		}
		run.Operations = appendOperation(run.GetOperations(), operation)
		store.PutJob(job)
		store.PutRun(run)
		store.PutOperation(operation)
		response = &tgsrlv1.ApplyJobCommandResponse{Operation: cloneOperation(operation), Run: cloneRun(run)}
		return nil
	})
	return response, err
}

func (c *Controller) finalizeCommandPhase(plan *commandPlan, runtimeErr error) (*tgsrlv1.ApplyJobCommandResponse, error) {
	now := c.clock.Now().UTC()
	var response *tgsrlv1.ApplyJobCommandResponse
	err := c.repository.Update(func(store state.Store) error {
		job, run, operation, err := c.loadCommittedOperation(store, plan.job.GetJobId(), plan.run.GetRunId(), plan.operation.GetOperationId())
		if err != nil {
			return err
		}
		if operation.GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
			response = &tgsrlv1.ApplyJobCommandResponse{Operation: operation, Run: run}
			return nil
		}
		if runtimeErr != nil {
			if isAmbiguousRuntimeCommandError(runtimeErr) {
				response = &tgsrlv1.ApplyJobCommandResponse{Operation: cloneOperation(operation), Run: cloneRun(run)}
				return nil
			}
			operation.State = tgsrlv1.OperationState_OPERATION_STATE_FAILED
			operation.ErrorCode = "RUNTIME_COMMAND_FAILED"
			operation.ErrorMessage = runtimeErr.Error()
			operation.CompletedAt = timestamppb.New(now)
			if plan.stableJob != nil {
				job = cloneJob(plan.stableJob)
			}
			if plan.stableRun != nil {
				run = cloneRun(plan.stableRun)
			}
			run.Operations = appendOperation(run.GetOperations(), operation)
		} else {
			if operation.Annotations == nil {
				operation.Annotations = map[string]string{}
			}
			operation.Annotations["runtime.dispatch.accepted"] = "true"
			run.Operations = appendOperation(run.GetOperations(), operation)
		}
		store.PutJob(job)
		store.PutRun(run)
		store.PutOperation(operation)
		response = &tgsrlv1.ApplyJobCommandResponse{Operation: cloneOperation(operation), Run: cloneRun(run)}
		return nil
	})
	return response, err
}

func (c *Controller) resumeCommandPhase(ctx context.Context, request *tgsrlv1.ApplyJobCommandRequest, requestHash, operationID, runID, jobID string) (*tgsrlv1.ApplyJobCommandResponse, error) {
	plan, response, err := c.loadCommandRunningPlan(request.GetCommand(), jobID, runID, operationID)
	if err != nil || plan == nil {
		return response, err
	}
	resultAny, err := c.runInflight(operationID, func() (any, error) {
		if plan.isRetry {
			result, prepareErr := c.prepareRun(ctx, cloneJob(plan.job), cloneRun(plan.run), cloneOperation(plan.operation))
			if prepareErr != nil {
				return nil, prepareErr
			}
			return c.finalizeRetryPhase(request, requestHash, plan, result)
		}
		runtimeErr := c.invokeRuntime(ctx, plan.command, cloneRun(plan.run), cloneOperation(plan.operation), request)
		return c.finalizeCommandPhase(plan, runtimeErr)
	})
	if err != nil {
		return nil, err
	}
	return resultAny.(*tgsrlv1.ApplyJobCommandResponse), nil
}

func (c *Controller) loadCommandRunningPlan(command tgsrlv1.JobCommandType, jobID, runID, operationID string) (*commandPlan, *tgsrlv1.ApplyJobCommandResponse, error) {
	var (
		plan     *commandPlan
		response *tgsrlv1.ApplyJobCommandResponse
	)
	err := c.repository.View(func(query state.Query) error {
		job, run, operation, err := c.loadCommittedOperation(query, jobID, runID, operationID)
		if err != nil {
			return err
		}
		if operation.GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
			response = &tgsrlv1.ApplyJobCommandResponse{Operation: operation, Run: run}
			return nil
		}
		plan = &commandPlan{
			job:       job,
			run:       run,
			operation: operation,
			command:   command,
			isRetry:   command == tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RETRY,
			stableJob: cloneJob(job),
			stableRun: stableRunForCommand(run, command),
		}
		return nil
	})
	return plan, response, err
}

func isAmbiguousRuntimeCommandError(err error) bool {
	if err == nil {
		return false
	}
	rpcStatus, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch rpcStatus.Code() {
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable, codes.Unknown:
		return true
	default:
		return false
	}
}

func commandDispatchAccepted(operation *tgsrlv1.Operation) bool {
	if operation == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(operation.GetAnnotations()["runtime.dispatch.accepted"]), "true")
}

func stableRunForCommand(run *tgsrlv1.JobRun, command tgsrlv1.JobCommandType) *tgsrlv1.JobRun {
	if run == nil {
		return nil
	}
	stableRun := cloneRun(run)
	switch command {
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START:
		if stableRun.GetRunState() == tgsrlv1.JobRunState_JOB_RUN_STATE_STOPPED {
			stableRun.State = tgsrlv1.JobState_JOB_STATE_CANCELLED
		} else {
			stableRun.State = tgsrlv1.JobState_JOB_STATE_PENDING
		}
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE:
		stableRun.State = tgsrlv1.JobState_JOB_STATE_RUNNING
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME:
		stableRun.State = tgsrlv1.JobState_JOB_STATE_PAUSED
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP:
		switch stableRun.GetRunState() {
		case tgsrlv1.JobRunState_JOB_RUN_STATE_ADMITTED:
			stableRun.State = tgsrlv1.JobState_JOB_STATE_PENDING
		case tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSED:
			stableRun.State = tgsrlv1.JobState_JOB_STATE_PAUSED
		default:
			stableRun.State = tgsrlv1.JobState_JOB_STATE_RUNNING
		}
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE:
		stableRun.State = stateFromRun(stableRun.GetRunState())
	default:
		stableRun.State = stateFromRun(stableRun.GetRunState())
	}
	return stableRun
}

func (c *Controller) invokeRuntime(ctx context.Context, command tgsrlv1.JobCommandType, run *tgsrlv1.JobRun, operation *tgsrlv1.Operation, request *tgsrlv1.ApplyJobCommandRequest) error {
	_ = operation
	switch command {
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START:
		_, err := c.runtime.StartRuntime(ctx, &tgsrlv1.StartRuntimeRequest{RunId: run.GetRunId(), RequestId: request.GetRequestId(), IdempotencyKey: request.GetIdempotencyKey()})
		return err
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE:
		_, err := c.runtime.PauseRuntime(ctx, &tgsrlv1.PauseRuntimeRequest{RunId: run.GetRunId(), RequestId: request.GetRequestId(), IdempotencyKey: request.GetIdempotencyKey(), Reason: request.GetReason()})
		return err
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME:
		_, err := c.runtime.ResumeRuntime(ctx, &tgsrlv1.ResumeRuntimeRequest{RunId: run.GetRunId(), RequestId: request.GetRequestId(), IdempotencyKey: request.GetIdempotencyKey()})
		return err
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP:
		_, err := c.runtime.StopRuntime(ctx, &tgsrlv1.StopRuntimeRequest{RunId: run.GetRunId(), RequestId: request.GetRequestId(), IdempotencyKey: request.GetIdempotencyKey(), Reason: request.GetReason()})
		return err
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE:
		_, err := c.runtime.TerminateRuntime(ctx, &tgsrlv1.TerminateRuntimeRequest{RunId: run.GetRunId(), RequestId: request.GetRequestId(), IdempotencyKey: request.GetIdempotencyKey(), Reason: request.GetReason()})
		return err
	default:
		return status.Error(codes.InvalidArgument, "unsupported runtime command")
	}
}
