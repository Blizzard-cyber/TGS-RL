package controller

import (
	"context"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/runtimeclient"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AdmitJob validates transition into an admitted run.
func (c *Controller) AdmitJob(ctx context.Context, request *tgsrlv1.AdmitJobRequest) (*tgsrlv1.AdmitJobResponse, error) {
	if request == nil || strings.TrimSpace(request.GetJobId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	requestHash := hashAdmitRequest(request.GetJobId(), request.GetReason())
	if response, ok, err := c.loadAdmitIdempotency(request.GetIdempotencyKey(), requestHash); ok || err != nil {
		if err != nil || response == nil || response.GetOperation().GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
			return response, err
		}
		if inflightErr, waited := c.waitInflight(response.GetOperation().GetOperationId()); waited {
			if inflightErr != nil {
				return nil, inflightErr
			}
			reloaded, reloadedOK, reloadErr := c.loadAdmitIdempotency(request.GetIdempotencyKey(), requestHash)
			if reloadErr != nil {
				return nil, reloadErr
			}
			if reloadedOK {
				return reloaded, nil
			}
		}
		return c.resumeAdmitPhase(ctx, request, requestHash, response.GetOperation().GetOperationId(), response.GetOperation().GetRunId(), response.GetOperation().GetJobId())
	}
	now := c.clock.Now().UTC()
	plan, response, err := c.persistAdmitPhase(request, requestHash, now)
	if err != nil || plan == nil {
		return response, err
	}
	resultAny, err := c.runInflight(plan.operation.GetOperationId(), func() (any, error) {
		result, prepareErr := c.prepareRun(ctx, cloneJob(plan.job), cloneRun(plan.run), cloneOperation(plan.operation))
		if prepareErr != nil {
			return nil, prepareErr
		}
		return c.finalizeAdmitPhase(request, requestHash, plan, result)
	})
	if err != nil {
		return nil, err
	}
	return resultAny.(*tgsrlv1.AdmitJobResponse), nil
}

func (c *Controller) persistAdmitPhase(request *tgsrlv1.AdmitJobRequest, requestHash string, now time.Time) (*admitPlan, *tgsrlv1.AdmitJobResponse, error) {
	var (
		plan     *admitPlan
		response *tgsrlv1.AdmitJobResponse
	)
	err := c.repository.Update(func(store state.Store) error {
		job, latestRun, err := loadJobAndLatestRun(store, request.GetJobId())
		if err != nil {
			return err
		}
		if latestRun != nil && (latestRun.GetRunState() == tgsrlv1.JobRunState_JOB_RUN_STATE_ADMITTED || latestRun.GetRunState() == tgsrlv1.JobRunState_JOB_RUN_STATE_WAITING) {
			operation := c.ensureOperation(store, tgsrlv1.OperationType_OPERATION_TYPE_ADMIT, job.GetJobId(), latestRun.GetRunId(), request.GetRequestId(), request.GetIdempotencyKey(), request.GetReason(), job.GetDataKind(), now)
			if request.GetIdempotencyKey() != "" {
				store.PutIdempotency("admit", request.GetIdempotencyKey(), &state.IdempotencyRecord{
					Scope:       "admit",
					Key:         request.GetIdempotencyKey(),
					RequestHash: requestHash,
					OperationID: operation.GetOperationId(),
					JobID:       job.GetJobId(),
					RunID:       latestRun.GetRunId(),
				})
			}
			response = &tgsrlv1.AdmitJobResponse{Job: job, Operation: operation}
			return nil
		}
		if latestRun != nil && isTerminalRunState(latestRun.GetRunState()) {
			return invalidTransition("admit", latestRun.GetRunState())
		}

		mutatedJob := cloneJob(job)
		mutatedJob.State = tgsrlv1.JobState_JOB_STATE_PENDING
		mutatedRun := cloneRun(latestRun)
		if mutatedRun == nil {
			mutatedRun = c.newRun(job, 1, tgsrlv1.JobRunState_JOB_RUN_STATE_ADMITTING, now)
		} else {
			mutatedRun.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_ADMITTING
			mutatedRun.State = tgsrlv1.JobState_JOB_STATE_PENDING
		}
		operation := c.newOperation(tgsrlv1.OperationType_OPERATION_TYPE_ADMIT, mutatedJob.GetJobId(), mutatedRun.GetRunId(), "", request.GetReason(), request.GetRequestId(), request.GetIdempotencyKey(), mutatedJob.GetDataKind(), nil, now)
		operation.State = tgsrlv1.OperationState_OPERATION_STATE_RUNNING
		operation.CompletedAt = nil
		mutatedRun.Operations = appendOperation(mutatedRun.GetOperations(), operation)

		store.PutJob(mutatedJob)
		store.PutRun(mutatedRun)
		store.PutOperation(operation)
		store.AppendEvent(c.newEvent(tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_ADMITTED, mutatedJob, mutatedRun, operation, nil, "job admitted", now))
		store.AppendEvent(c.newEvent(tgsrlv1.JobEventType_JOB_EVENT_TYPE_OPERATION_RECORDED, mutatedJob, mutatedRun, operation, nil, "operation recorded", now))
		if request.GetIdempotencyKey() != "" {
			store.PutIdempotency("admit", request.GetIdempotencyKey(), &state.IdempotencyRecord{
				Scope:       "admit",
				Key:         request.GetIdempotencyKey(),
				RequestHash: requestHash,
				OperationID: operation.GetOperationId(),
				JobID:       mutatedJob.GetJobId(),
				RunID:       mutatedRun.GetRunId(),
			})
		}
		plan = &admitPlan{
			job:       cloneJob(mutatedJob),
			run:       cloneRun(mutatedRun),
			operation: cloneOperation(operation),
		}
		return nil
	})
	return plan, response, err
}

func (c *Controller) finalizeAdmitPhase(request *tgsrlv1.AdmitJobRequest, requestHash string, plan *admitPlan, result prepareResult) (*tgsrlv1.AdmitJobResponse, error) {
	now := c.clock.Now().UTC()
	var response *tgsrlv1.AdmitJobResponse
	err := c.repository.Update(func(store state.Store) error {
		job, run, operation, err := c.loadCommittedOperation(store, plan.job.GetJobId(), plan.run.GetRunId(), plan.operation.GetOperationId())
		if err != nil {
			return err
		}
		if operation.GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
			response = &tgsrlv1.AdmitJobResponse{Job: job, Operation: operation}
			return nil
		}
		if result.err != nil {
			c.failOperation(job, run, operation, "RUNTIME_PREPARE_FAILED", result.err.Error(), now)
		} else if len(result.diagnostics) > 0 {
			c.failOperation(job, run, operation, "RUNTIME_PREPARE_FAILED", strings.Join(result.diagnostics, "; "), now)
		} else {
			run.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_WAITING
			run.State = tgsrlv1.JobState_JOB_STATE_PENDING
			job.State = tgsrlv1.JobState_JOB_STATE_PENDING
			operation.State = tgsrlv1.OperationState_OPERATION_STATE_SUCCEEDED
			operation.CompletedAt = timestamppb.New(now)
			if request.GetIdempotencyKey() != "" {
				store.PutIdempotency("admit", request.GetIdempotencyKey(), &state.IdempotencyRecord{
					Scope:       "admit",
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
		response = &tgsrlv1.AdmitJobResponse{Job: cloneJob(job), Operation: cloneOperation(operation)}
		return nil
	})
	return response, err
}

func (c *Controller) resumeAdmitPhase(ctx context.Context, request *tgsrlv1.AdmitJobRequest, requestHash, operationID, runID, jobID string) (*tgsrlv1.AdmitJobResponse, error) {
	plan, response, err := c.loadAdmitRunningPlan(jobID, runID, operationID)
	if err != nil || plan == nil {
		return response, err
	}
	resultAny, err := c.runInflight(operationID, func() (any, error) {
		result, prepareErr := c.prepareRun(ctx, cloneJob(plan.job), cloneRun(plan.run), cloneOperation(plan.operation))
		if prepareErr != nil {
			return nil, prepareErr
		}
		return c.finalizeAdmitPhase(request, requestHash, plan, result)
	})
	if err != nil {
		return nil, err
	}
	return resultAny.(*tgsrlv1.AdmitJobResponse), nil
}

func (c *Controller) loadAdmitRunningPlan(jobID, runID, operationID string) (*admitPlan, *tgsrlv1.AdmitJobResponse, error) {
	var (
		plan     *admitPlan
		response *tgsrlv1.AdmitJobResponse
	)
	err := c.repository.View(func(query state.Query) error {
		job, run, operation, err := c.loadCommittedOperation(query, jobID, runID, operationID)
		if err != nil {
			return err
		}
		if operation.GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
			response = &tgsrlv1.AdmitJobResponse{Job: job, Operation: operation}
			return nil
		}
		plan = &admitPlan{job: job, run: run, operation: operation}
		return nil
	})
	return plan, response, err
}

func (c *Controller) prepareRun(ctx context.Context, job *tgsrlv1.RLTrainingJob, run *tgsrlv1.JobRun, operation *tgsrlv1.Operation) (prepareResult, error) {
	_ = job
	manifest, err := runtimeclient.BuildManifest(run)
	if err != nil {
		return prepareResult{err: err}, nil
	}
	validateResponse, err := c.runtime.ValidateRuntime(ctx, &tgsrlv1.ValidateRuntimeRequest{
		Manifest:       manifest,
		RequestId:      operation.GetRequestId(),
		IdempotencyKey: operation.GetIdempotencyKey(),
	})
	if err != nil {
		return prepareResult{err: err}, nil
	}
	if !validateResponse.GetValid() {
		return prepareResult{diagnostics: append([]string(nil), validateResponse.GetDiagnostics()...)}, nil
	}
	if _, err := c.runtime.CompileRuntime(ctx, &tgsrlv1.CompileRuntimeRequest{
		Manifest:       manifest,
		RequestId:      operation.GetRequestId(),
		IdempotencyKey: operation.GetIdempotencyKey(),
	}); err != nil {
		return prepareResult{err: err}, nil
	}
	if _, err := c.runtime.PrepareRuntime(ctx, &tgsrlv1.PrepareRuntimeRequest{
		RunId:          run.GetRunId(),
		RequestId:      operation.GetRequestId(),
		IdempotencyKey: operation.GetIdempotencyKey(),
	}); err != nil {
		return prepareResult{err: err}, nil
	}
	return prepareResult{}, nil
}
