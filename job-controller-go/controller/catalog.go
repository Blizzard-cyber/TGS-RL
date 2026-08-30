package controller

import (
	"context"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/compiler"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/runtimeclient"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CreateJob validates, normalizes, and stores a job.
func (c *Controller) CreateJob(_ context.Context, request *tgsrlv1.CreateJobRequest) (*tgsrlv1.CreateJobResponse, error) {
	if request == nil || request.Job == nil {
		return nil, status.Error(codes.InvalidArgument, "job is required")
	}
	now := c.clock.Now().UTC()
	normalized, diagnostics := c.normalizeJob(request.Job, now)
	if len(diagnostics) > 0 {
		return nil, status.Error(codes.InvalidArgument, strings.Join(diagnostics, "; "))
	}
	requestHash := hashNormalizedJobRequest("create-job", normalized, request.Job)
	if response, ok, err := c.loadCreateJobIdempotency(request.GetIdempotencyKey(), requestHash); ok || err != nil {
		return response, err
	}

	var response *tgsrlv1.CreateJobResponse
	err := c.repository.Update(func(store state.Store) error {
		if existing, ok := store.GetJob(normalized.GetJobId()); ok {
			if !proto.Equal(existing, normalized) {
				return status.Errorf(codes.AlreadyExists, "job %q already exists with different content", normalized.GetJobId())
			}
			operation, _ := store.GetLatestOperation(normalized.GetJobId(), "", tgsrlv1.OperationType_OPERATION_TYPE_VALIDATE)
			if operation == nil {
				operation = c.newOperation(tgsrlv1.OperationType_OPERATION_TYPE_VALIDATE, normalized.GetJobId(), "", "", "", request.GetRequestId(), request.GetIdempotencyKey(), normalized.GetDataKind(), nil, now)
				store.PutOperation(operation)
			}
			response = &tgsrlv1.CreateJobResponse{Job: existing, Operation: operation}
			if request.GetIdempotencyKey() != "" {
				store.PutIdempotency("create-job", request.GetIdempotencyKey(), &state.IdempotencyRecord{
					Scope:       "create-job",
					Key:         request.GetIdempotencyKey(),
					RequestHash: requestHash,
					OperationID: operation.GetOperationId(),
					JobID:       normalized.GetJobId(),
				})
			}
			return nil
		}
		operation := c.newOperation(tgsrlv1.OperationType_OPERATION_TYPE_VALIDATE, normalized.GetJobId(), "", "", "", request.GetRequestId(), request.GetIdempotencyKey(), normalized.GetDataKind(), nil, now)
		store.PutJob(normalized)
		store.PutOperation(operation)
		store.AppendEvent(c.newEvent(tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_CREATED, normalized, nil, operation, nil, "job created", now))
		store.AppendEvent(c.newEvent(tgsrlv1.JobEventType_JOB_EVENT_TYPE_OPERATION_RECORDED, normalized, nil, operation, nil, "operation recorded", now))
		if request.GetIdempotencyKey() != "" {
			store.PutIdempotency("create-job", request.GetIdempotencyKey(), &state.IdempotencyRecord{
				Scope:       "create-job",
				Key:         request.GetIdempotencyKey(),
				RequestHash: requestHash,
				OperationID: operation.GetOperationId(),
				JobID:       normalized.GetJobId(),
			})
		}
		response = &tgsrlv1.CreateJobResponse{Job: cloneJob(normalized), Operation: operation}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

// ValidateJob checks semantic correctness without storing the job.
func (c *Controller) ValidateJob(ctx context.Context, request *tgsrlv1.ValidateJobRequest) (*tgsrlv1.ValidateJobResponse, error) {
	if request == nil || request.Job == nil {
		return nil, status.Error(codes.InvalidArgument, "job is required")
	}
	now := c.clock.Now().UTC()
	normalized, diagnostics := c.normalizeJob(request.Job, now)
	valid := len(diagnostics) == 0
	jobID := ""
	dataKind := tgsrlv1.DataKind_DATA_KIND_UNKNOWN
	if normalized != nil {
		jobID = normalized.GetJobId()
		dataKind = normalized.GetDataKind()
	}
	operation := c.newOperation(tgsrlv1.OperationType_OPERATION_TYPE_VALIDATE, jobID, "", "", "", request.GetRequestId(), request.GetIdempotencyKey(), dataKind, nil, now)
	if valid && normalized != nil {
		if manifest, err := runtimeclient.BuildManifest(compiler.NewRun(normalized, 1, tgsrlv1.JobRunState_JOB_RUN_STATE_VALIDATING, now)); err == nil {
			runtimeResponse, runtimeErr := c.runtime.ValidateRuntime(ctx, &tgsrlv1.ValidateRuntimeRequest{
				Manifest:       manifest,
				RequestId:      request.GetRequestId(),
				IdempotencyKey: request.GetIdempotencyKey(),
			})
			if runtimeErr != nil {
				valid = false
				diagnostics = append(diagnostics, runtimeErr.Error())
			} else if !runtimeResponse.GetValid() {
				valid = false
				diagnostics = append(diagnostics, runtimeResponse.GetDiagnostics()...)
			}
		}
	}
	if !valid {
		operation.State = tgsrlv1.OperationState_OPERATION_STATE_FAILED
		operation.ErrorCode = "JOB_VALIDATION_FAILED"
		operation.ErrorMessage = strings.Join(diagnostics, "; ")
	}
	return &tgsrlv1.ValidateJobResponse{
		Valid:         valid,
		NormalizedJob: normalized,
		Diagnostics:   diagnostics,
		Operation:     operation,
	}, nil
}

// ListJobs returns normalized stored jobs.
func (c *Controller) ListJobs(_ context.Context, request *tgsrlv1.ListJobsRequest) (*tgsrlv1.ListJobsResponse, error) {
	if request == nil {
		request = &tgsrlv1.ListJobsRequest{}
	}
	if request.GetPageToken() != "" && request.GetAfterJobId() != "" {
		return nil, status.Error(codes.InvalidArgument, "page_token is mutually exclusive with after_job_id")
	}
	var response *tgsrlv1.ListJobsResponse
	err := c.repository.View(func(query state.Query) error {
		jobs, next, err := query.ListJobs(request.GetDataKind(), request.GetLimit(), request.GetPageToken(), request.GetAfterJobId())
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		response = &tgsrlv1.ListJobsResponse{Jobs: jobs, NextPageToken: next}
		return nil
	})
	return response, err
}

// GetJob returns one stored job.
func (c *Controller) GetJob(_ context.Context, request *tgsrlv1.GetJobRequest) (*tgsrlv1.GetJobResponse, error) {
	if request == nil || strings.TrimSpace(request.GetJobId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	var response *tgsrlv1.GetJobResponse
	err := c.repository.View(func(query state.Query) error {
		job, ok := query.GetJob(request.GetJobId())
		if !ok {
			return status.Errorf(codes.NotFound, "job %q not found", request.GetJobId())
		}
		response = &tgsrlv1.GetJobResponse{Job: job, Cursor: deterministicID("cursor", job.GetJobId(), job.GetCreatedAt().AsTime().UTC().Format(time.RFC3339Nano))}
		return nil
	})
	return response, err
}

// CreateJobRun compiles an immutable execution specification with mutable lifecycle status.
func (c *Controller) CreateJobRun(_ context.Context, request *tgsrlv1.CreateJobRunRequest) (*tgsrlv1.CreateJobRunResponse, error) {
	if request == nil || (request.Job == nil && strings.TrimSpace(request.GetJobId()) == "") {
		return nil, status.Error(codes.InvalidArgument, "job or job_id is required")
	}
	now := c.clock.Now().UTC()
	requestJobID := strings.TrimSpace(request.GetJobId())
	var normalized *tgsrlv1.RLTrainingJob
	if request.Job != nil {
		var diagnostics []string
		normalized, diagnostics = c.normalizeJob(request.Job, now)
		if len(diagnostics) > 0 {
			return nil, status.Error(codes.InvalidArgument, strings.Join(diagnostics, "; "))
		}
		if requestJobID != "" && normalized.GetJobId() != requestJobID {
			return nil, status.Error(codes.InvalidArgument, "job.job_id must match job_id when both are set")
		}
	} else {
		err := c.repository.View(func(query state.Query) error {
			var ok bool
			normalized, ok = query.GetJob(requestJobID)
			if !ok {
				return status.Errorf(codes.NotFound, "job %q not found", requestJobID)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	requestHash := hashStrings("create-run", requestJobID)
	if request.Job != nil {
		requestHash = hashNormalizedJobRequest("create-run", normalized, request.Job)
	}
	if response, ok, err := c.loadCreateRunIdempotency(request.GetIdempotencyKey(), requestHash); ok || err != nil {
		return response, err
	}
	var response *tgsrlv1.CreateJobRunResponse
	err := c.repository.Update(func(store state.Store) error {
		job, ok := store.GetJob(normalized.GetJobId())
		if !ok {
			job = cloneJob(normalized)
			store.PutJob(job)
		}
		attempt := uint64(1)
		if latest, exists := store.GetLatestRun(job.GetJobId()); exists {
			attempt = latest.GetAttempt() + 1
			if attempt == 0 {
				return status.Error(codes.ResourceExhausted, "run attempt counter is exhausted")
			}
		}
		run := c.newRun(job, attempt, tgsrlv1.JobRunState_JOB_RUN_STATE_VALIDATING, now)
		store.PutRun(run)
		if request.GetIdempotencyKey() != "" {
			store.PutIdempotency("create-run", request.GetIdempotencyKey(), &state.IdempotencyRecord{
				Scope:       "create-run",
				Key:         request.GetIdempotencyKey(),
				RequestHash: requestHash,
				JobID:       job.GetJobId(),
				RunID:       run.GetRunId(),
			})
		}
		response = &tgsrlv1.CreateJobRunResponse{Run: cloneRun(run)}
		return nil
	})
	return response, err
}

// ListJobRuns lists runs for one job.
func (c *Controller) ListJobRuns(_ context.Context, request *tgsrlv1.ListJobRunsRequest) (*tgsrlv1.ListJobRunsResponse, error) {
	if request == nil || strings.TrimSpace(request.GetJobId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if request.GetPageToken() != "" && request.GetAfterRunId() != "" {
		return nil, status.Error(codes.InvalidArgument, "page_token is mutually exclusive with after_run_id")
	}
	var response *tgsrlv1.ListJobRunsResponse
	err := c.repository.View(func(query state.Query) error {
		if _, ok := query.GetJob(request.GetJobId()); !ok {
			return status.Errorf(codes.NotFound, "job %q not found", request.GetJobId())
		}
		runs, next, err := query.ListRuns(request.GetJobId(), request.GetLimit(), request.GetPageToken(), request.GetAfterRunId())
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		response = &tgsrlv1.ListJobRunsResponse{Runs: runs, NextPageToken: next}
		return nil
	})
	return response, err
}

// GetJobRun returns one run generation and its current lifecycle projection.
func (c *Controller) GetJobRun(_ context.Context, request *tgsrlv1.GetJobRunRequest) (*tgsrlv1.GetJobRunResponse, error) {
	if request == nil || strings.TrimSpace(request.GetJobId()) == "" || strings.TrimSpace(request.GetRunId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id and run_id are required")
	}
	var response *tgsrlv1.GetJobRunResponse
	err := c.repository.View(func(query state.Query) error {
		run, ok := query.GetRun(request.GetJobId(), request.GetRunId())
		if !ok {
			return status.Errorf(codes.NotFound, "run %q for job %q not found", request.GetRunId(), request.GetJobId())
		}
		response = &tgsrlv1.GetJobRunResponse{Run: run, Cursor: run.GetCursor()}
		return nil
	})
	return response, err
}

// ListJobEvents returns the immutable event log.
func (c *Controller) ListJobEvents(_ context.Context, request *tgsrlv1.ListJobEventsRequest) (*tgsrlv1.ListJobEventsResponse, error) {
	if request == nil || strings.TrimSpace(request.GetJobId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if strings.TrimSpace(request.GetPageToken()) != "" && (request.GetAfterSequence() != 0 || strings.TrimSpace(request.GetAfterEventId()) != "") {
		return nil, status.Error(codes.InvalidArgument, "page_token is mutually exclusive with after_sequence and after_event_id")
	}
	var response *tgsrlv1.ListJobEventsResponse
	err := c.repository.View(func(query state.Query) error {
		events, next, err := query.ListEvents(
			request.GetJobId(),
			request.GetRunId(),
			request.GetAfterEventId(),
			request.GetAfterSequence(),
			request.GetLimit(),
			request.GetPageToken(),
		)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		cursor := ""
		if len(events) > 0 {
			cursor = events[len(events)-1].GetCursor()
		}
		response = &tgsrlv1.ListJobEventsResponse{Events: events, NextPageToken: next, Cursor: cursor}
		return nil
	})
	return response, err
}

// WatchJobEvents streams immutable event notifications.
func (c *Controller) WatchJobEvents(jobID, runID string, afterSequence uint64, afterEventID string) (<-chan *tgsrlv1.JobEvent, func(), error) {
	if strings.TrimSpace(jobID) == "" {
		return nil, nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	ch, cancel, err := c.repository.Watch(jobID, runID, afterSequence, afterEventID)
	if err != nil {
		return nil, nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return ch, cancel, nil
}

// GetOperation returns a recorded operation.
func (c *Controller) GetOperation(_ context.Context, request *tgsrlv1.GetOperationRequest) (*tgsrlv1.GetOperationResponse, error) {
	if request == nil || strings.TrimSpace(request.GetOperationId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "operation_id is required")
	}
	var response *tgsrlv1.GetOperationResponse
	err := c.repository.View(func(query state.Query) error {
		operation, ok := query.GetOperation(request.GetOperationId())
		if !ok {
			return status.Errorf(codes.NotFound, "operation %q not found", request.GetOperationId())
		}
		response = &tgsrlv1.GetOperationResponse{Operation: operation, Cursor: operation.GetCursor()}
		return nil
	})
	return response, err
}

// ListOperations returns stable, filtered operation pages.
func (c *Controller) ListOperations(_ context.Context, request *tgsrlv1.ListOperationsRequest) (*tgsrlv1.ListOperationsResponse, error) {
	if request == nil {
		request = &tgsrlv1.ListOperationsRequest{}
	}
	var response *tgsrlv1.ListOperationsResponse
	err := c.repository.View(func(query state.Query) error {
		operations, next, err := query.ListOperations(
			request.GetJobId(), request.GetRunId(), request.GetType(), request.GetState(),
			request.GetLimit(), request.GetPageToken(),
		)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		cursor := ""
		if len(operations) > 0 {
			cursor = operations[len(operations)-1].GetCursor()
		}
		response = &tgsrlv1.ListOperationsResponse{
			Operations: operations, NextPageToken: next, Cursor: cursor,
		}
		return nil
	})
	return response, err
}

// RecordOperation stores an external operation observation.
func (c *Controller) RecordOperation(_ context.Context, request *tgsrlv1.RecordOperationRequest) (*tgsrlv1.RecordOperationResponse, error) {
	if request == nil || request.Operation == nil {
		return nil, status.Error(codes.InvalidArgument, "operation is required")
	}
	now := c.clock.Now().UTC()
	incoming := cloneOperation(request.Operation)
	if strings.TrimSpace(incoming.GetOperationId()) == "" {
		incoming.OperationId = deterministicID("op", incoming.GetType().String(), incoming.GetJobId(), incoming.GetRunId(), incoming.GetRequestId(), incoming.GetIdempotencyKey())
	}
	if incoming.CreatedAt == nil {
		incoming.CreatedAt = timestamppb.New(now)
	}
	if incoming.CompletedAt == nil {
		incoming.CompletedAt = timestamppb.New(now)
	}
	if incoming.State == tgsrlv1.OperationState_OPERATION_STATE_UNKNOWN {
		incoming.State = tgsrlv1.OperationState_OPERATION_STATE_SUCCEEDED
	}
	var response *tgsrlv1.RecordOperationResponse
	err := c.repository.Update(func(store state.Store) error {
		job, ok := store.GetJob(incoming.GetJobId())
		if !ok {
			return status.Errorf(codes.NotFound, "job %q not found", incoming.GetJobId())
		}
		if existing, ok := store.GetOperation(incoming.GetOperationId()); ok {
			if !proto.Equal(existing, incoming) {
				return status.Errorf(codes.AlreadyExists, "operation %q already exists with different content", incoming.GetOperationId())
			}
			response = &tgsrlv1.RecordOperationResponse{Operation: existing}
			return nil
		}
		var run *tgsrlv1.JobRun
		if incoming.GetRunId() != "" {
			var found bool
			run, found = store.GetRun(incoming.GetJobId(), incoming.GetRunId())
			if !found {
				return status.Errorf(codes.NotFound, "run %q not found", incoming.GetRunId())
			}
			run.Operations = appendOperation(run.GetOperations(), incoming)
			store.PutRun(run)
		}
		store.PutOperation(incoming)
		store.AppendEvent(c.newEvent(tgsrlv1.JobEventType_JOB_EVENT_TYPE_OPERATION_RECORDED, job, run, incoming, nil, "operation recorded", now))
		response = &tgsrlv1.RecordOperationResponse{Operation: cloneOperation(incoming)}
		return nil
	})
	return response, err
}
