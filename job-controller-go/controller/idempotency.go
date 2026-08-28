package controller

import (
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *Controller) resolveTargetRun(store state.Store, jobID, runID string) (*tgsrlv1.RLTrainingJob, *tgsrlv1.JobRun, error) {
	job, ok := store.GetJob(jobID)
	if !ok {
		return nil, nil, status.Errorf(codes.NotFound, "job %q not found", jobID)
	}
	if runID != "" {
		run, ok := store.GetRun(jobID, runID)
		if !ok {
			return nil, nil, status.Errorf(codes.NotFound, "run %q not found", runID)
		}
		return job, run, nil
	}
	run, ok := store.GetLatestRun(jobID)
	if !ok {
		return job, nil, nil
	}
	return job, run, nil
}

func (c *Controller) semanticIdempotentOperation(store state.Store, job *tgsrlv1.RLTrainingJob, run *tgsrlv1.JobRun, opType tgsrlv1.OperationType) (*tgsrlv1.Operation, bool) {
	if operation, ok := store.GetLatestOperation(job.GetJobId(), run.GetRunId(), opType); ok {
		switch opType {
		case tgsrlv1.OperationType_OPERATION_TYPE_START:
			if run.GetRunState() == tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING {
				return operation, true
			}
		case tgsrlv1.OperationType_OPERATION_TYPE_PAUSE:
			if run.GetRunState() == tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSED {
				return operation, true
			}
		case tgsrlv1.OperationType_OPERATION_TYPE_RESUME:
			if run.GetRunState() == tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING {
				return operation, true
			}
		case tgsrlv1.OperationType_OPERATION_TYPE_STOP:
			if run.GetRunState() == tgsrlv1.JobRunState_JOB_RUN_STATE_STOPPED {
				return operation, true
			}
		case tgsrlv1.OperationType_OPERATION_TYPE_TERMINATE:
			if run.GetRunState() == tgsrlv1.JobRunState_JOB_RUN_STATE_TERMINATED {
				return operation, true
			}
		case tgsrlv1.OperationType_OPERATION_TYPE_RETRY:
			if latest, ok := store.GetLatestRun(job.GetJobId()); ok && latest.GetAttempt() > run.GetAttempt() {
				return operation, true
			}
		}
	}
	return nil, false
}

func (c *Controller) ensureOperation(store state.Store, opType tgsrlv1.OperationType, jobID, runID, requestID, idempotencyKey, reason string, kind tgsrlv1.DataKind, now time.Time) *tgsrlv1.Operation {
	if existing, ok := store.GetLatestOperation(jobID, runID, opType); ok {
		return existing
	}
	operation := c.newOperation(opType, jobID, runID, "", reason, requestID, idempotencyKey, kind, nil, now)
	store.PutOperation(operation)
	return operation
}

func loadJobAndLatestRun(query state.Query, jobID string) (*tgsrlv1.RLTrainingJob, *tgsrlv1.JobRun, error) {
	job, ok := query.GetJob(jobID)
	if !ok {
		return nil, nil, status.Errorf(codes.NotFound, "job %q not found", jobID)
	}
	run, _ := query.GetLatestRun(jobID)
	return job, run, nil
}

func (c *Controller) loadCreateJobIdempotency(key, requestHash string) (*tgsrlv1.CreateJobResponse, bool, error) {
	if key == "" {
		return nil, false, nil
	}
	var response *tgsrlv1.CreateJobResponse
	err := c.repository.View(func(query state.Query) error {
		record, ok := query.GetIdempotency("create-job", key)
		if !ok {
			return nil
		}
		if record.RequestHash != requestHash {
			return status.Errorf(codes.AlreadyExists, "idempotency key %q already used for a different create-job request", key)
		}
		job, _ := query.GetJob(record.JobID)
		operation, _ := query.GetOperation(record.OperationID)
		response = &tgsrlv1.CreateJobResponse{Job: job, Operation: operation}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return response, response != nil, nil
}

func (c *Controller) loadAdmitIdempotency(key, requestHash string) (*tgsrlv1.AdmitJobResponse, bool, error) {
	if key == "" {
		return nil, false, nil
	}
	var response *tgsrlv1.AdmitJobResponse
	err := c.repository.View(func(query state.Query) error {
		record, ok := query.GetIdempotency("admit", key)
		if !ok {
			return nil
		}
		if record.RequestHash != requestHash {
			return status.Errorf(codes.AlreadyExists, "idempotency key %q already used for a different admit request", key)
		}
		job, _ := query.GetJob(record.JobID)
		operation, _ := query.GetOperation(record.OperationID)
		response = &tgsrlv1.AdmitJobResponse{Job: job, Operation: operation}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return response, response != nil, nil
}

func (c *Controller) loadCreateRunIdempotency(key, requestHash string) (*tgsrlv1.CreateJobRunResponse, bool, error) {
	if key == "" {
		return nil, false, nil
	}
	var response *tgsrlv1.CreateJobRunResponse
	err := c.repository.View(func(query state.Query) error {
		record, ok := query.GetIdempotency("create-run", key)
		if !ok {
			return nil
		}
		if record.RequestHash != requestHash {
			return status.Errorf(codes.AlreadyExists, "idempotency key %q already used for a different create-run request", key)
		}
		run, _ := query.GetRun(record.JobID, record.RunID)
		response = &tgsrlv1.CreateJobRunResponse{Run: run}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return response, response != nil, nil
}

func (c *Controller) loadCommandIdempotency(key, requestHash string) (*tgsrlv1.ApplyJobCommandResponse, bool, error) {
	if key == "" {
		return nil, false, nil
	}
	var response *tgsrlv1.ApplyJobCommandResponse
	err := c.repository.View(func(query state.Query) error {
		record, ok := query.GetIdempotency("command", key)
		if !ok {
			return nil
		}
		if record.RequestHash != requestHash {
			return status.Errorf(codes.AlreadyExists, "idempotency key %q already used for a different command request", key)
		}
		operation, _ := query.GetOperation(record.OperationID)
		run, _ := query.GetRun(record.JobID, record.RunID)
		response = &tgsrlv1.ApplyJobCommandResponse{Operation: operation, Run: run}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return response, response != nil, nil
}

func (c *Controller) loadCommittedOperation(query state.Query, jobID, runID, operationID string) (*tgsrlv1.RLTrainingJob, *tgsrlv1.JobRun, *tgsrlv1.Operation, error) {
	job, ok := query.GetJob(jobID)
	if !ok {
		return nil, nil, nil, status.Errorf(codes.NotFound, "job %q not found", jobID)
	}
	run, ok := query.GetRun(jobID, runID)
	if !ok {
		return nil, nil, nil, status.Errorf(codes.NotFound, "run %q not found", runID)
	}
	operation, ok := query.GetOperation(operationID)
	if !ok {
		return nil, nil, nil, status.Errorf(codes.NotFound, "operation %q not found", operationID)
	}
	return job, run, operation, nil
}

func (c *Controller) runInflight(key string, fn func() (any, error)) (any, error) {
	c.inflightMu.Lock()
	if call, ok := c.inflight[key]; ok {
		c.inflightMu.Unlock()
		<-call.done
		return call.result, call.err
	}
	call := &inflightCall{done: make(chan struct{})}
	c.inflight[key] = call
	c.inflightMu.Unlock()

	call.result, call.err = fn()
	close(call.done)

	c.inflightMu.Lock()
	delete(c.inflight, key)
	c.inflightMu.Unlock()
	return call.result, call.err
}

func (c *Controller) waitInflight(key string) (error, bool) {
	c.inflightMu.Lock()
	call, ok := c.inflight[key]
	c.inflightMu.Unlock()
	if !ok {
		return nil, false
	}
	<-call.done
	return call.err, true
}
