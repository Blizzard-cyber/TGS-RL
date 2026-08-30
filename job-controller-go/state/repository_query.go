package state

import (
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

type memoryQuery struct {
	repository *MemoryRepository
}

func (q memoryQuery) GetJob(jobID string) (*tgsrlv1.RLTrainingJob, bool) {
	job, ok := q.repository.jobs[jobID]
	if !ok {
		return nil, false
	}
	return cloneJob(job), true
}

func (q memoryQuery) ListJobs(kind tgsrlv1.DataKind, limit uint64, pageToken, afterJobID string) ([]*tgsrlv1.RLTrainingJob, string, error) {
	jobs := make([]*tgsrlv1.RLTrainingJob, 0, len(q.repository.jobs))
	for _, job := range q.repository.jobs {
		if kind != tgsrlv1.DataKind_DATA_KIND_UNKNOWN && job.GetDataKind() != kind {
			continue
		}
		jobs = append(jobs, cloneJob(job))
	}
	sort.SliceStable(jobs, func(i, j int) bool {
		left := jobs[i].GetCreatedAt().AsTime()
		right := jobs[j].GetCreatedAt().AsTime()
		if !left.Equal(right) {
			return left.Before(right)
		}
		return jobs[i].GetJobId() < jobs[j].GetJobId()
	})
	start, err := decodePageToken(pageToken)
	if err != nil {
		return nil, "", err
	}
	if pageToken == "" {
		start = offsetAfter(jobs, afterJobID, func(job *tgsrlv1.RLTrainingJob) string { return job.GetJobId() })
	}
	items, next := paginate(jobs, start, normalizeLimit(limit))
	return items, next, nil
}

func (q memoryQuery) GetRun(jobID, runID string) (*tgsrlv1.JobRun, bool) {
	jobRuns, ok := q.repository.runs[jobID]
	if !ok {
		return nil, false
	}
	run, ok := jobRuns[runID]
	if !ok {
		return nil, false
	}
	return cloneRun(run), true
}

func (q memoryQuery) ListRuns(jobID string, limit uint64, pageToken, afterRunID string) ([]*tgsrlv1.JobRun, string, error) {
	jobRuns := q.repository.runs[jobID]
	runs := make([]*tgsrlv1.JobRun, 0, len(jobRuns))
	for _, run := range jobRuns {
		runs = append(runs, cloneRun(run))
	}
	sort.SliceStable(runs, func(i, j int) bool {
		if runs[i].GetAttempt() != runs[j].GetAttempt() {
			return runs[i].GetAttempt() > runs[j].GetAttempt()
		}
		return runs[i].GetRunId() > runs[j].GetRunId()
	})
	start, err := decodePageToken(pageToken)
	if err != nil {
		return nil, "", err
	}
	if pageToken == "" {
		start = offsetAfter(runs, afterRunID, func(run *tgsrlv1.JobRun) string { return run.GetRunId() })
	}
	items, next := paginate(runs, start, normalizeLimit(limit))
	return items, next, nil
}

func (q memoryQuery) GetLatestRun(jobID string) (*tgsrlv1.JobRun, bool) {
	jobRuns := q.repository.runs[jobID]
	if len(jobRuns) == 0 {
		return nil, false
	}
	var latest *tgsrlv1.JobRun
	for _, run := range jobRuns {
		if latest == nil || run.GetAttempt() > latest.GetAttempt() || (run.GetAttempt() == latest.GetAttempt() && run.GetRunId() > latest.GetRunId()) {
			latest = run
		}
	}
	return cloneRun(latest), true
}

func (q memoryQuery) GetOperation(operationID string) (*tgsrlv1.Operation, bool) {
	operation, ok := q.repository.operations[operationID]
	if !ok {
		return nil, false
	}
	return cloneOperation(operation), true
}

func (q memoryQuery) ListOperations(jobID, runID string, opType tgsrlv1.OperationType, opState tgsrlv1.OperationState, limit uint64, pageToken string) ([]*tgsrlv1.Operation, string, error) {
	operations := make([]*tgsrlv1.Operation, 0, len(q.repository.operations))
	for _, operation := range q.repository.operations {
		if jobID != "" && operation.GetJobId() != jobID {
			continue
		}
		if runID != "" && operation.GetRunId() != runID {
			continue
		}
		if opType != tgsrlv1.OperationType_OPERATION_TYPE_UNKNOWN && operation.GetType() != opType {
			continue
		}
		if opState != tgsrlv1.OperationState_OPERATION_STATE_UNKNOWN && operation.GetState() != opState {
			continue
		}
		clone := cloneOperation(operation)
		if clone.Cursor == "" {
			clone.Cursor = clone.GetOperationId()
		}
		operations = append(operations, clone)
	}
	sort.SliceStable(operations, func(i, j int) bool {
		left := operations[i].GetCreatedAt().AsTime()
		right := operations[j].GetCreatedAt().AsTime()
		if !left.Equal(right) {
			return left.Before(right)
		}
		return operations[i].GetOperationId() < operations[j].GetOperationId()
	})
	start, err := decodePageToken(pageToken)
	if err != nil {
		return nil, "", err
	}
	items, next := paginate(operations, start, normalizeLimit(limit))
	return items, next, nil
}

func (q memoryQuery) GetLatestOperation(jobID, runID string, opType tgsrlv1.OperationType) (*tgsrlv1.Operation, bool) {
	var latest *tgsrlv1.Operation
	for _, operation := range q.repository.operations {
		if operation.GetType() != opType || operation.GetJobId() != jobID || operation.GetRunId() != runID {
			continue
		}
		if latest == nil {
			latest = operation
			continue
		}
		currentCreated := operation.GetCreatedAt().AsTime()
		latestCreated := latest.GetCreatedAt().AsTime()
		if currentCreated.After(latestCreated) || (currentCreated.Equal(latestCreated) && operation.GetOperationId() > latest.GetOperationId()) {
			latest = operation
		}
	}
	if latest == nil {
		return nil, false
	}
	return cloneOperation(latest), true
}

func (q memoryQuery) GetIdempotency(scope, key string) (*IdempotencyRecord, bool) {
	record, ok := q.repository.idempotency[idempotencyMapKey(scope, key)]
	if !ok {
		return nil, false
	}
	return cloneIdempotency(record), true
}

func (q memoryQuery) ListEvents(jobID, runID, afterEventID string, afterSequence, limit uint64, pageToken string) ([]*tgsrlv1.JobEvent, string, error) {
	sequenceFloor, err := q.repository.sequenceFloorLocked(afterSequence, afterEventID)
	if err != nil {
		return nil, "", err
	}
	filtered := q.repository.filteredEventsLocked(jobID, runID, sequenceFloor)
	events := make([]*tgsrlv1.JobEvent, len(filtered))
	for index, entry := range filtered {
		events[index] = cloneEvent(entry.event)
	}
	start, err := decodePageToken(pageToken)
	if err != nil {
		return nil, "", err
	}
	items, next := paginate(events, start, normalizeLimit(limit))
	return items, next, nil
}
