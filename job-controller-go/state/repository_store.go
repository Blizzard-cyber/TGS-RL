package state

import (
	"strconv"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

type memoryStore struct {
	memoryQuery
	deliveries []delivery
}

func (s *memoryStore) PutJob(job *tgsrlv1.RLTrainingJob) {
	s.repository.jobs[job.GetJobId()] = cloneJob(job)
}

func (s *memoryStore) PutRun(run *tgsrlv1.JobRun) {
	if s.repository.runs[run.GetJobId()] == nil {
		s.repository.runs[run.GetJobId()] = make(map[string]*tgsrlv1.JobRun)
	}
	s.repository.runs[run.GetJobId()][run.GetRunId()] = cloneRun(run)
}

func (s *memoryStore) PutOperation(operation *tgsrlv1.Operation) {
	operation = cloneOperation(operation)
	if operation.Cursor == "" {
		operation.Cursor = operation.GetOperationId()
	}
	s.repository.operations[operation.GetOperationId()] = operation
}

func (s *memoryStore) PutIdempotency(scope, key string, record *IdempotencyRecord) {
	s.repository.idempotency[idempotencyMapKey(scope, key)] = cloneIdempotency(record)
}

func (s *memoryStore) AppendEvent(event *tgsrlv1.JobEvent) uint64 {
	s.repository.nextSequence++
	sequence := s.repository.nextSequence
	event = cloneEvent(event)
	event.Sequence = sequence
	event.Cursor = strconv.FormatUint(sequence, 10)
	entry := &eventEntry{
		sequence: sequence,
		event:    cloneEvent(event),
	}
	s.repository.events = append(s.repository.events, entry)
	for watcherID, watcher := range s.repository.watchers {
		if !matchesTarget(event, watcher.jobID, watcher.runID) {
			continue
		}
		s.deliveries = append(s.deliveries, delivery{
			watcherID: watcherID,
			event:     cloneEvent(event),
		})
	}
	return sequence
}

func idempotencyMapKey(scope, key string) string {
	return scope + "\x00" + key
}
