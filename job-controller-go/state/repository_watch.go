package state

import (
	"fmt"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// Watch streams matching events appended after afterSequence or afterEventID.
func (r *MemoryRepository) Watch(jobID, runID string, afterSequence uint64, afterEventID string) (<-chan *tgsrlv1.JobEvent, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	sequenceFloor, err := r.sequenceFloorLocked(afterSequence, afterEventID)
	if err != nil {
		return nil, nil, err
	}
	backlog := r.filteredEventsLocked(jobID, runID, sequenceFloor)
	r.nextWatcherID++
	watcherID := r.nextWatcherID
	bufferSize := len(backlog) + watchBufferSlack
	if bufferSize < watchBufferSlack {
		bufferSize = watchBufferSlack
	}
	ch := make(chan *tgsrlv1.JobEvent, bufferSize)
	termination := make(chan WatchTermination, 1)
	for _, entry := range backlog {
		ch <- cloneEvent(entry.event)
	}
	r.watchers[watcherID] = &watcher{
		id:          watcherID,
		jobID:       jobID,
		runID:       runID,
		ch:          ch,
		termination: termination,
	}
	registerWatchResult(ch, termination)
	cancel := func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		clearWatchResult(ch)
		if existing, ok := r.watchers[watcherID]; ok {
			delete(r.watchers, watcherID)
			close(existing.ch)
			close(existing.termination)
		}
	}
	return ch, cancel, nil
}

// deliverLocked must be called with r.mu held. Its send is non-blocking so a
// slow watcher never holds up repository writers.
func (r *MemoryRepository) deliverLocked(item delivery) {
	subscriber, ok := r.watchers[item.watcherID]
	if !ok {
		return
	}
	select {
	case subscriber.ch <- cloneEvent(item.event):
	default:
		// Publish the typed cause before close so callers can recover from their
		// last cursor without interpreting channel closure or blocking writers.
		subscriber.termination <- WatchTermination{Reason: WatchTerminationConsumerLagged}
		close(subscriber.termination)
		delete(r.watchers, item.watcherID)
		close(subscriber.ch)
	}
}

func (r *MemoryRepository) sequenceFloorLocked(afterSequence uint64, afterEventID string) (uint64, error) {
	sequenceFloor := afterSequence
	if afterEventID == "" {
		return sequenceFloor, nil
	}
	for _, entry := range r.events {
		if entry.event.GetEventId() == afterEventID {
			if entry.sequence > sequenceFloor {
				sequenceFloor = entry.sequence
			}
			return sequenceFloor, nil
		}
	}
	return 0, fmt.Errorf("state: event %q not found", afterEventID)
}

func (r *MemoryRepository) filteredEventsLocked(jobID, runID string, afterSequence uint64) []*eventEntry {
	filtered := make([]*eventEntry, 0, len(r.events))
	for _, entry := range r.events {
		if entry.sequence <= afterSequence {
			continue
		}
		if !matchesTarget(entry.event, jobID, runID) {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func matchesTarget(event *tgsrlv1.JobEvent, jobID, runID string) bool {
	if jobID != "" && event.GetJobId() != jobID {
		return false
	}
	if runID != "" && event.GetRunId() != runID {
		return false
	}
	return true
}
