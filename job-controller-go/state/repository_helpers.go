package state

import (
	"fmt"
	"strconv"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

func decodePageToken(pageToken string) (int, error) {
	if pageToken == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(pageToken)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("state: invalid page token %q", pageToken)
	}
	return value, nil
}

func normalizeLimit(limit uint64) int {
	if limit == 0 {
		return defaultPageLimit
	}
	if limit > uint64(defaultPageLimit) {
		return defaultPageLimit
	}
	return int(limit)
}

func paginate[T any](items []T, start, limit int) ([]T, string) {
	if start >= len(items) {
		return []T{}, ""
	}
	end := start + limit
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return items[start:end], next
}

func offsetAfter[T any](items []T, afterID string, id func(T) string) int {
	if afterID == "" {
		return 0
	}
	for index, item := range items {
		if id(item) == afterID {
			return index + 1
		}
	}
	return len(items)
}

func cloneJob(job *tgsrlv1.RLTrainingJob) *tgsrlv1.RLTrainingJob {
	if job == nil {
		return nil
	}
	return proto.Clone(job).(*tgsrlv1.RLTrainingJob)
}

func cloneRun(run *tgsrlv1.JobRun) *tgsrlv1.JobRun {
	if run == nil {
		return nil
	}
	return proto.Clone(run).(*tgsrlv1.JobRun)
}

func cloneOperation(operation *tgsrlv1.Operation) *tgsrlv1.Operation {
	if operation == nil {
		return nil
	}
	return proto.Clone(operation).(*tgsrlv1.Operation)
}

func cloneEvent(event *tgsrlv1.JobEvent) *tgsrlv1.JobEvent {
	if event == nil {
		return nil
	}
	return proto.Clone(event).(*tgsrlv1.JobEvent)
}

func cloneIdempotency(record *IdempotencyRecord) *IdempotencyRecord {
	if record == nil {
		return nil
	}
	copy := *record
	return &copy
}
