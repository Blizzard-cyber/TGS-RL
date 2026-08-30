package state

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemoryRepositoryUpdateRollsBackFailedCallback(t *testing.T) {
	t.Parallel()

	repository, err := NewMemoryRepository()
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	wantErr := errors.New("reject update")
	err = repository.Update(func(store Store) error {
		store.PutJob(&tgsrlv1.RLTrainingJob{JobId: "job-rolled-back"})
		store.AppendEvent(&tgsrlv1.JobEvent{EventId: "event-rolled-back", JobId: "job-rolled-back"})
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Update() error = %v, want %v", err, wantErr)
	}
	if err := repository.View(func(query Query) error {
		if job, ok := query.GetJob("job-rolled-back"); ok || job != nil {
			t.Fatalf("GetJob() = %+v, %v, want nil, false", job, ok)
		}
		events, _, listErr := query.ListEvents("job-rolled-back", "", "", 0, 10, "")
		if listErr != nil {
			return listErr
		}
		if len(events) != 0 {
			t.Fatalf("ListEvents() = %+v, want empty", events)
		}
		return nil
	}); err != nil {
		t.Fatalf("View() error = %v", err)
	}
}

func TestAtomicWriteGobWritesCommittedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "nested", snapshotFileName)
	value := &persistedState{
		Jobs: map[string]*tgsrlv1.RLTrainingJob{
			"job-1": {
				JobId:       "job-1",
				DisplayName: "persisted",
			},
		},
		NextSequence: 7,
	}

	if err := atomicWriteGob(path, value, 0o640); err != nil {
		t.Fatalf("atomicWriteGob() error = %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("mode = %o, want 640", info.Mode().Perm())
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != snapshotFileName {
		t.Fatalf("dir entries = %+v, want only %q", entries, snapshotFileName)
	}

	recovered, err := readSnapshotFile(path)
	if err != nil {
		t.Fatalf("readSnapshotFile() error = %v", err)
	}
	if recovered.Jobs["job-1"].GetDisplayName() != "persisted" {
		t.Fatalf("recovered display_name = %q, want persisted", recovered.Jobs["job-1"].GetDisplayName())
	}
	if recovered.NextSequence != 7 {
		t.Fatalf("recovered next_sequence = %d, want 7", recovered.NextSequence)
	}
}

func TestFileRepositoryPersistsAndRecovers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	now := time.Date(2026, 8, 27, 13, 0, 0, 0, time.UTC)
	repository, err := NewFileRepository(dir, WithClock(ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewFileRepository() error = %v", err)
	}

	job := &tgsrlv1.RLTrainingJob{
		JobId:       "job-1",
		DisplayName: "persisted",
		CreatedAt:   timestamppb.New(now),
		DataKind:    tgsrlv1.DataKind_DATA_KIND_SYNTHETIC,
	}
	run := &tgsrlv1.JobRun{
		RunId:     "run-1",
		JobId:     "job-1",
		CreatedAt: timestamppb.New(now),
		RunState:  tgsrlv1.JobRunState_JOB_RUN_STATE_WAITING,
	}
	operation := &tgsrlv1.Operation{
		OperationId: "op-1",
		JobId:       "job-1",
		RunId:       "run-1",
		CreatedAt:   timestamppb.New(now),
		CompletedAt: timestamppb.New(now),
	}
	event := &tgsrlv1.JobEvent{
		EventId:    "event-1",
		JobId:      "job-1",
		RunId:      "run-1",
		OccurredAt: timestamppb.New(now),
	}

	if err := repository.Update(func(store Store) error {
		store.PutJob(job)
		store.PutRun(run)
		store.PutOperation(operation)
		store.PutIdempotency("command", "idem-1", &IdempotencyRecord{
			Scope:       "command",
			Key:         "idem-1",
			RequestHash: "hash",
			OperationID: "op-1",
			JobID:       "job-1",
			RunID:       "run-1",
		})
		store.AppendEvent(event)
		return nil
	}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	recovered, err := NewFileRepository(dir, WithClock(ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewFileRepository(recover) error = %v", err)
	}

	if err := recovered.View(func(query Query) error {
		gotJob, ok := query.GetJob("job-1")
		if !ok || gotJob.GetDisplayName() != "persisted" {
			t.Fatalf("GetJob() = %+v, %v", gotJob, ok)
		}
		gotRun, ok := query.GetRun("job-1", "run-1")
		if !ok || gotRun.GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_WAITING {
			t.Fatalf("GetRun() = %+v, %v", gotRun, ok)
		}
		gotOperation, ok := query.GetOperation("op-1")
		if !ok || gotOperation.GetOperationId() != "op-1" {
			t.Fatalf("GetOperation() = %+v, %v", gotOperation, ok)
		}
		events, _, err := query.ListEvents("job-1", "", "", 0, 10, "")
		if err != nil {
			t.Fatalf("ListEvents() error = %v", err)
		}
		if len(events) != 1 || events[0].GetEventId() != "event-1" {
			t.Fatalf("ListEvents() = %+v", events)
		}
		record, ok := query.GetIdempotency("command", "idem-1")
		if !ok || record.OperationID != "op-1" {
			t.Fatalf("GetIdempotency() = %+v, %v", record, ok)
		}
		return nil
	}); err != nil {
		t.Fatalf("View() error = %v", err)
	}
}

func TestMemoryRepositoryListEventsConsumesPageToken(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 13, 5, 0, 0, time.UTC)
	repository, err := NewMemoryRepository(WithClock(ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	if err := repository.Update(func(store Store) error {
		for index := 0; index < 3; index++ {
			store.AppendEvent(&tgsrlv1.JobEvent{
				EventId:    "event-" + string(rune('1'+index)),
				JobId:      "job-1",
				RunId:      "run-1",
				OccurredAt: timestamppb.New(now.Add(time.Duration(index) * time.Second)),
			})
		}
		return nil
	}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	if err := repository.View(func(query Query) error {
		first, next, err := query.ListEvents("job-1", "", "", 0, 1, "")
		if err != nil {
			t.Fatalf("ListEvents(first) error = %v", err)
		}
		if len(first) != 1 || next == "" {
			t.Fatalf("ListEvents(first) = len %d next %q, want len 1 and next token", len(first), next)
		}
		second, next2, err := query.ListEvents("job-1", "", "", 0, 1, next)
		if err != nil {
			t.Fatalf("ListEvents(second) error = %v", err)
		}
		if len(second) != 1 {
			t.Fatalf("ListEvents(second) len = %d, want 1", len(second))
		}
		if first[0].GetEventId() == second[0].GetEventId() {
			t.Fatalf("page token did not advance, both pages returned %q", first[0].GetEventId())
		}
		if next2 == "" {
			t.Fatal("ListEvents(second) next token = empty, want third page token")
		}
		return nil
	}); err != nil {
		t.Fatalf("View() error = %v", err)
	}
}

func TestMemoryRepositoryLatestOperationUsesCreationOrder(t *testing.T) {
	t.Parallel()

	repository, err := NewMemoryRepository()
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	earlier := time.Date(2026, 8, 27, 15, 0, 0, 0, time.UTC)
	later := earlier.Add(time.Minute)
	if err := repository.Update(func(store Store) error {
		store.PutOperation(&tgsrlv1.Operation{
			OperationId: "completed-old", JobId: "job-1", RunId: "run-1",
			Type: tgsrlv1.OperationType_OPERATION_TYPE_START, CreatedAt: timestamppb.New(earlier),
			CompletedAt: timestamppb.New(later.Add(time.Hour)), State: tgsrlv1.OperationState_OPERATION_STATE_SUCCEEDED,
		})
		store.PutOperation(&tgsrlv1.Operation{
			OperationId: "running-new", JobId: "job-1", RunId: "run-1",
			Type: tgsrlv1.OperationType_OPERATION_TYPE_START, CreatedAt: timestamppb.New(later),
			State: tgsrlv1.OperationState_OPERATION_STATE_RUNNING,
		})
		return nil
	}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if err := repository.View(func(query Query) error {
		latest, ok := query.GetLatestOperation("job-1", "run-1", tgsrlv1.OperationType_OPERATION_TYPE_START)
		if !ok || latest.GetOperationId() != "running-new" {
			t.Fatalf("GetLatestOperation() = %+v, %v", latest, ok)
		}
		return nil
	}); err != nil {
		t.Fatalf("View() error = %v", err)
	}
}
