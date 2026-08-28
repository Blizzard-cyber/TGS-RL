package state

import (
	"fmt"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func TestMemoryRepositoryWatchDeliversLiveEventsAndCancels(t *testing.T) {
	t.Parallel()

	repository, err := NewMemoryRepository()
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	events, cancel, err := repository.Watch("job-1", "", 0, "")
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	result, ok := WatchResultFor(events)
	if !ok {
		t.Fatal("WatchResultFor() did not return typed result")
	}

	appendWatchEvents(t, repository, "job-1", 2)
	for sequence := uint64(1); sequence <= 2; sequence++ {
		event := receiveWatchEvent(t, events)
		if event.GetSequence() != sequence {
			t.Fatalf("Watch() sequence = %d, want %d", event.GetSequence(), sequence)
		}
		if event.GetCursor() != fmt.Sprint(sequence) {
			t.Fatalf("Watch() cursor = %q, want %d", event.GetCursor(), sequence)
		}
	}

	cancel()
	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("Watch() channel remained open after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("Watch() channel did not close after cancel")
	}
	if termination, open := <-result.Termination; open {
		t.Fatalf("Watch() cancellation termination = %+v, want closed channel", termination)
	}
}

func TestMemoryRepositoryWatchOverflowClosesAndResumes(t *testing.T) {
	t.Parallel()

	repository, err := NewMemoryRepository()
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	testRepositoryWatchOverflowClosesAndResumes(t, repository)
}

func TestFileRepositoryWatchOverflowClosesAndResumes(t *testing.T) {
	t.Parallel()

	repository, err := NewFileRepository(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileRepository() error = %v", err)
	}
	testRepositoryWatchOverflowClosesAndResumes(t, repository)
}

func testRepositoryWatchOverflowClosesAndResumes(t *testing.T, repository Repository) {
	t.Helper()
	events, cancel, err := repository.Watch("job-1", "", 0, "")
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	result, ok := WatchResultFor(events)
	if !ok {
		t.Fatal("WatchResultFor() did not return typed result")
	}

	updateDone := make(chan error, 1)
	go func() {
		updateDone <- appendEvents(repository, "job-1", watchBufferSlack+1)
	}()
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("Update() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Update() blocked on a slow watch consumer")
	}

	received := make([]*tgsrlv1.JobEvent, 0, watchBufferSlack)
	for event := range events {
		received = append(received, event)
	}
	termination, open := <-result.Termination
	if !open || termination.Reason != WatchTerminationConsumerLagged {
		t.Fatalf("Watch() termination = %+v, %v, want %q, true", termination, open, WatchTerminationConsumerLagged)
	}
	if len(received) != watchBufferSlack {
		t.Fatalf("Watch() received %d events before overflow, want %d", len(received), watchBufferSlack)
	}
	for index, event := range received {
		want := uint64(index + 1)
		if event.GetSequence() != want {
			t.Fatalf("Watch() event[%d] sequence = %d, want %d", index, event.GetSequence(), want)
		}
	}

	lastSequence := received[len(received)-1].GetSequence()
	resumed, cancelResumed, err := repository.Watch("job-1", "", lastSequence, "")
	if err != nil {
		t.Fatalf("Watch(resume) error = %v", err)
	}
	defer cancelResumed()
	missed := receiveWatchEvent(t, resumed)
	if missed.GetSequence() != lastSequence+1 {
		t.Fatalf("Watch(resume) sequence = %d, want %d", missed.GetSequence(), lastSequence+1)
	}

	appendWatchEvents(t, repository, "job-1", 1)
	live := receiveWatchEvent(t, resumed)
	if live.GetSequence() != missed.GetSequence()+1 {
		t.Fatalf("Watch(resume live) sequence = %d, want %d", live.GetSequence(), missed.GetSequence()+1)
	}

	// An overflowed watch is already unregistered; cancellation remains safe.
	cancel()
}

func TestMemoryRepositoryWatchCancelConcurrentWithDelivery(t *testing.T) {
	t.Parallel()

	repository, err := NewMemoryRepository()
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	for iteration := 0; iteration < 100; iteration++ {
		events, cancel, err := repository.Watch("job-1", "", 0, "")
		if err != nil {
			t.Fatalf("Watch() error = %v", err)
		}
		start := make(chan struct{})
		done := make(chan error, 2)
		go func() {
			<-start
			cancel()
			done <- nil
		}()
		go func() {
			<-start
			done <- appendEvents(repository, "job-1", 1)
		}()
		close(start)
		for completed := 0; completed < 2; completed++ {
			if err := <-done; err != nil {
				t.Fatalf("Update() error = %v", err)
			}
		}
		for range events {
		}
	}
}

func appendWatchEvents(t *testing.T, repository Repository, jobID string, count int) {
	t.Helper()
	if err := appendEvents(repository, jobID, count); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
}

func appendEvents(repository Repository, jobID string, count int) error {
	return repository.Update(func(store Store) error {
		for index := 0; index < count; index++ {
			store.AppendEvent(&tgsrlv1.JobEvent{
				EventId: fmt.Sprintf("event-%d", index+1),
				JobId:   jobID,
			})
		}
		return nil
	})
}

func receiveWatchEvent(t *testing.T, events <-chan *tgsrlv1.JobEvent) *tgsrlv1.JobEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("Watch() channel closed before event")
		}
		return event
	case <-time.After(time.Second):
		t.Fatal("Watch() did not deliver event")
		return nil
	}
}
