package state

import (
	"errors"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

const (
	defaultPageLimit = 100
	watchBufferSlack = 32
)

// WatchTerminationReason identifies why a watch stopped asynchronously.
type WatchTerminationReason string

const (
	// WatchTerminationConsumerLagged requires reconnecting from the last event
	// cursor successfully received by the caller.
	WatchTerminationConsumerLagged WatchTerminationReason = "consumer_lagged"
)

// WatchTermination describes an asynchronous watch termination.
type WatchTermination struct {
	Reason WatchTerminationReason
}

// WatchResult separates persisted events from typed termination causes.
type WatchResult struct {
	Events      <-chan *tgsrlv1.JobEvent
	Termination <-chan WatchTermination
}

var watchResults sync.Map

// WatchResultFor returns the typed result channels associated with events.
func WatchResultFor(events <-chan *tgsrlv1.JobEvent) (WatchResult, bool) {
	value, ok := watchResults.Load(events)
	if !ok {
		return WatchResult{}, false
	}
	termination, ok := value.(<-chan WatchTermination)
	if !ok {
		return WatchResult{}, false
	}
	return WatchResult{Events: events, Termination: termination}, true
}

func registerWatchResult(events <-chan *tgsrlv1.JobEvent, termination <-chan WatchTermination) {
	watchResults.Store(events, termination)
}

func clearWatchResult(events <-chan *tgsrlv1.JobEvent) {
	watchResults.Delete(events)
}

// Clock makes repository timestamps deterministic in tests.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

// Now implements Clock.
func (f ClockFunc) Now() time.Time { return f() }

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// Query exposes immutable repository reads.
type Query interface {
	GetJob(jobID string) (*tgsrlv1.RLTrainingJob, bool)
	ListJobs(kind tgsrlv1.DataKind, limit uint64, pageToken string) ([]*tgsrlv1.RLTrainingJob, string, error)
	GetRun(jobID, runID string) (*tgsrlv1.JobRun, bool)
	ListRuns(jobID string, limit uint64, pageToken string) ([]*tgsrlv1.JobRun, string, error)
	GetLatestRun(jobID string) (*tgsrlv1.JobRun, bool)
	GetOperation(operationID string) (*tgsrlv1.Operation, bool)
	ListOperations(jobID, runID string, opType tgsrlv1.OperationType, opState tgsrlv1.OperationState, limit uint64, pageToken string) ([]*tgsrlv1.Operation, string, error)
	GetLatestOperation(jobID, runID string, opType tgsrlv1.OperationType) (*tgsrlv1.Operation, bool)
	GetIdempotency(scope, key string) (*IdempotencyRecord, bool)
	ListEvents(jobID, runID, afterEventID string, afterSequence, limit uint64, pageToken string) ([]*tgsrlv1.JobEvent, string, error)
}

// Store extends Query with atomic mutation methods.
type Store interface {
	Query
	PutJob(job *tgsrlv1.RLTrainingJob)
	PutRun(run *tgsrlv1.JobRun)
	PutOperation(operation *tgsrlv1.Operation)
	PutIdempotency(scope, key string, record *IdempotencyRecord)
	AppendEvent(event *tgsrlv1.JobEvent) uint64
}

// Repository provides serialized read/write access and event watches.
type Repository interface {
	View(func(Query) error) error
	Update(func(Store) error) error
	// Watch closes the channel if the consumer falls behind. Callers can use
	// WatchResultFor to distinguish overflow from normal cancellation and
	// reconnect from the last received cursor without gaps.
	Watch(jobID, runID string, afterSequence uint64, afterEventID string) (<-chan *tgsrlv1.JobEvent, func(), error)
}

// IdempotencyRecord stores the immutable result of a prior command.
type IdempotencyRecord struct {
	Scope       string
	Key         string
	RequestHash string
	OperationID string
	JobID       string
	RunID       string
}

type eventEntry struct {
	sequence uint64
	event    *tgsrlv1.JobEvent
}

type watcher struct {
	id          uint64
	jobID       string
	runID       string
	ch          chan *tgsrlv1.JobEvent
	termination chan WatchTermination
}

type delivery struct {
	watcherID uint64
	event     *tgsrlv1.JobEvent
}

// MemoryRepository is an in-memory implementation of Repository.
type MemoryRepository struct {
	mu sync.RWMutex

	clock Clock

	jobs        map[string]*tgsrlv1.RLTrainingJob
	runs        map[string]map[string]*tgsrlv1.JobRun
	operations  map[string]*tgsrlv1.Operation
	idempotency map[string]*IdempotencyRecord
	events      []*eventEntry
	watchers    map[uint64]*watcher

	nextSequence  uint64
	nextWatcherID uint64
}

// Option configures a MemoryRepository.
type Option func(*MemoryRepository) error

// WithClock injects a deterministic clock.
func WithClock(clock Clock) Option {
	return func(repository *MemoryRepository) error {
		if clock == nil {
			return errors.New("state: nil clock")
		}
		repository.clock = clock
		return nil
	}
}

// NewMemoryRepository constructs an empty repository.
func NewMemoryRepository(options ...Option) (*MemoryRepository, error) {
	repository := &MemoryRepository{
		clock:       wallClock{},
		jobs:        make(map[string]*tgsrlv1.RLTrainingJob),
		runs:        make(map[string]map[string]*tgsrlv1.JobRun),
		operations:  make(map[string]*tgsrlv1.Operation),
		idempotency: make(map[string]*IdempotencyRecord),
		watchers:    make(map[uint64]*watcher),
	}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("state: nil repository option")
		}
		if err := option(repository); err != nil {
			return nil, err
		}
	}
	return repository, nil
}

// View runs fn against an immutable snapshot.
func (r *MemoryRepository) View(fn func(Query) error) error {
	if fn == nil {
		return errors.New("state: nil view callback")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return fn(memoryQuery{repository: r})
}

// Update runs fn atomically and publishes resulting event notifications.
func (r *MemoryRepository) Update(fn func(Store) error) error {
	if fn == nil {
		return errors.New("state: nil update callback")
	}
	r.mu.Lock()
	store := &memoryStore{memoryQuery: memoryQuery{repository: r}}
	err := fn(store)
	if err == nil {
		for _, item := range store.deliveries {
			r.deliverLocked(item)
		}
	}
	r.mu.Unlock()
	return err
}
