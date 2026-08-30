// Package state owns the scheduler's versioned resource snapshot and intent
// cache. Store serializes writes, while every read receives an independent
// protobuf clone that callers may safely mutate.
package state

import (
	"errors"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrRevisionConflict     = errors.New("state: snapshot revision conflict")
	ErrRevisionExhausted    = errors.New("state: snapshot revision exhausted")
	ErrInvalidIntent        = errors.New("state: invalid scheduling intent")
	ErrIntentExpired        = errors.New("state: scheduling intent expired")
	ErrStaleIntent          = errors.New("state: stale scheduling intent")
	ErrIntentConflict       = errors.New("state: scheduling intent version conflict")
	ErrIdempotencyConflict  = errors.New("state: idempotency key already used")
	ErrIntentNotFound       = errors.New("state: valid scheduling intent not found")
	ErrPlanRevisionConflict = errors.New("state: plan snapshot revision conflict")
	ErrPlanIntentConflict   = errors.New("state: plan intent version conflict")
	ErrPlanAlreadyReserved  = errors.New("state: plan already reserved")
	ErrPlanNotReserved      = errors.New("state: plan is not reserved")
	ErrPendingUnitNotFound  = errors.New("state: pending unit not found")
	ErrInsufficientResource = errors.New("state: insufficient allocatable resources")
	ErrTransactionNotFound  = errors.New("state: transaction not found")
	ErrTransactionConflict  = errors.New("state: transaction conflict")
	ErrTransactionImmutable = errors.New("state: transaction is immutable")
	ErrTransactionLocked    = errors.New("state: transaction lock conflict")
)

// Clock makes expiry checks and committed timestamps deterministic in tests
// and replay. Implementations must be safe for concurrent use.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

// Now implements Clock.
func (f ClockFunc) Now() time.Time { return f() }

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// Option configures a Store.
type Option func(*Store) error

// WithClock injects the clock used for expiry checks and state timestamps.
func WithClock(clock Clock) Option {
	return func(store *Store) error {
		if clock == nil {
			return errors.New("state: nil clock")
		}
		store.clock = clock
		return nil
	}
}

type intentKey struct {
	executionID string
	stageID     string
}

type intentIdentity struct {
	key     intentKey
	version uint64
	intent  *tgsrlv1.SchedulingIntent
}

type planReservation struct {
	plan                  *tgsrlv1.PlacementPlan
	beforeSnapshot        *tgsrlv1.ClusterSnapshot
	deviceAllocatable     map[string]*tgsrlv1.ResourceVector
	pendingUnits          []*tgsrlv1.PendingUnit
	allocationIDs         []string
	finalized             bool
	succeeded             bool
	retainedAllocationIDs []string
}

// ReservationRecord is a clone-safe persistence view of one plan reservation.
type ReservationRecord struct {
	Plan                  *tgsrlv1.PlacementPlan
	BeforeSnapshot        *tgsrlv1.ClusterSnapshot
	DeviceAllocatable     map[string]*tgsrlv1.ResourceVector
	PendingUnits          []*tgsrlv1.PendingUnit
	AllocationIDs         []string
	Finalized             bool
	Succeeded             bool
	RetainedAllocationIDs []string
}

// ProviderResourceCursor is the durable fence for one provider resource
// stream. Revision is authoritative when non-zero; EventID deduplicates an
// exact replay at the accepted revision.
type ProviderResourceCursor struct {
	Revision uint64
	EventID  string
}

// ProviderSandboxCursor is the durable fence for one sandbox event stream.
// ProviderRevision orders current events, while OccurredAt is the compatibility
// fence for legacy events whose provider_revision is zero. EventID and
// IdempotencyKey independently identify retries.
type ProviderSandboxCursor struct {
	Generation          uint64
	ProviderRevision    uint64
	EventID             string
	IdempotencyKey      string
	OccurredAt          *timestamppb.Timestamp
	SeenEventIDs        []string
	SeenIdempotencyKeys []string
}

// DurableState is a complete persistence view of Store-owned authority.
type DurableState struct {
	Snapshot           *tgsrlv1.ClusterSnapshot
	Intents            []*tgsrlv1.SchedulingIntent
	Reservations       []ReservationRecord
	Transactions       []TransactionRecord
	ProjectedSandboxes []*tgsrlv1.Sandbox
	ResourceCursors    map[string]ProviderResourceCursor
	SandboxCursors     map[string]ProviderSandboxCursor
}

// Store is an in-memory, single-writer-equivalent state machine. Mutations are
// committed atomically under mu; readers never observe the committed pointer.
type Store struct {
	mu sync.RWMutex

	clock Clock

	snapshot           *tgsrlv1.ClusterSnapshot
	intents            map[intentKey]*tgsrlv1.SchedulingIntent
	providerProjection providerProjectionState
	// idempotencyKeys is deliberately not pruned when an intent expires. A key
	// identifies one immutable publish operation for the Store's lifetime.
	idempotencyKeys map[string]intentIdentity
	reservations    map[string]*planReservation
	transactions    map[string]*TransactionRecord

	// revisionChanged is closed after each committed revision and immediately
	// replaced. Snapshot waiters copy the channel while holding mu.
	revisionChanged chan struct{}
}

type storeDurableState struct {
	snapshot           *tgsrlv1.ClusterSnapshot
	intents            map[intentKey]*tgsrlv1.SchedulingIntent
	providerProjection providerProjectionState
	idempotencyKeys    map[string]intentIdentity
	reservations       map[string]*planReservation
	transactions       map[string]*TransactionRecord
}

// NewStore creates a Store from an optional initial snapshot. The input is
// cloned and can be mutated by its owner after this function returns.
func NewStore(initial *tgsrlv1.ClusterSnapshot, options ...Option) (*Store, error) {
	store := &Store{
		clock:              wallClock{},
		intents:            make(map[intentKey]*tgsrlv1.SchedulingIntent),
		providerProjection: newProviderProjectionState(),
		idempotencyKeys:    make(map[string]intentIdentity),
		reservations:       make(map[string]*planReservation),
		transactions:       make(map[string]*TransactionRecord),
		revisionChanged:    make(chan struct{}),
	}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("state: nil Store option")
		}
		if err := option(store); err != nil {
			return nil, err
		}
	}

	if initial == nil {
		store.snapshot = &tgsrlv1.ClusterSnapshot{
			SnapshotId: "snapshot-0",
			ObservedAt: timestamppb.New(store.clock.Now()),
		}
	} else {
		store.snapshot = cloneSnapshot(initial)
	}
	return store, nil
}
