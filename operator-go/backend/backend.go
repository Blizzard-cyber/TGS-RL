package backend

import (
	"context"
	"errors"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/bundleadapter"
)

var (
	ErrGenerationConflict  = errors.New("generation conflict")
	ErrFingerprintDrift    = errors.New("fingerprint drift")
	ErrRuntimeNotFound     = errors.New("runtime backend target not found")
	ErrIdempotencyConflict = errors.New("idempotency key already used for another request")
	ErrInvalidControl      = errors.New("invalid backend control request")
	// ErrControlOutcomeAmbiguous means a lifecycle request may have reached the
	// execution substrate, but its result could not be proven by readback. The
	// same idempotency key may be retried; the backend will reconcile durable
	// per-target progress before issuing another mutation.
	ErrControlOutcomeAmbiguous = errors.New("backend control outcome is ambiguous")
)

type Backend interface {
	Apply(ctx context.Context, bundle *api.Bundle) (*ApplyResult, error)
	Get(ctx context.Context, key string) (*api.Bundle, bool, error)
	List(ctx context.Context) ([]*api.Bundle, error)
	Control(ctx context.Context, request ControlRequest) (*ControlResult, error)
}

// ControlRequest is the narrow, transport-independent lifecycle mutation
// accepted by an execution backend. Generation is fenced per concrete target.
type ControlRequest struct {
	Action         tgsrlv1.JobCommandType
	JobID          string
	RunID          string
	TraceID        string
	Targets        []ControlTarget
	RequestID      string
	IdempotencyKey string
	Reason         string
	Deadline       time.Time
}

type ControlTarget struct {
	RuntimeUnitID      string
	SandboxID          string
	ExpectedGeneration uint64
}

// ControlResult acknowledges a fenced backend mutation. Runtime persistence
// owns event sequencing, while ObservationManager owns all observed events.
type ControlResult struct {
	Accepted        bool
	Idempotent      bool
	BackendRevision uint64
	Detail          string
}

// ObservationSnapshot is the backend readback used by the decision observer.
// Fake mode obtains these values from the exact same backend state mutated by
// Control; Kubernetes mode obtains them from API-server reads.
type ObservationSnapshot struct {
	ObservedGeneration      uint64
	WorkloadAdmitted        bool
	ResourceClaimsAllocated bool
	JobActive               uint32
	JobSucceeded            uint32
	JobFailed               uint32
	JobPaused               bool
	JobDeleted              bool
	Reason                  string
	ObservedAt              time.Time
	ControlRequestID        string
	ControlIdempotencyKey   string
	ControlAction           tgsrlv1.JobCommandType
	ControlBackendRevision  uint64
	ControlCommitted        bool
}

type ApplyResult struct {
	Bundle     *api.Bundle
	Created    bool
	Updated    bool
	Idempotent bool
}

type FakeBackend struct {
	inner  *KubernetesBackend
	client *MemoryClient
}

func NewFake() *FakeBackend {
	client := NewMemoryClient()
	inner := &KubernetesBackend{client: client, adapter: bundleadapter.NewKubernetes(0), controls: make(map[string]controlRecord)}
	return &FakeBackend{inner: inner, client: client}
}

func (b *FakeBackend) Apply(ctx context.Context, bundle *api.Bundle) (*ApplyResult, error) {
	result, err := b.inner.Apply(ctx, bundle)
	if err != nil {
		return nil, err
	}
	if err := b.inner.RestoreControlMetadata(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (b *FakeBackend) Get(ctx context.Context, key string) (*api.Bundle, bool, error) {
	return b.inner.Get(ctx, key)
}

func (b *FakeBackend) List(ctx context.Context) ([]*api.Bundle, error) {
	return b.inner.List(ctx)
}

func (b *FakeBackend) Control(ctx context.Context, request ControlRequest) (*ControlResult, error) {
	return b.inner.Control(ctx, request)
}

func (b *FakeBackend) Snapshots(ctx context.Context, bundle *api.Bundle) ([]*ObservationSnapshot, error) {
	return b.inner.fakeSnapshots(ctx, bundle)
}

func (b *FakeBackend) Snapshot(ctx context.Context, bundle *api.Bundle) (*ObservationSnapshot, bool, error) {
	snapshots, err := b.inner.fakeSnapshots(ctx, bundle)
	if err != nil {
		return nil, false, err
	}
	if len(snapshots) == 0 {
		return nil, false, nil
	}
	snapshot := snapshots[len(snapshots)-1]
	return snapshot, snapshot.JobDeleted || snapshot.JobFailed > 0 || snapshot.JobSucceeded > 0, nil
}
