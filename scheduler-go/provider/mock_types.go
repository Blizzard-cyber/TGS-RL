package provider

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// Stable provider errors. Callers should use errors.Is rather than comparing
// error strings.
var (
	ErrInvalidArgument     = errors.New("provider: invalid argument")
	ErrNotFound            = errors.New("provider: resource not found")
	ErrUnsupported         = errors.New("provider: capability unsupported")
	ErrFailedPrecondition  = errors.New("provider: failed precondition")
	ErrDeadlineExceeded    = errors.New("provider: action deadline exceeded")
	ErrPartialFailure      = errors.New("provider: plan partially failed")
	ErrGenerationFenced    = errors.New("provider: late generation fenced")
	ErrIdempotencyConflict = errors.New("provider: idempotency conflict")
	ErrCursorExpired       = errors.New("provider: watch cursor expired")
)

var actionNames = map[tgsrlv1.ActionType]string{
	tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE:    "set_share",
	tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY: "set_priority",
	tgsrlv1.ActionType_ACTION_TYPE_PAUSE:        "pause",
	tgsrlv1.ActionType_ACTION_TYPE_RESUME:       "resume",
	tgsrlv1.ActionType_ACTION_TYPE_SLEEP:        "sleep",
	tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:      "offload",
	tgsrlv1.ActionType_ACTION_TYPE_REBIND:       "rebind",
	tgsrlv1.ActionType_ACTION_TYPE_RECREATE:     "recreate",
	tgsrlv1.ActionType_ACTION_TYPE_BIND:         "bind",
	tgsrlv1.ActionType_ACTION_TYPE_RELEASE:      "release",
	tgsrlv1.ActionType_ACTION_TYPE_RESIZE:       "resize",
}

const (
	// ErrorCodeInvalidArgument identifies malformed provider input.
	ErrorCodeInvalidArgument = "INVALID_ARGUMENT"
	// ErrorCodeNotFound identifies a missing mock resource.
	ErrorCodeNotFound = "NOT_FOUND"
	// ErrorCodeUnsupported identifies an undeclared capability or action.
	ErrorCodeUnsupported = "CAPABILITY_UNSUPPORTED"
	// ErrorCodeFailedPrecondition identifies an invalid state transition.
	ErrorCodeFailedPrecondition = "FAILED_PRECONDITION"
	// ErrorCodeRevisionConflict identifies a stale snapshot fence.
	ErrorCodeRevisionConflict = "REVISION_CONFLICT"
	// ErrorCodeGenerationConflict identifies a stale action generation.
	ErrorCodeGenerationConflict = "GENERATION_CONFLICT"
	// ErrorCodeDeadlineExceeded identifies a deadline reached before mutation.
	ErrorCodeDeadlineExceeded = "ACTION_DEADLINE_EXCEEDED"
	// ErrorCodeInjectedFailure identifies a configured mock failure.
	ErrorCodeInjectedFailure = "INJECTED_FAILURE"
	// ErrorCodeIdempotencyConflict identifies key reuse with another payload.
	ErrorCodeIdempotencyConflict = "IDEMPOTENCY_CONFLICT"
	// ErrorCodeLateEventFenced identifies an ignored old-generation event.
	ErrorCodeLateEventFenced = "LATE_EVENT_FENCED"
)

// Error is a structured provider error with stable machine-readable context.
type Error struct {
	Code               string
	Message            string
	PlanID             string
	ActionID           string
	SandboxID          string
	ExpectedRevision   uint64
	ObservedRevision   uint64
	ExpectedGeneration uint64
	ObservedGeneration uint64
	Cause              error
}

// Error implements error.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" {
		return fmt.Sprintf("provider: %s", e.Code)
	}
	return fmt.Sprintf("provider: %s: %s", e.Code, e.Message)
}

// Unwrap returns the stable error category.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// SandboxState is provider-local because protocol v0.3 has no Sandbox DTO.
type SandboxState string

const (
	SandboxStateRequested  SandboxState = "requested"
	SandboxStateBound      SandboxState = "bound"
	SandboxStateRunning    SandboxState = "running"
	SandboxStatePaused     SandboxState = "paused"
	SandboxStateSleeping   SandboxState = "sleeping"
	SandboxStateFailed     SandboxState = "failed"
	SandboxStateTerminated SandboxState = "terminated"
)

// Sandbox is an immutable view of mock runtime state.
type Sandbox struct {
	SandboxID  string
	State      SandboxState
	Generation uint64
	Binding    *tgsrlv1.Binding
	Share      float64
	Priority   int32
	SafePoint  bool
	Offloaded  bool
	UpdatedAt  time.Time
}

// SandboxEvent is an observed runtime update. Generation fences make old
// events harmless after an L4 replacement.
type SandboxEvent struct {
	// EventID is required for exact retry deduplication. If omitted, an equal
	// same-generation event is still treated as a no-op.
	EventID    string
	SandboxID  string
	Generation uint64
	State      SandboxState
	Binding    *tgsrlv1.Binding
	SafePoint  *bool
}

// ResourceEvent is a cloned provider state notification.
type ResourceEvent struct {
	Revision uint64
	Sandbox  Sandbox
}

// ResourceProvider is the vendor-neutral scheduler resource boundary.
// Implementations must clone protobuf messages at every ownership boundary.
type ResourceProvider interface {
	Capabilities(context.Context) (*tgsrlv1.CapabilitySet, error)
	Snapshot(context.Context) (*tgsrlv1.ClusterSnapshot, error)
	ListDevices(context.Context) ([]*tgsrlv1.Device, error)
	ListSandboxes(context.Context) ([]Sandbox, error)
	GetSandbox(context.Context, string) (Sandbox, error)
	ExecuteAction(context.Context, *tgsrlv1.Action) (*tgsrlv1.ActionResult, error)
	ExecutePlan(context.Context, *tgsrlv1.PlacementPlan) ([]*tgsrlv1.ActionResult, error)
}

// SandboxEventInjector is an optional test/helper seam for providers that can
// project synthetic runtime sandbox events into their local provider state.
// It is intentionally excluded from the product ResourceProvider contract.
type SandboxEventInjector interface {
	ApplySandboxEvent(context.Context, SandboxEvent) error
}

// HealthStatus reports provider readiness without exposing provider-internal
// implementation details.
type HealthStatus struct {
	ProviderID string
	Source     string
	Healthy    bool
	Reason     string
	CheckedAt  time.Time
}

// WatchedResourceEvent couples a replayable cursor with one immutable resource
// event.
type WatchedResourceEvent struct {
	Cursor uint64
	Event  *tgsrlv1.ResourceEvent
}

// WatchedSandboxEvent couples a replayable cursor with one immutable sandbox
// event.
type WatchedSandboxEvent struct {
	Cursor uint64
	Event  *tgsrlv1.SandboxEvent
}

// PlanStatus describes the terminal or recoverable execution state of a plan.
type PlanStatus string

const (
	PlanStatusUnknown   PlanStatus = "unknown"
	PlanStatusInFlight  PlanStatus = "in_flight"
	PlanStatusSucceeded PlanStatus = "succeeded"
	PlanStatusFailed    PlanStatus = "failed"
)

// PlanRecord is the recovery/reconciliation view of one provider plan.
type PlanRecord struct {
	Plan             *tgsrlv1.PlacementPlan
	Status           PlanStatus
	Results          []*tgsrlv1.ActionResult
	ErrorCode        string
	ErrorMessage     string
	ObservedRevision uint64
	UpdatedAt        time.Time
}

// CompleteResourceProvider extends the baseline provider API without breaking
// existing interface consumers that only require ResourceProvider.
type CompleteResourceProvider interface {
	ResourceProvider
	ID(context.Context) (string, error)
	Health(context.Context) (*HealthStatus, error)
	WatchResources(context.Context, uint64) (<-chan WatchedResourceEvent, error)
	WatchSandboxes(context.Context, uint64) (<-chan WatchedSandboxEvent, error)
	ReconcilePlan(context.Context, *tgsrlv1.PlacementPlan) (*PlanRecord, error)
	RecoverInFlightPlans(context.Context) ([]*PlanRecord, error)
}

// InjectedFailure describes a deterministic mock failure.
type InjectedFailure struct {
	Code    string
	Message string
}

// FaultOptions controls deterministic delay and failure injection. A
// PartialFailureAt value greater than zero fails that one-based action
// position in every plan, after earlier actions have committed.
type FaultOptions struct {
	Delay                 time.Duration
	DelayByActionType     map[tgsrlv1.ActionType]time.Duration
	FailActionIDs         map[string]InjectedFailure
	FailRollbackActionIDs map[string]InjectedFailure
	PartialFailureAt      int
}

// MockResourceProvider is a race-safe provider simulator for local validation.
type MockResourceProvider struct {
	mu           sync.Mutex
	devices      []*tgsrlv1.Device
	capabilities *tgsrlv1.CapabilitySet
	sandboxes    map[string]Sandbox
	providerID   string
	source       string
	faults       FaultOptions
	now          func() time.Time
	revision     uint64
	retention    int
	actions      map[string]actionLedgerEntry
	plans        map[string][]*tgsrlv1.ActionResult
	planErrors   map[string]error
	planRequests map[string]*tgsrlv1.PlacementPlan
	planRecords  map[string]*PlanRecord
	events       map[string]SandboxEvent
	actionPlans  map[string]string
	resourceSeq  uint64
	sandboxSeq   uint64
	resourceLog  []WatchedResourceEvent
	sandboxLog   []WatchedSandboxEvent
	nextWatchID  uint64
	resourceSubs map[uint64]chan WatchedResourceEvent
	sandboxSubs  map[uint64]chan WatchedSandboxEvent
}

type actionLedgerEntry struct {
	action *tgsrlv1.Action
	result *tgsrlv1.ActionResult
	err    error
}
