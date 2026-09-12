package provider

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
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
	SandboxID       string
	State           SandboxState
	Generation      uint64
	Binding         *tgsrlv1.Binding
	Share           float64
	Priority        int32
	SafePoint       bool
	Offloaded       bool
	SemanticContext *tgsrlv1.SemanticEnvelope
	StateChangedAt  time.Time
	UpdatedAt       time.Time
}

// ProjectSandboxObservation validates and applies one external observation to
// a provider-owned sandbox projection. Providers share this implementation so
// generation and same-generation transition rules cannot drift by backend.
func ProjectSandboxObservation(current Sandbox, event *tgsrlv1.SandboxEvent, confirmedAt time.Time) (Sandbox, error) {
	if event == nil {
		return Sandbox{}, fmt.Errorf("%w: sandbox event is required", ErrInvalidArgument)
	}
	state, validState := SandboxStateFromRuntime(event.GetState())
	if strings.TrimSpace(event.GetEventId()) == "" || strings.TrimSpace(event.GetSandboxId()) == "" || event.GetGeneration() == 0 || !validState {
		return Sandbox{}, fmt.Errorf("%w: sandbox event is invalid", ErrInvalidArgument)
	}
	exists := current.SandboxID != ""
	if exists && event.GetGeneration() < current.Generation {
		return Sandbox{}, &Error{Code: ErrorCodeLateEventFenced, Message: "event generation is older than current sandbox generation", SandboxID: event.GetSandboxId(), ExpectedGeneration: current.Generation, ObservedGeneration: event.GetGeneration(), Cause: ErrGenerationFenced}
	}
	if exists && event.GetGeneration() == current.Generation {
		if event.GetBinding() != nil && current.Binding != nil && !proto.Equal(event.GetBinding(), current.Binding) {
			return Sandbox{}, &Error{Code: ErrorCodeLateEventFenced, Message: "same-generation event cannot replace binding", SandboxID: event.GetSandboxId(), ExpectedGeneration: current.Generation, ObservedGeneration: event.GetGeneration(), Cause: ErrGenerationFenced}
		}
		if eventWouldRegress(current.State, state) {
			return Sandbox{}, &Error{Code: ErrorCodeLateEventFenced, Message: "same-generation event would regress sandbox state", SandboxID: event.GetSandboxId(), ExpectedGeneration: current.Generation, ObservedGeneration: event.GetGeneration(), Cause: ErrGenerationFenced}
		}
	}
	lifecycleChanged := !exists || current.State != state || current.Generation != event.GetGeneration()
	current.SandboxID = event.GetSandboxId()
	current.Generation = event.GetGeneration()
	current.State = state
	if event.GetBinding() != nil {
		current.Binding = proto.Clone(event.GetBinding()).(*tgsrlv1.Binding)
	}
	if event.SafePoint != nil {
		current.SafePoint = event.GetSafePoint()
	} else if !exists || lifecycleChanged {
		current.SafePoint = false
	}
	if event.Share != nil {
		current.Share = event.GetShare()
	}
	if event.Priority != nil {
		current.Priority = event.GetPriority()
	}
	if event.Offloaded != nil {
		current.Offloaded = event.GetOffloaded()
	}
	if event.GetSemanticContext() != nil {
		current.SemanticContext = proto.Clone(event.GetSemanticContext()).(*tgsrlv1.SemanticEnvelope)
	}
	if lifecycleChanged {
		current.StateChangedAt = confirmedAt
		if event.GetOccurredAt() != nil && event.GetOccurredAt().CheckValid() == nil {
			current.StateChangedAt = event.GetOccurredAt().AsTime()
		}
	}
	current.UpdatedAt = confirmedAt
	return current, nil
}

// NormalizeSandboxObservation returns the canonical event accepted into the
// provider revision domain. Missing mutable values are filled from the
// resulting projection, while safe_point keeps its presence semantics because
// absence means that the execution substrate supplied no fresh safe-point
// evidence. Event ID is the deterministic idempotency key of that observation
// when no action-scoped key exists.
func NormalizeSandboxObservation(event *tgsrlv1.SandboxEvent, projected Sandbox, revision uint64) *tgsrlv1.SandboxEvent {
	accepted := proto.Clone(event).(*tgsrlv1.SandboxEvent)
	accepted.ProviderRevision = revision
	if accepted.GetIdempotencyKey() == "" {
		accepted.IdempotencyKey = accepted.GetEventId()
	}
	if accepted.Share == nil {
		value := projected.Share
		accepted.Share = &value
	}
	if accepted.Priority == nil {
		value := projected.Priority
		accepted.Priority = &value
	}
	if accepted.Offloaded == nil {
		value := projected.Offloaded
		accepted.Offloaded = &value
	}
	return accepted
}

// SandboxStateFromRuntime converts the wire state to the provider state model.
func SandboxStateFromRuntime(state tgsrlv1.RuntimeState) (SandboxState, bool) {
	switch state {
	case tgsrlv1.RuntimeState_RUNTIME_STATE_REQUESTED:
		return SandboxStateRequested, true
	case tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND:
		return SandboxStateBound, true
	case tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING:
		return SandboxStateRunning, true
	case tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED:
		return SandboxStatePaused, true
	case tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING:
		return SandboxStateSleeping, true
	case tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED:
		return SandboxStateFailed, true
	case tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED:
		return SandboxStateTerminated, true
	default:
		return "", false
	}
}

// ResourceEvent is a cloned provider state notification.
type ResourceEvent struct {
	Revision uint64
	Sandbox  Sandbox
}

// CapabilityVersionAttributeKey returns the transitional CapabilitySet
// attribute used to advertise one named provider capability's semantic
// version. Capability names are matched after trim/lower/hyphen normalization.
func CapabilityVersionAttributeKey(name string) string {
	return "capability.version." + normalizeCapabilityName(name)
}

// ValidateProviderCapabilityRequirements validates required provider feature
// names and their capability-specific versions. CapabilitySet.revision is an
// evidence revision and must never be interpreted as a semantic version.
func ValidateProviderCapabilityRequirements(available *tgsrlv1.CapabilitySet, plan *tgsrlv1.PlacementPlan) error {
	for _, required := range plan.GetCapabilityRequirements() {
		if required == nil {
			return fmt.Errorf("%w: nil plan capability requirement", ErrInvalidArgument)
		}
		if !required.GetRequired() || required.GetKind() != tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_PROVIDER_CAPABILITY {
			continue
		}
		if available == nil || !containsCapabilityName(available.GetNames(), required.GetName()) {
			return &Error{Code: ErrorCodeUnsupported, Message: fmt.Sprintf("provider capability %q is unavailable", required.GetName()), PlanID: plan.GetPlanId(), Cause: ErrUnsupported}
		}
		if required.GetMinVersion() == "" {
			continue
		}
		advertised := available.GetAttributes()[CapabilityVersionAttributeKey(required.GetName())]
		versionOK, versionErr := versionAtLeast(advertised, required.GetMinVersion())
		if versionErr != nil || !versionOK {
			return &Error{Code: ErrorCodeUnsupported, Message: fmt.Sprintf("provider capability %q version requirement is not satisfied", required.GetName()), PlanID: plan.GetPlanId(), Cause: ErrUnsupported}
		}
	}
	return nil
}

type semanticVersion struct {
	core       [3]string
	prerelease []string
}

func versionAtLeast(available, required string) (bool, error) {
	if required == "" {
		return true, nil
	}
	availableVersion, err := parseSemanticVersion(available)
	if err != nil {
		return false, err
	}
	requiredVersion, err := parseSemanticVersion(required)
	if err != nil {
		return false, err
	}
	return compareSemanticVersions(availableVersion, requiredVersion) >= 0, nil
}

func parseSemanticVersion(value string) (semanticVersion, error) {
	malformed := func() (semanticVersion, error) {
		return semanticVersion{}, fmt.Errorf("malformed capability version %q", value)
	}
	if value == "" || strings.TrimSpace(value) != value || strings.Count(value, "+") > 1 {
		return malformed()
	}
	withoutBuild, build, hasBuild := strings.Cut(value, "+")
	if hasBuild && !validSemanticIdentifiers(build, false) {
		return malformed()
	}
	core, prerelease, hasPrerelease := strings.Cut(withoutBuild, "-")
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return malformed()
	}
	var parsed semanticVersion
	for index, part := range parts {
		if !validSemanticNumber(part) {
			return malformed()
		}
		parsed.core[index] = part
	}
	if hasPrerelease {
		if !validSemanticIdentifiers(prerelease, true) {
			return malformed()
		}
		parsed.prerelease = strings.Split(prerelease, ".")
	}
	return parsed, nil
}

func validSemanticNumber(value string) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func validSemanticIdentifiers(value string, rejectNumericLeadingZeros bool) bool {
	if value == "" {
		return false
	}
	for _, identifier := range strings.Split(value, ".") {
		if identifier == "" {
			return false
		}
		numeric := true
		for _, character := range identifier {
			if (character < '0' || character > '9') && (character < 'A' || character > 'Z') && (character < 'a' || character > 'z') && character != '-' {
				return false
			}
			if character < '0' || character > '9' {
				numeric = false
			}
		}
		if rejectNumericLeadingZeros && numeric && len(identifier) > 1 && identifier[0] == '0' {
			return false
		}
	}
	return true
}

func compareSemanticVersions(left, right semanticVersion) int {
	for index := range left.core {
		if comparison := compareSemanticNumbers(left.core[index], right.core[index]); comparison != 0 {
			return comparison
		}
	}
	if len(left.prerelease) == 0 && len(right.prerelease) == 0 {
		return 0
	}
	if len(left.prerelease) == 0 {
		return 1
	}
	if len(right.prerelease) == 0 {
		return -1
	}
	for index := 0; index < len(left.prerelease) && index < len(right.prerelease); index++ {
		leftIdentifier := left.prerelease[index]
		rightIdentifier := right.prerelease[index]
		leftNumeric := isSemanticNumber(leftIdentifier)
		rightNumeric := isSemanticNumber(rightIdentifier)
		switch {
		case leftNumeric && rightNumeric:
			if comparison := compareSemanticNumbers(leftIdentifier, rightIdentifier); comparison != 0 {
				return comparison
			}
		case leftNumeric:
			return -1
		case rightNumeric:
			return 1
		case leftIdentifier < rightIdentifier:
			return -1
		case leftIdentifier > rightIdentifier:
			return 1
		}
	}
	switch {
	case len(left.prerelease) < len(right.prerelease):
		return -1
	case len(left.prerelease) > len(right.prerelease):
		return 1
	default:
		return 0
	}
}

func compareSemanticNumbers(left, right string) int {
	switch {
	case len(left) < len(right):
		return -1
	case len(left) > len(right):
		return 1
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func isSemanticNumber(value string) bool {
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return value != ""
}

func normalizeCapabilityName(value string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "-", "_"))
}

func containsCapabilityName(values []string, expected string) bool {
	expected = normalizeCapabilityName(expected)
	for _, value := range values {
		if normalizeCapabilityName(value) == expected {
			return true
		}
	}
	return false
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
	txPlans      map[string]*tgsrlv1.PlacementPlan
	txReceipts   map[string]*TransactionReceipt
	txBefore     map[string][]actionBeforeImage
	txApplied    map[string][]bool
	events       map[string]*tgsrlv1.SandboxEvent
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
