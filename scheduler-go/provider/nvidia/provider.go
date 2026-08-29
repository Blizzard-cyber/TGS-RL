package nvidia

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	base "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Provider is the NVIDIA-backed complete provider implementation.
type Provider struct {
	mu           sync.Mutex
	driver       Driver
	now          func() time.Time
	retention    int
	providerID   string
	capabilities *tgsrlv1.CapabilitySet
	devices      []*tgsrlv1.Device
	sandboxes    map[string]base.Sandbox
	revision     uint64
	healthy      bool
	healthReason string
	plans        map[string]*base.PlanRecord
	planErrors   map[string]error
	actions      map[string]*actionLedgerEntry
	resourceSeq  uint64
	sandboxSeq   uint64
	resourceLog  []base.WatchedResourceEvent
	sandboxLog   []base.WatchedSandboxEvent
	resourceSubs map[uint64]chan base.WatchedResourceEvent
	sandboxSubs  map[uint64]chan base.WatchedSandboxEvent
	nextWatchID  uint64
}

type actionExecutionMode uint8

const (
	actionExecutionStandalone actionExecutionMode = iota
	actionExecutionPlan
)

var _ base.CompleteResourceProvider = (*Provider)(nil)
var _ base.PlanCapabilityProvider = (*Provider)(nil)
var _ base.PlanCapabilityValidator = (*Provider)(nil)

type actionLedgerEntry struct {
	action    *tgsrlv1.Action
	result    *tgsrlv1.ActionResult
	err       error
	before    actionBeforeImage
	applied   bool
	done      chan struct{}
	completed bool
	waiters   int
}

type actionBeforeImage struct {
	sandboxID string
	existed   bool
	sandbox   base.Sandbox
	mutations sandboxMutation
}

type sandboxMutation uint16

const (
	sandboxMutationExistence sandboxMutation = 1 << iota
	sandboxMutationState
	sandboxMutationGeneration
	sandboxMutationBinding
	sandboxMutationShare
	sandboxMutationPriority
	sandboxMutationOffloaded
)

func New(options ...Option) (*Provider, error) {
	cfg := config{
		driver:    NewLocalDriver(),
		now:       time.Now,
		retention: 128,
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil nvidia option", base.ErrInvalidArgument)
		}
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}
	probe, err := cfg.driver.Probe(context.Background())
	if err != nil {
		return nil, err
	}
	capabilities := probe.Capabilities
	if capabilities == nil {
		capabilities = defaultCapabilities(probe.Available)
	}
	capabilities = cloneCapabilities(capabilities)
	if capabilities.Attributes == nil {
		capabilities.Attributes = map[string]string{}
	}
	if _, exists := capabilities.Attributes[base.CapabilityVersionAttributeKey(CapabilityName)]; !exists {
		capabilities.Attributes[base.CapabilityVersionAttributeKey(CapabilityName)] = "1.0.0"
	}
	devices := probe.Devices
	if !probe.Available {
		devices = unavailableDevices(probe.Reason)
	}
	if len(devices) == 0 {
		devices = unavailableDevices("no usable nvidia devices discovered")
	}
	provider := &Provider{
		driver:       cfg.driver,
		now:          cfg.now,
		retention:    cfg.retention,
		providerID:   ProviderID,
		capabilities: cloneCapabilities(capabilities),
		devices:      cloneDevices(devices),
		sandboxes:    map[string]base.Sandbox{},
		revision:     1,
		healthy:      probe.Available,
		healthReason: strings.TrimSpace(probe.Reason),
		plans:        map[string]*base.PlanRecord{},
		planErrors:   map[string]error{},
		actions:      map[string]*actionLedgerEntry{},
		resourceSubs: map[uint64]chan base.WatchedResourceEvent{},
		sandboxSubs:  map[uint64]chan base.WatchedSandboxEvent{},
	}
	provider.publishCapabilityRefreshLocked()
	provider.publishSnapshotLocked(tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED)
	return provider, nil
}

// PlanCapabilities reports only executor semantics that this provider can
// currently guarantee. Atomic replacement remains absent.
func (p *Provider) PlanCapabilities() []*tgsrlv1.CapabilityRequirement {
	result := make([]*tgsrlv1.CapabilityRequirement, 0, len(supportedPlanCapabilities()))
	for _, capability := range supportedPlanCapabilities() {
		result = append(result, proto.Clone(capability).(*tgsrlv1.CapabilityRequirement))
	}
	return result
}

// ValidatePlanCapabilities validates provider features and executor semantics.
// Atomic replacement remains unsupported and therefore fails closed.
func (p *Provider) ValidatePlanCapabilities(plan *tgsrlv1.PlacementPlan) error {
	p.mu.Lock()
	capabilities := cloneCapabilities(p.capabilities)
	p.mu.Unlock()
	if err := base.ValidateProviderCapabilityRequirements(capabilities, plan); err != nil {
		return err
	}
	return base.ValidatePlanCapabilitiesForRequirements(supportedPlanCapabilities(), plan)
}

func (p *Provider) ExecuteAction(ctx context.Context, action *tgsrlv1.Action) (*tgsrlv1.ActionResult, error) {
	result, _, _, err := p.executeAction(ctx, action, actionExecutionStandalone)
	return result, err
}

func (p *Provider) executeAction(ctx context.Context, action *tgsrlv1.Action, mode actionExecutionMode) (*tgsrlv1.ActionResult, actionBeforeImage, bool, error) {
	var before actionBeforeImage
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, before, false, err
	}
	startedAt := p.now()
	p.mu.Lock()
	if !p.healthy {
		err := &base.Error{Code: ErrorCodeUnavailable, Message: p.healthReason, PlanID: action.GetPlanId(), ActionID: action.GetActionId(), Cause: base.ErrFailedPrecondition}
		result := actionErrorResult(action, p.revision, startedAt, p.now(), err)
		p.recordStandaloneResultLocked(mode, action, result, err)
		p.mu.Unlock()
		return result, before, false, err
	}
	if action != nil && strings.TrimSpace(action.GetIdempotencyKey()) != "" {
		if existing, ok := p.actions[action.GetIdempotencyKey()]; ok {
			if !proto.Equal(existing.action, action) {
				err := &base.Error{Code: base.ErrorCodeIdempotencyConflict, Message: "idempotency key was reused with different action content", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), Cause: base.ErrIdempotencyConflict}
				result := actionErrorResult(action, p.revision, startedAt, p.now(), err)
				p.recordStandaloneResultLocked(mode, action, result, err)
				p.mu.Unlock()
				return result, before, false, err
			}
			if existing.completed {
				p.mu.Unlock()
				return cloneActionResult(existing.result), cloneActionBeforeImage(existing.before), existing.applied, existing.err
			}
			done := existing.done
			existing.waiters++
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				p.mu.Lock()
				existing.waiters--
				p.mu.Unlock()
				return nil, before, false, ctx.Err()
			case <-done:
				p.mu.Lock()
				result := cloneActionResult(existing.result)
				before = cloneActionBeforeImage(existing.before)
				applied := existing.applied
				err := existing.err
				existing.waiters--
				p.mu.Unlock()
				return result, before, applied, err
			}
		}
	}
	if err := validateActionAt(action, p.now(), p.capabilities); err != nil {
		result := actionErrorResult(action, p.revision, startedAt, p.now(), err)
		p.recordStandaloneResultLocked(mode, action, result, err)
		if key := strings.TrimSpace(action.GetIdempotencyKey()); key != "" {
			entry := &actionLedgerEntry{action: cloneAction(action), done: make(chan struct{})}
			p.completeActionLocked(action, entry, result, before, false, err)
		}
		p.mu.Unlock()
		return result, before, false, err
	}
	if err := p.validateActionFencesLocked(action, mode == actionExecutionStandalone); err != nil {
		result := actionErrorResult(action, p.revision, startedAt, p.now(), err)
		p.recordStandaloneResultLocked(mode, action, result, err)
		p.mu.Unlock()
		return result, before, false, err
	}
	before = p.captureActionBeforeImageLocked(action)
	entry := &actionLedgerEntry{action: cloneAction(action), before: cloneActionBeforeImage(before), done: make(chan struct{})}
	p.actions[action.GetIdempotencyKey()] = entry
	state := p.driverStateLocked()
	p.mu.Unlock()

	execution, err := p.driver.ExecuteAction(ctx, state, action)
	err = normalizeDriverError(action, err)

	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		result := actionErrorResult(action, p.revision, startedAt, p.now(), err)
		p.completeActionLocked(action, entry, result, before, false, err)
		p.recordStandaloneResultLocked(mode, action, result, err)
		return result, before, false, err
	}
	if err := p.validateActionFencesLocked(action, mode == actionExecutionStandalone); err != nil {
		result := actionErrorResult(action, p.revision, startedAt, p.now(), err)
		rollbackAttempted, rollbackErr := p.rollbackActionLocked(ctx, action, before, 0, false)
		if rollbackAttempted {
			result.RollbackAttempted = true
			if rollbackErr != nil {
				result.RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED
				result.ErrorMessage = err.Error() + "; rollback failed: " + rollbackErr.Error()
			} else {
				result.RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK
			}
		}
		applied := !rollbackAttempted || rollbackErr != nil
		p.completeActionLocked(action, entry, result, before, applied, err)
		p.recordStandaloneResultLocked(mode, action, result, err)
		return result, before, applied, err
	}
	if err := p.applyActionLocked(action); err != nil {
		err = normalizeDriverError(action, err)
		result := actionErrorResult(action, p.revision, startedAt, p.now(), err)
		rollbackAttempted, rollbackErr := p.rollbackActionLocked(ctx, action, before, 0, false)
		if rollbackAttempted {
			result.RollbackAttempted = true
			if rollbackErr != nil {
				result.RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED
				result.ErrorMessage = err.Error() + "; rollback failed: " + rollbackErr.Error()
			} else {
				result.RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK
			}
		}
		applied := !rollbackAttempted || rollbackErr != nil
		p.completeActionLocked(action, entry, result, before, applied, err)
		p.recordStandaloneResultLocked(mode, action, result, err)
		return result, before, applied, err
	}
	p.revision++
	result := &tgsrlv1.ActionResult{
		ActionId:           action.GetActionId(),
		Status:             tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED,
		StartedAt:          timestamppb.New(startedAt),
		CompletedAt:        timestamppb.New(p.now()),
		ObservedRevision:   p.revision,
		PlanId:             action.GetPlanId(),
		IdempotencyKey:     action.GetIdempotencyKey(),
		ObservedGeneration: p.observedGenerationLocked(action),
	}
	p.publishActionEffectsLocked(action, driverExecutionDetail(execution))
	p.completeActionLocked(action, entry, result, before, true, nil)
	p.recordStandaloneResultLocked(mode, action, result, nil)
	return cloneActionResult(result), before, true, nil
}

func (p *Provider) ExecutePlan(ctx context.Context, plan *tgsrlv1.PlacementPlan) ([]*tgsrlv1.ActionResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if plan == nil || strings.TrimSpace(plan.GetPlanId()) == "" {
		return nil, fmt.Errorf("%w: plan_id is required", base.ErrInvalidArgument)
	}
	if err := validatePlanAt(plan); err != nil {
		return nil, err
	}
	if err := p.ValidatePlanCapabilities(plan); err != nil {
		return nil, err
	}
	ordered := append([]*tgsrlv1.Action(nil), plan.GetActions()...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].GetOrder() < ordered[j].GetOrder() })
	p.mu.Lock()
	if record, ok := p.plans[plan.GetPlanId()]; ok && record != nil {
		if record.Plan == nil || !proto.Equal(record.Plan, plan) {
			p.mu.Unlock()
			return nil, fmt.Errorf("%w: plan_id %q was reused with different content", base.ErrIdempotencyConflict, plan.GetPlanId())
		}
		if record.Status == base.PlanStatusInFlight {
			p.mu.Unlock()
			return nil, &base.Error{Code: base.ErrorCodeFailedPrecondition, Message: "plan execution is already in flight", PlanID: plan.GetPlanId(), Cause: base.ErrFailedPrecondition}
		}
		results := cloneActionResults(record.Results)
		err := p.planErrors[plan.GetPlanId()]
		p.mu.Unlock()
		return results, err
	}
	p.plans[plan.GetPlanId()] = &base.PlanRecord{Plan: clonePlacementPlan(plan), Status: base.PlanStatusInFlight, UpdatedAt: p.now(), ObservedRevision: p.revision}
	p.mu.Unlock()
	results := make([]*tgsrlv1.ActionResult, 0, len(ordered))
	before := make([]actionBeforeImage, len(ordered))
	applied := make([]bool, len(ordered))
	for index, action := range ordered {
		result, actionBefore, actionApplied, err := p.executeAction(ctx, action, actionExecutionPlan)
		before[index] = actionBefore
		applied[index] = actionApplied
		results = append(results, result)
		if err != nil {
			p.mu.Lock()
			protected := make(map[string]sandboxMutation)
			for rollbackIndex := index - 1; rollbackIndex >= 0; rollbackIndex-- {
				if !applied[rollbackIndex] {
					continue
				}
				rollbackResult := results[rollbackIndex]
				rollbackResult.RollbackAttempted = true
				rollbackBefore := before[rollbackIndex]
				_, rollbackErr := p.rollbackActionLocked(ctx, ordered[rollbackIndex], rollbackBefore, protected[rollbackBefore.sandboxID], true)
				if rollbackErr != nil {
					rollbackResult.RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED
					rollbackResult.ErrorCode = errorCode(rollbackErr)
					rollbackResult.ErrorMessage = "rollback failed: " + rollbackErr.Error()
					protected[rollbackBefore.sandboxID] |= rollbackBefore.mutations
				} else {
					rollbackResult.RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK
					rollbackResult.ObservedRevision = p.revision
				}
				p.cacheActionResultLocked(ordered[rollbackIndex], rollbackResult, rollbackBefore, rollbackErr != nil, nil)
			}
			for skippedIndex := index + 1; skippedIndex < len(ordered); skippedIndex++ {
				results = append(results, skippedActionResult(ordered[skippedIndex], p.revision, p.now(), "previous action failed"))
			}
			planErr := fmt.Errorf("%w: action %d: %v", base.ErrPartialFailure, index+1, err)
			p.setPlanTerminalLocked(plan, base.PlanStatusFailed, results, planErr)
			p.mu.Unlock()
			return cloneActionResults(results), planErr
		}
	}
	p.mu.Lock()
	p.setPlanTerminalLocked(plan, base.PlanStatusSucceeded, results, nil)
	p.mu.Unlock()
	return cloneActionResults(results), nil
}

func (p *Provider) ApplySandboxEvent(ctx context.Context, event base.SandboxEvent) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(event.SandboxID) == "" {
		return fmt.Errorf("%w: sandbox event is invalid", base.ErrInvalidArgument)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	current := p.sandboxes[event.SandboxID]
	exists := current.SandboxID != ""
	if exists && event.Generation < current.Generation {
		return &base.Error{Code: base.ErrorCodeLateEventFenced, Message: "event generation is older than current sandbox generation", SandboxID: event.SandboxID, ExpectedGeneration: current.Generation, ObservedGeneration: event.Generation, Cause: base.ErrGenerationFenced}
	}
	if !exists {
		current = base.Sandbox{SandboxID: event.SandboxID}
	}
	confirmedAt := p.now()
	lifecycleChanged := !exists || current.State != event.State || current.Generation != event.Generation
	current.State = event.State
	current.Generation = event.Generation
	if event.Binding != nil {
		current.Binding = cloneBinding(event.Binding)
	}
	if event.SafePoint != nil {
		current.SafePoint = *event.SafePoint
	}
	if event.SemanticContext != nil {
		current.SemanticContext = cloneSemanticEnvelope(event.SemanticContext)
	}
	if lifecycleChanged {
		current.StateChangedAt = confirmedAt
		if !event.StateChangedAt.IsZero() {
			current.StateChangedAt = event.StateChangedAt
		}
	} else if current.StateChangedAt.IsZero() && !event.StateChangedAt.IsZero() {
		current.StateChangedAt = event.StateChangedAt
	}
	current.UpdatedAt = confirmedAt
	p.sandboxes[event.SandboxID] = current
	p.revision++
	p.publishSandboxSnapshotLocked(current, "runtime event applied")
	p.publishSnapshotLocked(tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED)
	return nil
}

func (p *Provider) applyActionLocked(action *tgsrlv1.Action) error {
	sandboxID := actionSandboxID(action)
	sandbox := p.sandboxes[sandboxID]
	created := false
	if sandbox.SandboxID == "" && action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND {
		sandbox = base.Sandbox{SandboxID: sandboxID, State: base.SandboxStateRequested, Generation: maxUint64(1, action.GetExpectedGeneration())}
		created = true
	}
	if sandbox.SandboxID == "" {
		return &base.Error{Code: base.ErrorCodeNotFound, Message: "sandbox does not exist", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: sandboxID, Cause: base.ErrNotFound}
	}
	if action.GetExpectedGeneration() != 0 && sandbox.Generation != action.GetExpectedGeneration() {
		return &base.Error{Code: base.ErrorCodeGenerationConflict, Message: "sandbox generation does not match", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: sandboxID, ExpectedGeneration: action.GetExpectedGeneration(), ObservedGeneration: sandbox.Generation, Cause: base.ErrFailedPrecondition}
	}
	if action.GetRequiresSafePoint() && action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND && !sandbox.SafePoint {
		return &base.Error{Code: base.ErrorCodeFailedPrecondition, Message: "sandbox is not at a safe point", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: sandboxID, Cause: base.ErrFailedPrecondition}
	}
	previousState := sandbox.State
	previousGeneration := sandbox.Generation
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_BIND:
		if action.GetBinding() == nil || len(action.GetBinding().GetDeviceIds()) == 0 {
			return fmt.Errorf("%w: bind requires binding device_ids", base.ErrInvalidArgument)
		}
		sandbox.State = base.SandboxStateBound
		sandbox.Binding = cloneBinding(action.GetBinding())
		sandbox.Share = action.GetShare()
		sandbox.Priority = action.GetPriority()
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		if sandbox.State == base.SandboxStateTerminated {
			return &base.Error{Code: base.ErrorCodeFailedPrecondition, Message: "sandbox is already terminated", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: sandboxID, Cause: base.ErrFailedPrecondition}
		}
		sandbox.State = base.SandboxStateTerminated
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE:
		if action.GetShare() < 0 || action.GetShare() > 1 {
			return fmt.Errorf("%w: share must be within [0,1]", base.ErrInvalidArgument)
		}
		sandbox.Share = action.GetShare()
	case tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY:
		sandbox.Priority = action.GetPriority()
	case tgsrlv1.ActionType_ACTION_TYPE_RESIZE:
		if action.GetBinding() == nil || action.GetBinding().GetResources() == nil {
			return fmt.Errorf("%w: resize requires binding resources", base.ErrInvalidArgument)
		}
		sandbox.Binding = cloneBinding(action.GetBinding())
	case tgsrlv1.ActionType_ACTION_TYPE_PAUSE:
		if sandbox.State != base.SandboxStateRunning {
			return transitionError(action, sandbox, "pause requires running state")
		}
		sandbox.State = base.SandboxStatePaused
	case tgsrlv1.ActionType_ACTION_TYPE_RESUME:
		if sandbox.State != base.SandboxStatePaused && sandbox.State != base.SandboxStateSleeping {
			return transitionError(action, sandbox, "resume requires paused or sleeping state")
		}
		sandbox.State = base.SandboxStateRunning
		sandbox.Offloaded = false
	case tgsrlv1.ActionType_ACTION_TYPE_SLEEP:
		if sandbox.State != base.SandboxStateRunning && sandbox.State != base.SandboxStatePaused {
			return transitionError(action, sandbox, "sleep requires running or paused state")
		}
		sandbox.State = base.SandboxStateSleeping
	case tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		if sandbox.State != base.SandboxStateRunning && sandbox.State != base.SandboxStatePaused && sandbox.State != base.SandboxStateSleeping {
			return transitionError(action, sandbox, "offload requires running, paused, or sleeping state")
		}
		sandbox.State = base.SandboxStateSleeping
		sandbox.Offloaded = true
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND, tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		sandbox.Generation++
		sandbox.State = base.SandboxStateBound
		if action.GetBinding() != nil {
			sandbox.Binding = cloneBinding(action.GetBinding())
			sandbox.Binding.Generation = sandbox.Generation
		}
		sandbox.Offloaded = false
	default:
		return fmt.Errorf("%w: unsupported action type %s", base.ErrUnsupported, action.GetActionType())
	}
	confirmedAt := p.now()
	sandbox.UpdatedAt = confirmedAt
	if created || sandbox.State != previousState || sandbox.Generation != previousGeneration {
		sandbox.StateChangedAt = confirmedAt
	}
	p.sandboxes[sandboxID] = sandbox
	return nil
}

func (p *Provider) validateActionFencesLocked(action *tgsrlv1.Action, checkProviderRevision bool) error {
	if checkProviderRevision && action.GetExpectedSnapshotRevision() != p.revision {
		return &base.Error{Code: base.ErrorCodeRevisionConflict, Message: "action snapshot revision is stale", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: actionSandboxID(action), ExpectedRevision: action.GetExpectedSnapshotRevision(), ObservedRevision: p.revision, Cause: base.ErrFailedPrecondition}
	}
	sandboxID := actionSandboxID(action)
	sandbox, exists := p.sandboxes[sandboxID]
	if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND && !exists {
		return nil
	}
	if !exists {
		return &base.Error{Code: base.ErrorCodeNotFound, Message: "sandbox does not exist", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: sandboxID, Cause: base.ErrNotFound}
	}
	if action.GetExpectedGeneration() != 0 && sandbox.Generation != action.GetExpectedGeneration() {
		return &base.Error{Code: base.ErrorCodeGenerationConflict, Message: "sandbox generation does not match", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: sandboxID, ExpectedGeneration: action.GetExpectedGeneration(), ObservedGeneration: sandbox.Generation, Cause: base.ErrFailedPrecondition}
	}
	if action.GetRequiresSafePoint() && action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND && !sandbox.SafePoint {
		return &base.Error{Code: base.ErrorCodeFailedPrecondition, Message: "sandbox is not at a safe point", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: sandboxID, Cause: base.ErrFailedPrecondition}
	}
	return nil
}

func (p *Provider) captureActionBeforeImageLocked(action *tgsrlv1.Action) actionBeforeImage {
	sandboxID := actionSandboxID(action)
	sandbox, existed := p.sandboxes[sandboxID]
	return actionBeforeImage{sandboxID: sandboxID, existed: existed, sandbox: cloneSandbox(sandbox), mutations: mutationsForAction(action, existed)}
}

func (p *Provider) restoreActionBeforeImageLocked(before actionBeforeImage, protected sandboxMutation) error {
	if before.mutations&sandboxMutationExistence != 0 {
		if protected == 0 {
			delete(p.sandboxes, before.sandboxID)
		}
	} else {
		current, exists := p.sandboxes[before.sandboxID]
		if !exists {
			return fmt.Errorf("%w: rollback target no longer exists", base.ErrFailedPrecondition)
		}
		mutations := before.mutations &^ protected
		if mutations&sandboxMutationState != 0 {
			current.State = before.sandbox.State
		}
		if mutations&sandboxMutationGeneration != 0 {
			current.Generation = before.sandbox.Generation
		}
		if mutations&sandboxMutationBinding != 0 {
			current.Binding = cloneBinding(before.sandbox.Binding)
		}
		if mutations&sandboxMutationShare != 0 {
			current.Share = before.sandbox.Share
		}
		if mutations&sandboxMutationPriority != 0 {
			current.Priority = before.sandbox.Priority
		}
		if mutations&sandboxMutationOffloaded != 0 {
			current.Offloaded = before.sandbox.Offloaded
		}
		if mutations&(sandboxMutationState|sandboxMutationGeneration) != 0 {
			current.StateChangedAt = before.sandbox.StateChangedAt
		}
		current.UpdatedAt = p.now()
		p.sandboxes[before.sandboxID] = current
	}
	if !before.existed && before.mutations&sandboxMutationExistence == 0 {
		delete(p.sandboxes, before.sandboxID)
	}
	p.revision++
	if sandbox, exists := p.sandboxes[before.sandboxID]; exists {
		p.publishSandboxSnapshotLocked(sandbox, "action rolled back")
	}
	p.publishSnapshotLocked(tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED)
	return nil
}

func (p *Provider) publishActionEffectsLocked(action *tgsrlv1.Action, detail string) {
	if sandbox, ok := p.sandboxes[actionSandboxID(action)]; ok {
		p.publishSandboxSnapshotLocked(sandbox, detail)
	}
	p.publishSnapshotLocked(tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED)
}

func (p *Provider) recordPlanResultLocked(action *tgsrlv1.Action, result *tgsrlv1.ActionResult, err error) {
	if action == nil || action.GetPlanId() == "" {
		return
	}
	record := p.plans[action.GetPlanId()]
	if record == nil {
		record = &base.PlanRecord{Plan: &tgsrlv1.PlacementPlan{PlanId: action.GetPlanId(), SnapshotRevision: action.GetExpectedSnapshotRevision(), Actions: []*tgsrlv1.Action{cloneAction(action)}}, Status: base.PlanStatusInFlight}
	}
	updated := false
	for index, existing := range record.Results {
		if existing != nil && existing.GetActionId() == result.GetActionId() {
			record.Results[index] = cloneActionResult(result)
			updated = true
			break
		}
	}
	if !updated {
		record.Results = append(record.Results, cloneActionResult(result))
	}
	record.ObservedRevision = p.revision
	record.UpdatedAt = p.now()
	if err != nil {
		record.Status = base.PlanStatusFailed
		record.ErrorMessage = err.Error()
		var providerError *base.Error
		if errors.As(err, &providerError) {
			record.ErrorCode = providerError.Code
		}
		p.planErrors[action.GetPlanId()] = err
	} else {
		record.Status = base.PlanStatusSucceeded
		record.ErrorCode = ""
		record.ErrorMessage = ""
		delete(p.planErrors, action.GetPlanId())
	}
	p.plans[action.GetPlanId()] = clonePlanRecord(record)
}

func (p *Provider) recordStandaloneResultLocked(mode actionExecutionMode, action *tgsrlv1.Action, result *tgsrlv1.ActionResult, err error) {
	if mode == actionExecutionStandalone {
		if record := p.plans[action.GetPlanId()]; record != nil && record.Status == base.PlanStatusInFlight && len(record.Plan.GetActions()) > 1 {
			return
		}
		p.recordPlanResultLocked(action, result, err)
	}
}

func (p *Provider) setPlanTerminalLocked(plan *tgsrlv1.PlacementPlan, status base.PlanStatus, results []*tgsrlv1.ActionResult, err error) {
	record := &base.PlanRecord{
		Plan:             clonePlacementPlan(plan),
		Status:           status,
		Results:          cloneActionResults(results),
		ObservedRevision: p.revision,
		UpdatedAt:        p.now(),
	}
	if err != nil {
		record.ErrorMessage = err.Error()
		record.ErrorCode = errorCode(err)
		p.planErrors[plan.GetPlanId()] = err
	} else {
		delete(p.planErrors, plan.GetPlanId())
	}
	p.plans[plan.GetPlanId()] = record
}

func validateActionAt(action *tgsrlv1.Action, now time.Time, capabilities *tgsrlv1.CapabilitySet) error {
	if action == nil {
		return fmt.Errorf("%w: action is required", base.ErrInvalidArgument)
	}
	if strings.TrimSpace(action.GetActionId()) == "" || strings.TrimSpace(action.GetPlanId()) == "" || strings.TrimSpace(action.GetIdempotencyKey()) == "" {
		return fmt.Errorf("%w: action_id, plan_id, and idempotency_key are required", base.ErrInvalidArgument)
	}
	if err := validateActionPolicy(action); err != nil {
		return err
	}
	if !containsSupportedAction(capabilities.GetSupportedActions(), actionNames[action.GetActionType()]) {
		return &base.Error{Code: base.ErrorCodeUnsupported, Message: "action is not advertised by provider capabilities", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: actionSandboxID(action), Cause: base.ErrUnsupported}
	}
	if detail := base.MissingCapabilities(capabilities, action.GetRequiredCapabilities()); detail != "" {
		return &base.Error{Code: base.ErrorCodeUnsupported, Message: detail, PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: actionSandboxID(action), Cause: base.ErrUnsupported}
	}
	if action.GetDeadline() == nil {
		return fmt.Errorf("%w: action deadline is required", base.ErrInvalidArgument)
	}
	if err := action.GetDeadline().CheckValid(); err != nil {
		return fmt.Errorf("%w: invalid action deadline: %v", base.ErrInvalidArgument, err)
	}
	if !now.Before(action.GetDeadline().AsTime()) {
		return &base.Error{Code: base.ErrorCodeDeadlineExceeded, Message: "action deadline has elapsed", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: actionSandboxID(action), Cause: base.ErrDeadlineExceeded}
	}
	return nil
}

func actionErrorResult(action *tgsrlv1.Action, revision uint64, startedAt, completedAt time.Time, err error) *tgsrlv1.ActionResult {
	result := &tgsrlv1.ActionResult{
		Status:           tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
		StartedAt:        timestamppb.New(startedAt),
		CompletedAt:      timestamppb.New(completedAt),
		ObservedRevision: revision,
		ErrorMessage:     err.Error(),
	}
	if action != nil {
		result.ActionId = action.GetActionId()
		result.PlanId = action.GetPlanId()
		result.IdempotencyKey = action.GetIdempotencyKey()
		result.ObservedGeneration = action.GetExpectedGeneration()
	}
	var providerError *base.Error
	if errors.As(err, &providerError) {
		result.ErrorCode = providerError.Code
		if providerError.ObservedGeneration != 0 {
			result.ObservedGeneration = providerError.ObservedGeneration
		}
	}
	return result
}

func actionSandboxID(action *tgsrlv1.Action) string {
	if action.GetSandboxId() != "" {
		return action.GetSandboxId()
	}
	if binding := action.GetBinding(); binding != nil && binding.GetSandboxId() != "" {
		return binding.GetSandboxId()
	}
	return action.GetTargetId()
}

func cloneSandbox(sandbox base.Sandbox) base.Sandbox {
	sandbox.Binding = cloneBinding(sandbox.Binding)
	sandbox.SemanticContext = cloneSemanticEnvelope(sandbox.SemanticContext)
	return sandbox
}

func cloneSemanticEnvelope(envelope *tgsrlv1.SemanticEnvelope) *tgsrlv1.SemanticEnvelope {
	if envelope == nil {
		return nil
	}
	return proto.Clone(envelope).(*tgsrlv1.SemanticEnvelope)
}

func cloneActionBeforeImage(before actionBeforeImage) actionBeforeImage {
	before.sandbox = cloneSandbox(before.sandbox)
	return before
}

func cloneBinding(binding *tgsrlv1.Binding) *tgsrlv1.Binding {
	if binding == nil {
		return nil
	}
	return proto.Clone(binding).(*tgsrlv1.Binding)
}

func cloneAction(action *tgsrlv1.Action) *tgsrlv1.Action {
	if action == nil {
		return nil
	}
	return proto.Clone(action).(*tgsrlv1.Action)
}

func cloneActionResult(result *tgsrlv1.ActionResult) *tgsrlv1.ActionResult {
	if result == nil {
		return nil
	}
	return proto.Clone(result).(*tgsrlv1.ActionResult)
}

func cloneActionResults(results []*tgsrlv1.ActionResult) []*tgsrlv1.ActionResult {
	clones := make([]*tgsrlv1.ActionResult, len(results))
	for index, result := range results {
		clones[index] = cloneActionResult(result)
	}
	return clones
}

func clonePlacementPlan(plan *tgsrlv1.PlacementPlan) *tgsrlv1.PlacementPlan {
	if plan == nil {
		return nil
	}
	return proto.Clone(plan).(*tgsrlv1.PlacementPlan)
}

func clonePlanRecord(record *base.PlanRecord) *base.PlanRecord {
	if record == nil {
		return nil
	}
	return &base.PlanRecord{
		Plan:             clonePlacementPlan(record.Plan),
		Status:           record.Status,
		Results:          cloneActionResults(record.Results),
		ErrorCode:        record.ErrorCode,
		ErrorMessage:     record.ErrorMessage,
		ObservedRevision: record.ObservedRevision,
		UpdatedAt:        record.UpdatedAt,
	}
}

func cloneResourceEvent(event *tgsrlv1.ResourceEvent) *tgsrlv1.ResourceEvent {
	if event == nil {
		return nil
	}
	return proto.Clone(event).(*tgsrlv1.ResourceEvent)
}

func cloneRuntimeSandboxEvent(event *tgsrlv1.SandboxEvent) *tgsrlv1.SandboxEvent {
	if event == nil {
		return nil
	}
	return proto.Clone(event).(*tgsrlv1.SandboxEvent)
}

func cloneWatchedResourceEvent(event base.WatchedResourceEvent) base.WatchedResourceEvent {
	event.Event = cloneResourceEvent(event.Event)
	return event
}

func cloneWatchedSandboxEvent(event base.WatchedSandboxEvent) base.WatchedSandboxEvent {
	event.Event = cloneRuntimeSandboxEvent(event.Event)
	return event
}

func (p *Provider) driverStateLocked() *DriverState {
	sandboxes := make(map[string]base.Sandbox, len(p.sandboxes))
	for id, sandbox := range p.sandboxes {
		sandboxes[id] = cloneSandbox(sandbox)
	}
	return &DriverState{
		Healthy:      p.healthy,
		HealthReason: p.healthReason,
		Devices:      cloneDevices(p.devices),
		Sandboxes:    sandboxes,
	}
}

func (p *Provider) cacheActionResultLocked(action *tgsrlv1.Action, result *tgsrlv1.ActionResult, before actionBeforeImage, applied bool, err error) {
	if action == nil || strings.TrimSpace(action.GetIdempotencyKey()) == "" {
		return
	}
	entry := &actionLedgerEntry{action: cloneAction(action), before: cloneActionBeforeImage(before), done: make(chan struct{})}
	p.completeActionLocked(action, entry, result, before, applied, err)
}

func (p *Provider) completeActionLocked(action *tgsrlv1.Action, entry *actionLedgerEntry, result *tgsrlv1.ActionResult, before actionBeforeImage, applied bool, err error) {
	if action == nil || entry == nil || entry.completed {
		return
	}
	entry.result = cloneActionResult(result)
	entry.err = err
	entry.before = cloneActionBeforeImage(before)
	entry.applied = applied
	entry.completed = true
	if entry.done == nil {
		entry.done = make(chan struct{})
	}
	p.actions[action.GetIdempotencyKey()] = entry
	close(entry.done)
}

func (p *Provider) observedGenerationLocked(action *tgsrlv1.Action) uint64 {
	sandbox, ok := p.sandboxes[actionSandboxID(action)]
	if !ok {
		return 0
	}
	return sandbox.Generation
}

func (p *Provider) rollbackActionLocked(ctx context.Context, action *tgsrlv1.Action, before actionBeforeImage, protected sandboxMutation, restoreProjection bool) (bool, error) {
	rollback := action.GetRollback()
	if rollback == nil {
		return false, nil
	}
	rollbackAction := &tgsrlv1.Action{
		ActionId:       action.GetActionId() + "-rollback",
		ActionType:     rollback.GetActionType(),
		Level:          action.GetLevel(),
		TargetId:       rollback.GetTargetId(),
		Binding:        cloneBinding(rollback.GetRestoreBinding()),
		Share:          before.sandbox.Share,
		Priority:       before.sandbox.Priority,
		PlanId:         action.GetPlanId(),
		SandboxId:      actionSandboxID(action),
		Deadline:       action.GetDeadline(),
		IdempotencyKey: action.GetIdempotencyKey() + ":rollback",
	}
	state := p.driverStateLocked()
	p.mu.Unlock()
	_, err := p.driver.ExecuteAction(context.WithoutCancel(ctx), state, rollbackAction)
	err = normalizeDriverError(rollbackAction, err)
	p.mu.Lock()
	if err == nil && restoreProjection {
		err = p.restoreActionBeforeImageLocked(before, protected)
	}
	return true, err
}

func mutationsForAction(action *tgsrlv1.Action, sandboxExists bool) sandboxMutation {
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_BIND:
		if !sandboxExists {
			return sandboxMutationExistence
		}
		return sandboxMutationState | sandboxMutationBinding | sandboxMutationShare | sandboxMutationPriority
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE, tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionType_ACTION_TYPE_SLEEP:
		return sandboxMutationState
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE:
		return sandboxMutationShare
	case tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY:
		return sandboxMutationPriority
	case tgsrlv1.ActionType_ACTION_TYPE_RESIZE:
		return sandboxMutationBinding
	case tgsrlv1.ActionType_ACTION_TYPE_RESUME, tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		return sandboxMutationState | sandboxMutationOffloaded
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND, tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		mutations := sandboxMutationGeneration | sandboxMutationState | sandboxMutationOffloaded
		if action.GetBinding() != nil {
			mutations |= sandboxMutationBinding
		}
		return mutations
	default:
		return 0
	}
}

func skippedActionResult(action *tgsrlv1.Action, revision uint64, now time.Time, message string) *tgsrlv1.ActionResult {
	return &tgsrlv1.ActionResult{
		ActionId:         action.GetActionId(),
		Status:           tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SKIPPED,
		StartedAt:        timestamppb.New(now),
		CompletedAt:      timestamppb.New(now),
		ObservedRevision: revision,
		ErrorCode:        base.ErrorCodeFailedPrecondition,
		ErrorMessage:     message,
		PlanId:           action.GetPlanId(),
		IdempotencyKey:   action.GetIdempotencyKey(),
	}
}

func errorCode(err error) string {
	var providerError *base.Error
	if errors.As(err, &providerError) {
		return providerError.Code
	}
	return base.ErrorCodeFailedPrecondition
}

func (p *Provider) applyReconcileStateLocked(state *ReconcileState) {
	if state == nil {
		return
	}
	p.healthy = state.Healthy
	p.healthReason = strings.TrimSpace(state.Reason)
	if len(state.Devices) > 0 {
		p.devices = cloneDevices(state.Devices)
	}
}

func firstPlanAction(plan *tgsrlv1.PlacementPlan) *tgsrlv1.Action {
	if plan == nil || len(plan.GetActions()) == 0 {
		return nil
	}
	return plan.GetActions()[0]
}

func transitionError(action *tgsrlv1.Action, sandbox base.Sandbox, message string) error {
	return &base.Error{Code: base.ErrorCodeFailedPrecondition, Message: message, PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: sandbox.SandboxID, ExpectedGeneration: action.GetExpectedGeneration(), ObservedGeneration: sandbox.Generation, Cause: base.ErrFailedPrecondition}
}

func validateActionPolicy(action *tgsrlv1.Action) error {
	err := actionpolicy.ValidateAction(action, tgsrlv1.TickKind_TICK_KIND_UNKNOWN, actionpolicy.ValidationOptions{
		AllowLegacyUnknownTickL1: true,
	})
	if err == nil {
		return nil
	}
	return translatePolicyError(action, err)
}

func validatePlanAt(plan *tgsrlv1.PlacementPlan) error {
	err := actionpolicy.ValidatePlan(plan, tgsrlv1.TickKind_TICK_KIND_UNKNOWN, actionpolicy.ValidationOptions{
		AllowLegacyUnknownTickL1: true,
	})
	if err == nil {
		return nil
	}
	return translatePolicyError(planActionForPolicyError(plan, err), err)
}

func supportedPlanCapabilities() []*tgsrlv1.CapabilityRequirement {
	capabilities := []*tgsrlv1.CapabilityRequirement{
		actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION),
		actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK),
	}
	for _, capability := range capabilities {
		capability.MinVersion = "1.0.0"
	}
	return capabilities
}

func translatePolicyError(action *tgsrlv1.Action, err error) error {
	var policyErr *actionpolicy.ValidationError
	if !errors.As(err, &policyErr) {
		return err
	}
	planID := ""
	actionID := ""
	sandboxID := ""
	if action != nil {
		planID = action.GetPlanId()
		actionID = action.GetActionId()
		sandboxID = actionSandboxID(action)
	}
	switch policyErr.Kind {
	case actionpolicy.ViolationInvalidArgument, actionpolicy.ViolationPrecondition,
		actionpolicy.ViolationExpectedImpact, actionpolicy.ViolationRollbackPolicy:
		return fmt.Errorf("%w: %s", base.ErrInvalidArgument, policyErr.Message)
	default:
		return &base.Error{Code: base.ErrorCodeUnsupported, Message: policyErr.Message, PlanID: planID, ActionID: actionID, SandboxID: sandboxID, Cause: base.ErrUnsupported}
	}
}

func containsSupportedAction(values []string, expected string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.ReplaceAll(value, "-", "_"), expected) {
			return true
		}
	}
	return false
}

func planActionForPolicyError(plan *tgsrlv1.PlacementPlan, err error) *tgsrlv1.Action {
	var policyErr *actionpolicy.ValidationError
	if errors.As(err, &policyErr) && policyErr.ActionID != "" && plan != nil {
		for _, action := range plan.GetActions() {
			if action != nil && action.GetActionId() == policyErr.ActionID {
				return action
			}
		}
	}
	return firstPlanAction(plan)
}

func appendResourceEvent(values []base.WatchedResourceEvent, value base.WatchedResourceEvent, retention int) []base.WatchedResourceEvent {
	values = append(values, value)
	if len(values) <= retention {
		return values
	}
	trimmed := make([]base.WatchedResourceEvent, retention)
	copy(trimmed, values[len(values)-retention:])
	return trimmed
}

func appendSandboxEvent(values []base.WatchedSandboxEvent, value base.WatchedSandboxEvent, retention int) []base.WatchedSandboxEvent {
	values = append(values, value)
	if len(values) <= retention {
		return values
	}
	trimmed := make([]base.WatchedSandboxEvent, retention)
	copy(trimmed, values[len(values)-retention:])
	return trimmed
}

func sandboxStateToRuntimeState(state base.SandboxState) tgsrlv1.RuntimeState {
	switch state {
	case base.SandboxStateRequested:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_REQUESTED
	case base.SandboxStateBound:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND
	case base.SandboxStateRunning:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING
	case base.SandboxStatePaused:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED
	case base.SandboxStateSleeping:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING
	case base.SandboxStateFailed:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED
	case base.SandboxStateTerminated:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED
	default:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN
	}
}

func sandboxStateToEventType(state base.SandboxState) tgsrlv1.SandboxEventType {
	switch state {
	case base.SandboxStateRequested:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_REQUESTED
	case base.SandboxStateBound:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_BOUND
	case base.SandboxStateRunning:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_RUNNING
	case base.SandboxStatePaused:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSED
	case base.SandboxStateSleeping:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_SLEEPING
	case base.SandboxStateFailed:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_FAILED
	case base.SandboxStateTerminated:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_TERMINATED
	default:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_UNKNOWN
	}
}

func nonZeroTime(value, fallback time.Time) time.Time {
	if value.IsZero() {
		return fallback
	}
	return value
}

func maxUint64(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}
