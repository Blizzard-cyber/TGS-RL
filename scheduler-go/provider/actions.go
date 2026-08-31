package provider

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

type actionBeforeImage struct {
	sandboxID string
	existed   bool
	sandbox   Sandbox
	mutations sandboxMutation
}

type orderedAction struct {
	index  int
	action *tgsrlv1.Action
}

// SetFaults atomically replaces the deterministic fault script.
func (p *MockResourceProvider) SetFaults(faults FaultOptions) {
	p.mu.Lock()
	p.faults = cloneFaults(faults)
	p.mu.Unlock()
}

// ExecuteAction validates, fences, and applies one idempotent action.
func (p *MockResourceProvider) ExecuteAction(ctx context.Context, action *tgsrlv1.Action) (*tgsrlv1.ActionResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.setActionPlanLocked(action)
	if action != nil && strings.TrimSpace(action.GetIdempotencyKey()) != "" {
		if existing, ok := p.actions[action.GetIdempotencyKey()]; ok {
			if proto.Equal(existing.action, action) {
				return cloneActionResult(existing.result), existing.err
			}
			err := &Error{Code: ErrorCodeIdempotencyConflict, Message: "idempotency key was reused with different action content", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), Cause: ErrIdempotencyConflict}
			result := p.resultForError(action, p.now(), err)
			p.updatePlanRecordForActionResultLocked(action, result, err)
			return result, err
		}
	}
	// A standalone action is fenced against the provider snapshot that its
	// caller observed. Plan execution deliberately bypasses this comparison:
	// PlacementPlan.snapshot_revision belongs to the Store's authoritative
	// revision domain and is atomically checked by Store.ReservePlan.
	if action != nil && action.GetExpectedSnapshotRevision() != p.revision {
		err := &Error{
			Code:             ErrorCodeRevisionConflict,
			Message:          "action snapshot revision is stale",
			PlanID:           action.GetPlanId(),
			ActionID:         action.GetActionId(),
			ExpectedRevision: action.GetExpectedSnapshotRevision(),
			ObservedRevision: p.revision,
			Cause:            ErrFailedPrecondition,
		}
		result := p.resultForError(action, p.now(), err)
		p.updatePlanRecordForActionResultLocked(action, result, err)
		return result, err
	}
	return p.executeActionLocked(ctx, action)
}

func (p *MockResourceProvider) validateRollbackDeclarationLocked(action *tgsrlv1.Action) error {
	rollback := action.GetRollback()
	expectedType, supported := rollbackActionType(action.GetActionType())
	if !supported || rollback.GetActionType() != expectedType {
		return fmt.Errorf("%w: action %q rollback action_type %s does not compensate %s", ErrInvalidArgument, action.GetActionId(), rollback.GetActionType(), action.GetActionType())
	}

	sandboxID := actionSandboxID(action)
	if strings.TrimSpace(sandboxID) == "" {
		return fmt.Errorf("%w: action %q has no rollback sandbox target", ErrInvalidArgument, action.GetActionId())
	}
	expectedTarget := sandboxID
	if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND {
		expectedTarget = action.GetBinding().GetBindingId()
	}
	if strings.TrimSpace(expectedTarget) == "" || rollback.GetTargetId() != expectedTarget {
		return fmt.Errorf("%w: action %q rollback target_id %q does not match %q", ErrInvalidArgument, action.GetActionId(), rollback.GetTargetId(), expectedTarget)
	}

	requiresBinding := rollbackRequiresBinding(action.GetActionType())
	if requiresBinding && rollback.GetRestoreBinding() == nil {
		return fmt.Errorf("%w: action %q rollback requires restore_binding", ErrInvalidArgument, action.GetActionId())
	}
	if !requiresBinding && rollback.GetRestoreBinding() != nil {
		return fmt.Errorf("%w: action %q rollback must not declare restore_binding", ErrInvalidArgument, action.GetActionId())
	}
	if restore := rollback.GetRestoreBinding(); restore != nil {
		if strings.TrimSpace(restore.GetBindingId()) == "" || strings.TrimSpace(restore.GetSandboxId()) == "" {
			return fmt.Errorf("%w: action %q rollback restore_binding requires binding_id and sandbox_id", ErrInvalidArgument, action.GetActionId())
		}
		if restore.GetSandboxId() != sandboxID {
			return fmt.Errorf("%w: action %q rollback restore_binding targets another sandbox", ErrInvalidArgument, action.GetActionId())
		}
	}
	return nil
}

func (p *MockResourceProvider) validateRollbackBeforeImageLocked(action *tgsrlv1.Action, before actionBeforeImage) error {
	if !rollbackRequiresBinding(action.GetActionType()) || !before.existed {
		return nil
	}
	if !proto.Equal(action.GetRollback().GetRestoreBinding(), before.sandbox.Binding) {
		return fmt.Errorf("%w: action %q rollback restore_binding does not match the target before-image", ErrInvalidArgument, action.GetActionId())
	}
	return nil
}

func (p *MockResourceProvider) captureActionBeforeImageLocked(action *tgsrlv1.Action) actionBeforeImage {
	sandboxID := actionSandboxID(action)
	sandbox, existed := p.sandboxes[sandboxID]
	return actionBeforeImage{
		sandboxID: sandboxID,
		existed:   existed,
		sandbox:   cloneSandbox(sandbox),
		mutations: mutationsForAction(action, existed),
	}
}

func (p *MockResourceProvider) restoreActionBeforeImageLocked(action *tgsrlv1.Action, before actionBeforeImage, protected sandboxMutation) error {
	if err := p.validateRollbackDeclarationLocked(action); err != nil {
		return err
	}
	if err := p.validateRollbackBeforeImageLocked(action, before); err != nil {
		return err
	}
	if before.mutations&sandboxMutationExistence != 0 {
		if protected == 0 {
			delete(p.sandboxes, before.sandboxID)
		}
		return nil
	}

	current, exists := p.sandboxes[before.sandboxID]
	if !exists {
		return fmt.Errorf("%w: action %q rollback target no longer exists", ErrFailedPrecondition, action.GetActionId())
	}
	mutations := before.mutations &^ protected
	if mutations == 0 {
		return nil
	}
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
	return nil
}

func rollbackActionType(actionType tgsrlv1.ActionType) (tgsrlv1.ActionType, bool) {
	switch actionType {
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE:
		return tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, true
	case tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY:
		return tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, true
	case tgsrlv1.ActionType_ACTION_TYPE_RESIZE:
		return tgsrlv1.ActionType_ACTION_TYPE_RESIZE, true
	case tgsrlv1.ActionType_ACTION_TYPE_PAUSE:
		return tgsrlv1.ActionType_ACTION_TYPE_RESUME, true
	case tgsrlv1.ActionType_ACTION_TYPE_RESUME:
		return tgsrlv1.ActionType_ACTION_TYPE_PAUSE, true
	case tgsrlv1.ActionType_ACTION_TYPE_SLEEP, tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		return tgsrlv1.ActionType_ACTION_TYPE_RESUME, true
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND:
		return tgsrlv1.ActionType_ACTION_TYPE_REBIND, true
	case tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		return tgsrlv1.ActionType_ACTION_TYPE_RECREATE, true
	case tgsrlv1.ActionType_ACTION_TYPE_BIND:
		return tgsrlv1.ActionType_ACTION_TYPE_RELEASE, true
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		return tgsrlv1.ActionType_ACTION_TYPE_BIND, true
	default:
		return tgsrlv1.ActionType_ACTION_TYPE_UNKNOWN, false
	}
}

func rollbackRequiresBinding(actionType tgsrlv1.ActionType) bool {
	switch actionType {
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE, tgsrlv1.ActionType_ACTION_TYPE_RESIZE, tgsrlv1.ActionType_ACTION_TYPE_REBIND, tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		return true
	default:
		return false
	}
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

// ObserveSandbox applies an execution-substrate observation unless its
// generation is older than the authoritative sandbox generation.
func (p *MockResourceProvider) ObserveSandbox(ctx context.Context, event *tgsrlv1.SandboxEvent) (*tgsrlv1.SandboxEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if event == nil {
		return nil, fmt.Errorf("%w: sandbox event is required", ErrInvalidArgument)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if previous, duplicate := p.events[event.GetEventId()]; duplicate {
		candidate := NormalizeSandboxObservation(event, p.sandboxes[event.GetSandboxId()], previous.GetProviderRevision())
		if proto.Equal(previous, candidate) {
			return proto.Clone(previous).(*tgsrlv1.SandboxEvent), nil
		}
		return nil, fmt.Errorf("%w: event_id %q was reused with different content", ErrIdempotencyConflict, event.GetEventId())
	}
	updated, err := ProjectSandboxObservation(p.sandboxes[event.GetSandboxId()], event, p.now())
	if err != nil {
		return nil, err
	}
	p.sandboxes[event.GetSandboxId()] = updated
	p.revision++
	accepted := NormalizeSandboxObservation(event, updated, p.revision)
	p.events[event.GetEventId()] = accepted
	p.publishSandboxEventLocked(accepted)
	p.publishSnapshotLocked(tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED)
	return proto.Clone(accepted).(*tgsrlv1.SandboxEvent), nil
}

func (p *MockResourceProvider) executeActionLocked(ctx context.Context, action *tgsrlv1.Action) (*tgsrlv1.ActionResult, error) {
	startedAt := p.now()
	if action != nil && strings.TrimSpace(action.GetIdempotencyKey()) != "" {
		if existing, ok := p.actions[action.GetIdempotencyKey()]; ok {
			if proto.Equal(existing.action, action) {
				return cloneActionResult(existing.result), existing.err
			}
			err := &Error{Code: ErrorCodeIdempotencyConflict, Message: "idempotency key was reused with different action content", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), Cause: ErrIdempotencyConflict}
			result := p.resultForError(action, startedAt, err)
			p.updatePlanRecordForActionResultLocked(action, result, err)
			return result, err
		}
	}
	if err := p.validateActionLocked(action); err != nil {
		result := p.resultForError(action, startedAt, err)
		p.updatePlanRecordForActionResultLocked(action, result, err)
		return result, err
	}
	key := action.GetIdempotencyKey()
	if err := p.waitLocked(ctx, action); err != nil {
		result := p.resultForError(action, startedAt, err)
		p.actions[key] = actionLedgerEntry{action: cloneAction(action), result: cloneActionResult(result), err: err}
		p.updatePlanRecordForActionResultLocked(action, result, err)
		return result, err
	}
	if failure, fail := p.faults.FailActionIDs[action.GetActionId()]; fail {
		code := failure.Code
		if code == "" {
			code = ErrorCodeInjectedFailure
		}
		err := &Error{Code: code, Message: failure.Message, PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: actionSandboxID(action), Cause: ErrFailedPrecondition}
		result := p.resultForError(action, startedAt, err)
		p.actions[key] = actionLedgerEntry{action: cloneAction(action), result: cloneActionResult(result), err: err}
		p.updatePlanRecordForActionResultLocked(action, result, err)
		return result, err
	}

	if err := p.applyActionLocked(action); err != nil {
		result := p.resultForError(action, startedAt, err)
		p.actions[key] = actionLedgerEntry{action: cloneAction(action), result: cloneActionResult(result), err: err}
		p.updatePlanRecordForActionResultLocked(action, result, err)
		return result, err
	}
	p.revision++
	result := &tgsrlv1.ActionResult{
		ActionId:         action.GetActionId(),
		Status:           tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED,
		StartedAt:        timestamppb.New(startedAt),
		CompletedAt:      timestamppb.New(p.now()),
		ObservedRevision: p.revision,
		PlanId:           action.GetPlanId(),
		IdempotencyKey:   action.GetIdempotencyKey(),
	}
	p.actions[key] = actionLedgerEntry{action: cloneAction(action), result: cloneActionResult(result)}
	p.updatePlanRecordForActionResultLocked(action, result, nil)
	sandboxID := actionSandboxID(action)
	if sandbox, ok := p.sandboxes[sandboxID]; ok {
		p.publishSandboxSnapshotLocked(sandbox, "action applied")
	}
	p.publishSnapshotLocked(tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED)
	return cloneActionResult(result), nil
}

func (p *MockResourceProvider) validateActionLocked(action *tgsrlv1.Action) error {
	if action == nil {
		return fmt.Errorf("%w: action is nil", ErrInvalidArgument)
	}
	if strings.TrimSpace(action.GetActionId()) == "" || strings.TrimSpace(action.GetPlanId()) == "" || strings.TrimSpace(action.GetIdempotencyKey()) == "" {
		return fmt.Errorf("%w: action_id, plan_id, and idempotency_key are required", ErrInvalidArgument)
	}
	if err := validateActionPolicy(action); err != nil {
		return err
	}
	name := actionNames[action.GetActionType()]
	if !containsString(p.capabilities.GetSupportedActions(), name) {
		return &Error{Code: ErrorCodeUnsupported, Message: "action is not advertised by provider capabilities", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), Cause: ErrUnsupported}
	}
	if detail := MissingCapabilities(p.capabilities, action.GetRequiredCapabilities()); detail != "" {
		return &Error{Code: ErrorCodeUnsupported, Message: detail, PlanID: action.GetPlanId(), ActionID: action.GetActionId(), Cause: ErrUnsupported}
	}
	if action.GetDeadline() == nil {
		return fmt.Errorf("%w: action deadline is required", ErrInvalidArgument)
	}
	if err := action.GetDeadline().CheckValid(); err != nil {
		return fmt.Errorf("%w: invalid action deadline: %v", ErrInvalidArgument, err)
	}
	if !p.now().Before(action.GetDeadline().AsTime()) {
		return &Error{Code: ErrorCodeDeadlineExceeded, Message: "action deadline has elapsed", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), Cause: ErrDeadlineExceeded}
	}
	if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND {
		if action.GetBinding() == nil || len(action.GetBinding().GetDeviceIds()) == 0 {
			return fmt.Errorf("%w: bind requires a binding and logical device", ErrInvalidArgument)
		}
		for _, deviceID := range action.GetBinding().GetDeviceIds() {
			if !p.hasDeviceLocked(deviceID) {
				return &Error{Code: ErrorCodeFailedPrecondition, Message: "binding references unknown logical device", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), Cause: ErrFailedPrecondition}
			}
		}
	}
	return nil
}

func (p *MockResourceProvider) waitLocked(ctx context.Context, action *tgsrlv1.Action) error {
	delay := p.faults.Delay
	if configured := p.faults.DelayByActionType[action.GetActionType()]; configured > 0 {
		delay = configured
	}
	if delay <= 0 {
		return ctx.Err()
	}
	remaining := action.GetDeadline().AsTime().Sub(p.now())
	if remaining <= 0 || delay >= remaining {
		return &Error{Code: ErrorCodeDeadlineExceeded, Message: "configured action delay exceeds deadline", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), Cause: ErrDeadlineExceeded}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return &Error{Code: ErrorCodeDeadlineExceeded, Message: ctx.Err().Error(), PlanID: action.GetPlanId(), ActionID: action.GetActionId(), Cause: errors.Join(ErrDeadlineExceeded, ctx.Err())}
	case <-timer.C:
		return nil
	}
}

func (p *MockResourceProvider) applyActionLocked(action *tgsrlv1.Action) error {
	sandboxID := actionSandboxID(action)
	sandbox, exists := p.sandboxes[sandboxID]
	created := false
	if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND && !exists {
		sandbox = Sandbox{SandboxID: sandboxID, State: SandboxStateRequested, Generation: action.GetExpectedGeneration()}
		if sandbox.Generation == 0 {
			sandbox.Generation = 1
		}
		exists = true
		created = true
	}
	if !exists {
		return &Error{Code: ErrorCodeNotFound, Message: "sandbox does not exist", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: sandboxID, Cause: ErrNotFound}
	}
	if action.GetExpectedGeneration() != 0 && action.GetExpectedGeneration() != sandbox.Generation {
		return &Error{Code: ErrorCodeGenerationConflict, Message: "sandbox generation does not match", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: sandboxID, ExpectedGeneration: action.GetExpectedGeneration(), ObservedGeneration: sandbox.Generation, Cause: ErrFailedPrecondition}
	}
	// Initial binding creates a sandbox and therefore has no prior runtime
	// state whose mutation must wait for a safe point. Safe-point fencing still
	// applies to every action that mutates an existing sandbox.
	if action.GetRequiresSafePoint() && action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND && !sandbox.SafePoint {
		return &Error{Code: ErrorCodeFailedPrecondition, Message: "sandbox is not at a safe point", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: sandboxID, Cause: ErrFailedPrecondition}
	}

	previousState := sandbox.State
	previousGeneration := sandbox.Generation
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_BIND:
		if sandbox.State != SandboxStateRequested && sandbox.State != SandboxStateTerminated {
			return transitionError(action, sandbox, "bind requires requested or terminated state")
		}
		sandbox.State = SandboxStateBound
		sandbox.Binding = cloneBinding(action.GetBinding())
		sandbox.Share = action.GetShare()
		sandbox.Priority = action.GetPriority()
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		if sandbox.State == SandboxStateTerminated {
			return transitionError(action, sandbox, "sandbox is already terminated")
		}
		sandbox.State = SandboxStateTerminated
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE:
		if math.IsNaN(action.GetShare()) || math.IsInf(action.GetShare(), 0) || action.GetShare() < 0 || action.GetShare() > 1 {
			return fmt.Errorf("%w: share must be finite and within [0,1]", ErrInvalidArgument)
		}
		sandbox.Share = action.GetShare()
	case tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY:
		sandbox.Priority = action.GetPriority()
	case tgsrlv1.ActionType_ACTION_TYPE_RESIZE:
		if action.GetBinding() == nil || action.GetBinding().GetResources() == nil {
			return fmt.Errorf("%w: resize requires binding resources", ErrInvalidArgument)
		}
		sandbox.Binding = cloneBinding(action.GetBinding())
	case tgsrlv1.ActionType_ACTION_TYPE_PAUSE:
		if sandbox.State != SandboxStateRunning {
			return transitionError(action, sandbox, "pause requires running state")
		}
		sandbox.State = SandboxStatePaused
	case tgsrlv1.ActionType_ACTION_TYPE_RESUME:
		if sandbox.State != SandboxStatePaused && sandbox.State != SandboxStateSleeping {
			return transitionError(action, sandbox, "resume requires paused or sleeping state")
		}
		sandbox.State = SandboxStateRunning
		sandbox.Offloaded = false
	case tgsrlv1.ActionType_ACTION_TYPE_SLEEP:
		if sandbox.State != SandboxStateRunning && sandbox.State != SandboxStatePaused {
			return transitionError(action, sandbox, "sleep requires running or paused state")
		}
		sandbox.State = SandboxStateSleeping
	case tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		if sandbox.State != SandboxStateRunning && sandbox.State != SandboxStatePaused && sandbox.State != SandboxStateSleeping {
			return transitionError(action, sandbox, "offload requires running, paused, or sleeping state")
		}
		sandbox.State = SandboxStateSleeping
		sandbox.Offloaded = true
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND, tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		sandbox.Generation++
		sandbox.State = SandboxStateBound
		if action.GetBinding() != nil {
			sandbox.Binding = cloneBinding(action.GetBinding())
			sandbox.Binding.Generation = sandbox.Generation
		}
		sandbox.Offloaded = false
	}
	confirmedAt := p.now()
	sandbox.UpdatedAt = confirmedAt
	if created || sandbox.State != previousState || sandbox.Generation != previousGeneration {
		sandbox.StateChangedAt = confirmedAt
	}
	p.sandboxes[sandboxID] = sandbox
	return nil
}

func (p *MockResourceProvider) resultForError(action *tgsrlv1.Action, startedAt time.Time, err error) *tgsrlv1.ActionResult {
	result := &tgsrlv1.ActionResult{Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED, StartedAt: timestamppb.New(startedAt), CompletedAt: timestamppb.New(p.now()), ErrorMessage: err.Error(), ObservedRevision: p.revision}
	if action != nil {
		result.ActionId = action.GetActionId()
		result.PlanId = action.GetPlanId()
		result.IdempotencyKey = action.GetIdempotencyKey()
	}
	var providerError *Error
	if errors.As(err, &providerError) {
		result.ErrorCode = providerError.Code
	} else {
		result.ErrorCode = ErrorCodeInvalidArgument
	}
	return result
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

func validatePlanPolicy(plan *tgsrlv1.PlacementPlan) error {
	err := actionpolicy.ValidatePlan(plan, tgsrlv1.TickKind_TICK_KIND_UNKNOWN, actionpolicy.ValidationOptions{
		AllowLegacyUnknownTickL1: true,
	})
	if err == nil {
		return nil
	}
	action := planActionForPolicyError(plan, err)
	return translatePolicyError(action, err)
}

func translatePolicyError(action *tgsrlv1.Action, err error) error {
	var policyErr *actionpolicy.ValidationError
	if !errors.As(err, &policyErr) {
		return err
	}
	planID := ""
	actionID := ""
	if action != nil {
		planID = action.GetPlanId()
		actionID = action.GetActionId()
	}
	switch policyErr.Kind {
	case actionpolicy.ViolationInvalidArgument, actionpolicy.ViolationPrecondition,
		actionpolicy.ViolationExpectedImpact, actionpolicy.ViolationRollbackPolicy:
		return fmt.Errorf("%w: %s", ErrInvalidArgument, policyErr.Message)
	default:
		return &Error{Code: ErrorCodeUnsupported, Message: policyErr.Message, PlanID: planID, ActionID: actionID, Cause: ErrUnsupported}
	}
}

func actionSandboxID(action *tgsrlv1.Action) string {
	if action.GetSandboxId() != "" {
		return action.GetSandboxId()
	}
	if action.GetBinding().GetSandboxId() != "" {
		return action.GetBinding().GetSandboxId()
	}
	return action.GetTargetId()
}

func firstPlanAction(plan *tgsrlv1.PlacementPlan) *tgsrlv1.Action {
	if plan == nil || len(plan.GetActions()) == 0 {
		return nil
	}
	return plan.GetActions()[0]
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

func transitionError(action *tgsrlv1.Action, sandbox Sandbox, message string) error {
	return &Error{Code: ErrorCodeFailedPrecondition, Message: message, PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: sandbox.SandboxID, ExpectedGeneration: action.GetExpectedGeneration(), ObservedGeneration: sandbox.Generation, Cause: ErrFailedPrecondition}
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.ReplaceAll(value, "-", "_"), expected) {
			return true
		}
	}
	return false
}

// MissingCapabilities returns a stable reason when available capabilities do
// not contain the complete required capability surface.
func MissingCapabilities(available, required *tgsrlv1.CapabilitySet) string {
	if required == nil {
		return ""
	}
	if available == nil {
		return "provider capability evidence is unavailable"
	}
	if required.GetSource() != "" && required.GetSource() != available.GetSource() {
		return "required capability source is unavailable"
	}
	if available.GetRevision() < required.GetRevision() {
		return "required capability revision is unavailable"
	}
	for _, requiredName := range required.GetNames() {
		if !containsExact(available.GetNames(), requiredName) {
			return "required capability is unavailable: " + requiredName
		}
	}
	for _, algorithm := range required.GetAlgorithms() {
		if !containsExact(available.GetAlgorithms(), algorithm) {
			return "required algorithm capability is unavailable: " + algorithm
		}
	}
	for _, rolloutMode := range required.GetRolloutModes() {
		if !containsExact(available.GetRolloutModes(), rolloutMode) {
			return "required rollout mode capability is unavailable: " + rolloutMode
		}
	}
	for key, value := range required.GetAttributes() {
		if available.GetAttributes()[key] != value {
			return "required capability attribute is unavailable: " + key
		}
	}
	for key, value := range required.GetLimits() {
		if available.GetLimits()[key] < value {
			return "required capability limit is unavailable: " + key
		}
	}
	for _, requiredAction := range required.GetSupportedActions() {
		if !containsString(available.GetSupportedActions(), requiredAction) {
			return "required action is unavailable: " + requiredAction
		}
	}
	return ""
}

func containsExact(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func (p *MockResourceProvider) hasDeviceLocked(deviceID string) bool {
	for _, device := range p.devices {
		if device.GetDeviceId() == deviceID {
			return true
		}
	}
	return false
}

func eventWouldRegress(current, next SandboxState) bool {
	rank := map[SandboxState]int{
		SandboxStateRequested: 1, SandboxStateBound: 2, SandboxStateRunning: 3, SandboxStatePaused: 4, SandboxStateSleeping: 5, SandboxStateFailed: 6, SandboxStateTerminated: 7,
	}
	if current == SandboxStatePaused && next == SandboxStateRunning {
		return false
	}
	if current == SandboxStateSleeping && next == SandboxStateRunning {
		return false
	}
	return rank[next] < rank[current]
}
