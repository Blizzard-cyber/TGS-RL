package nvidia

import (
	"context"
	"errors"
	"fmt"
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
	actions      map[string]actionLedgerEntry
	resourceSeq  uint64
	sandboxSeq   uint64
	resourceLog  []base.WatchedResourceEvent
	sandboxLog   []base.WatchedSandboxEvent
	resourceSubs map[uint64]chan base.WatchedResourceEvent
	sandboxSubs  map[uint64]chan base.WatchedSandboxEvent
	nextWatchID  uint64
}

var _ base.CompleteResourceProvider = (*Provider)(nil)

type actionLedgerEntry struct {
	action *tgsrlv1.Action
	result *tgsrlv1.ActionResult
	err    error
}

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
		actions:      map[string]actionLedgerEntry{},
		resourceSubs: map[uint64]chan base.WatchedResourceEvent{},
		sandboxSubs:  map[uint64]chan base.WatchedSandboxEvent{},
	}
	provider.publishCapabilityRefreshLocked()
	provider.publishSnapshotLocked(tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED)
	return provider, nil
}

func (p *Provider) ExecuteAction(ctx context.Context, action *tgsrlv1.Action) (*tgsrlv1.ActionResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	startedAt := p.now()
	p.mu.Lock()
	if !p.healthy {
		err := &base.Error{Code: ErrorCodeUnavailable, Message: p.healthReason, PlanID: action.GetPlanId(), ActionID: action.GetActionId(), Cause: base.ErrFailedPrecondition}
		result := actionErrorResult(action, p.revision, startedAt, p.now(), err)
		p.recordPlanResultLocked(action, result, err)
		p.mu.Unlock()
		return result, err
	}
	if action != nil && strings.TrimSpace(action.GetIdempotencyKey()) != "" {
		if existing, ok := p.actions[action.GetIdempotencyKey()]; ok {
			if proto.Equal(existing.action, action) {
				p.mu.Unlock()
				return cloneActionResult(existing.result), existing.err
			}
			err := &base.Error{Code: base.ErrorCodeIdempotencyConflict, Message: "idempotency key was reused with different action content", PlanID: action.GetPlanId(), ActionID: action.GetActionId(), Cause: base.ErrIdempotencyConflict}
			result := actionErrorResult(action, p.revision, startedAt, p.now(), err)
			p.recordPlanResultLocked(action, result, err)
			p.mu.Unlock()
			return result, err
		}
	}
	if err := validateActionAt(action, p.now(), p.capabilities); err != nil {
		result := actionErrorResult(action, p.revision, startedAt, p.now(), err)
		p.recordPlanResultLocked(action, result, err)
		if key := strings.TrimSpace(action.GetIdempotencyKey()); key != "" {
			p.actions[key] = actionLedgerEntry{action: cloneAction(action), result: cloneActionResult(result), err: err}
		}
		p.mu.Unlock()
		return result, err
	}
	state := p.driverStateLocked()
	p.mu.Unlock()

	execution, err := p.driver.ExecuteAction(ctx, state, action)
	err = normalizeDriverError(action, err)

	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		result := actionErrorResult(action, p.revision, startedAt, p.now(), err)
		p.cacheActionResultLocked(action, result, err)
		p.recordPlanResultLocked(action, result, err)
		return result, err
	}
	if err := p.applyActionLocked(action); err != nil {
		err = normalizeDriverError(action, err)
		result := actionErrorResult(action, p.revision, startedAt, p.now(), err)
		rollbackAttempted, rollbackErr := p.rollbackActionLocked(ctx, action)
		if rollbackAttempted {
			result.RollbackAttempted = true
			if rollbackErr != nil {
				result.RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED
				result.ErrorMessage = err.Error() + "; rollback failed: " + rollbackErr.Error()
			} else {
				result.RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED
			}
		}
		p.cacheActionResultLocked(action, result, err)
		p.recordPlanResultLocked(action, result, err)
		return result, err
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
	p.cacheActionResultLocked(action, result, nil)
	p.recordPlanResultLocked(action, result, nil)
	return cloneActionResult(result), nil
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
	p.mu.Lock()
	p.plans[plan.GetPlanId()] = &base.PlanRecord{Plan: clonePlacementPlan(plan), Status: base.PlanStatusInFlight, UpdatedAt: p.now(), ObservedRevision: p.revision}
	p.mu.Unlock()
	results := make([]*tgsrlv1.ActionResult, 0, len(plan.GetActions()))
	for _, action := range plan.GetActions() {
		result, err := p.ExecuteAction(ctx, action)
		results = append(results, result)
		if err != nil {
			p.mu.Lock()
			p.plans[plan.GetPlanId()] = &base.PlanRecord{
				Plan:             clonePlacementPlan(plan),
				Status:           base.PlanStatusFailed,
				Results:          cloneActionResults(results),
				ErrorMessage:     err.Error(),
				ObservedRevision: p.revision,
				UpdatedAt:        p.now(),
			}
			p.planErrors[plan.GetPlanId()] = err
			p.mu.Unlock()
			return cloneActionResults(results), err
		}
	}
	p.mu.Lock()
	p.plans[plan.GetPlanId()] = &base.PlanRecord{
		Plan:             clonePlacementPlan(plan),
		Status:           base.PlanStatusSucceeded,
		Results:          cloneActionResults(results),
		ObservedRevision: p.revision,
		UpdatedAt:        p.now(),
	}
	delete(p.planErrors, plan.GetPlanId())
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
	if current.SandboxID != "" && event.Generation < current.Generation {
		return &base.Error{Code: base.ErrorCodeLateEventFenced, Message: "event generation is older than current sandbox generation", SandboxID: event.SandboxID, ExpectedGeneration: current.Generation, ObservedGeneration: event.Generation, Cause: base.ErrGenerationFenced}
	}
	if current.SandboxID == "" {
		current = base.Sandbox{SandboxID: event.SandboxID}
	}
	current.State = event.State
	current.Generation = event.Generation
	if event.Binding != nil {
		current.Binding = cloneBinding(event.Binding)
	}
	if event.SafePoint != nil {
		current.SafePoint = *event.SafePoint
	}
	current.UpdatedAt = p.now()
	p.sandboxes[event.SandboxID] = current
	p.revision++
	p.publishSandboxSnapshotLocked(current, "runtime event applied")
	p.publishSnapshotLocked(tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED)
	return nil
}

func (p *Provider) applyActionLocked(action *tgsrlv1.Action) error {
	sandboxID := actionSandboxID(action)
	sandbox := p.sandboxes[sandboxID]
	if sandbox.SandboxID == "" && action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND {
		sandbox = base.Sandbox{SandboxID: sandboxID, State: base.SandboxStateRequested, Generation: maxUint64(1, action.GetExpectedGeneration())}
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
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_BIND:
		if action.GetBinding() == nil || len(action.GetBinding().GetDeviceIds()) == 0 {
			return fmt.Errorf("%w: bind requires binding device_ids", base.ErrInvalidArgument)
		}
		sandbox.State = base.SandboxStateBound
		sandbox.Binding = cloneBinding(action.GetBinding())
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
	sandbox.UpdatedAt = p.now()
	p.sandboxes[sandboxID] = sandbox
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
	return sandbox
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

func (p *Provider) cacheActionResultLocked(action *tgsrlv1.Action, result *tgsrlv1.ActionResult, err error) {
	if action == nil || strings.TrimSpace(action.GetIdempotencyKey()) == "" {
		return
	}
	p.actions[action.GetIdempotencyKey()] = actionLedgerEntry{
		action: cloneAction(action),
		result: cloneActionResult(result),
		err:    err,
	}
}

func (p *Provider) observedGenerationLocked(action *tgsrlv1.Action) uint64 {
	sandbox, ok := p.sandboxes[actionSandboxID(action)]
	if !ok {
		return 0
	}
	return sandbox.Generation
}

func (p *Provider) rollbackActionLocked(ctx context.Context, action *tgsrlv1.Action) (bool, error) {
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
		PlanId:         action.GetPlanId(),
		SandboxId:      actionSandboxID(action),
		Deadline:       action.GetDeadline(),
		IdempotencyKey: action.GetIdempotencyKey() + ":rollback",
	}
	state := p.driverStateLocked()
	p.mu.Unlock()
	_, err := p.driver.ExecuteAction(ctx, state, rollbackAction)
	err = normalizeDriverError(rollbackAction, err)
	p.mu.Lock()
	return true, err
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
	return translatePolicyError(firstPlanAction(plan), err)
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
	case actionpolicy.ViolationInvalidArgument:
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
