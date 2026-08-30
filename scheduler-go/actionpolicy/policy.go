package actionpolicy

import (
	"fmt"
	"sort"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

// ValidationOptions controls compatibility exceptions. Executor capability
// support is checked separately at the provider boundary.
type ValidationOptions struct {
	AllowLegacyUnknownTickL1 bool
}

type ViolationKind string

const (
	ViolationInvalidArgument       ViolationKind = "invalid_argument"
	ViolationUnknownAction         ViolationKind = "unknown_action"
	ViolationLevelMismatch         ViolationKind = "level_mismatch"
	ViolationUnknownTick           ViolationKind = "unknown_tick"
	ViolationTickMismatch          ViolationKind = "tick_mismatch"
	ViolationTickExceedsCeiling    ViolationKind = "tick_exceeds_ceiling"
	ViolationUnknownPurpose        ViolationKind = "unknown_purpose"
	ViolationPurposeMismatch       ViolationKind = "purpose_mismatch"
	ViolationPrecondition          ViolationKind = "precondition"
	ViolationExpectedImpact        ViolationKind = "expected_impact"
	ViolationRollbackPolicy        ViolationKind = "rollback_policy"
	ViolationCapabilityRequirement ViolationKind = "capability_requirement"
)

type ValidationError struct {
	Kind                  ViolationKind
	Message               string
	ActionID              string
	ActionType            tgsrlv1.ActionType
	DeclaredLevel         tgsrlv1.ActionLevel
	ExpectedLevel         tgsrlv1.ActionLevel
	TickKind              tgsrlv1.TickKind
	ExpectedTick          tgsrlv1.TickKind
	Ceiling               tgsrlv1.ActionLevel
	Purpose               tgsrlv1.PlanPurpose
	CapabilityRequirement tgsrlv1.CapabilityRequirementKind
}

func (e *ValidationError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Message
}

type Definition struct {
	Level                    tgsrlv1.ActionLevel
	CapabilityName           string
	CompensationType         tgsrlv1.ActionType
	CompensationNeedsBinding bool
	ExpectedImpacts          []tgsrlv1.ExpectedImpact
}

func DefinitionForAction(actionType tgsrlv1.ActionType) (Definition, bool) {
	runtimeImpact := []tgsrlv1.ExpectedImpact{tgsrlv1.ExpectedImpact_EXPECTED_IMPACT_RUNTIME_STATE_CHANGED}
	switch actionType {
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE:
		return Definition{Level: tgsrlv1.ActionLevel_ACTION_LEVEL_L1, CapabilityName: "set_share", CompensationType: actionType, ExpectedImpacts: runtimeImpact}, true
	case tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY:
		return Definition{Level: tgsrlv1.ActionLevel_ACTION_LEVEL_L1, CapabilityName: "set_priority", CompensationType: actionType, ExpectedImpacts: runtimeImpact}, true
	case tgsrlv1.ActionType_ACTION_TYPE_RESIZE:
		return Definition{Level: tgsrlv1.ActionLevel_ACTION_LEVEL_L1, CapabilityName: "resize", CompensationType: actionType, CompensationNeedsBinding: true, ExpectedImpacts: runtimeImpact}, true
	case tgsrlv1.ActionType_ACTION_TYPE_BIND:
		return Definition{Level: tgsrlv1.ActionLevel_ACTION_LEVEL_L1, CapabilityName: "bind", CompensationType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, ExpectedImpacts: []tgsrlv1.ExpectedImpact{tgsrlv1.ExpectedImpact_EXPECTED_IMPACT_ALLOCATION_CREATED, tgsrlv1.ExpectedImpact_EXPECTED_IMPACT_CAPACITY_RESERVED}}, true
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		return Definition{Level: tgsrlv1.ActionLevel_ACTION_LEVEL_L1, CapabilityName: "release", CompensationType: tgsrlv1.ActionType_ACTION_TYPE_BIND, CompensationNeedsBinding: true, ExpectedImpacts: []tgsrlv1.ExpectedImpact{tgsrlv1.ExpectedImpact_EXPECTED_IMPACT_ALLOCATION_RELEASED, tgsrlv1.ExpectedImpact_EXPECTED_IMPACT_CAPACITY_RELEASED}}, true
	case tgsrlv1.ActionType_ACTION_TYPE_PAUSE:
		return Definition{Level: tgsrlv1.ActionLevel_ACTION_LEVEL_L2, CapabilityName: "pause", CompensationType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, ExpectedImpacts: runtimeImpact}, true
	case tgsrlv1.ActionType_ACTION_TYPE_RESUME:
		return Definition{Level: tgsrlv1.ActionLevel_ACTION_LEVEL_L2, CapabilityName: "resume", CompensationType: tgsrlv1.ActionType_ACTION_TYPE_PAUSE, ExpectedImpacts: runtimeImpact}, true
	case tgsrlv1.ActionType_ACTION_TYPE_SLEEP:
		return Definition{Level: tgsrlv1.ActionLevel_ACTION_LEVEL_L3, CapabilityName: "sleep", CompensationType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, ExpectedImpacts: runtimeImpact}, true
	case tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		return Definition{Level: tgsrlv1.ActionLevel_ACTION_LEVEL_L3, CapabilityName: "offload", CompensationType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, ExpectedImpacts: runtimeImpact}, true
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND:
		return Definition{Level: tgsrlv1.ActionLevel_ACTION_LEVEL_L4, CapabilityName: "rebind", CompensationType: actionType, CompensationNeedsBinding: true, ExpectedImpacts: runtimeImpact}, true
	case tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		return Definition{Level: tgsrlv1.ActionLevel_ACTION_LEVEL_L4, CapabilityName: "recreate", CompensationType: actionType, CompensationNeedsBinding: true, ExpectedImpacts: runtimeImpact}, true
	default:
		return Definition{}, false
	}
}

func LevelForAction(actionType tgsrlv1.ActionType) (tgsrlv1.ActionLevel, bool) {
	definition, ok := DefinitionForAction(actionType)
	return definition.Level, ok
}

func CeilingForTick(tick tgsrlv1.TickKind) (tgsrlv1.ActionLevel, bool) {
	switch tick {
	case tgsrlv1.TickKind_TICK_KIND_FAST:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_L1, true
	case tgsrlv1.TickKind_TICK_KIND_MEDIUM:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_L3, true
	case tgsrlv1.TickKind_TICK_KIND_SLOW:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_L4, true
	default:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_UNKNOWN, false
	}
}

func ValidateAction(action *tgsrlv1.Action, expectedTick tgsrlv1.TickKind, options ValidationOptions) error {
	if action == nil {
		return &ValidationError{Kind: ViolationInvalidArgument, Message: "action is required"}
	}
	definition, ok := DefinitionForAction(action.GetActionType())
	if !ok {
		return actionError(ViolationUnknownAction, action, fmt.Sprintf("action %q has unsupported action_type %s", action.GetActionId(), action.GetActionType()))
	}
	if action.GetLevel() != definition.Level {
		err := actionError(ViolationLevelMismatch, action, fmt.Sprintf("action %q declares level %s, want %s for %s", action.GetActionId(), action.GetLevel(), definition.Level, action.GetActionType()))
		err.ExpectedLevel = definition.Level
		return err
	}
	if action.GetTickKind() == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		if options.AllowLegacyUnknownTickL1 && definition.Level == tgsrlv1.ActionLevel_ACTION_LEVEL_L1 {
			return validateSuppliedActionMetadata(action, definition)
		}
		err := actionError(ViolationUnknownTick, action, fmt.Sprintf("action %q uses unknown tick_kind for level %s", action.GetActionId(), action.GetLevel()))
		err.ExpectedTick = expectedTick
		return err
	}
	if expectedTick != tgsrlv1.TickKind_TICK_KIND_UNKNOWN && action.GetTickKind() != expectedTick {
		err := actionError(ViolationTickMismatch, action, fmt.Sprintf("action %q tick_kind %s does not match expected tick %s", action.GetActionId(), action.GetTickKind(), expectedTick))
		err.ExpectedTick = expectedTick
		return err
	}
	ceiling, ok := CeilingForTick(action.GetTickKind())
	if !ok {
		return actionError(ViolationUnknownTick, action, fmt.Sprintf("action %q uses unsupported tick_kind %s", action.GetActionId(), action.GetTickKind()))
	}
	if action.GetLevel() > ceiling {
		err := actionError(ViolationTickExceedsCeiling, action, fmt.Sprintf("action %q level %s exceeds tick %s ceiling %s", action.GetActionId(), action.GetLevel(), action.GetTickKind(), ceiling))
		err.Ceiling = ceiling
		return err
	}
	return validateSuppliedActionMetadata(action, definition)
}

func ValidatePlan(plan *tgsrlv1.PlacementPlan, expectedTick tgsrlv1.TickKind, options ValidationOptions) error {
	if plan == nil {
		return &ValidationError{Kind: ViolationInvalidArgument, Message: "plan is required"}
	}
	purpose, legacy, err := EffectivePurpose(plan)
	if err != nil {
		return err
	}
	resolvedTick := expectedTick
	if resolvedTick == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		resolvedTick = derivePlanTick(plan)
	}
	if !legacy {
		if err := validatePlanContract(plan, purpose); err != nil {
			return err
		}
		if err := validateActionOrder(plan); err != nil {
			return err
		}
	}
	for index, action := range plan.GetActions() {
		if action == nil {
			return &ValidationError{Kind: ViolationInvalidArgument, Message: fmt.Sprintf("plan %q contains nil action at index %d", plan.GetPlanId(), index), Purpose: purpose}
		}
		if err := ValidateAction(action, resolvedTick, options); err != nil {
			return err
		}
		if legacy {
			continue
		}
		if err := validatePurposeAction(plan, purpose, action); err != nil {
			return err
		}
		if plan.GetRollbackPolicy() == tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION {
			if err := ValidateRollback(action); err != nil {
				return err
			}
		}
		definition, _ := DefinitionForAction(action.GetActionType())
		if err := validateExplicitActionContract(plan, action, definition); err != nil {
			return err
		}
	}
	return nil
}

func EffectivePurpose(plan *tgsrlv1.PlacementPlan) (tgsrlv1.PlanPurpose, bool, error) {
	if plan == nil {
		return tgsrlv1.PlanPurpose_PLAN_PURPOSE_UNKNOWN, false, &ValidationError{Kind: ViolationInvalidArgument, Message: "plan is required"}
	}
	switch plan.GetPurpose() {
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION, tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE, tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION, tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY, tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION:
		return plan.GetPurpose(), false, nil
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_UNKNOWN:
		if isLegacyBindOnlyPlan(plan) {
			return tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION, true, nil
		}
		return plan.GetPurpose(), false, &ValidationError{Kind: ViolationUnknownPurpose, Message: fmt.Sprintf("plan %q has unknown purpose and is not a legacy bind-only plan", plan.GetPlanId())}
	default:
		return plan.GetPurpose(), false, &ValidationError{Kind: ViolationUnknownPurpose, Message: fmt.Sprintf("plan %q has unsupported purpose %d", plan.GetPlanId(), plan.GetPurpose()), Purpose: plan.GetPurpose()}
	}
}

func isLegacyBindOnlyPlan(plan *tgsrlv1.PlacementPlan) bool {
	if plan == nil || len(plan.GetActions()) == 0 || plan.GetRollbackPolicy() != tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_UNKNOWN || len(plan.GetCapabilityRequirements()) != 0 || len(plan.GetAffectedAllocationIds()) != 0 {
		return false
	}
	for _, action := range plan.GetActions() {
		if action == nil || action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND || action.GetLevel() != tgsrlv1.ActionLevel_ACTION_LEVEL_L1 || action.GetRollback() == nil || action.GetRollback().GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_RELEASE || len(action.GetPreconditions()) != 0 || len(action.GetExpectedImpacts()) != 0 {
			return false
		}
	}
	return true
}

func validatePlanContract(plan *tgsrlv1.PlacementPlan, purpose tgsrlv1.PlanPurpose) error {
	if len(plan.GetActions()) == 0 {
		if plan.GetRollbackPolicy() != tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_NOT_REQUIRED || len(plan.GetCapabilityRequirements()) != 0 || len(plan.GetAffectedAllocationIds()) != 0 {
			return &ValidationError{Kind: ViolationRollbackPolicy, Message: fmt.Sprintf("empty plan %q must declare rollback not required and no mutation requirements", plan.GetPlanId()), Purpose: purpose}
		}
		return nil
	}
	if plan.GetRollbackPolicy() == tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_UNKNOWN {
		return &ValidationError{Kind: ViolationRollbackPolicy, Message: fmt.Sprintf("plan %q has unknown rollback policy", plan.GetPlanId()), Purpose: purpose}
	}
	if plan.GetRollbackPolicy() == tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION && !containsCapabilityRequirement(plan.GetCapabilityRequirements(), tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK) {
		return &ValidationError{Kind: ViolationCapabilityRequirement, Message: fmt.Sprintf("plan %q requires compensation without declaring its capability", plan.GetPlanId()), Purpose: purpose, CapabilityRequirement: tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK}
	}
	if len(plan.GetActions()) > 1 && plan.GetRollbackPolicy() != tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION {
		return &ValidationError{Kind: ViolationRollbackPolicy, Message: fmt.Sprintf("multi-action plan %q must require compensation", plan.GetPlanId()), Purpose: purpose}
	}
	if err := validateCapabilityRequirements(plan.GetCapabilityRequirements()); err != nil {
		return err
	}
	if len(plan.GetActions()) > 1 {
		for _, kind := range []tgsrlv1.CapabilityRequirementKind{tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION, tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK} {
			if !containsCapabilityRequirement(plan.GetCapabilityRequirements(), kind) {
				return &ValidationError{Kind: ViolationCapabilityRequirement, Message: fmt.Sprintf("plan %q omits required capability %s", plan.GetPlanId(), kind), Purpose: purpose, CapabilityRequirement: kind}
			}
		}
	}
	if err := validateAffectedAllocationIDs(plan.GetAffectedAllocationIds()); err != nil {
		return err
	}
	if purpose == tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION && len(plan.GetAffectedAllocationIds()) != 0 {
		return &ValidationError{Kind: ViolationPurposeMismatch, Message: fmt.Sprintf("admission plan %q must not affect existing allocations", plan.GetPlanId()), Purpose: purpose}
	}
	if purpose == tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE && len(plan.GetAffectedAllocationIds()) == 0 {
		return &ValidationError{Kind: ViolationPurposeMismatch, Message: fmt.Sprintf("rebalance plan %q must identify affected allocations", plan.GetPlanId()), Purpose: purpose}
	}
	if purpose == tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION {
		if plan.GetRollbackPolicy() != tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION {
			return &ValidationError{Kind: ViolationRollbackPolicy, Message: fmt.Sprintf("preemption plan %q must require rollback compensation", plan.GetPlanId()), Purpose: purpose}
		}
		for _, kind := range []tgsrlv1.CapabilityRequirementKind{
			tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION,
			tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK,
			tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_TRANSACTIONAL_PLAN_EXECUTION,
			tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ATOMIC_REPLACEMENT,
		} {
			if !containsCapabilityRequirement(plan.GetCapabilityRequirements(), kind) {
				return &ValidationError{Kind: ViolationCapabilityRequirement, Message: fmt.Sprintf("preemption plan %q omits required capability %s", plan.GetPlanId(), kind), Purpose: purpose, CapabilityRequirement: kind}
			}
		}
		hasRelease, hasBind := false, false
		releaseTargets := make([]string, 0, len(plan.GetActions()))
		for _, action := range plan.GetActions() {
			if action == nil {
				continue
			}
			if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_RELEASE {
				hasRelease = true
				releaseTargets = append(releaseTargets, action.GetTargetId())
			}
			if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND {
				hasBind = true
			}
		}
		if !hasRelease || !hasBind {
			return &ValidationError{Kind: ViolationPurposeMismatch, Message: fmt.Sprintf("preemption plan %q requires release and replacement bind actions", plan.GetPlanId()), Purpose: purpose}
		}
		sort.Strings(releaseTargets)
		if !equalStrings(releaseTargets, plan.GetAffectedAllocationIds()) {
			return &ValidationError{Kind: ViolationPurposeMismatch, Message: fmt.Sprintf("preemption plan %q affected allocations do not match release targets", plan.GetPlanId()), Purpose: purpose}
		}
	}
	return nil
}

func validatePurposeAction(plan *tgsrlv1.PlacementPlan, purpose tgsrlv1.PlanPurpose, action *tgsrlv1.Action) error {
	switch purpose {
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION:
		if action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND {
			return purposeActionError(plan, action, purpose, "admission plans may contain only bind actions")
		}
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE:
		if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND || action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_RELEASE {
			return purposeActionError(plan, action, purpose, "rebalance must not disguise migration as bind/release")
		}
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION:
		if action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND && action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_RELEASE {
			return purposeActionError(plan, action, purpose, "preemption plans allow only release and replacement bind")
		}
		if err := validatePreemptionActionContract(action); err != nil {
			err.Purpose = purpose
			return err
		}
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY:
		switch action.GetActionType() {
		case tgsrlv1.ActionType_ACTION_TYPE_RESUME, tgsrlv1.ActionType_ACTION_TYPE_REBIND, tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		default:
			return purposeActionError(plan, action, purpose, "recovery plans allow only resume, rebind, or recreate")
		}
	}
	return nil
}

func validatePreemptionActionContract(action *tgsrlv1.Action) *ValidationError {
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		if strings.TrimSpace(action.GetSandboxId()) == "" {
			return actionError(ViolationPrecondition, action, fmt.Sprintf("preemption release %q must declare sandbox_id", action.GetActionId()))
		}
		if !action.GetRequiresSafePoint() {
			return actionError(ViolationPrecondition, action, fmt.Sprintf("preemption release %q must require a safe point fence", action.GetActionId()))
		}
	case tgsrlv1.ActionType_ACTION_TYPE_BIND:
		binding := action.GetBinding()
		if binding == nil {
			return actionError(ViolationPrecondition, action, fmt.Sprintf("preemption bind %q must declare replacement binding identity", action.GetActionId()))
		}
		switch {
		case strings.TrimSpace(binding.GetBindingId()) == "":
			return actionError(ViolationPrecondition, action, fmt.Sprintf("preemption bind %q must declare replacement binding_id", action.GetActionId()))
		case strings.TrimSpace(binding.GetPendingUnitId()) == "":
			return actionError(ViolationPrecondition, action, fmt.Sprintf("preemption bind %q must declare replacement pending_unit_id", action.GetActionId()))
		case strings.TrimSpace(binding.GetSandboxId()) == "":
			return actionError(ViolationPrecondition, action, fmt.Sprintf("preemption bind %q must declare replacement sandbox_id", action.GetActionId()))
		case strings.TrimSpace(binding.GetRuntimeUnitId()) == "":
			return actionError(ViolationPrecondition, action, fmt.Sprintf("preemption bind %q must declare replacement runtime_unit_id", action.GetActionId()))
		case binding.GetGeneration() == 0:
			return actionError(ViolationPrecondition, action, fmt.Sprintf("preemption bind %q must declare replacement generation", action.GetActionId()))
		}
		if action.GetSandboxId() != "" && action.GetSandboxId() != binding.GetSandboxId() {
			return actionError(ViolationPrecondition, action, fmt.Sprintf("preemption bind %q sandbox_id %q does not match replacement sandbox_id %q", action.GetActionId(), action.GetSandboxId(), binding.GetSandboxId()))
		}
	}
	return nil
}

func validateExplicitActionContract(plan *tgsrlv1.PlacementPlan, action *tgsrlv1.Action, definition Definition) error {
	if plan.GetSnapshotRevision() == 0 || action.GetExpectedSnapshotRevision() == 0 || action.GetExpectedSnapshotRevision() != plan.GetSnapshotRevision() {
		return actionError(ViolationPrecondition, action, fmt.Sprintf("action %q must carry the plan snapshot revision fence", action.GetActionId()))
	}
	if action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND && action.GetExpectedGeneration() == 0 {
		return actionError(ViolationPrecondition, action, fmt.Sprintf("action %q must carry a target generation fence", action.GetActionId()))
	}
	affected := containsString(plan.GetAffectedAllocationIds(), action.GetTargetId())
	if len(plan.GetAffectedAllocationIds()) > 0 && action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND && !affected {
		return actionError(ViolationPrecondition, action, fmt.Sprintf("action %q target %q is not an affected allocation", action.GetActionId(), action.GetTargetId()))
	}
	if action.GetRequiredCapabilities() == nil || !containsNormalized(action.GetRequiredCapabilities().GetSupportedActions(), definition.CapabilityName) {
		return actionError(ViolationCapabilityRequirement, action, fmt.Sprintf("action %q must require provider capability %q", action.GetActionId(), definition.CapabilityName))
	}
	want := RequiredPreconditions(action.GetActionType(), action.GetRequiresSafePoint(), affected)
	if !equalPreconditions(action.GetPreconditions(), want) {
		return actionError(ViolationPrecondition, action, fmt.Sprintf("action %q preconditions are incomplete or not stably ordered", action.GetActionId()))
	}
	if !equalExpectedImpacts(action.GetExpectedImpacts(), definition.ExpectedImpacts) {
		return actionError(ViolationExpectedImpact, action, fmt.Sprintf("action %q expected_impacts do not match %s", action.GetActionId(), action.GetActionType()))
	}
	return nil
}

func ValidateRollback(action *tgsrlv1.Action) error {
	definition, ok := DefinitionForAction(action.GetActionType())
	if !ok {
		return actionError(ViolationUnknownAction, action, "unsupported action rollback")
	}
	rollback := action.GetRollback()
	if rollback == nil || rollback.GetActionType() != definition.CompensationType {
		return actionError(ViolationRollbackPolicy, action, fmt.Sprintf("action %q does not declare %s compensation", action.GetActionId(), definition.CompensationType))
	}
	sandboxID := action.GetSandboxId()
	if sandboxID == "" && action.GetBinding() != nil {
		sandboxID = action.GetBinding().GetSandboxId()
	}
	if sandboxID == "" {
		sandboxID = action.GetTargetId()
	}
	wantTarget := sandboxID
	if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND {
		wantTarget = action.GetBinding().GetBindingId()
	}
	if wantTarget == "" || rollback.GetTargetId() != wantTarget {
		return actionError(ViolationRollbackPolicy, action, fmt.Sprintf("action %q rollback target %q does not match %q", action.GetActionId(), rollback.GetTargetId(), wantTarget))
	}
	if definition.CompensationNeedsBinding != (rollback.GetRestoreBinding() != nil) {
		return actionError(ViolationRollbackPolicy, action, fmt.Sprintf("action %q rollback restore_binding does not match compensation contract", action.GetActionId()))
	}
	return nil
}

func RequiredPreconditions(actionType tgsrlv1.ActionType, requiresSafePoint, affected bool) []tgsrlv1.ActionPrecondition {
	values := []tgsrlv1.ActionPrecondition{tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_SNAPSHOT_REVISION_MATCH}
	if actionType != tgsrlv1.ActionType_ACTION_TYPE_BIND {
		values = append(values, tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_TARGET_GENERATION_MATCH)
	}
	if requiresSafePoint {
		values = append(values, tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_SAFE_POINT_REACHED)
	}
	if affected && actionType != tgsrlv1.ActionType_ACTION_TYPE_BIND {
		values = append(values, tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_AFFECTED_ALLOCATIONS_ACTIVE)
	}
	return values
}

func validateSuppliedActionMetadata(action *tgsrlv1.Action, definition Definition) error {
	if len(action.GetPreconditions()) > 0 {
		if err := validatePreconditions(action.GetPreconditions()); err != nil {
			return actionError(ViolationPrecondition, action, err.Error())
		}
	}
	if len(action.GetExpectedImpacts()) > 0 && !equalExpectedImpacts(action.GetExpectedImpacts(), definition.ExpectedImpacts) {
		return actionError(ViolationExpectedImpact, action, fmt.Sprintf("action %q expected_impacts do not match %s", action.GetActionId(), action.GetActionType()))
	}
	return nil
}

func validatePreconditions(values []tgsrlv1.ActionPrecondition) error {
	previous := tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_UNKNOWN
	for _, value := range values {
		if value < tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_SNAPSHOT_REVISION_MATCH || value > tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_AFFECTED_ALLOCATIONS_ACTIVE || value <= previous {
			return fmt.Errorf("preconditions must be known, unique, and sorted")
		}
		previous = value
	}
	return nil
}

func validateCapabilityRequirements(values []*tgsrlv1.CapabilityRequirement) error {
	previousKey := ""
	for _, value := range values {
		if value == nil || value.GetKind() < tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION || value.GetKind() > tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_PROVIDER_CAPABILITY {
			return &ValidationError{Kind: ViolationCapabilityRequirement, Message: "capability requirements must use a known kind"}
		}
		if value.GetKind() == tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_PROVIDER_CAPABILITY {
			if strings.TrimSpace(value.GetName()) == "" {
				return &ValidationError{Kind: ViolationCapabilityRequirement, Message: "provider capability requirement requires name"}
			}
		} else if value.GetName() != "" {
			return &ValidationError{Kind: ViolationCapabilityRequirement, Message: "built-in capability requirements must not use name"}
		}
		key := capabilityKey(value)
		if previousKey != "" && key <= previousKey {
			return &ValidationError{Kind: ViolationCapabilityRequirement, Message: "capability requirements must be unique and stably ordered"}
		}
		previousKey = key
	}
	return nil
}

func validateAffectedAllocationIDs(values []string) error {
	for index, value := range values {
		if strings.TrimSpace(value) == "" || (index > 0 && values[index-1] >= value) {
			return &ValidationError{Kind: ViolationInvalidArgument, Message: "affected allocation IDs must be nonblank, unique, and sorted"}
		}
	}
	return nil
}

func validateActionOrder(plan *tgsrlv1.PlacementPlan) error {
	for index, action := range plan.GetActions() {
		if action != nil && action.GetOrder() != uint32(index+1) {
			return actionError(ViolationInvalidArgument, action, fmt.Sprintf("action %q order is %d, want %d", action.GetActionId(), action.GetOrder(), index+1))
		}
	}
	return nil
}

func containsCapabilityRequirement(values []*tgsrlv1.CapabilityRequirement, expected tgsrlv1.CapabilityRequirementKind) bool {
	for _, value := range values {
		if value != nil && value.GetKind() == expected && value.GetRequired() {
			return true
		}
	}
	return false
}

func capabilityKey(value *tgsrlv1.CapabilityRequirement) string {
	return fmt.Sprintf("%010d\x00%s\x00%s", value.GetKind(), value.GetName(), value.GetMinVersion())
}

func StableCapabilityRequirements(values ...*tgsrlv1.CapabilityRequirement) []*tgsrlv1.CapabilityRequirement {
	seen := make(map[string]struct{}, len(values))
	result := make([]*tgsrlv1.CapabilityRequirement, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		cloned := proto.Clone(value).(*tgsrlv1.CapabilityRequirement)
		key := capabilityKey(cloned)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, cloned)
	}
	sort.Slice(result, func(i, j int) bool { return capabilityKey(result[i]) < capabilityKey(result[j]) })
	return result
}

func NewCapabilityRequirement(kind tgsrlv1.CapabilityRequirementKind) *tgsrlv1.CapabilityRequirement {
	return &tgsrlv1.CapabilityRequirement{Kind: kind, Required: true}
}

func equalPreconditions(left, right []tgsrlv1.ActionPrecondition) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalExpectedImpacts(left, right []tgsrlv1.ExpectedImpact) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func containsNormalized(values []string, expected string) bool {
	expected = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(expected), "-", "_"))
	for _, value := range values {
		if strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "-", "_")) == expected {
			return true
		}
	}
	return false
}

func actionError(kind ViolationKind, action *tgsrlv1.Action, message string) *ValidationError {
	return &ValidationError{Kind: kind, Message: message, ActionID: action.GetActionId(), ActionType: action.GetActionType(), DeclaredLevel: action.GetLevel(), TickKind: action.GetTickKind()}
}

func purposeActionError(plan *tgsrlv1.PlacementPlan, action *tgsrlv1.Action, purpose tgsrlv1.PlanPurpose, detail string) *ValidationError {
	err := actionError(ViolationPurposeMismatch, action, fmt.Sprintf("plan %q purpose %s rejects action %q: %s", plan.GetPlanId(), purpose, action.GetActionId(), detail))
	err.Purpose = purpose
	return err
}

func derivePlanTick(plan *tgsrlv1.PlacementPlan) tgsrlv1.TickKind {
	for _, action := range plan.GetActions() {
		if action != nil && action.GetTickKind() != tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
			return action.GetTickKind()
		}
	}
	return tgsrlv1.TickKind_TICK_KIND_UNKNOWN
}
