package scheduler

import (
	"math"
	"sort"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/candidates"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/constraints"
	"google.golang.org/protobuf/proto"
)

type AdmissionPlanner struct{ Utility PlannerUtilityConfig }
type FastMutationPlanner struct{ Utility PlannerUtilityConfig }
type MediumLifecyclePlanner struct{ Utility PlannerUtilityConfig }
type SlowReconfigurationPlanner struct{ Utility PlannerUtilityConfig }

func (p AdmissionPlanner) Propose(input PlanningInput) []PlannerProposal {
	if input.AdmissionPlan == nil {
		return nil
	}
	if input.AdmissionPlan.GetPurpose() != tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION {
		return existingPlanProposal(input, p.Utility)
	}
	if len(input.AdmissionPlan.GetActions()) == 0 {
		evidence := plannerEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_ADMISSION, tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION, tgsrlv1.ActionType_ACTION_TYPE_BIND, "", 0, tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED, "ADMISSION_NO_ACTION")
		return []PlannerProposal{{Evidence: evidence}}
	}
	utility := normalizePlannerUtilityConfig(p.Utility).Bind
	proposals := make([]PlannerProposal, 0, len(input.AdmissionPlan.GetActions()))
	for index, action := range input.AdmissionPlan.GetActions() {
		target := action.GetTargetId()
		if target == "" {
			target = action.GetBinding().GetPendingUnitId()
		}
		reason := "ELIGIBLE"
		if input.ContractAggregate.Blocking {
			reason = "CONTRACT_BLOCKED"
		}
		evidence := plannerEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_ADMISSION, tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION, action.GetActionType(), target, utility, tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_DEFERRED, reason,
			semanticUintField("planner.admission_action_index", uint64(index)),
			semanticUintField("planner.admission_action_count", uint64(len(input.AdmissionPlan.GetActions()))))
		plan, planReason := admissionActionPlan(input.AdmissionPlan, action)
		if reason != "ELIGIBLE" {
			plan = nil
		}
		if plan == nil {
			evidence.Disposition = tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED
			if reason == "ELIGIBLE" {
				evidence.Reason = planReason
			}
		}
		proposals = append(proposals, PlannerProposal{Plan: plan, Evidence: evidence})
	}
	return proposals
}

func admissionActionPlan(source *tgsrlv1.PlacementPlan, sourceAction *tgsrlv1.Action) (*tgsrlv1.PlacementPlan, string) {
	if source == nil || sourceAction == nil || sourceAction.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND || sourceAction.GetBinding() == nil {
		return nil, "INVALID_ADMISSION_ACTION"
	}
	bindingID := sourceAction.GetBinding().GetBindingId()
	var binding *tgsrlv1.Binding
	for _, candidate := range source.GetBindings() {
		if candidate != nil && candidate.GetBindingId() == bindingID {
			binding = proto.Clone(candidate).(*tgsrlv1.Binding)
			break
		}
	}
	if binding == nil {
		return nil, "MISSING_ADMISSION_BINDING"
	}
	plan := proto.Clone(source).(*tgsrlv1.PlacementPlan)
	plan.PlanId = stableID("admission-action-plan", source.GetPlanId(), sourceAction.GetActionId())
	plan.Bindings = []*tgsrlv1.Binding{binding}
	action := proto.Clone(sourceAction).(*tgsrlv1.Action)
	action.Order = 1
	action.PlanId = plan.GetPlanId()
	action.ActionId = stableID("admission-action", plan.GetPlanId(), sourceAction.GetActionId())
	action.IdempotencyKey = stableID("admission-action-idempotency", sourceAction.GetIdempotencyKey(), plan.GetPlanId())
	plan.Actions = []*tgsrlv1.Action{action}
	plan.RollbackPolicy = tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_NOT_REQUIRED
	plan.CapabilityRequirements = nil
	return plan, ""
}

func existingPlanProposal(input PlanningInput, utilityConfig PlannerUtilityConfig) []PlannerProposal {
	plan := input.AdmissionPlan
	if plan == nil || len(plan.GetActions()) == 0 {
		return nil
	}
	utility := int64(0)
	config := normalizePlannerUtilityConfig(utilityConfig)
	for _, action := range plan.GetActions() {
		switch action.GetActionType() {
		case tgsrlv1.ActionType_ACTION_TYPE_BIND:
			utility = saturatingUtilityAdd(utility, config.Bind)
		case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
			utility = saturatingUtilityAdd(utility, config.Release)
		}
	}
	first := plan.GetActions()[0]
	reason := "ELIGIBLE"
	if input.ContractAggregate.Blocking && plan.GetPurpose() != tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY {
		reason = "CONTRACT_BLOCKED"
	}
	evidence := plannerEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_ADMISSION, plan.GetPurpose(), first.GetActionType(), first.GetTargetId(), utility, tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_DEFERRED, reason, semanticUintField("planner.action_count", uint64(len(plan.GetActions()))))
	if reason != "ELIGIBLE" {
		evidence.Disposition = tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED
		return []PlannerProposal{{Evidence: evidence}}
	}
	return []PlannerProposal{{Plan: proto.Clone(plan).(*tgsrlv1.PlacementPlan), Evidence: evidence}}
}

func (p FastMutationPlanner) Propose(input PlanningInput) []PlannerProposal {
	if input.EvaluationContext.GetTickKind() != tgsrlv1.TickKind_TICK_KIND_FAST {
		return wrongTickDirectiveEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_FAST_MUTATION, requestedFastActions(input))
	}
	explicitFast := input.Directives.TargetShare != nil || input.Directives.TargetPriority != nil || input.Directives.AllowResize || input.Directives.AllowScaleIn || input.Directives.TargetResources != nil || len(input.Directives.ResizeResources) > 0
	targetPriority := input.Directives.TargetPriority
	if targetPriority == nil && !explicitFast && hasPriorityDrift(input, input.Intent.GetPriority()) {
		value := input.Intent.GetPriority()
		targetPriority = &value
	}
	autoScaleIn := !explicitFast && hasObservedScaleIn(input)
	autoResize := !explicitFast && hasResourceDrift(input)
	if input.Directives.TargetShare == nil && targetPriority == nil && !autoResize && !autoScaleIn && !input.Directives.AllowResize && !input.Directives.AllowScaleIn && !directionalShareSignalPresent(input) {
		return nil
	}
	utility := normalizePlannerUtilityConfig(p.Utility)
	targets, missing, stale := activeAdaptiveTargetsWithFreshness(input)
	result := missingTargetEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_FAST_MUTATION, missing)
	result = append(result, staleTargetEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_FAST_MUTATION, stale)...)
	for _, target := range targets {
		targetShare := input.Directives.TargetShare
		var directional *DirectionalTarget
		if targetShare == nil && !explicitFast {
			directional = directionalTargetForAdaptiveTarget(input, target)
			if directional != nil {
				value := directional.TargetValue
				targetShare = &value
			} else if !directionalShareSignalPresent(input) {
				value := input.Intent.GetResourcesPerUnit().GetAcceleratorUnits()
				if value >= 0 && value <= 1 && target.sandbox.GetShare() != value {
					targetShare = &value
				}
			}
		}
		if targetShare != nil {
			desired := *targetShare
			fields := []*tgsrlv1.SemanticField{semanticDoubleField("planner.current_share", target.sandbox.GetShare()), semanticDoubleField("planner.target_share", desired)}
			shareUtility := utility.SetShare
			if directional != nil {
				adjustment := directionalSignalUtility(input, utility.FastSignalStep)
				directional.ExpectedBenefitNanos = saturatingUtilityAdd(utility.SetShare, adjustment)
				if input.Signals.RecoveryCostNanosPresent {
					directional.RecoveryCostNanos = input.Signals.RecoveryCostNanos
				}
				shareUtility = saturatingSubtract(directional.ExpectedBenefitNanos, directional.RecoveryCostNanos)
			}
			reason := "ELIGIBLE"
			if math.IsNaN(desired) || math.IsInf(desired, 0) || desired < 0 || desired > 1 {
				reason = "INVALID_TARGET_SHARE"
			} else if target.sandbox.GetShare() == desired {
				reason = "ALREADY_SATISFIED"
			}
			fields = append(fields, directionalTargetEvidence(directional)...)
			result = append(result, actionProposal(input, tgsrlv1.PlannerKind_PLANNER_KIND_FAST_MUTATION, tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION, adaptiveActionSpec{actionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, target: target, share: desired}, shareUtility, reason, fields...))
		}
		if targetPriority != nil {
			desired := *targetPriority
			reason := "ELIGIBLE"
			if target.sandbox.GetPriority() == desired {
				reason = "ALREADY_SATISFIED"
			}
			fields := []*tgsrlv1.SemanticField{semanticIntField("planner.current_priority", int64(target.sandbox.GetPriority())), semanticIntField("planner.target_priority", int64(desired))}
			result = append(result, actionProposal(input, tgsrlv1.PlannerKind_PLANNER_KIND_FAST_MUTATION, tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION, adaptiveActionSpec{actionType: tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, target: target, priority: desired}, utility.SetPriority, reason, fields...))
		}
		if input.Directives.AllowResize || autoResize {
			desired := input.Directives.ResizeResources[target.allocation.GetAllocationId()]
			if desired == nil {
				desired = input.Directives.TargetResources
			}
			if desired == nil && autoResize {
				desired = input.Intent.GetResourcesPerUnit()
			}
			reason := "ELIGIBLE"
			if desired == nil {
				reason = "MISSING_TARGET_RESOURCES"
			} else if proto.Equal(desired, target.allocation.GetResources()) {
				reason = "ALREADY_SATISFIED"
			} else if !resizeWithinAllocation(target.allocation.GetResources(), desired) {
				reason = resizeGrowthEligibility(input, target, desired)
			}
			binding := cloneAdaptiveBinding(target.sandbox.GetBinding(), desired, target.sandbox.GetGeneration())
			fields := []*tgsrlv1.SemanticField{semanticStringField("planner.target_resources", formatResourceVector(desired))}
			result = append(result, actionProposal(input, tgsrlv1.PlannerKind_PLANNER_KIND_FAST_MUTATION, tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE, adaptiveActionSpec{actionType: tgsrlv1.ActionType_ACTION_TYPE_RESIZE, target: target, binding: binding}, utility.Resize, reason, fields...))
		}
	}
	if (input.Directives.AllowScaleIn || autoScaleIn) && uint32(len(targets)) > input.Intent.GetUnitCount() {
		releaseCount := len(targets) - int(input.Intent.GetUnitCount())
		// Emit one deterministic victim per tick. The transactional executor
		// serializes each release and makes retries recoverable.
		target := targets[len(targets)-1]
		result = append(result, actionProposal(
			input,
			tgsrlv1.PlannerKind_PLANNER_KIND_FAST_MUTATION,
			tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
			adaptiveActionSpec{actionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, target: target, requiresSafePoint: true},
			utility.Release,
			"ELIGIBLE",
			semanticUintField("planner.desired_units", uint64(input.Intent.GetUnitCount())),
			semanticUintField("planner.active_units", uint64(len(targets))),
			semanticUintField("planner.remaining_release_count", uint64(releaseCount)),
		))
	}
	return result
}

func (p MediumLifecyclePlanner) Propose(input PlanningInput) []PlannerProposal {
	if input.EvaluationContext.GetTickKind() != tgsrlv1.TickKind_TICK_KIND_MEDIUM {
		return wrongTickDirectiveEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_MEDIUM_LIFECYCLE, requestedMediumActions(input))
	}
	contractPause := input.ContractAggregate.Action == tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED
	autoResume := hasRecentSchedulerSuspension(input)
	if !contractPause && !autoResume && !input.Directives.AllowPause && !input.Directives.AllowResume && !input.Directives.AllowSleep && !input.Directives.AllowOffload && input.Directives.SleepAfter == 0 && input.Directives.OffloadAfter == 0 && input.Directives.DesiredState == tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN {
		return nil
	}
	utility := normalizePlannerUtilityConfig(p.Utility)
	targetInput := input
	if contractPause {
		// Safety actions apply to the complete active allocation set even when a
		// caller supplied a narrower optimization target filter.
		targetInput.Directives.TargetSandboxIDs = nil
	}
	targets, missing, stale := activeAdaptiveTargetsWithFreshness(targetInput)
	result := missingTargetEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_MEDIUM_LIFECYCLE, missing)
	result = append(result, staleTargetEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_MEDIUM_LIFECYCLE, stale)...)
	for _, target := range targets {
		type option struct {
			enabled bool
			action  tgsrlv1.ActionType
			score   int64
		}
		autoOffload := input.Directives.OffloadAfter > 0 && oldEnough(input, target.sandbox, input.Directives.OffloadAfter)
		autoSleep := input.Directives.SleepAfter > 0 && oldEnough(input, target.sandbox, input.Directives.SleepAfter) && !autoOffload
		options := []option{
			{contractPause || input.Directives.AllowPause || input.Directives.DesiredState == tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, tgsrlv1.ActionType_ACTION_TYPE_PAUSE, utility.Pause},
			{(autoResume && recentSchedulerSuspension(input, target)) || input.Directives.AllowResume || input.Directives.DesiredState == tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, tgsrlv1.ActionType_ACTION_TYPE_RESUME, utility.Resume},
			{input.Directives.AllowSleep || autoSleep || input.Directives.DesiredState == tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING, tgsrlv1.ActionType_ACTION_TYPE_SLEEP, utility.Sleep},
			{input.Directives.AllowOffload || autoOffload, tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD, utility.Offload},
		}
		for _, current := range options {
			if !current.enabled {
				continue
			}
			reason := lifecycleEligibility(input, target.sandbox, current.action)
			if reason == "ELIGIBLE" {
				reason = mediumSignalEligibility(input, target.sandbox, current.action)
			}
			result = append(result, actionProposal(input, tgsrlv1.PlannerKind_PLANNER_KIND_MEDIUM_LIFECYCLE, lifecyclePurpose(current.action), adaptiveActionSpec{actionType: current.action, target: target, requiresSafePoint: current.action != tgsrlv1.ActionType_ACTION_TYPE_RESUME}, current.score, reason, semanticIntField("planner.current_runtime_state", int64(target.sandbox.GetState())), semanticIntField("planner.sandbox_residency_nanos", sandboxResidencyNanos(input, target.sandbox))))
		}
	}
	return result
}

func (p SlowReconfigurationPlanner) Propose(input PlanningInput) []PlannerProposal {
	if input.EvaluationContext.GetTickKind() != tgsrlv1.TickKind_TICK_KIND_SLOW {
		return wrongTickDirectiveEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_SLOW_RECONFIGURATION, requestedSlowActions(input))
	}
	autoRebind := hasUnhealthyBoundDevice(input)
	autoRecreate := hasFailedSandbox(input)
	if !autoRebind && !autoRecreate && !input.Directives.AllowRebind && !input.Directives.AllowRecreate {
		return nil
	}
	utility := normalizePlannerUtilityConfig(p.Utility)
	targets, missing, stale := activeAdaptiveTargetsWithFreshness(input)
	result := missingTargetEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_SLOW_RECONFIGURATION, missing)
	result = append(result, staleTargetEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_SLOW_RECONFIGURATION, stale)...)
	for _, target := range targets {
		for _, option := range []struct {
			enabled bool
			action  tgsrlv1.ActionType
			score   int64
			purpose tgsrlv1.PlanPurpose
		}{
			{targetHasUnhealthyDevice(input, target) || input.Directives.AllowRebind, tgsrlv1.ActionType_ACTION_TYPE_REBIND, utility.Rebind, rebindPurpose(input, target)},
			{(autoRecreate && target.sandbox.GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED) || input.Directives.AllowRecreate, tgsrlv1.ActionType_ACTION_TYPE_RECREATE, utility.Recreate, tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY},
		} {
			if !option.enabled {
				continue
			}
			replacement := input.Directives.ReplacementBindings[target.allocation.GetAllocationId()]
			if replacement == nil && option.action == tgsrlv1.ActionType_ACTION_TYPE_REBIND {
				replacement = stableRebindCandidate(input, target)
			}
			if replacement == nil && option.action == tgsrlv1.ActionType_ACTION_TYPE_RECREATE && target.sandbox.GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED {
				replacement = stableRecreateCandidate(target)
			}
			reason := "ELIGIBLE"
			if replacement == nil {
				reason = "MISSING_REPLACEMENT_BINDING"
			} else {
				var completionReason string
				replacement, completionReason = completeReplacementBinding(input, target, replacement, option.action)
				if completionReason != "" {
					reason = completionReason
				} else if replacementReason := validateReplacementBinding(input, target, replacement, option.action); replacementReason != "" {
					reason = replacementReason
				} else if !input.SafePoint.Present || !input.SafePoint.Value || !target.sandbox.GetSafePoint() {
					reason = "SAFE_POINT_REQUIRED"
				}
			}
			benefit, cost, risk, score := slowUtility(input, target, option.action, option.score, utility)
			result = append(result, actionProposal(input, tgsrlv1.PlannerKind_PLANNER_KIND_SLOW_RECONFIGURATION, option.purpose, adaptiveActionSpec{actionType: option.action, target: target, binding: replacement, requiresSafePoint: true}, score, reason,
				semanticIntField("planner.benefit_base_nanos", benefit),
				semanticIntField("planner.cost_recovery_nanos", cost),
				semanticIntField("planner.risk_reconfiguration_nanos", risk),
			))
		}
	}
	return result
}

func rebindPurpose(input PlanningInput, target adaptiveTarget) tgsrlv1.PlanPurpose {
	if targetHasUnhealthyDevice(input, target) {
		return tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY
	}
	return tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE
}

func actionProposal(input PlanningInput, kind tgsrlv1.PlannerKind, purpose tgsrlv1.PlanPurpose, spec adaptiveActionSpec, utility int64, reason string, fields ...*tgsrlv1.SemanticField) PlannerProposal {
	if reason == "ELIGIBLE" {
		reason = actionEligibility(input, spec)
	}
	if utility < input.Directives.MinimumUtilityNanos && reason == "ELIGIBLE" {
		reason = "UTILITY_BELOW_THRESHOLD"
	}
	disposition := tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_DEFERRED
	var plan *tgsrlv1.PlacementPlan
	if reason == "ELIGIBLE" {
		var buildReason string
		plan, buildReason = buildAdaptivePlan(input, kind, purpose, []adaptiveActionSpec{spec})
		if plan == nil {
			reason = buildReason
		}
	}
	if reason != "ELIGIBLE" {
		disposition = tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED
	}
	fields = append(fields, semanticUintField("planner.target_generation", spec.target.sandbox.GetGeneration()), semanticIntField("planner.action_level", int64(func() tgsrlv1.ActionLevel { value, _ := actionpolicy.LevelForAction(spec.actionType); return value }())))
	if spec.target.sandbox.GetObservedAt() != nil {
		fields = append(fields, semanticIntField("planner.sandbox_observed_at_unix_nanos", spec.target.sandbox.GetObservedAt().AsTime().UnixNano()))
	}
	if confirmedAt := effectiveSandboxLastConfirmedAt(spec.target.sandbox); confirmedAt != nil {
		fields = append(fields, semanticIntField("planner.sandbox_last_confirmed_at_unix_nanos", confirmedAt.AsTime().UnixNano()))
	}
	if stateChangedAt := effectiveSandboxStateChangedAt(spec.target.sandbox); stateChangedAt != nil {
		fields = append(fields, semanticIntField("planner.sandbox_state_changed_at_unix_nanos", stateChangedAt.AsTime().UnixNano()))
	}
	return PlannerProposal{Plan: plan, Evidence: plannerEvidence(input, kind, purpose, spec.actionType, spec.target.allocation.GetAllocationId(), utility, disposition, reason, fields...)}
}

func actionEligibility(input PlanningInput, spec adaptiveActionSpec) string {
	if input.ContractAggregate.Blocking && !(input.ContractAggregate.Action == tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED && spec.actionType == tgsrlv1.ActionType_ACTION_TYPE_PAUSE) {
		return "CONTRACT_BLOCKED"
	}
	if spec.actionType != tgsrlv1.ActionType_ACTION_TYPE_RESUME && spec.requiresSafePoint && (!input.SafePoint.Present || !input.SafePoint.Value || !spec.target.sandbox.GetSafePoint()) {
		return "SAFE_POINT_REQUIRED"
	}
	if ceiling, ok := actionpolicy.CeilingForTick(input.EvaluationContext.GetTickKind()); !ok {
		return "UNKNOWN_TICK"
	} else if level, _ := actionpolicy.LevelForAction(spec.actionType); level > ceiling {
		return "TICK_NOT_ELIGIBLE"
	}
	if spec.actionType != tgsrlv1.ActionType_ACTION_TYPE_REBIND && spec.actionType != tgsrlv1.ActionType_ACTION_TYPE_RECREATE {
		if ok, reason := providerSupports(input, spec.target.allocation, spec.actionType); !ok {
			return reason
		}
	}
	return "ELIGIBLE"
}

func missingTargetEvidence(input PlanningInput, kind tgsrlv1.PlannerKind, ids []string) []PlannerProposal {
	result := make([]PlannerProposal, 0, len(ids))
	for _, id := range ids {
		result = append(result, PlannerProposal{Evidence: plannerEvidence(input, kind, tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION, tgsrlv1.ActionType_ACTION_TYPE_UNKNOWN, id, 0, tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED, "MISSING_SANDBOX_OBSERVATION")})
	}
	return result
}

func staleTargetEvidence(input PlanningInput, kind tgsrlv1.PlannerKind, targets map[string]string) []PlannerProposal {
	ids := make([]string, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]PlannerProposal, 0, len(ids))
	for _, id := range ids {
		result = append(result, PlannerProposal{Evidence: plannerEvidence(input, kind, tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION, tgsrlv1.ActionType_ACTION_TYPE_UNKNOWN, id, 0, tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED, targets[id], semanticIntField("planner.sandbox_maximum_age_nanos", input.sandboxMaximumAge.Nanoseconds()))})
	}
	return result
}

func wrongTickDirectiveEvidence(input PlanningInput, kind tgsrlv1.PlannerKind, actions []tgsrlv1.ActionType) []PlannerProposal {
	result := make([]PlannerProposal, 0, len(actions))
	for _, action := range actions {
		result = append(result, PlannerProposal{Evidence: plannerEvidence(input, kind, wrongTickPurpose(action), action, "", 0, tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_DEFERRED, "TICK_NOT_ELIGIBLE")})
	}
	return result
}

func requestedFastActions(input PlanningInput) []tgsrlv1.ActionType {
	var result []tgsrlv1.ActionType
	if input.Directives.TargetShare != nil {
		result = append(result, tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)
	}
	if input.Directives.TargetPriority != nil {
		result = append(result, tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY)
	}
	if input.Directives.AllowResize || input.Directives.TargetResources != nil || len(input.Directives.ResizeResources) > 0 {
		result = append(result, tgsrlv1.ActionType_ACTION_TYPE_RESIZE)
	}
	if input.Directives.AllowScaleIn {
		result = append(result, tgsrlv1.ActionType_ACTION_TYPE_RELEASE)
	}
	return result
}

func requestedMediumActions(input PlanningInput) []tgsrlv1.ActionType {
	var result []tgsrlv1.ActionType
	if input.Directives.AllowPause || input.Directives.DesiredState == tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED {
		result = append(result, tgsrlv1.ActionType_ACTION_TYPE_PAUSE)
	}
	if input.Directives.AllowResume || input.Directives.DesiredState == tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		result = append(result, tgsrlv1.ActionType_ACTION_TYPE_RESUME)
	}
	if input.Directives.AllowSleep || input.Directives.SleepAfter > 0 || input.Directives.DesiredState == tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING {
		result = append(result, tgsrlv1.ActionType_ACTION_TYPE_SLEEP)
	}
	if input.Directives.AllowOffload || input.Directives.OffloadAfter > 0 {
		result = append(result, tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD)
	}
	return result
}

func requestedSlowActions(input PlanningInput) []tgsrlv1.ActionType {
	var result []tgsrlv1.ActionType
	if input.Directives.AllowRebind {
		result = append(result, tgsrlv1.ActionType_ACTION_TYPE_REBIND)
	}
	if input.Directives.AllowRecreate {
		result = append(result, tgsrlv1.ActionType_ACTION_TYPE_RECREATE)
	}
	return result
}

func lifecycleEligibility(input PlanningInput, sandbox *tgsrlv1.Sandbox, action tgsrlv1.ActionType) string {
	switch action {
	case tgsrlv1.ActionType_ACTION_TYPE_PAUSE:
		if sandbox.GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED {
			return "ALREADY_SATISFIED"
		}
		if sandbox.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
			return "INVALID_STATE_TRANSITION"
		}
	case tgsrlv1.ActionType_ACTION_TYPE_RESUME:
		if sandbox.GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
			return "ALREADY_SATISFIED"
		}
		if sandbox.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED && sandbox.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING {
			return "INVALID_STATE_TRANSITION"
		}
	case tgsrlv1.ActionType_ACTION_TYPE_SLEEP:
		if sandbox.GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING && !sandbox.GetOffloaded() {
			return "ALREADY_SATISFIED"
		}
		if sandbox.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING && sandbox.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED {
			return "INVALID_STATE_TRANSITION"
		}
		if input.Directives.SleepAfter > 0 && !oldEnough(input, sandbox, input.Directives.SleepAfter) {
			return "AGE_THRESHOLD_NOT_REACHED"
		}
	case tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		if sandbox.GetOffloaded() {
			return "ALREADY_SATISFIED"
		}
		if sandbox.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING && sandbox.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED && sandbox.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING {
			return "INVALID_STATE_TRANSITION"
		}
		if input.Directives.OffloadAfter > 0 && !oldEnough(input, sandbox, input.Directives.OffloadAfter) {
			return "AGE_THRESHOLD_NOT_REACHED"
		}
	}
	return "ELIGIBLE"
}

func oldEnough(input PlanningInput, sandbox *tgsrlv1.Sandbox, threshold time.Duration) bool {
	stateChangedAt := effectiveSandboxStateChangedAt(sandbox)
	if stateChangedAt == nil || stateChangedAt.CheckValid() != nil || input.EvaluationContext.GetEvaluationTime() == nil {
		return false
	}
	return !input.EvaluationContext.GetEvaluationTime().AsTime().Before(stateChangedAt.AsTime().Add(threshold))
}

func sandboxResidencyNanos(input PlanningInput, sandbox *tgsrlv1.Sandbox) int64 {
	stateChangedAt := effectiveSandboxStateChangedAt(sandbox)
	if stateChangedAt == nil || stateChangedAt.CheckValid() != nil || input.EvaluationContext.GetEvaluationTime() == nil {
		return -1
	}
	residency := input.EvaluationContext.GetEvaluationTime().AsTime().Sub(stateChangedAt.AsTime())
	if residency < 0 {
		return -1
	}
	return residency.Nanoseconds()
}

func mediumSignalEligibility(input PlanningInput, sandbox *tgsrlv1.Sandbox, action tgsrlv1.ActionType) string {
	if action == tgsrlv1.ActionType_ACTION_TYPE_RESUME {
		if input.Signals.ActionInFlightPresent && input.Signals.ActionInFlight {
			return "ACTION_IN_FLIGHT"
		}
		return "ELIGIBLE"
	}
	if !input.Signals.CheckpointCapablePresent {
		return "CHECKPOINT_CAPABILITY_UNKNOWN"
	}
	if !input.Signals.CheckpointCapable {
		return "CHECKPOINT_CAPABILITY_REQUIRED"
	}
	if !input.Signals.BufferBelowWatermarkPresent {
		return "BUFFER_WATERMARK_UNKNOWN"
	}
	if !input.Signals.BufferBelowWatermark {
		return "BUFFER_ABOVE_WATERMARK"
	}
	if !input.Signals.MinimumResidencyPresent {
		return "MINIMUM_RESIDENCY_UNKNOWN"
	}
	residency := sandboxResidencyNanos(input, sandbox)
	if residency < 0 {
		return "MINIMUM_RESIDENCY_UNKNOWN"
	}
	if residency < input.Signals.MinimumResidency.Nanoseconds() {
		return "MINIMUM_RESIDENCY_NOT_MET"
	}
	if !input.Signals.ActionInFlightPresent {
		return "ACTION_IN_FLIGHT_UNKNOWN"
	}
	if input.Signals.ActionInFlight {
		return "ACTION_IN_FLIGHT"
	}
	return "ELIGIBLE"
}

func directionalSignalUtility(input PlanningInput, step int64) int64 {
	signals := input.Signals
	var adjustment int64
	add := func(condition bool) {
		if condition {
			adjustment = saturatingUtilityAdd(adjustment, step)
		}
	}
	add(signals.BufferPressurePresent && signals.BufferPressure >= BufferPressureHigh)
	add(signals.PolicyLagPresent && signals.PolicyLag > 0)
	if producerPhase(input.Intent.GetPhaseKind()) {
		add(signals.SampleStalenessPresent && signals.SampleStaleness)
		add(signals.ESSRatioPresent && signals.ESSRatio < DefaultPlannerESSRatioFloor)
	}
	return adjustment
}

func slowUtility(input PlanningInput, target adaptiveTarget, action tgsrlv1.ActionType, base int64, config PlannerUtilityConfig) (int64, int64, int64, int64) {
	benefit := base
	if action == tgsrlv1.ActionType_ACTION_TYPE_RECREATE && target.sandbox.GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED {
		benefit = saturatingUtilityAdd(benefit, config.SlowBenefit)
	}
	if action == tgsrlv1.ActionType_ACTION_TYPE_REBIND && targetHasUnhealthyDevice(input, target) {
		benefit = saturatingUtilityAdd(benefit, config.SlowBenefit)
	}
	cost := recoveryCostForTarget(input.Signals, target)
	risk := config.SlowRisk
	if input.Signals.CheckpointCapablePresent && input.Signals.CheckpointCapable {
		risk = 0
	}
	return benefit, cost, risk, saturatingSubtract(saturatingSubtract(benefit, cost), risk)
}

func recoveryCostForTarget(signals PlanningSignals, target adaptiveTarget) int64 {
	for _, key := range []string{target.allocation.GetAllocationId(), target.sandbox.GetSandboxId()} {
		if value, ok := signals.RecoveryCostNanosByTarget[key]; ok {
			return value
		}
	}
	if signals.RecoveryCostNanosPresent {
		return signals.RecoveryCostNanos
	}
	return 0
}

func saturatingUtilityAdd(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	if right < 0 && left < math.MinInt64-right {
		return math.MinInt64
	}
	return left + right
}

func saturatingSubtract(left, right int64) int64 {
	if right == math.MinInt64 {
		if left >= 0 {
			return math.MaxInt64
		}
		return left - right
	}
	return saturatingUtilityAdd(left, -right)
}

func lifecyclePurpose(action tgsrlv1.ActionType) tgsrlv1.PlanPurpose {
	if action == tgsrlv1.ActionType_ACTION_TYPE_RESUME {
		return tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY
	}
	return tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION
}

func wrongTickPurpose(action tgsrlv1.ActionType) tgsrlv1.PlanPurpose {
	switch action {
	case tgsrlv1.ActionType_ACTION_TYPE_RESIZE, tgsrlv1.ActionType_ACTION_TYPE_REBIND:
		return tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE
	case tgsrlv1.ActionType_ACTION_TYPE_RESUME, tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		return tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY
	default:
		return tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION
	}
}

func cloneAdaptiveBinding(binding *tgsrlv1.Binding, resources *tgsrlv1.ResourceVector, generation uint64) *tgsrlv1.Binding {
	if binding == nil {
		return nil
	}
	result := proto.Clone(binding).(*tgsrlv1.Binding)
	if resources != nil {
		result.Resources = cloneResources(resources)
	}
	result.Generation = generation
	return result
}

func resizeWithinAllocation(current, desired *tgsrlv1.ResourceVector) bool {
	if current == nil || desired == nil {
		return false
	}
	return constraints.ResourceLessOrEqual(desired, current)
}

func activeAllocationCount(input PlanningInput) int {
	count := 0
	for _, allocation := range input.Snapshot.GetAllocations() {
		if allocation != nil && allocation.GetState() == tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE && allocation.GetExecutionId() == input.Intent.GetExecutionId() && allocation.GetStageId() == input.Intent.GetStageId() && allocation.GetIntentVersion() == input.Intent.GetVersion() {
			count++
		}
	}
	return count
}

func hasObservedScaleIn(input PlanningInput) bool {
	targets, _ := activeAdaptiveTargets(input)
	return len(targets) > int(input.Intent.GetUnitCount())
}

func hasResourceDrift(input PlanningInput) bool {
	desired := input.Intent.GetResourcesPerUnit()
	targets, _ := activeAdaptiveTargets(input)
	for _, target := range targets {
		if !proto.Equal(target.allocation.GetResources(), desired) {
			return true
		}
	}
	return false
}

func hasPriorityDrift(input PlanningInput, desired int32) bool {
	targets, _ := activeAdaptiveTargets(input)
	for _, target := range targets {
		if target.sandbox.GetPriority() != desired {
			return true
		}
	}
	return false
}

func hasRecentSchedulerSuspension(input PlanningInput) bool {
	for _, decision := range input.RecentDecisions {
		for _, action := range decision.GetSelectedPlan().GetActions() {
			if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_PAUSE || action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_SLEEP || action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD {
				return true
			}
		}
	}
	return false
}

func recentSchedulerSuspension(input PlanningInput, target adaptiveTarget) bool {
	if input.ContractAggregate.Blocking || (target.sandbox.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED && target.sandbox.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING) {
		return false
	}
	for index := len(input.RecentDecisions) - 1; index >= 0; index-- {
		decision := input.RecentDecisions[index]
		if decision == nil {
			continue
		}
		for _, action := range decision.GetSelectedPlan().GetActions() {
			if action.GetTargetId() != target.allocation.GetAllocationId() || action.GetExpectedGeneration() != target.sandbox.GetGeneration() {
				continue
			}
			if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_PAUSE || action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_SLEEP || action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD {
				return true
			}
		}
	}
	return false
}

func hasFailedSandbox(input PlanningInput) bool {
	targets, _ := activeAdaptiveTargets(input)
	for _, target := range targets {
		if target.sandbox.GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED {
			return true
		}
	}
	return false
}

func resizeGrowthEligibility(input PlanningInput, target adaptiveTarget, desired *tgsrlv1.ResourceVector) string {
	if target.allocation == nil || len(target.allocation.GetDeviceIds()) != 1 || desired == nil {
		return "RESIZE_FEASIBILITY_UNKNOWN"
	}
	result, err := (candidates.Engine{}).Evaluate(candidates.Request{
		Snapshot: input.Snapshot, Intent: input.Intent,
		Decision:         candidates.DecisionMetadata{DecisionID: input.DecisionID, RequiresSafePoint: false, SafePoint: input.SafePoint.Value},
		LedgerAdjustment: candidates.LedgerAdjustment{ReclaimAllocationIDs: []string{target.allocation.GetAllocationId()}, IncludeDeviceIDs: append([]string(nil), target.allocation.GetDeviceIds()...)},
		Units:            []candidates.Unit{{ID: target.allocation.GetPendingUnitId(), Pending: &tgsrlv1.PendingUnit{PendingUnitId: target.allocation.GetPendingUnitId(), RequestedResources: cloneResources(desired), RequiredCapabilities: cloneCapabilities(input.Intent.GetRequiredCapabilities())}}},
		Config:           candidates.Config{TopK: 1, EvidenceBudget: 1},
	})
	if err != nil {
		return "RESIZE_FEASIBILITY_ERROR"
	}
	if len(result.Selected) != 1 {
		return "RESIZE_INSUFFICIENT_RESOURCES"
	}
	return "ELIGIBLE"
}

func hasUnhealthyBoundDevice(input PlanningInput) bool {
	devices := make(map[string]tgsrlv1.DeviceHealth, len(input.Snapshot.GetDevices()))
	for _, device := range input.Snapshot.GetDevices() {
		if device != nil {
			devices[device.GetDeviceId()] = device.GetHealth()
		}
	}
	for _, allocation := range input.Snapshot.GetAllocations() {
		if allocation == nil || allocation.GetExecutionId() != input.Intent.GetExecutionId() || allocation.GetStageId() != input.Intent.GetStageId() || allocation.GetIntentVersion() != input.Intent.GetVersion() || allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
			continue
		}
		for _, deviceID := range allocation.GetDeviceIds() {
			if devices[deviceID] != tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY {
				return true
			}
		}
	}
	return false
}

func targetHasUnhealthyDevice(input PlanningInput, target adaptiveTarget) bool {
	devices := make(map[string]tgsrlv1.DeviceHealth, len(input.Snapshot.GetDevices()))
	for _, device := range input.Snapshot.GetDevices() {
		if device != nil {
			devices[device.GetDeviceId()] = device.GetHealth()
		}
	}
	for _, deviceID := range target.allocation.GetDeviceIds() {
		if devices[deviceID] != tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY {
			return true
		}
	}
	return false
}

func stableRebindCandidate(input PlanningInput, target adaptiveTarget) *tgsrlv1.Binding {
	if target.sandbox == nil || target.sandbox.GetBinding() == nil {
		return nil
	}
	required := cloneCapabilities(input.Intent.GetRequiredCapabilities())
	required.SupportedActions = append(required.SupportedActions, "rebind")
	result, err := (candidates.Engine{}).Evaluate(candidates.Request{
		Snapshot: input.Snapshot, Intent: input.Intent, Decision: candidates.DecisionMetadata{DecisionID: input.DecisionID},
		LedgerAdjustment: candidates.LedgerAdjustment{ReclaimAllocationIDs: []string{target.allocation.GetAllocationId()}, ExcludeDeviceIDs: append([]string(nil), target.allocation.GetDeviceIds()...)},
		Units:            []candidates.Unit{{ID: target.allocation.GetPendingUnitId(), Pending: &tgsrlv1.PendingUnit{PendingUnitId: target.allocation.GetPendingUnitId(), RequestedResources: cloneResources(target.allocation.GetResources()), RequiredCapabilities: required}}},
		Config:           candidates.Config{TopK: 1, EvidenceBudget: 1},
	})
	if err == nil && len(result.Selected) == 1 {
		binding := proto.Clone(target.sandbox.GetBinding()).(*tgsrlv1.Binding)
		binding.DeviceIds = []string{result.Selected[0].DeviceID}
		binding.Generation = target.sandbox.GetGeneration() + 1
		return binding
	}
	return nil
}

func stableRecreateCandidate(target adaptiveTarget) *tgsrlv1.Binding {
	if target.sandbox == nil || target.sandbox.GetBinding() == nil {
		return nil
	}
	candidate := proto.Clone(target.sandbox.GetBinding()).(*tgsrlv1.Binding)
	candidate.Generation = target.sandbox.GetGeneration() + 1
	return candidate
}

// completeReplacementBinding treats a caller-provided replacement as a
// device/resource override, not as an authority for workload identity. The
// observed binding remains the identity base, and every replacement receives
// a fresh deterministic binding ID so reservation cannot alias the source.
func completeReplacementBinding(input PlanningInput, target adaptiveTarget, override *tgsrlv1.Binding, action tgsrlv1.ActionType) (*tgsrlv1.Binding, string) {
	if target.allocation == nil || target.sandbox == nil || target.sandbox.GetBinding() == nil || override == nil {
		return nil, "INVALID_REPLACEMENT_BINDING"
	}
	current := target.sandbox.GetBinding()
	if current.GetBindingId() == "" || target.sandbox.GetSandboxId() == "" || target.allocation.GetPendingUnitId() == "" || target.allocation.GetRuntimeUnitId() == "" ||
		current.GetPendingUnitId() != target.allocation.GetPendingUnitId() || current.GetSandboxId() != target.sandbox.GetSandboxId() ||
		current.GetRuntimeUnitId() != target.allocation.GetRuntimeUnitId() || current.GetGeneration() != target.sandbox.GetGeneration() {
		return nil, "INVALID_CURRENT_BINDING_IDENTITY"
	}
	if (override.GetPendingUnitId() != "" && override.GetPendingUnitId() != target.allocation.GetPendingUnitId()) ||
		(override.GetSandboxId() != "" && override.GetSandboxId() != target.sandbox.GetSandboxId()) ||
		(override.GetRuntimeUnitId() != "" && override.GetRuntimeUnitId() != target.allocation.GetRuntimeUnitId()) {
		return nil, "REPLACEMENT_IDENTITY_MISMATCH"
	}
	if override.GetGeneration() != 0 && override.GetGeneration() != target.sandbox.GetGeneration()+1 {
		return nil, "REPLACEMENT_GENERATION_MISMATCH"
	}

	replacement := proto.Clone(current).(*tgsrlv1.Binding)
	if len(override.GetDeviceIds()) > 0 {
		replacement.DeviceIds = append([]string(nil), override.GetDeviceIds()...)
		sort.Strings(replacement.DeviceIds)
	}
	if override.GetResources() != nil {
		replacement.Resources = cloneResources(override.GetResources())
	}
	replacement.PendingUnitId = target.allocation.GetPendingUnitId()
	replacement.SandboxId = target.sandbox.GetSandboxId()
	replacement.RuntimeUnitId = target.allocation.GetRuntimeUnitId()
	replacement.Generation = target.sandbox.GetGeneration() + 1
	replacement.BindingId = ""
	replacement.BindingId = stableID("replacement-binding", input.DecisionID, action.String(), target.allocation.GetAllocationId(), current.GetBindingId(), bindingIdentity(replacement))
	if replacement.GetBindingId() == "" || replacement.GetBindingId() == current.GetBindingId() {
		return nil, "INVALID_REPLACEMENT_BINDING"
	}
	return replacement, ""
}

func validateReplacementBinding(input PlanningInput, target adaptiveTarget, replacement *tgsrlv1.Binding, action tgsrlv1.ActionType) string {
	if replacement == nil || replacement.GetBindingId() == "" || replacement.GetPendingUnitId() == "" || replacement.GetSandboxId() == "" || replacement.GetRuntimeUnitId() == "" || len(replacement.GetDeviceIds()) != 1 || replacement.GetResources() == nil {
		return "INVALID_REPLACEMENT_BINDING"
	}
	if replacement.GetBindingId() == target.sandbox.GetBinding().GetBindingId() || replacement.GetSandboxId() != target.sandbox.GetSandboxId() {
		return "REPLACEMENT_IDENTITY_MISMATCH"
	}
	if replacement.GetPendingUnitId() != target.allocation.GetPendingUnitId() {
		return "REPLACEMENT_IDENTITY_MISMATCH"
	}
	if replacement.GetRuntimeUnitId() != target.allocation.GetRuntimeUnitId() {
		return "REPLACEMENT_IDENTITY_MISMATCH"
	}
	if replacement.GetGeneration() != target.sandbox.GetGeneration()+1 {
		return "REPLACEMENT_GENERATION_MISMATCH"
	}
	if !proto.Equal(replacement.GetResources(), target.allocation.GetResources()) {
		return "REPLACEMENT_RESOURCE_MISMATCH"
	}
	if action == tgsrlv1.ActionType_ACTION_TYPE_RECREATE && !equalStringSlices(replacement.GetDeviceIds(), target.allocation.GetDeviceIds()) {
		return "REPLACEMENT_DEVICE_MISMATCH"
	}
	definition, _ := actionpolicy.DefinitionForAction(action)
	required := cloneCapabilities(input.Intent.GetRequiredCapabilities())
	required.SupportedActions = append(required.SupportedActions, definition.CapabilityName)
	result, err := (candidates.Engine{}).Evaluate(candidates.Request{
		Snapshot: input.Snapshot, Intent: input.Intent, Decision: candidates.DecisionMetadata{DecisionID: input.DecisionID},
		LedgerAdjustment: candidates.LedgerAdjustment{ReclaimAllocationIDs: []string{target.allocation.GetAllocationId()}, IncludeDeviceIDs: append([]string(nil), replacement.GetDeviceIds()...)},
		Units:            []candidates.Unit{{ID: target.allocation.GetPendingUnitId(), Pending: &tgsrlv1.PendingUnit{PendingUnitId: target.allocation.GetPendingUnitId(), RequestedResources: cloneResources(replacement.GetResources()), RequiredCapabilities: required}}},
		Config:           candidates.Config{TopK: 1, EvidenceBudget: 1},
	})
	if err != nil {
		return "REPLACEMENT_FEASIBILITY_ERROR"
	}
	if len(result.Selected) != 1 {
		return "REPLACEMENT_INFEASIBLE"
	}
	if action == tgsrlv1.ActionType_ACTION_TYPE_REBIND && equalStringSlices(replacement.GetDeviceIds(), target.sandbox.GetBinding().GetDeviceIds()) {
		return "ALREADY_SATISFIED"
	}
	return ""
}

func equalStringSlices(left, right []string) bool {
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
