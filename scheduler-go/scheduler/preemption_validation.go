package scheduler

import (
	"fmt"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
)

func validatePreemptionPlan(plan *tgsrlv1.PlacementPlan, ctx *tgsrlv1.EvaluationContext) error {
	if plan == nil {
		return nil
	}
	if plan.GetPurpose() != tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION {
		return fmt.Errorf("preemption validator requires preemption purpose")
	}
	if plan.GetRollbackPolicy() != tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION {
		return fmt.Errorf("preemption plan must require compensation")
	}
	for _, kind := range []tgsrlv1.CapabilityRequirementKind{
		tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION,
		tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK,
		tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_TRANSACTIONAL_PLAN_EXECUTION,
		tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ATOMIC_REPLACEMENT,
	} {
		if !containsPreemptionCapability(plan.GetCapabilityRequirements(), kind) {
			return fmt.Errorf("preemption plan omits required capability %s", kind)
		}
	}
	if len(plan.GetActions()) < 2 {
		return fmt.Errorf("preemption plan requires release victims and one replacement bind")
	}
	releaseTargets := make([]string, 0, len(plan.GetActions())-1)
	bindCount := 0
	for index, action := range plan.GetActions() {
		if action == nil || action.GetOrder() != uint32(index+1) {
			return fmt.Errorf("preemption action order is invalid")
		}
		if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_RELEASE {
			releaseTargets = append(releaseTargets, action.GetTargetId())
		} else if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND {
			bindCount++
		} else {
			return fmt.Errorf("preemption plan allows only release and bind actions")
		}
		if err := actionpolicy.ValidateAction(action, tickForValidation(ctx), actionpolicy.ValidationOptions{}); err != nil {
			return err
		}
		if err := actionpolicy.ValidateRollback(action); err != nil {
			return err
		}
	}
	if bindCount != 1 || len(releaseTargets) == 0 {
		return fmt.Errorf("preemption plan requires one replacement bind and at least one release victim")
	}
	sortedTargets := append([]string(nil), releaseTargets...)
	sort.Strings(sortedTargets)
	if !equalStringSlices(sortedTargets, plan.GetAffectedAllocationIds()) {
		return fmt.Errorf("preemption affected allocations must match release targets")
	}
	return nil
}

func tickForValidation(ctx *tgsrlv1.EvaluationContext) tgsrlv1.TickKind {
	if ctx == nil {
		return tgsrlv1.TickKind_TICK_KIND_SLOW
	}
	if ctx.GetTickKind() == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		return tgsrlv1.TickKind_TICK_KIND_SLOW
	}
	return ctx.GetTickKind()
}

func containsPreemptionCapability(values []*tgsrlv1.CapabilityRequirement, expected tgsrlv1.CapabilityRequirementKind) bool {
	for _, value := range values {
		if value != nil && value.GetKind() == expected && value.GetRequired() {
			return true
		}
	}
	return false
}
