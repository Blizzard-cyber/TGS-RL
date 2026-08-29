package state

import (
	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func effectivePlanPurpose(plan *tgsrlv1.PlacementPlan) (tgsrlv1.PlanPurpose, bool) {
	if plan == nil {
		return tgsrlv1.PlanPurpose_PLAN_PURPOSE_UNKNOWN, false
	}
	switch plan.GetPurpose() {
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION,
		tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE,
		tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION,
		tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY,
		tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION:
		return plan.GetPurpose(), false
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_UNKNOWN:
		if isLegacyBindOnlyPlan(plan) {
			return tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION, true
		}
	}
	return plan.GetPurpose(), false
}

func isLegacyBindOnlyPlan(plan *tgsrlv1.PlacementPlan) bool {
	if plan == nil || len(plan.GetActions()) == 0 || plan.GetRollbackPolicy() != tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_UNKNOWN || len(plan.GetCapabilityRequirements()) != 0 || len(plan.GetAffectedAllocationIds()) != 0 {
		return false
	}
	for _, action := range plan.GetActions() {
		if action == nil || action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND || action.GetRollback() != nil {
			return false
		}
	}
	return true
}
