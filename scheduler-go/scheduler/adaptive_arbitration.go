package scheduler

import (
	"sort"
	"strconv"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	"google.golang.org/protobuf/proto"
)

func annotateArbitrationBudget(proposals []PlannerProposal, budget PlannerBudgetConfig) {
	for index := range proposals {
		evidence := proposals[index].Evidence
		if evidence == nil {
			continue
		}
		evidence.Inputs = append(evidence.Inputs,
			semanticIntField("planner.budget.max_actions", int64(budget.MaxActions)),
			semanticIntField("planner.budget.max_affected_sandboxes", int64(budget.MaxAffectedSandboxes)),
			semanticIntField("planner.budget.max_gpu_reconfigurations", int64(budget.MaxGPUReconfigurations)),
			semanticIntField("planner.budget.max_recovery_cost_nanos", budget.MaxRecoveryCostNanos),
			semanticBoolField("planner.budget.l4_disabled", budget.DisableL4),
		)
		sort.Slice(evidence.Inputs, func(i, j int) bool { return evidence.Inputs[i].GetKey() < evidence.Inputs[j].GetKey() })
	}
}

// arbitrateProposals chooses a deterministic set of compatible proposals. A
// transaction never mixes PlanPurpose values because the Store gives each
// purpose distinct reservation and compensation semantics.
func arbitrateProposals(input PlanningInput, proposals []PlannerProposal, budget PlannerBudgetConfig) ([]int, *tgsrlv1.PlacementPlan) {
	ordered := make([]int, len(proposals))
	for index := range proposals {
		ordered[index] = index
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		left, right := proposals[ordered[i]], proposals[ordered[j]]
		if plannerPriority(left) != plannerPriority(right) {
			return plannerPriority(left) < plannerPriority(right)
		}
		if left.Evidence.GetUtilityNanos() != right.Evidence.GetUtilityNanos() {
			return left.Evidence.GetUtilityNanos() > right.Evidence.GetUtilityNanos()
		}
		if left.Evidence.GetPlanner() != right.Evidence.GetPlanner() {
			return left.Evidence.GetPlanner() < right.Evidence.GetPlanner()
		}
		if left.Evidence.GetActionType() != right.Evidence.GetActionType() {
			return left.Evidence.GetActionType() < right.Evidence.GetActionType()
		}
		if left.Evidence.GetTargetId() != right.Evidence.GetTargetId() {
			return left.Evidence.GetTargetId() < right.Evidence.GetTargetId()
		}
		return left.Evidence.GetProposalId() < right.Evidence.GetProposalId()
	})

	selected := make([]int, 0, budget.MaxActions)
	selectedPurpose := tgsrlv1.PlanPurpose_PLAN_PURPOSE_UNKNOWN
	selectedClass := planMergeUnknown
	targets := make(map[string]struct{})
	actionCount, gpuReconfigurations := 0, 0
	var recoveryCost int64
	for _, index := range ordered {
		proposal := &proposals[index]
		if proposal.Plan == nil || proposal.Evidence.GetDisposition() == tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED {
			continue
		}
		cost := proposalCost(*proposal)
		mergeClass := classifyPlanMerge(proposal.Plan)
		switch {
		case selectedPurpose != tgsrlv1.PlanPurpose_PLAN_PURPOSE_UNKNOWN && proposal.Plan.GetPurpose() != selectedPurpose:
			deferProposal(proposal, "INCOMPATIBLE_PLAN_PURPOSE")
		case actionCount+cost.actions > budget.MaxActions:
			deferProposal(proposal, "PER_TICK_ACTION_BUDGET")
		case len(targets)+countNewTargets(proposal.Plan, targets) > budget.MaxAffectedSandboxes:
			deferProposal(proposal, "AFFECTED_SANDBOX_BUDGET")
		case gpuReconfigurations+cost.gpuReconfigurations > budget.MaxGPUReconfigurations:
			deferProposal(proposal, "GPU_RECONFIGURATION_BUDGET")
		case cost.containsL4 && budget.DisableL4:
			deferProposal(proposal, "L4_DISABLED_BY_BUDGET")
		case recoveryCost > budget.MaxRecoveryCostNanos || cost.recoveryCost > budget.MaxRecoveryCostNanos-recoveryCost:
			deferProposal(proposal, "RECOVERY_COST_BUDGET")
		case len(selected) > 0 && (selectedClass == planMergeExclusive || mergeClass != selectedClass):
			deferProposal(proposal, "INCOMPATIBLE_TRANSACTION_SHAPE")
		case conflictsWithSelected(proposal.Plan, proposals, selected):
			deferProposal(proposal, "TARGET_CONFLICT")
		default:
			selected = append(selected, index)
			selectedPurpose = proposal.Plan.GetPurpose()
			selectedClass = mergeClass
			actionCount += cost.actions
			gpuReconfigurations += cost.gpuReconfigurations
			recoveryCost += cost.recoveryCost
			for _, target := range proposalTargets(proposal.Plan) {
				targets[target] = struct{}{}
			}
			proposal.Evidence.Disposition = tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_SELECTED
			proposal.Evidence.Reason = "SELECTED_BY_PRIORITY_AND_BUDGET"
		}
	}
	if len(selected) == 0 {
		return nil, nil
	}
	if !contractActionSetComplete(input, proposals, selected) {
		for _, index := range selected {
			deferProposal(&proposals[index], "CONTRACT_ACTION_SET_INCOMPLETE")
		}
		return nil, nil
	}
	return selected, mergeSelectedPlans(input, proposals, selected)
}

type planMergeClass uint8

const (
	planMergeUnknown planMergeClass = iota
	planMergeAdmission
	planMergeLifecycle
	planMergeExclusive
)

func classifyPlanMerge(plan *tgsrlv1.PlacementPlan) planMergeClass {
	if plan == nil || len(plan.GetActions()) == 0 {
		return planMergeUnknown
	}
	if plan.GetPurpose() == tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION {
		return planMergeAdmission
	}
	for _, action := range plan.GetActions() {
		switch action.GetActionType() {
		case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE,
			tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY,
			tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
			tgsrlv1.ActionType_ACTION_TYPE_RESUME,
			tgsrlv1.ActionType_ACTION_TYPE_SLEEP,
			tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		default:
			return planMergeExclusive
		}
	}
	return planMergeLifecycle
}

func contractActionSetComplete(input PlanningInput, proposals []PlannerProposal, selected []int) bool {
	if !input.ContractAggregate.Blocking || input.ContractAggregate.Action != tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED {
		return true
	}
	actions := make([]*tgsrlv1.Action, 0, len(selected))
	for _, index := range selected {
		actions = append(actions, proposals[index].Plan.GetActions()...)
	}
	return contractPauseActionsComplete(input, actions)
}

func contractPauseActionsComplete(input PlanningInput, actions []*tgsrlv1.Action) bool {
	required := make(map[string]struct{})
	for _, allocation := range input.Snapshot.GetAllocations() {
		if allocation != nil && allocation.GetState() == tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE &&
			allocation.GetExecutionId() == input.Intent.GetExecutionId() && allocation.GetStageId() == input.Intent.GetStageId() && allocation.GetIntentVersion() == input.Intent.GetVersion() {
			required[allocation.GetAllocationId()] = struct{}{}
		}
	}
	if len(required) == 0 {
		return false
	}

	// A caller-owned target filter may narrow an optimization request, but it
	// must never narrow a blocking contract action. Fresh observations that
	// already report PAUSED count as satisfied; every other active allocation
	// must have a selected pause action.
	contractInput := input
	contractInput.Directives.TargetSandboxIDs = nil
	freshTargets, _, _ := activeAdaptiveTargetsWithFreshness(contractInput)
	satisfied := make(map[string]struct{}, len(required))
	for _, target := range freshTargets {
		if target.sandbox.GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED {
			satisfied[target.allocation.GetAllocationId()] = struct{}{}
		}
	}
	for _, action := range actions {
		if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_PAUSE {
			satisfied[action.GetTargetId()] = struct{}{}
		}
	}
	for allocationID := range required {
		if _, ok := satisfied[allocationID]; !ok {
			return false
		}
	}
	return true
}

type proposalBudgetCost struct {
	actions             int
	gpuReconfigurations int
	recoveryCost        int64
	containsL4          bool
}

func proposalCost(proposal PlannerProposal) proposalBudgetCost {
	cost := proposalBudgetCost{actions: len(proposal.Plan.GetActions())}
	for _, action := range proposal.Plan.GetActions() {
		level, _ := actionpolicy.LevelForAction(action.GetActionType())
		if level == tgsrlv1.ActionLevel_ACTION_LEVEL_L4 {
			cost.containsL4 = true
			cost.gpuReconfigurations++
		}
	}
	for _, field := range proposal.Evidence.GetInputs() {
		if field.GetKey() != "planner.cost_recovery_nanos" {
			continue
		}
		if value, ok := field.GetValue().GetKind().(*tgsrlv1.SemanticValue_Int64Value); ok && value.Int64Value > 0 {
			cost.recoveryCost = value.Int64Value
		}
	}
	return cost
}

func plannerPriority(proposal PlannerProposal) int {
	if proposal.Evidence == nil {
		return 99
	}
	if proposal.Evidence.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_PAUSE {
		for _, field := range proposal.Evidence.GetInputs() {
			if field.GetKey() == "planner.contract_blocking" {
				if value, ok := field.GetValue().GetKind().(*tgsrlv1.SemanticValue_BoolValue); ok && value.BoolValue {
					return 0
				}
			}
		}
	}
	switch proposal.Plan.GetPurpose() {
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY:
		return 1
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION:
		return 2
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION:
		return 3
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE:
		return 4
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION:
		return 5
	default:
		return 5
	}
}

func proposalTargets(plan *tgsrlv1.PlacementPlan) []string {
	set := make(map[string]struct{})
	for _, action := range plan.GetActions() {
		target := action.GetSandboxId()
		if target == "" {
			target = action.GetTargetId()
		}
		if target != "" {
			set[target] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for target := range set {
		result = append(result, target)
	}
	sort.Strings(result)
	return result
}

func countNewTargets(plan *tgsrlv1.PlacementPlan, selected map[string]struct{}) int {
	count := 0
	for _, target := range proposalTargets(plan) {
		if _, exists := selected[target]; !exists {
			count++
		}
	}
	return count
}

func conflictsWithSelected(plan *tgsrlv1.PlacementPlan, proposals []PlannerProposal, selected []int) bool {
	for _, candidate := range plan.GetActions() {
		for _, selectedIndex := range selected {
			for _, existing := range proposals[selectedIndex].Plan.GetActions() {
				if actionTarget(candidate) == actionTarget(existing) && !compatibleSameTargetActions(candidate.GetActionType(), existing.GetActionType()) {
					return true
				}
			}
		}
	}
	return false
}

func actionTarget(action *tgsrlv1.Action) string {
	if action.GetSandboxId() != "" {
		return action.GetSandboxId()
	}
	return action.GetTargetId()
}

func compatibleSameTargetActions(left, right tgsrlv1.ActionType) bool {
	return left == tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE && right == tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY ||
		left == tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY && right == tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE
}

func deferProposal(proposal *PlannerProposal, reason string) {
	proposal.Evidence.Disposition = tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_DEFERRED
	proposal.Evidence.Reason = reason
	proposal.Plan = nil
}

func mergeSelectedPlans(input PlanningInput, proposals []PlannerProposal, selected []int) *tgsrlv1.PlacementPlan {
	if input.AdmissionPlan != nil &&
		input.AdmissionPlan.GetPurpose() == tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION &&
		len(selected) == len(input.AdmissionPlan.GetActions()) {
		allAdmission := true
		for _, index := range selected {
			allAdmission = allAdmission && proposals[index].Plan.GetPurpose() == tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION
		}
		if allAdmission {
			return proto.Clone(input.AdmissionPlan).(*tgsrlv1.PlacementPlan)
		}
	}
	base := proto.Clone(proposals[selected[0]].Plan).(*tgsrlv1.PlacementPlan)
	if len(selected) == 1 {
		return base
	}
	planID := base.GetPlanId()
	for _, proposalIndex := range selected[1:] {
		if proposals[proposalIndex].Plan.GetPlanId() != planID {
			planID = ""
			break
		}
	}
	if planID == "" {
		planID = stableID("arbitrated-plan", input.DecisionID, strconv.FormatUint(input.Snapshot.GetRevision(), 10))
	}
	base.PlanId = planID
	base.Actions = nil
	base.Bindings = nil
	base.AffectedAllocationIds = nil
	base.CapabilityRequirements = nil
	base.RollbackPolicy = tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_NOT_REQUIRED
	for _, proposalIndex := range selected {
		plan := proposals[proposalIndex].Plan
		base.Bindings = append(base.Bindings, cloneMessages(plan.GetBindings())...)
		base.AffectedAllocationIds = append(base.AffectedAllocationIds, plan.GetAffectedAllocationIds()...)
		base.CapabilityRequirements = append(base.CapabilityRequirements, cloneMessages(plan.GetCapabilityRequirements())...)
		for _, source := range plan.GetActions() {
			action := proto.Clone(source).(*tgsrlv1.Action)
			action.Order = uint32(len(base.Actions) + 1)
			action.PlanId = base.GetPlanId()
			if source.GetPlanId() != base.GetPlanId() {
				action.ActionId = stableID("arbitrated-action", base.GetPlanId(), source.GetActionId(), strconv.FormatUint(uint64(action.GetOrder()), 10))
				action.IdempotencyKey = stableID("arbitrated-action-idempotency", source.GetIdempotencyKey(), base.GetPlanId(), strconv.FormatUint(uint64(action.GetOrder()), 10))
			}
			base.Actions = append(base.Actions, action)
		}
	}
	sort.Strings(base.AffectedAllocationIds)
	base.AffectedAllocationIds = uniqueStrings(base.AffectedAllocationIds)
	base.CapabilityRequirements = actionpolicy.StableCapabilityRequirements(base.CapabilityRequirements...)
	if len(base.Actions) > 1 {
		base.RollbackPolicy = tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION
		base.CapabilityRequirements = actionpolicy.StableCapabilityRequirements(append(base.CapabilityRequirements,
			actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION),
			actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK),
		)...)
	}
	return base
}

func uniqueStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}
