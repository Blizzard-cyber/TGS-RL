package scheduler

import (
	"fmt"
	"sort"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

// Coordinator evaluates all eligible planner tiers, selects by fixed-point
// utility, and projects bounded audit evidence. It is stateless and safe for
// concurrent use.
type Coordinator struct {
	evidenceBudget    int
	actionBudget      int
	utility           PlannerUtilityConfig
	idleSleepAfter    time.Duration
	idleOffloadAfter  time.Duration
	sandboxMaximumAge time.Duration
}

func NewCoordinator(config Config) *Coordinator {
	budget := config.PlannerEvidenceBudget
	if budget <= 0 {
		budget = DefaultPlannerEvidenceBudget
	}
	actionBudget := config.PlannerPerTickActionBudget
	if actionBudget <= 0 {
		actionBudget = DefaultPlannerActionBudget
	}
	sleepAfter := config.PlannerIdleSleepAfter
	if sleepAfter <= 0 {
		sleepAfter = DefaultPlannerIdleSleepAfter
	}
	offloadAfter := config.PlannerIdleOffloadAfter
	if offloadAfter <= 0 {
		offloadAfter = DefaultPlannerIdleOffloadAfter
	}
	sandboxMaximumAge := config.PlannerSandboxMaximumAge
	return &Coordinator{evidenceBudget: budget, actionBudget: actionBudget, utility: normalizePlannerUtilityConfig(config.PlannerUtility), idleSleepAfter: sleepAfter, idleOffloadAfter: offloadAfter, sandboxMaximumAge: sandboxMaximumAge}
}

func (c *Coordinator) Plan(input PlanningInput) (PlannerResult, error) {
	if c == nil {
		return PlannerResult{}, fmt.Errorf("scheduler: planner coordinator is nil")
	}
	if input.Snapshot == nil {
		return PlannerResult{}, &ValidationError{Field: "planning_input.snapshot", Reason: "must not be nil"}
	}
	if input.Intent == nil {
		return PlannerResult{}, &ValidationError{Field: "planning_input.intent", Reason: "must not be nil"}
	}
	if input.EvaluationContext == nil {
		return PlannerResult{}, &ValidationError{Field: "planning_input.evaluation_context", Reason: "must not be nil"}
	}
	if input.EvaluationContext.GetEvaluationTime() == nil || input.EvaluationContext.GetDecisionSequence() == 0 || input.EvaluationContext.GetTickKind() == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		return PlannerResult{}, &ValidationError{Field: "planning_input.evaluation_context", Reason: "requires evaluation_time, decision_sequence, and tick_kind"}
	}
	input = clonePlanningInput(input)
	input.sandboxMaximumAge = c.sandboxMaximumAge
	if input.sandboxMaximumAge <= 0 {
		input.sandboxMaximumAge = defaultSandboxMaximumAge(input.EvaluationContext.GetTickKind())
	}
	input.sandboxFutureSkew = DefaultPlannerSandboxFutureSkew
	if err := normalizePlanningSignals(&input, c.actionBudget); err != nil {
		return PlannerResult{}, &ValidationError{Field: "planning_input.signals", Reason: err.Error()}
	}
	if input.Intent.GetPhaseKind() == tgsrlv1.PhaseKind_PHASE_KIND_IDLE {
		if input.Directives.SleepAfter == 0 {
			input.Directives.SleepAfter = c.idleSleepAfter
		}
		if input.Directives.OffloadAfter == 0 {
			input.Directives.OffloadAfter = c.idleOffloadAfter
		}
	}
	if strings.TrimSpace(input.DecisionID) == "" {
		input.DecisionID = stableID("planner-decision", input.Snapshot.GetSnapshotId(), fmt.Sprint(input.Snapshot.GetRevision()), input.Intent.GetExecutionId(), input.Intent.GetStageId(), fmt.Sprint(input.EvaluationContext.GetDecisionSequence()), fmt.Sprint(input.Intent.GetDeterministicSeed()))
	}

	var planners []Planner
	if input.AdmissionPlan != nil {
		planners = []Planner{AdmissionPlanner{Utility: c.utility}}
	} else {
		switch input.EvaluationContext.GetTickKind() {
		case tgsrlv1.TickKind_TICK_KIND_FAST:
			planners = []Planner{FastMutationPlanner{Utility: c.utility}}
		case tgsrlv1.TickKind_TICK_KIND_MEDIUM:
			planners = []Planner{MediumLifecyclePlanner{Utility: c.utility}}
		case tgsrlv1.TickKind_TICK_KIND_SLOW:
			planners = []Planner{SlowReconfigurationPlanner{Utility: c.utility}}
		}
	}
	proposals := make([]PlannerProposal, 0)
	for _, planner := range planners {
		proposals = append(proposals, planner.Propose(input)...)
	}
	for index := range proposals {
		if proposals[index].Evidence == nil {
			return PlannerResult{}, fmt.Errorf("scheduler: planner returned proposal without evidence")
		}
	}
	// Proposals rejected solely because their requested state is already true
	// describe convergence, not a scheduling failure. Keep their evidence, but
	// return the normal no-action result instead of NO_ELIGIBLE fallback.
	allAlreadySatisfied := len(proposals) > 0
	for _, proposal := range proposals {
		if proposal.Evidence.GetReason() != "ALREADY_SATISFIED" && proposal.Evidence.GetReason() != "TICK_NOT_ELIGIBLE" {
			allAlreadySatisfied = false
			break
		}
	}
	sortProposals(proposals)
	applyActionBudget(proposals, input.Signals.PerTickActionBudget)
	selected := -1
	for index := range proposals {
		if proposals[index].Plan != nil && proposals[index].Evidence.GetDisposition() != tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED {
			selected = index
			break
		}
	}
	if selected >= 0 {
		proposals[selected].Evidence.Disposition = tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_SELECTED
		proposals[selected].Evidence.Reason = "SELECTED_MAX_UTILITY"
		for index := range proposals {
			if index == selected || proposals[index].Plan == nil || proposals[index].Evidence.GetDisposition() == tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED {
				continue
			}
			proposals[index].Evidence.Disposition = tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_DEFERRED
			proposals[index].Evidence.Reason = "LOWER_UTILITY_OR_STABLE_TIE_BREAK"
		}
	}
	result := PlannerResult{TotalProposalCount: uint64(len(proposals))}
	if selected >= 0 {
		result.Plan = proto.Clone(proposals[selected].Plan).(*tgsrlv1.PlacementPlan)
	} else if len(proposals) > 0 && !allAlreadySatisfied {
		result.FallbackReason = "NO_ELIGIBLE_PLANNER_PROPOSAL"
	} else if len(proposals) == 0 {
		evidence := plannerEvidence(input, plannerKindForTick(input.EvaluationContext.GetTickKind()), tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION, tgsrlv1.ActionType_ACTION_TYPE_UNKNOWN, "", 0, tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_DEFERRED, "CONVERGED_NO_TRIGGER")
		proposals = append(proposals, PlannerProposal{Evidence: evidence})
	}
	result.Evidence = projectPlannerEvidence(proposals, c.evidenceBudget, selected)
	result.EvidenceTruncated = int(result.TotalProposalCount) > len(result.Evidence)
	return result, nil
}

func plannerKindForTick(tick tgsrlv1.TickKind) tgsrlv1.PlannerKind {
	switch tick {
	case tgsrlv1.TickKind_TICK_KIND_FAST:
		return tgsrlv1.PlannerKind_PLANNER_KIND_FAST_MUTATION
	case tgsrlv1.TickKind_TICK_KIND_MEDIUM:
		return tgsrlv1.PlannerKind_PLANNER_KIND_MEDIUM_LIFECYCLE
	case tgsrlv1.TickKind_TICK_KIND_SLOW:
		return tgsrlv1.PlannerKind_PLANNER_KIND_SLOW_RECONFIGURATION
	default:
		return tgsrlv1.PlannerKind_PLANNER_KIND_UNKNOWN
	}
}

func projectPlannerEvidence(proposals []PlannerProposal, budget, selected int) []*tgsrlv1.PlannerEvidence {
	if budget <= 0 {
		budget = DefaultPlannerEvidenceBudget
	}
	indexes := make([]int, 0, len(proposals))
	if selected >= 0 {
		indexes = append(indexes, selected)
	}
	for index := range proposals {
		if index == selected || len(indexes) >= budget {
			continue
		}
		indexes = append(indexes, index)
	}
	sort.Slice(indexes, func(i, j int) bool {
		return proposals[indexes[i]].Evidence.GetProposalId() < proposals[indexes[j]].Evidence.GetProposalId()
	})
	result := make([]*tgsrlv1.PlannerEvidence, 0, len(indexes))
	for _, index := range indexes {
		result = append(result, proto.Clone(proposals[index].Evidence).(*tgsrlv1.PlannerEvidence))
	}
	return result
}
