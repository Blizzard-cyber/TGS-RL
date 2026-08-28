package scheduler

import (
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/semantics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Scheduler) normalizeEvaluationContext(input *tgsrlv1.EvaluationContext) (*tgsrlv1.EvaluationContext, time.Time, uint64, error) {
	defaultsApplied := input == nil
	var ctx *tgsrlv1.EvaluationContext
	if input == nil {
		ctx = &tgsrlv1.EvaluationContext{}
	} else {
		ctx = proto.Clone(input).(*tgsrlv1.EvaluationContext)
	}

	now := time.Time{}
	if ctx.GetEvaluationTime() != nil {
		if err := ctx.GetEvaluationTime().CheckValid(); err != nil {
			return nil, time.Time{}, 0, &ValidationError{Field: "evaluation_context.evaluation_time", Reason: err.Error()}
		}
		now = ctx.GetEvaluationTime().AsTime().UTC()
	} else {
		defaultsApplied = true
		now = s.clock.Now().UTC()
		if now.IsZero() {
			return nil, time.Time{}, 0, &ValidationError{Field: "clock", Reason: "returned zero time"}
		}
		ctx.EvaluationTime = timestamppb.New(now)
	}
	if err := ctx.GetEvaluationTime().CheckValid(); err != nil {
		return nil, time.Time{}, 0, &ValidationError{Field: "evaluation_context.evaluation_time", Reason: err.Error()}
	}

	sequence := ctx.GetDecisionSequence()
	if sequence == 0 {
		defaultsApplied = true
		sequence = s.nextSequence()
		ctx.DecisionSequence = sequence
	}
	if sequence == 0 {
		return nil, time.Time{}, 0, &ValidationError{Field: "evaluation_context.decision_sequence", Reason: "must not be zero"}
	}

	if ctx.GetTickKind() == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		defaultsApplied = true
		ctx.TickKind = tgsrlv1.TickKind_TICK_KIND_FAST
	}
	ctx.CompatibilityDefaultsApplied = ctx.GetCompatibilityDefaultsApplied() || defaultsApplied

	return ctx, now, sequence, nil
}

func applyEvaluationContext(record *tgsrlv1.DecisionRecord, ctx *tgsrlv1.EvaluationContext) {
	if record == nil || ctx == nil {
		return
	}
	record.TickKind = ctx.GetTickKind()
	record.EvaluationContext = proto.Clone(ctx).(*tgsrlv1.EvaluationContext)
}

// evaluateContract consumes the safe-point fact already resolved by the
// scheduler so contract and candidate feasibility cannot disagree.
func evaluateContract(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, ctx *tgsrlv1.EvaluationContext, safePoint semantics.SafePointResolution) ([]*tgsrlv1.ContractEvaluation, semantics.AggregateResult, error) {
	contract := intent.GetExecutionContract()
	if contract == nil {
		return nil, semantics.AggregateResult{}, nil
	}
	evals, aggregate, err := semantics.Evaluate(semantics.Input{
		Contract:  contract,
		Intent:    intent,
		Snapshot:  snapshot,
		Context:   ctx,
		SafePoint: &safePoint,
	})
	if err != nil {
		return nil, semantics.AggregateResult{}, err
	}
	return evals, aggregate, nil
}

func appendContractEvaluations(record *tgsrlv1.DecisionRecord, evaluations []*tgsrlv1.ContractEvaluation) {
	if record == nil || len(evaluations) == 0 {
		return
	}
	record.ContractEvaluations = make([]*tgsrlv1.ContractEvaluation, 0, len(evaluations))
	for _, evaluation := range evaluations {
		if evaluation == nil {
			continue
		}
		record.ContractEvaluations = append(record.ContractEvaluations, proto.Clone(evaluation).(*tgsrlv1.ContractEvaluation))
	}
}

func appendBlockingContractRejections(record *tgsrlv1.DecisionRecord, evaluations []*tgsrlv1.ContractEvaluation) {
	if record == nil {
		return
	}
	for _, evaluation := range evaluations {
		if evaluation == nil || !semanticsBlocking(evaluation) {
			continue
		}
		record.RejectedCandidates = append(record.RejectedCandidates, &tgsrlv1.CandidateRejection{
			CandidateId:        stableID("contract", record.GetDecisionId(), evaluation.GetClauseKind().String(), evaluation.GetClauseId(), evaluation.GetStatus().String()),
			Reason:             contractRejectionReason(evaluation),
			Detail:             contractRejectionDetail(evaluation),
			ContractEvaluation: proto.Clone(evaluation).(*tgsrlv1.ContractEvaluation),
		})
	}
}

func semanticsBlocking(evaluation *tgsrlv1.ContractEvaluation) bool {
	if evaluation == nil {
		return false
	}
	switch evaluation.GetRecommendedAction() {
	case tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ABORT_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_WAIT_FOR_SAFE_POINT:
		return true
	default:
		return false
	}
}

func contractRejectionReason(evaluation *tgsrlv1.ContractEvaluation) tgsrlv1.CandidateRejectionReason {
	if evaluation == nil {
		return tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VALIDITY_RULE
	}
	switch evaluation.GetClauseKind() {
	case tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_VERSION_CONSTRAINT:
		return tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VERSION_CONSTRAINT
	default:
		return tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VALIDITY_RULE
	}
}

func contractRejectionDetail(evaluation *tgsrlv1.ContractEvaluation) string {
	if evaluation == nil {
		return "contract evaluation blocked scheduling"
	}
	var parts []string
	if evaluation.GetClauseKind() != tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_UNKNOWN {
		parts = append(parts, evaluation.GetClauseKind().String())
	}
	if strings.TrimSpace(evaluation.GetClauseId()) != "" {
		parts = append(parts, evaluation.GetClauseId())
	}
	if strings.TrimSpace(evaluation.GetDetail()) != "" {
		parts = append(parts, evaluation.GetDetail())
	}
	return strings.Join(parts, ": ")
}

func contractFallbackReason(aggregate semantics.AggregateResult) string {
	if aggregate.Action == tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_WAIT_FOR_SAFE_POINT {
		return FallbackReasonSafePoint
	}
	action := aggregate.Action.String()
	const prefix = "CONTRACT_DECISION_ACTION_"
	if strings.HasPrefix(action, prefix) {
		action = strings.TrimPrefix(action, prefix)
	}
	action = strings.TrimSpace(action)
	if action == "" || action == "UNKNOWN" {
		return "CONTRACT_BLOCKED"
	}
	return "CONTRACT_" + action
}

func validateActionPlan(plan *tgsrlv1.PlacementPlan, ctx *tgsrlv1.EvaluationContext) error {
	if plan == nil || ctx == nil {
		return nil
	}
	return actionpolicy.ValidatePlan(plan, ctx.GetTickKind(), actionpolicy.ValidationOptions{
		AllowLegacyUnknownTickL1: ctx.GetCompatibilityDefaultsApplied(),
	})
}
