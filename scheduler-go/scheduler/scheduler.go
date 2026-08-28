// Package scheduler implements the deterministic, provider-independent scheduling
// decision core. It deliberately depends only on generated protocol DTOs.
package scheduler

import (
	"strconv"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/candidates"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/policy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/semantics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Evaluate deterministically evaluates a cloned snapshot/intent pair and
// returns independent PlacementPlan and DecisionRecord object graphs. It never
// mutates, retains, or aliases either input.
func (s *Scheduler) Evaluate(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	return s.EvaluateWithContext(snapshot, intent, nil)
}

// EvaluateWithContext extends Evaluate with explicit deterministic scheduling
// context while preserving the legacy Evaluate compatibility boundary.
func (s *Scheduler) EvaluateWithContext(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, evaluationContext *tgsrlv1.EvaluationContext) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	if s == nil {
		return nil, nil, &ValidationError{Field: "scheduler", Reason: "must not be nil"}
	}
	if snapshot == nil {
		return nil, nil, &ValidationError{Field: "snapshot", Reason: "must not be nil"}
	}
	if intent == nil {
		return nil, nil, &ValidationError{Field: "intent", Reason: "must not be nil"}
	}

	// Clone before validation so even getters and future normalization logic are
	// isolated from concurrent caller mutation.
	snapshot = proto.Clone(snapshot).(*tgsrlv1.ClusterSnapshot)
	intent = proto.Clone(intent).(*tgsrlv1.SchedulingIntent)
	ctx, now, sequence, err := s.normalizeEvaluationContext(evaluationContext)
	if err != nil {
		return nil, nil, err
	}
	if err := validateSnapshot(snapshot); err != nil {
		return nil, nil, err
	}
	if err := validateIntent(intent); err != nil {
		return nil, nil, err
	}

	nowTimestamp := timestamppb.New(now)
	decisionID := stableID("decision", snapshot.GetSnapshotId(), strconv.FormatUint(snapshot.GetRevision(), 10), intent.GetExecutionId(), intent.GetStageId(), strconv.FormatUint(intent.GetVersion(), 10), strconv.FormatUint(sequence, 10))
	record := &tgsrlv1.DecisionRecord{
		DecisionId:        decisionID,
		Sequence:          sequence,
		ExecutionId:       intent.GetExecutionId(),
		StageId:           intent.GetStageId(),
		IntentVersion:     intent.GetVersion(),
		SnapshotRevision:  snapshot.GetRevision(),
		DataKind:          s.resolveDataKind(snapshot, intent),
		CodeRevision:      s.codeRevision,
		DecidedAt:         nowTimestamp,
		PolicyVersion:     intent.GetPolicyVersion(),
		DeterministicSeed: intent.GetDeterministicSeed(),
		ConfigRevision:    s.configRevision,
		JobId:             intent.GetJobId(),
		RunId:             intent.GetRunId(),
		TraceId:           intent.GetTraceId(),
		SemanticContext:   cloneSemanticEnvelope(intent.GetSemanticContext()),
		Generation:        intent.GetGeneration(),
		Cursor:            intent.GetCursor(),
	}
	applyEvaluationContext(record, ctx)
	safePointResolution := semantics.ResolveSafePoint(snapshot, intent, ctx, s.safePointKey)
	contractEvaluations, aggregate, err := evaluateContract(snapshot, intent, ctx, safePointResolution)
	if err != nil {
		return nil, nil, &ValidationError{Field: "intent.execution_contract", Reason: err.Error()}
	}
	appendContractEvaluations(record, contractEvaluations)

	if !now.Before(intent.GetValidUntil().AsTime()) {
		record.RejectedCandidates = []*tgsrlv1.CandidateRejection{{
			CandidateId: stableID("expired", intent.GetExecutionId(), intent.GetStageId(), strconv.FormatUint(intent.GetVersion(), 10)),
			Reason:      tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_EXPIRED,
			Detail:      "intent valid_until is not after evaluation time",
		}}
		return s.fallbackResult(snapshot, intent, now, record, FallbackReasonIntentExpired)
	}
	if aggregate.Blocking {
		appendBlockingContractRejections(record, contractEvaluations)
		return s.fallbackResult(snapshot, intent, now, record, contractFallbackReason(aggregate))
	}

	units, fallbackReason, fallbackDetail := workUnits(snapshot, intent)
	if fallbackReason != "" {
		record.RejectedCandidates = []*tgsrlv1.CandidateRejection{{
			CandidateId: stableID("pending-version", intent.GetExecutionId(), intent.GetStageId()),
			Reason:      tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VERSION_CONSTRAINT,
			Detail:      fallbackDetail,
		}}
		return s.fallbackResult(snapshot, intent, now, record, fallbackReason)
	}
	if s.policy != nil && (s.policy.Name() == "noop" || s.policy.Name() == "static") {
		_, reason := s.policy.Choose(snapshot, intent, policy.SelectionInput{})
		return s.fallbackResult(snapshot, intent, now, record, "POLICY_"+reason)
	}

	requiresSafePoint := contractRequiresSafePoint(intent.GetExecutionContract())
	safePoint := safePointResolution.Present && safePointResolution.Value
	engineUnits := make([]candidates.Unit, 0, len(units))
	for _, unit := range units {
		engineUnits = append(engineUnits, candidates.Unit{ID: unit.id, Pending: unit.pending})
	}
	var selectCandidate candidates.SelectFunc
	if s.policy != nil && s.policy.Name() != "score_first" {
		selectCandidate = func(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, unitID string, input []*tgsrlv1.PlacementCandidate, topK int) (*tgsrlv1.PlacementCandidate, string) {
			return s.policy.Choose(snapshot, intent, policy.SelectionInput{UnitID: unitID, Candidates: input, TopK: topK})
		}
	}
	result, err := (candidates.Engine{}).Evaluate(candidates.Request{
		Snapshot: snapshot,
		Intent:   intent,
		Decision: candidates.DecisionMetadata{
			DecisionID:        decisionID,
			RequiresSafePoint: requiresSafePoint,
			SafePoint:         safePoint,
		},
		Units:  engineUnits,
		Select: selectCandidate,
		CandidateID: func(unitID, deviceID string) string {
			return stableID("candidate", unitID, deviceID, strconv.FormatUint(snapshot.GetRevision(), 10), strconv.FormatUint(intent.GetVersion(), 10))
		},
		Config: candidates.Config{TopK: s.policyBundle.TopK, EvidenceBudget: candidates.DefaultEvidenceBudget},
	})
	if err != nil {
		return nil, nil, &ValidationError{Field: "candidates", Reason: err.Error()}
	}
	record.Candidates = result.Candidates
	record.RejectedCandidates = append(record.RejectedCandidates, result.Rejections...)
	record.TotalCandidateCount = result.TotalCandidateCount
	record.TotalRejectedCandidateCount += result.TotalRejectionCount
	record.EvidenceTruncated = result.EvidenceTruncated
	for _, candidate := range record.GetCandidates() {
		if candidate == nil || candidate.GetPlan() == nil || len(candidate.GetPlan().GetBindings()) != 1 {
			continue
		}
		populateBindingMetadata(candidate.Plan.Bindings[0], decisionID, intent)
		populateCandidatePlanMetadata(candidate.Plan, candidate.GetCandidateId(), decisionID, snapshot, intent, now, requiresSafePoint, record.GetTickKind())
	}

	if len(result.Selected) != len(units) {
		if len(result.Selected) == 0 && len(units) == 1 {
			if preemptionPlan, reason := s.preemptionPlan(snapshot, intent, now, record, units[0], safePoint); preemptionPlan != nil {
				if err := validateActionPlan(preemptionPlan, ctx); err != nil {
					return s.fallbackResult(snapshot, intent, now, record, "ACTION_POLICY_"+err.Error())
				}
				guardKey := intent.GetExecutionId() + "/" + intent.GetStageId()
				if guardDecision := s.guard.AllowN(guardKey, 0, len(preemptionPlan.GetActions())); !guardDecision.Allowed {
					return s.fallbackResult(snapshot, intent, now, record, "PROTECTION_"+guardDecision.Reason)
				}
				record.SelectedPlan = proto.Clone(preemptionPlan).(*tgsrlv1.PlacementPlan)
				return proto.Clone(preemptionPlan).(*tgsrlv1.PlacementPlan), proto.Clone(record).(*tgsrlv1.DecisionRecord), nil
			} else if reason != "" {
				return s.fallbackResult(snapshot, intent, now, record, reason)
			}
		}
		reason := result.Fallback
		if reason == "" || reason == candidates.FallbackNoCandidate {
			reason = FallbackReasonNoCandidate
			if requiresSafePoint && !safePoint {
				reason = FallbackReasonSafePoint
			}
		} else {
			reason = "POLICY_" + reason
		}
		s.guard.Reject(intent.GetExecutionId() + "/" + intent.GetStageId())
		return s.fallbackResult(snapshot, intent, now, record, reason)
	}

	bindings := make([]*tgsrlv1.Binding, 0, len(result.Selected))
	for _, choice := range result.Selected {
		binding := proto.Clone(choice.Candidate.GetPlan().GetBindings()[0]).(*tgsrlv1.Binding)
		populateBindingMetadata(binding, decisionID, intent)
		bindings = append(bindings, binding)
	}
	planID := stableID("plan", decisionID, snapshot.GetSnapshotId(), strconv.FormatUint(snapshot.GetRevision(), 10), intent.GetIdempotencyKey())
	plan := makePlan(decisionID, planID, snapshot, intent, now, bindings, requiresSafePoint, record.GetTickKind())
	if err := validateActionPlan(plan, ctx); err != nil {
		return s.fallbackResult(snapshot, intent, now, record, "ACTION_POLICY_"+err.Error())
	}
	guardKey := intent.GetExecutionId() + "/" + intent.GetStageId()
	if guardDecision := s.guard.AllowN(guardKey, recordCandidateScore(result.Selected), len(plan.GetActions())); !guardDecision.Allowed {
		return s.fallbackResult(snapshot, intent, now, record, "PROTECTION_"+guardDecision.Reason)
	}
	record.SelectedPlan = proto.Clone(plan).(*tgsrlv1.PlacementPlan)
	for _, choice := range result.Selected {
		record.Score += choice.Candidate.GetScore()
	}
	if len(result.Selected) > 0 {
		record.Score = roundScore(record.Score / float64(len(result.Selected)))
	}
	return proto.Clone(plan).(*tgsrlv1.PlacementPlan), proto.Clone(record).(*tgsrlv1.DecisionRecord), nil
}
