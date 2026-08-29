package scheduler

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/protection"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/semantics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	compatPlannerTargetShare    = "tgsrl.io/adaptive.target-share"
	compatPlannerTargetPriority = "tgsrl.io/adaptive.target-priority"
	compatPlannerTargetState    = "tgsrl.io/adaptive.target-state"
	compatPlannerAllowResize    = "tgsrl.io/adaptive.allow-resize"
	compatPlannerAllowScaleIn   = "tgsrl.io/adaptive.allow-scale-in"
	compatPlannerAllowRebind    = "tgsrl.io/adaptive.allow-rebind"
	compatPlannerAllowRecreate  = "tgsrl.io/adaptive.allow-recreate"
	compatPlannerAllowOffload   = "tgsrl.io/adaptive.allow-offload"
	compatPlannerSleepAfter     = "tgsrl.io/adaptive.sleep-after"
	compatPlannerOffloadAfter   = "tgsrl.io/adaptive.offload-after"
)

// EvaluateAdaptive preserves legacy admission by delegating it to
// EvaluateWithContext. Only already-satisfied work reaches the pure adaptive
// coordinator. Projected sandboxes and recent decisions are explicit inputs.
func (s *Scheduler) EvaluateAdaptive(input *AdaptiveEvaluationInput) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	if input == nil {
		return nil, nil, &ValidationError{Field: "adaptive_input", Reason: "must not be nil"}
	}
	if s == nil {
		return nil, nil, &ValidationError{Field: "scheduler", Reason: "must not be nil"}
	}
	if input.Snapshot == nil || input.Intent == nil {
		return nil, nil, &ValidationError{Field: "adaptive_input", Reason: "snapshot and intent are required"}
	}
	owned := clonePlanningInput(*input)
	input = &owned
	// Validate before workUnits so malformed input cannot steer evaluation to a
	// different planner path.
	if err := validateSnapshot(input.Snapshot); err != nil {
		return nil, nil, err
	}
	if err := validateIntent(input.Intent); err != nil {
		return nil, nil, err
	}
	units, fallbackReason, _ := workUnits(input.Snapshot, input.Intent)
	scaleIn := fallbackReason == FallbackReasonPendingCount && activeAllocationCount(*input) > int(input.Intent.GetUnitCount())
	if (fallbackReason != "" && !scaleIn) || len(units) > 0 {
		plan, record, err := s.EvaluateWithContext(input.Snapshot, input.Intent, input.EvaluationContext)
		if err != nil || plan == nil || record == nil {
			return plan, record, err
		}
		ctx := record.GetEvaluationContext()
		if ctx == nil {
			ctx = input.EvaluationContext
		}
		coordinated, coordinateErr := s.coordinator.Plan(PlanningInput{Snapshot: input.Snapshot, Intent: input.Intent, EvaluationContext: ctx, Sandboxes: input.Sandboxes, RecentDecisions: input.RecentDecisions, Directives: input.Directives, Signals: input.Signals, AdmissionPlan: plan, DecisionID: record.GetDecisionId(), SafePoint: input.SafePoint, ContractAggregate: input.ContractAggregate})
		if coordinateErr != nil {
			return nil, nil, coordinateErr
		}
		applyPlannerResult(record, coordinated)
		return plan, record, nil
	}

	// Reuse validation/context normalization and decision metadata without
	// consulting any external state. This local path mirrors admission setup.
	snapshot := proto.Clone(input.Snapshot).(*tgsrlv1.ClusterSnapshot)
	intent := proto.Clone(input.Intent).(*tgsrlv1.SchedulingIntent)
	ctx, now, sequence, err := s.normalizeEvaluationContext(input.EvaluationContext)
	if err != nil {
		return nil, nil, err
	}
	if !now.Before(intent.GetValidUntil().AsTime()) {
		return s.EvaluateWithContext(snapshot, intent, ctx)
	}
	decisionID := stableID("decision", snapshot.GetSnapshotId(), strconv.FormatUint(snapshot.GetRevision(), 10), intent.GetExecutionId(), intent.GetStageId(), strconv.FormatUint(intent.GetVersion(), 10), strconv.FormatUint(sequence, 10))
	record := &tgsrlv1.DecisionRecord{DecisionId: decisionID, Sequence: sequence, ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(), IntentVersion: intent.GetVersion(), SnapshotRevision: snapshot.GetRevision(), DataKind: s.resolveDataKind(snapshot, intent), CodeRevision: s.codeRevision, DecidedAt: proto.Clone(ctx.GetEvaluationTime()).(*timestamppb.Timestamp), PolicyVersion: intent.GetPolicyVersion(), DeterministicSeed: intent.GetDeterministicSeed(), ConfigRevision: s.configRevision, JobId: intent.GetJobId(), RunId: intent.GetRunId(), TraceId: intent.GetTraceId(), SemanticContext: cloneSemanticEnvelope(intent.GetSemanticContext()), Generation: intent.GetGeneration(), Cursor: intent.GetCursor()}
	applyEvaluationContext(record, ctx)
	safe, aggregate, evaluations, err := resolveAdaptiveContract(input, snapshot, intent, ctx, s.safePointKey)
	if err != nil {
		return nil, nil, &ValidationError{Field: "intent.execution_contract", Reason: err.Error()}
	}
	appendContractEvaluations(record, evaluations)
	directives := cloneDirectives(input.Directives)
	if directivesEmpty(directives) {
		directives, err = parseCompatibilityDirectives(intent.GetLabels())
		if err != nil {
			return nil, nil, &ValidationError{Field: "intent.labels", Reason: err.Error()}
		}
	}
	coordinated, err := s.coordinator.Plan(PlanningInput{Snapshot: snapshot, Intent: intent, EvaluationContext: ctx, Sandboxes: input.Sandboxes, RecentDecisions: input.RecentDecisions, SafePoint: safe, ContractAggregate: aggregate, Directives: directives, Signals: input.Signals, DecisionID: decisionID})
	if err != nil {
		return nil, nil, err
	}
	applyPlannerResult(record, coordinated)
	if aggregate.Blocking && !plannerResultSatisfiesPause(coordinated) {
		appendBlockingContractRejections(record, evaluations)
		return s.fallbackResult(snapshot, intent, now, record, contractFallbackReason(aggregate))
	}
	if coordinated.Plan == nil {
		if coordinated.FallbackReason == "" {
			plan := makeConvergedPlan(decisionID, snapshot, intent, ctx)
			record.SelectedPlan = proto.Clone(plan).(*tgsrlv1.PlacementPlan)
			return proto.Clone(plan).(*tgsrlv1.PlacementPlan), proto.Clone(record).(*tgsrlv1.DecisionRecord), nil
		}
		reason := coordinated.FallbackReason
		if reason == "" {
			reason = "NO_ELIGIBLE_PLANNER_PROPOSAL"
		}
		return s.fallbackResult(snapshot, intent, now, record, reason)
	}
	if err := validateActionPlan(coordinated.Plan, ctx); err != nil {
		markSelectedPlannerRejected(record, "ACTION_POLICY_"+err.Error())
		return s.fallbackResult(snapshot, intent, now, record, "ACTION_POLICY_"+err.Error())
	}
	guardKey := intent.GetExecutionId() + "/" + intent.GetStageId()
	utility := selectedPlannerUtility(record)
	if guardDecision := s.guard.CheckN(guardKey, float64(utility)/1e9, len(coordinated.Plan.GetActions())); !guardDecision.Allowed {
		reason := "PROTECTION_" + guardDecision.Reason
		markSelectedPlannerRejected(record, reason, guardEvidence(s.guard, guardDecision.Reason)...)
		return s.fallbackResult(snapshot, intent, now, record, reason)
	}
	record.SelectedPlan = proto.Clone(coordinated.Plan).(*tgsrlv1.PlacementPlan)
	return proto.Clone(coordinated.Plan).(*tgsrlv1.PlacementPlan), proto.Clone(record).(*tgsrlv1.DecisionRecord), nil
}

func plannerResultSatisfiesPause(result PlannerResult) bool {
	if result.Plan == nil || len(result.Plan.GetActions()) == 0 {
		return false
	}
	for _, action := range result.Plan.GetActions() {
		if action == nil || action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_PAUSE {
			return false
		}
	}
	return true
}

func makeConvergedPlan(decisionID string, snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, ctx *tgsrlv1.EvaluationContext) *tgsrlv1.PlacementPlan {
	return &tgsrlv1.PlacementPlan{
		PlanId:      stableID("converged-plan", decisionID, snapshot.GetSnapshotId(), strconv.FormatUint(snapshot.GetRevision(), 10)),
		ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(), IntentVersion: intent.GetVersion(), SnapshotRevision: snapshot.GetRevision(),
		CreatedAt: proto.Clone(ctx.GetEvaluationTime()).(*timestamppb.Timestamp), ExpiresAt: proto.Clone(intent.GetValidUntil()).(*timestamppb.Timestamp),
		DecisionId: decisionID, RunId: intent.GetRunId(), TraceId: intent.GetTraceId(), DataKind: intent.GetDataKind(), SemanticContext: cloneSemanticEnvelope(intent.GetSemanticContext()),
		Generation: intent.GetGeneration(), Cursor: intent.GetCursor(), JobId: intent.GetJobId(), Purpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION, RollbackPolicy: tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_NOT_REQUIRED,
	}
}

func directivesEmpty(value Directives) bool {
	return value.TargetShare == nil && value.TargetPriority == nil && value.TargetResources == nil &&
		len(value.ResizeResources) == 0 && len(value.ReplacementBindings) == 0 && len(value.SandboxByAllocation) == 0 && len(value.TargetSandboxIDs) == 0 &&
		value.DesiredState == tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN && !value.AllowPause && !value.AllowResume && !value.AllowSleep && !value.AllowOffload &&
		value.SleepAfter == 0 && value.OffloadAfter == 0 && !value.AllowResize && !value.AllowScaleIn && !value.AllowRebind && !value.AllowRecreate && value.MinimumUtilityNanos == 0
}

func applyPlannerResult(record *tgsrlv1.DecisionRecord, result PlannerResult) {
	if record == nil {
		return
	}
	record.PlannerEvidence = cloneMessages(result.Evidence)
	record.TotalPlannerProposalCount = result.TotalProposalCount
	record.PlannerEvidenceTruncated = result.EvidenceTruncated
}

func selectedPlannerUtility(record *tgsrlv1.DecisionRecord) int64 {
	for _, evidence := range record.GetPlannerEvidence() {
		if evidence.GetDisposition() == tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_SELECTED {
			return evidence.GetUtilityNanos()
		}
	}
	return 0
}

func markSelectedPlannerRejected(record *tgsrlv1.DecisionRecord, reason string, fields ...*tgsrlv1.SemanticField) {
	for _, evidence := range record.GetPlannerEvidence() {
		if evidence.GetDisposition() == tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_SELECTED {
			evidence.Disposition = tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED
			evidence.Reason = reason
			evidence.Inputs = append(evidence.Inputs, fields...)
			sort.Slice(evidence.Inputs, func(i, j int) bool { return evidence.Inputs[i].GetKey() < evidence.Inputs[j].GetKey() })
		}
	}
}

func guardEvidence(guard *protection.Guard, reason string) []*tgsrlv1.SemanticField {
	config := protection.Config{}
	if guard != nil {
		config = guard.Config()
	}
	return []*tgsrlv1.SemanticField{
		semanticStringField("planner.guard.reason", reason),
		semanticIntField("planner.guard.cooldown_nanos", config.Cooldown.Nanoseconds()),
		semanticDoubleField("planner.guard.hysteresis", config.Hysteresis),
		semanticIntField("planner.guard.max_actions_per_window", int64(config.MaxActionsPerWindow)),
	}
}

// parseCompatibilityDirectives is the sole legacy label ingress. Core planners
// consume only Directives. Values are strict and malformed values fail closed.
func parseCompatibilityDirectives(labels map[string]string) (Directives, error) {
	var result Directives
	parseBool := func(key string, target *bool) error {
		raw, exists := labels[key]
		if !exists {
			return nil
		}
		if raw != "true" && raw != "false" {
			return fmt.Errorf("%s must be true or false", key)
		}
		*target = raw == "true"
		return nil
	}
	if raw, ok := labels[compatPlannerTargetShare]; ok {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
			return Directives{}, fmt.Errorf("%s must be finite within [0,1]", compatPlannerTargetShare)
		}
		result.TargetShare = &value
	}
	if raw, ok := labels[compatPlannerTargetPriority]; ok {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return Directives{}, fmt.Errorf("%s must be an int32", compatPlannerTargetPriority)
		}
		value := int32(parsed)
		result.TargetPriority = &value
	}
	for _, item := range []struct {
		key    string
		target *bool
	}{
		{compatPlannerAllowResize, &result.AllowResize},
		{compatPlannerAllowScaleIn, &result.AllowScaleIn},
		{compatPlannerAllowRebind, &result.AllowRebind},
		{compatPlannerAllowRecreate, &result.AllowRecreate},
		{compatPlannerAllowOffload, &result.AllowOffload},
	} {
		if err := parseBool(item.key, item.target); err != nil {
			return Directives{}, err
		}
	}
	for _, item := range []struct {
		key    string
		target *time.Duration
	}{
		{compatPlannerSleepAfter, &result.SleepAfter},
		{compatPlannerOffloadAfter, &result.OffloadAfter},
	} {
		if raw, ok := labels[item.key]; ok {
			value, err := time.ParseDuration(raw)
			if err != nil || value <= 0 {
				return Directives{}, fmt.Errorf("%s must be a positive duration", item.key)
			}
			*item.target = value
		}
	}
	if raw, ok := labels[compatPlannerTargetState]; ok {
		switch strings.ToLower(raw) {
		case "running":
			result.DesiredState = tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING
		case "paused":
			result.DesiredState = tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED
		case "sleeping":
			result.DesiredState = tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING
		default:
			return Directives{}, fmt.Errorf("%s must be running, paused, or sleeping", compatPlannerTargetState)
		}
	}
	return result, nil
}

func resolveAdaptiveContract(input *AdaptiveEvaluationInput, snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, ctx *tgsrlv1.EvaluationContext, safePointKey string) (semantics.SafePointResolution, semantics.AggregateResult, []*tgsrlv1.ContractEvaluation, error) {
	safe := input.SafePoint
	if !safe.Present {
		safe = semantics.ResolveSafePoint(snapshot, intent, ctx, safePointKey)
	}
	aggregate := input.ContractAggregate
	evaluations, derived, err := evaluateContract(snapshot, intent, ctx, safe)
	if err != nil {
		return safe, aggregate, nil, err
	}
	if aggregate == (semantics.AggregateResult{}) {
		aggregate = derived
	}
	return safe, aggregate, evaluations, nil
}
