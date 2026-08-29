package semantics

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

// ProtocolVersion is the canonical wire-protocol release implemented by this
// scheduler. It is the only component version the evaluator may supply from
// code; every other component version must be observed on an input surface.
const ProtocolVersion = "0.3.0"
const safePointAnnotation = "tgsrl.io/safe-point"

// SafePointResolution is the immutable safe-point fact shared by contract
// evaluation and candidate/action planning. Present distinguishes an observed
// false value from a missing observation.
type SafePointResolution struct {
	Value    bool
	Present  bool
	Evidence []*tgsrlv1.SemanticField
}

type Input struct {
	Contract  *tgsrlv1.ExecutionContract
	Intent    *tgsrlv1.SchedulingIntent
	Snapshot  *tgsrlv1.ClusterSnapshot
	Context   *tgsrlv1.EvaluationContext
	SafePoint *SafePointResolution
}

type AggregateResult struct {
	Blocking       bool
	Action         tgsrlv1.ContractDecisionAction
	FallbackReason string
	BlockingAction string
}

func Evaluate(input Input) ([]*tgsrlv1.ContractEvaluation, AggregateResult, error) {
	if input.Contract == nil {
		return nil, AggregateResult{}, errors.New("contract is nil")
	}
	if input.Intent == nil {
		return nil, AggregateResult{}, errors.New("intent is nil")
	}
	if input.Snapshot == nil {
		return nil, AggregateResult{}, errors.New("snapshot is nil")
	}
	if input.Context == nil {
		return nil, AggregateResult{}, errors.New("context is nil")
	}
	if input.Context.GetEvaluationTime() == nil {
		return nil, AggregateResult{}, errors.New("evaluation context time is missing")
	}
	if err := input.Context.GetEvaluationTime().CheckValid(); err != nil {
		return nil, AggregateResult{}, fmt.Errorf("evaluation context time is invalid: %w", err)
	}

	if input.SafePoint == nil {
		resolved := ResolveSafePoint(input.Snapshot, input.Intent, input.Context, safePointAnnotation)
		input.SafePoint = &resolved
	}
	obs := effectiveObservation(input.Context, input.Intent)
	facts, err := buildFacts(input, obs)
	if err != nil {
		return nil, AggregateResult{}, err
	}
	versions, err := buildComponentRegistry(input, obs)
	if err != nil {
		return nil, AggregateResult{}, err
	}

	evals := make([]*tgsrlv1.ContractEvaluation, 0,
		len(input.Contract.GetValidityRules())+
			len(input.Contract.GetConditions())+
			len(input.Contract.GetCriticalFactPolicies())+
			len(input.Contract.GetVersionConstraints())+2)

	for _, rule := range input.Contract.GetValidityRules() {
		evals = append(evals, evaluateValidityRule(input, facts, rule))
	}
	for _, cond := range input.Contract.GetConditions() {
		evals = append(evals, evaluateConditionClause(input, facts, cond))
	}
	for _, policy := range input.Contract.GetCriticalFactPolicies() {
		evals = append(evals, evaluateCriticalFactPolicy(input, facts, policy))
	}
	for _, constraint := range input.Contract.GetVersionConstraints() {
		evals = append(evals, evaluateVersionConstraint(input, versions, constraint))
	}
	if input.Contract.GetBackpressurePolicy() != nil {
		evals = append(evals, evaluateBackpressure(input, facts, input.Contract.GetBackpressurePolicy()))
	}
	if input.Contract.GetSafePointPolicy() != nil || input.Contract.GetCommitPolicy() != nil {
		evals = append(evals, evaluateSafePoint(input, facts))
	}

	sort.SliceStable(evals, func(i, j int) bool {
		return evaluationLess(evals[i], evals[j])
	})
	return evals, Aggregate(evals), nil
}

func Aggregate(evals []*tgsrlv1.ContractEvaluation) AggregateResult {
	result := AggregateResult{
		Action: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW,
	}
	var best *tgsrlv1.ContractEvaluation
	for _, eval := range evals {
		if eval == nil {
			continue
		}
		if !isBlocking(eval) {
			continue
		}
		if best == nil || aggregatePriorityGreater(eval, best) {
			best = eval
		}
	}
	if best == nil {
		return result
	}
	result.Blocking = true
	result.Action = best.GetRecommendedAction()
	result.BlockingAction = blockingAction(best.GetRecommendedAction())
	result.FallbackReason = fmt.Sprintf("%s:%s:%s", best.GetClauseKind().String(), best.GetClauseId(), best.GetStatus().String())
	return result
}

func aggregatePriorityGreater(candidate, current *tgsrlv1.ContractEvaluation) bool {
	candidateDisposition := dispositionPriority(candidate.GetObservationDisposition())
	currentDisposition := dispositionPriority(current.GetObservationDisposition())
	if candidateDisposition != currentDisposition {
		return candidateDisposition > currentDisposition
	}
	return actionPriority(candidate.GetRecommendedAction()) > actionPriority(current.GetRecommendedAction())
}

func dispositionPriority(disposition tgsrlv1.ObservationDisposition) int {
	switch disposition {
	case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK:
		return 2
	case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD:
		return 1
	default:
		// Legacy evaluations have no disposition. DEGRADE and NOT_APPLICABLE
		// are non-blocking, but keep them at the same neutral rank so this
		// comparator remains safe for direct callers.
		return 0
	}
}

func effectiveObservation(ctx *tgsrlv1.EvaluationContext, intent *tgsrlv1.SchedulingIntent) *tgsrlv1.ContractObservation {
	if ctx != nil && ctx.GetContractObservation() != nil {
		return ctx.GetContractObservation()
	}
	if intent != nil {
		return intent.GetContractObservation()
	}
	return nil
}

// ResolveSafePoint applies the authoritative precedence: evaluation-context
// observation, intent observation, configured annotation, canonical annotation,
// then legacy annotation keys. It does not mutate any input.
func ResolveSafePoint(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, ctx *tgsrlv1.EvaluationContext, configuredKey string) SafePointResolution {
	if ctx != nil && ctx.GetContractObservation() != nil && ctx.GetContractObservation().SafePoint != nil {
		return SafePointResolution{Value: ctx.GetContractObservation().GetSafePoint(), Present: true}
	}
	if intent != nil && intent.GetContractObservation() != nil && intent.GetContractObservation().SafePoint != nil {
		return SafePointResolution{Value: intent.GetContractObservation().GetSafePoint(), Present: true}
	}
	if snapshot == nil || intent == nil {
		return SafePointResolution{}
	}
	keys := stableSafePointKeys(configuredKey, intent.GetExecutionId(), intent.GetStageId())
	for _, key := range keys {
		raw, exists := snapshot.GetAnnotations()[key]
		if !exists {
			continue
		}
		evidence := []*tgsrlv1.SemanticField{
			semanticField("snapshot.safe_point.annotation_key", semanticString(key)),
			semanticField("snapshot.safe_point.annotation_raw", semanticString(raw)),
		}
		value, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return SafePointResolution{Evidence: evidence}
		}
		return SafePointResolution{Value: value, Present: true, Evidence: evidence}
	}
	return SafePointResolution{}
}

func stableSafePointKeys(configuredKey, executionID, stageID string) []string {
	bases := []string{strings.TrimSpace(configuredKey), safePointAnnotation, "safe_point"}
	seen := make(map[string]struct{}, len(bases)*2)
	keys := make([]string, 0, len(bases)*2)
	for _, base := range bases {
		if base == "" {
			continue
		}
		for _, key := range []string{base + "/" + executionID + "/" + stageID, base} {
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	return keys
}

type factRegistry struct {
	values         map[string]*tgsrlv1.SemanticValue
	metadata       map[string]observationMetadata
	origins        map[string]observationOrigin
	issues         map[string]factIssue
	policies       map[string]*tgsrlv1.ObservationPolicy
	evaluationTime time.Time
}

type observationOrigin uint8

const (
	originBuiltIn observationOrigin = iota + 1
	originLegacy
	originExplicit
)

type observationMetadata struct {
	observedAt time.Time
	present    bool
	source     string
	revision   uint64
}

type factIssue struct {
	reason   string
	evidence []*tgsrlv1.SemanticField
}

func buildFacts(input Input, obs *tgsrlv1.ContractObservation) (*factRegistry, error) {
	evaluationTime := input.Context.GetEvaluationTime().AsTime().UTC()
	registry := &factRegistry{
		values:         make(map[string]*tgsrlv1.SemanticValue),
		metadata:       make(map[string]observationMetadata),
		origins:        make(map[string]observationOrigin),
		issues:         make(map[string]factIssue),
		policies:       make(map[string]*tgsrlv1.ObservationPolicy),
		evaluationTime: evaluationTime,
	}
	for _, critical := range input.Contract.GetCriticalFactPolicies() {
		if critical == nil || strings.TrimSpace(critical.GetFactPath()) == "" || critical.GetObservationPolicy() == nil {
			return nil, errors.New("critical fact policy is malformed")
		}
		path := strings.TrimSpace(critical.GetFactPath())
		if _, exists := registry.policies[path]; exists {
			return nil, fmt.Errorf("critical fact policy %q is duplicated", path)
		}
		if err := validateObservationPolicy(critical.GetObservationPolicy()); err != nil {
			return nil, fmt.Errorf("critical fact policy %q: %w", path, err)
		}
		registry.policies[path] = proto.Clone(critical.GetObservationPolicy()).(*tgsrlv1.ObservationPolicy)
	}
	if err := registry.register("intent.version.current", semanticUint(input.Intent.GetVersion()), observationMetadata{}, originBuiltIn); err != nil {
		return nil, err
	}
	if input.SafePoint != nil && input.SafePoint.Present {
		if err := registry.register("runtime.safe_point", semanticBool(input.SafePoint.Value), observationMetadata{}, originBuiltIn); err != nil {
			return nil, err
		}
	}
	if input.SafePoint != nil && len(input.SafePoint.Evidence) > 0 {
		registry.issues["runtime.safe_point"] = factIssue{
			reason:   "runtime safe point resolved from snapshot annotation fallback",
			evidence: stableFields(input.SafePoint.Evidence),
		}
	}

	if obs != nil {
		metadata, err := legacyObservationMetadata(obs)
		if err != nil {
			return nil, err
		}
		register := func(path string, value *tgsrlv1.SemanticValue) error {
			return registry.register(path, value, metadata, originLegacy)
		}
		if obs.PolicyLag != nil {
			if err := register("sample.policy_lag", semanticUint(obs.GetPolicyLag())); err != nil {
				return nil, err
			}
		}
		if obs.SampleStale != nil {
			if err := register("sample.stale", semanticBool(obs.GetSampleStale())); err != nil {
				return nil, err
			}
		}
		if obs.BufferLevel != nil {
			if err := register("buffer.level.current", semanticUint(obs.GetBufferLevel())); err != nil {
				return nil, err
			}
		}
		if obs.AcceptedSamples != nil {
			if err := register("batch.accepted_samples", semanticUint(obs.GetAcceptedSamples())); err != nil {
				return nil, err
			}
			if err := register("group.accepted_samples", semanticUint(obs.GetAcceptedSamples())); err != nil {
				return nil, err
			}
		}
		if obs.ExpectedSamples != nil {
			if err := register("group.expected_samples", semanticUint(obs.GetExpectedSamples())); err != nil {
				return nil, err
			}
		}
		if obs.SampleCount != nil {
			if err := register("sample.sample_count", semanticUint(obs.GetSampleCount())); err != nil {
				return nil, err
			}
		}
		if obs.EffectiveSampleSize != nil {
			if !finite(obs.GetEffectiveSampleSize()) || obs.GetEffectiveSampleSize() < 0 {
				registry.issues["batch.effective_sample_size"] = factIssue{
					reason: "effective_sample_size is invalid",
					evidence: []*tgsrlv1.SemanticField{
						semanticField("batch.effective_sample_size.invalid", semanticDouble(obs.GetEffectiveSampleSize())),
					},
				}
			} else {
				if err := register("batch.effective_sample_size", semanticDouble(obs.GetEffectiveSampleSize())); err != nil {
					return nil, err
				}
			}
		}
		if obs.EffectiveSampleSizeRatio != nil {
			if !finite(obs.GetEffectiveSampleSizeRatio()) || obs.GetEffectiveSampleSizeRatio() < 0 {
				registry.issues["batch.effective_sample_size_ratio"] = factIssue{
					reason: "effective_sample_size_ratio is invalid",
					evidence: []*tgsrlv1.SemanticField{
						semanticField("batch.effective_sample_size_ratio.invalid", semanticDouble(obs.GetEffectiveSampleSizeRatio())),
					},
				}
			} else {
				if err := register("batch.effective_sample_size_ratio", semanticDouble(obs.GetEffectiveSampleSizeRatio())); err != nil {
					return nil, err
				}
			}
		}
		if obs.GetOldestSampleAt() != nil {
			if err := obs.GetOldestSampleAt().CheckValid(); err != nil {
				return nil, fmt.Errorf("observation oldest_sample_at is invalid: %w", err)
			}
			age := evaluationTime.Sub(obs.GetOldestSampleAt().AsTime().UTC()).Milliseconds()
			if age < 0 {
				registry.issues["sample.age_ms"] = invalidObservationIssue("oldest sample timestamp is after evaluation time", obs.GetOldestSampleAt().AsTime().UTC(), evaluationTime)
			} else if err := registry.register("sample.age_ms", semanticInt(age), metadata, originLegacy); err != nil {
				return nil, err
			}
		}
		for _, field := range obs.GetTypedFacts() {
			if field == nil || strings.TrimSpace(field.GetKey()) == "" || field.GetKey() != strings.TrimSpace(field.GetKey()) || field.GetValue() == nil {
				return nil, errors.New("observation typed fact is malformed")
			}
			if !finiteSemanticValue(field.GetValue()) {
				return nil, fmt.Errorf("observation typed fact %q is not finite", field.GetKey())
			}
			if err := validateFactValue(field.GetKey(), field.GetValue()); err != nil {
				return nil, fmt.Errorf("observation typed fact %q: %w", field.GetKey(), err)
			}
			if _, exists := registry.issues[field.GetKey()]; exists {
				return nil, fmt.Errorf("typed fact %q conflicts with invalid registry fact", field.GetKey())
			}
			if err := registry.register(field.GetKey(), field.GetValue(), metadata, originLegacy); err != nil {
				return nil, err
			}
		}
		for _, observed := range obs.GetFactObservations() {
			if observed == nil || observed.GetFact() == nil || strings.TrimSpace(observed.GetFact().GetKey()) == "" || observed.GetFact().GetKey() != strings.TrimSpace(observed.GetFact().GetKey()) || observed.GetFact().GetValue() == nil {
				return nil, errors.New("observed fact is malformed")
			}
			if !finiteSemanticValue(observed.GetFact().GetValue()) {
				return nil, fmt.Errorf("observed fact %q is not finite", observed.GetFact().GetKey())
			}
			if err := validateFactValue(observed.GetFact().GetKey(), observed.GetFact().GetValue()); err != nil {
				return nil, fmt.Errorf("observed fact %q: %w", observed.GetFact().GetKey(), err)
			}
			observedMetadata, err := explicitObservationMetadata(observed.GetObservedAt(), observed.GetSource(), observed.GetRevision())
			if err != nil {
				return nil, fmt.Errorf("observed fact %q: %w", observed.GetFact().GetKey(), err)
			}
			if err := registry.register(observed.GetFact().GetKey(), observed.GetFact().GetValue(), observedMetadata, originExplicit); err != nil {
				return nil, err
			}
		}
	}
	return registry, nil
}

type conditionResult struct {
	status      tgsrlv1.ContractEvaluationStatus
	missing     []string
	evidence    []*tgsrlv1.SemanticField
	detail      string
	disposition tgsrlv1.ObservationDisposition
}

func evaluateValidityRule(input Input, facts *factRegistry, rule *tgsrlv1.ValidityRule) *tgsrlv1.ContractEvaluation {
	base := newEvaluation(input, tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_VALIDITY_RULE, rule.GetRuleId(), rule.GetPredicate(), rule.GetFailureMode())
	if rule.GetPredicate() == nil {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
		base.Detail = "legacy rule has no typed predicate"
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
		return finalizeEvaluation(input, base)
	}
	result := evalCondition(facts, rule.GetPredicate())
	applyConditionResult(base, result)
	base.Detail = fallbackDetail(result.detail, rule.GetDescription())
	if result.disposition == tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_UNKNOWN {
		base.RecommendedAction = actionFor(base.Status, rule.GetFailureMode())
	} else {
		base.RecommendedAction = actionForObservationDisposition(result.disposition)
	}
	return finalizeEvaluation(input, base)
}

func evaluateConditionClause(input Input, facts *factRegistry, cond *tgsrlv1.Condition) *tgsrlv1.ContractEvaluation {
	base := newEvaluation(input, tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_CONDITION, cond.GetConditionId(), cond, tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_REJECT)
	result := evalCondition(facts, cond)
	applyConditionResult(base, result)
	base.Detail = result.detail
	if result.disposition == tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_UNKNOWN {
		base.RecommendedAction = actionFor(base.Status, base.GetFailureMode())
	} else {
		base.RecommendedAction = actionForObservationDisposition(result.disposition)
	}
	return finalizeEvaluation(input, base)
}

func evaluateBackpressure(input Input, facts *factRegistry, policy *tgsrlv1.BackpressurePolicy) *tgsrlv1.ContractEvaluation {
	base := newEvaluation(input, tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_BACKPRESSURE_POLICY, "backpressure", nil, tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_REJECT)
	if guard, guarded := facts.observationGuard("buffer.level.current"); guarded {
		base.Evidence = append(base.Evidence, guard.evidence...)
		if guard.status != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_UNKNOWN {
			applyConditionResult(base, guard)
			base.Detail = guard.detail
			base.RecommendedAction = actionForObservationDisposition(guard.disposition)
			return finalizeEvaluation(input, base)
		}
	}
	current, ok := facts.values["buffer.level.current"]
	if !ok {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
		base.MissingKeys = []string{"buffer.level.current"}
		base.Detail = "missing current buffer level"
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
		return finalizeEvaluation(input, base)
	}
	level := current.GetUint64Value()
	base.Observations = append(base.Observations, semanticField("buffer.level.current", current))
	base.Evidence = append(base.Evidence,
		semanticField("buffer.low_watermark", semanticUint(policy.GetLowWatermark())),
		semanticField("buffer.high_watermark", semanticUint(policy.GetHighWatermark())),
		semanticField("buffer.maximum", semanticUint(policy.GetMaximumBufferLevel())),
	)
	switch {
	case level <= policy.GetLowWatermark():
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED
		base.Detail = "buffer level at or below low watermark"
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
	case level > policy.GetMaximumBufferLevel():
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED
		base.Detail = "buffer level exceeds maximum"
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT
	case level >= policy.GetHighWatermark():
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED
		base.Detail = "buffer level exceeds high watermark"
		base.RecommendedAction = backpressureDirective(policy.GetMode())
	default:
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
		base.Detail = "buffer level between watermarks without previous state"
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
	}
	return finalizeEvaluation(input, base)
}

func evaluateSafePoint(input Input, facts *factRegistry) *tgsrlv1.ContractEvaluation {
	base := newEvaluation(input, tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_SAFE_POINT_POLICY, "safe-point", nil, tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_PAUSE)
	require := input.Contract.GetCommitPolicy().GetRequireSafePoint()
	policyEnabled := input.Contract.GetSafePointPolicy().GetEnabled()
	base.Evidence = append(base.Evidence,
		semanticField("commit.require_safe_point", semanticBool(require)),
		semanticField("safe_point_policy.enabled", semanticBool(policyEnabled)),
	)
	value, ok := facts.values["runtime.safe_point"]
	if !require {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_NOT_APPLICABLE
		base.Detail = "safe point not required for commit"
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
		return finalizeEvaluation(input, base)
	}
	if guard, guarded := facts.observationGuard("runtime.safe_point"); guarded && guard.status != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_UNKNOWN {
		applyConditionResult(base, guard)
		base.Detail = guard.detail
		base.RecommendedAction = actionForObservationDisposition(guard.disposition)
		return finalizeEvaluation(input, base)
	}
	if !ok {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
		base.MissingKeys = []string{"runtime.safe_point"}
		base.Detail = "missing runtime safe point state"
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_WAIT_FOR_SAFE_POINT
		return finalizeEvaluation(input, base)
	}
	base.Observations = append(base.Observations, semanticField("runtime.safe_point", value))
	if issue, exists := facts.issues["runtime.safe_point"]; exists {
		base.Evidence = append(base.Evidence, issue.evidence...)
	}
	if value.GetBoolValue() {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED
		base.Detail = "runtime is at a safe point"
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
	} else {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED
		base.Detail = "runtime is not at a required safe point"
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_WAIT_FOR_SAFE_POINT
	}
	return finalizeEvaluation(input, base)
}

func evalCondition(facts *factRegistry, cond *tgsrlv1.Condition) conditionResult {
	if cond == nil {
		return conditionResult{
			status: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE,
			detail: "missing predicate",
		}
	}
	switch cond.GetOperator() {
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_AND:
		return evalLogicalAND(facts, cond)
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_OR:
		return evalLogicalOR(facts, cond)
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_NOT:
		return evalLogicalNOT(facts, cond)
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_EXISTS:
		return evalExists(facts, cond)
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_EQ,
		tgsrlv1.ConditionOperator_CONDITION_OPERATOR_NE,
		tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GT,
		tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GE,
		tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LT,
		tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LE,
		tgsrlv1.ConditionOperator_CONDITION_OPERATOR_IN,
		tgsrlv1.ConditionOperator_CONDITION_OPERATOR_NOT_IN:
		return evalBinaryCondition(facts, cond)
	default:
		return conditionResult{
			status: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE,
			detail: "unknown condition operator",
		}
	}
}

func evalLogicalAND(facts *factRegistry, cond *tgsrlv1.Condition) conditionResult {
	combined := conditionResult{
		status: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED,
	}
	indeterminate := false
	for _, pred := range cond.GetPredicates() {
		res := evalCondition(facts, pred)
		combined.evidence = append(combined.evidence, res.evidence...)
		combined.missing = append(combined.missing, res.missing...)
		combined.disposition = strongerDisposition(combined.disposition, res.disposition)
		switch res.status {
		case tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED:
			combined.status = res.status
			combined.detail = "AND predicate violated"
			combined.disposition = res.disposition
			return combined
		case tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE:
			indeterminate = true
		}
	}
	if indeterminate {
		combined.status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
		combined.detail = "AND predicate indeterminate"
	}
	return combined
}

func evalLogicalOR(facts *factRegistry, cond *tgsrlv1.Condition) conditionResult {
	combined := conditionResult{
		status: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED,
	}
	indeterminate := false
	for _, pred := range cond.GetPredicates() {
		res := evalCondition(facts, pred)
		combined.evidence = append(combined.evidence, res.evidence...)
		combined.missing = append(combined.missing, res.missing...)
		combined.disposition = strongerDisposition(combined.disposition, res.disposition)
		switch res.status {
		case tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED:
			combined.status = res.status
			combined.detail = "OR predicate satisfied"
			combined.disposition = tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_UNKNOWN
			return combined
		case tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE:
			indeterminate = true
		}
	}
	if indeterminate {
		combined.status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
		combined.detail = "OR predicate indeterminate"
	}
	return combined
}

func evalLogicalNOT(facts *factRegistry, cond *tgsrlv1.Condition) conditionResult {
	if len(cond.GetPredicates()) != 1 {
		return conditionResult{
			status: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE,
			detail: "NOT requires exactly one predicate",
		}
	}
	res := evalCondition(facts, cond.GetPredicates()[0])
	if res.disposition != tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_UNKNOWN {
		res.detail = "NOT predicate observation unavailable: " + res.detail
		return res
	}
	switch res.status {
	case tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED:
		res.status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED
	case tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED:
		res.status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED
	}
	res.detail = "NOT predicate evaluated"
	return res
}

func evalExists(facts *factRegistry, cond *tgsrlv1.Condition) conditionResult {
	guard, guarded := facts.observationGuard(cond.GetFactPath())
	if guarded && guard.status != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_UNKNOWN {
		return guard
	}
	value, ok := facts.values[cond.GetFactPath()]
	if !ok {
		if issue, exists := facts.issues[cond.GetFactPath()]; exists {
			return conditionResult{
				status:   tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE,
				missing:  []string{cond.GetFactPath()},
				evidence: issue.evidence,
				detail:   issue.reason,
			}
		}
		return conditionResult{
			status:  tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED,
			missing: []string{cond.GetFactPath()},
			detail:  "fact does not exist",
		}
	}
	return conditionResult{
		status:   tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED,
		evidence: append(guard.evidence, semanticField(cond.GetFactPath(), value)),
		detail:   "fact exists",
	}
}

func evalBinaryCondition(facts *factRegistry, cond *tgsrlv1.Condition) conditionResult {
	leftGuard, leftGuarded := facts.observationGuard(cond.GetFactPath())
	if leftGuarded && leftGuard.status != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_UNKNOWN {
		return leftGuard
	}
	rightGuard := conditionResult{}
	if cond.GetComparisonFactPath() != "" {
		var rightGuarded bool
		rightGuard, rightGuarded = facts.observationGuard(cond.GetComparisonFactPath())
		if rightGuarded && rightGuard.status != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_UNKNOWN {
			return rightGuard
		}
	}
	left, leftOk := facts.values[cond.GetFactPath()]
	if !leftOk {
		if issue, exists := facts.issues[cond.GetFactPath()]; exists {
			return conditionResult{
				status:   tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE,
				missing:  []string{cond.GetFactPath()},
				evidence: issue.evidence,
				detail:   issue.reason,
			}
		}
		return conditionResult{
			status:  tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE,
			missing: []string{cond.GetFactPath()},
			detail:  "missing fact",
		}
	}
	right, missing, err := comparisonValue(facts, cond)
	if err != nil {
		return conditionResult{
			status:  tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE,
			missing: missing,
			evidence: []*tgsrlv1.SemanticField{
				semanticField(cond.GetFactPath(), left),
			},
			detail: err.Error(),
		}
	}
	result, evalErr := compareValues(cond.GetOperator(), left, right)
	evidence := append(leftGuard.evidence, rightGuard.evidence...)
	evidence = append(evidence, semanticField(cond.GetFactPath(), left))
	if cond.GetComparisonFactPath() != "" {
		evidence = append(evidence, semanticField(cond.GetComparisonFactPath(), right))
	} else {
		evidence = append(evidence, semanticField("operand", right))
	}
	if evalErr != nil {
		return conditionResult{
			status:   tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE,
			evidence: evidence,
			detail:   evalErr.Error(),
		}
	}
	if result {
		return conditionResult{
			status:   tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED,
			evidence: evidence,
			detail:   "predicate satisfied",
		}
	}
	return conditionResult{
		status:   tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED,
		evidence: evidence,
		detail:   "predicate violated",
	}
}

func comparisonValue(facts *factRegistry, cond *tgsrlv1.Condition) (*tgsrlv1.SemanticValue, []string, error) {
	if cond.GetComparisonFactPath() != "" {
		value, ok := facts.values[cond.GetComparisonFactPath()]
		if !ok {
			if issue, exists := facts.issues[cond.GetComparisonFactPath()]; exists {
				return nil, []string{cond.GetComparisonFactPath()}, fmt.Errorf(issue.reason)
			}
			return nil, []string{cond.GetComparisonFactPath()}, fmt.Errorf("missing comparison fact")
		}
		return value, nil, nil
	}
	if len(cond.GetOperands()) != 1 {
		return nil, nil, fmt.Errorf("expected exactly one operand")
	}
	value := cond.GetOperands()[0]
	if value == nil {
		return nil, nil, fmt.Errorf("operand is nil")
	}
	if !finiteSemanticValue(value) {
		return nil, nil, fmt.Errorf("operand is not finite")
	}
	return value, nil, nil
}

func compareValues(op tgsrlv1.ConditionOperator, left, right *tgsrlv1.SemanticValue) (bool, error) {
	if left == nil || right == nil {
		return false, fmt.Errorf("nil comparison value")
	}
	switch op {
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_IN, tgsrlv1.ConditionOperator_CONDITION_OPERATOR_NOT_IN:
		list := right.GetListValue()
		if list == nil {
			return false, fmt.Errorf("IN/NOT_IN requires list operand")
		}
		matched := false
		for _, candidate := range list.GetValues() {
			equal, err := compareValues(tgsrlv1.ConditionOperator_CONDITION_OPERATOR_EQ, left, candidate)
			if err != nil {
				return false, err
			}
			if equal {
				matched = true
				break
			}
		}
		if op == tgsrlv1.ConditionOperator_CONDITION_OPERATOR_NOT_IN {
			return !matched, nil
		}
		return matched, nil
	}

	if left.GetKind() == nil || right.GetKind() == nil {
		return false, fmt.Errorf("missing typed value")
	}
	switch l := left.GetKind().(type) {
	case *tgsrlv1.SemanticValue_StringValue:
		r, ok := right.GetKind().(*tgsrlv1.SemanticValue_StringValue)
		if !ok {
			return false, fmt.Errorf("type mismatch")
		}
		return compareOrderedStrings(op, l.StringValue, r.StringValue)
	case *tgsrlv1.SemanticValue_BoolValue:
		r, ok := right.GetKind().(*tgsrlv1.SemanticValue_BoolValue)
		if !ok {
			return false, fmt.Errorf("type mismatch")
		}
		return compareOrderedBools(op, l.BoolValue, r.BoolValue)
	case *tgsrlv1.SemanticValue_Int64Value:
		r, ok := right.GetKind().(*tgsrlv1.SemanticValue_Int64Value)
		if !ok {
			return false, fmt.Errorf("type mismatch")
		}
		return compareOrderedInts(op, l.Int64Value, r.Int64Value)
	case *tgsrlv1.SemanticValue_Uint64Value:
		r, ok := right.GetKind().(*tgsrlv1.SemanticValue_Uint64Value)
		if !ok {
			return false, fmt.Errorf("type mismatch")
		}
		return compareOrderedUints(op, l.Uint64Value, r.Uint64Value)
	case *tgsrlv1.SemanticValue_DoubleValue:
		r, ok := right.GetKind().(*tgsrlv1.SemanticValue_DoubleValue)
		if !ok || !finite(l.DoubleValue) || !finite(r.DoubleValue) {
			return false, fmt.Errorf("invalid double comparison")
		}
		return compareOrderedFloats(op, l.DoubleValue, r.DoubleValue)
	case *tgsrlv1.SemanticValue_BytesValue:
		r, ok := right.GetKind().(*tgsrlv1.SemanticValue_BytesValue)
		if !ok {
			return false, fmt.Errorf("type mismatch")
		}
		return compareOrderedStrings(op, string(l.BytesValue), string(r.BytesValue))
	default:
		return false, fmt.Errorf("unsupported semantic type")
	}
}

func compareOrderedStrings(op tgsrlv1.ConditionOperator, left, right string) (bool, error) {
	switch op {
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_EQ:
		return left == right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_NE:
		return left != right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GT:
		return left > right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GE:
		return left >= right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LT:
		return left < right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LE:
		return left <= right, nil
	default:
		return false, fmt.Errorf("unsupported operator for string")
	}
}

func compareOrderedBools(op tgsrlv1.ConditionOperator, left, right bool) (bool, error) {
	switch op {
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_EQ:
		return left == right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_NE:
		return left != right, nil
	default:
		return false, fmt.Errorf("unsupported operator for bool")
	}
}

func compareOrderedInts(op tgsrlv1.ConditionOperator, left, right int64) (bool, error) {
	switch op {
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_EQ:
		return left == right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_NE:
		return left != right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GT:
		return left > right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GE:
		return left >= right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LT:
		return left < right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LE:
		return left <= right, nil
	default:
		return false, fmt.Errorf("unsupported operator for int64")
	}
}

func compareOrderedUints(op tgsrlv1.ConditionOperator, left, right uint64) (bool, error) {
	switch op {
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_EQ:
		return left == right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_NE:
		return left != right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GT:
		return left > right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GE:
		return left >= right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LT:
		return left < right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LE:
		return left <= right, nil
	default:
		return false, fmt.Errorf("unsupported operator for uint64")
	}
}

func compareOrderedFloats(op tgsrlv1.ConditionOperator, left, right float64) (bool, error) {
	switch op {
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_EQ:
		return left == right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_NE:
		return left != right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GT:
		return left > right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GE:
		return left >= right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LT:
		return left < right, nil
	case tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LE:
		return left <= right, nil
	default:
		return false, fmt.Errorf("unsupported operator for double")
	}
}

func newEvaluation(input Input, kind tgsrlv1.ContractClauseKind, clauseID string, predicate *tgsrlv1.Condition, failureMode tgsrlv1.ValidityFailureMode) *tgsrlv1.ContractEvaluation {
	return &tgsrlv1.ContractEvaluation{
		ContractId:        input.Contract.GetContractId(),
		ClauseKind:        kind,
		ClauseId:          clauseID,
		Predicate:         predicate,
		FailureMode:       failureMode,
		RecommendedAction: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW,
	}
}

func applyConditionResult(eval *tgsrlv1.ContractEvaluation, result conditionResult) {
	eval.Status = result.status
	eval.MissingKeys = stableStrings(result.missing)
	eval.Evidence = stableFields(result.evidence)
	eval.ObservationDisposition = result.disposition
}

func finalizeEvaluation(input Input, eval *tgsrlv1.ContractEvaluation) *tgsrlv1.ContractEvaluation {
	eval.Observations = stableFields(eval.GetObservations())
	eval.Evidence = stableFields(eval.GetEvidence())
	eval.MissingKeys = stableStrings(eval.GetMissingKeys())
	eval.EvaluationId = evaluationID(input, eval)
	return eval
}

func evaluationID(input Input, eval *tgsrlv1.ContractEvaluation) string {
	contextParts := []string{
		input.Contract.GetContractId(),
		input.Contract.GetVersion(),
		strconv.FormatUint(input.Intent.GetVersion(), 10),
		strconv.FormatUint(input.Snapshot.GetRevision(), 10),
		input.Context.GetTickKind().String(),
		strconv.FormatUint(input.Context.GetDecisionSequence(), 10),
		strconv.FormatUint(input.Context.GetObservedRevision(), 10),
		input.Context.GetCause(),
		strconv.FormatBool(input.Context.GetCompatibilityDefaultsApplied()),
	}
	if input.Context.GetEvaluationTime() != nil {
		contextParts = append(contextParts, input.Context.GetEvaluationTime().AsTime().UTC().Format(time.RFC3339Nano))
	}
	canonical := proto.Clone(eval).(*tgsrlv1.ContractEvaluation)
	canonical.EvaluationId = ""
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		// ContractEvaluation contains no marshaling extensions that can fail. Keep
		// a deterministic fail-closed identity if a future generated surface does.
		wire = []byte("marshal-error:" + err.Error())
	}
	material := append([]byte(strings.Join(contextParts, "|")), 0)
	material = append(material, wire...)
	sum := sha256.Sum256(material)
	return "eval-sha256-" + hex.EncodeToString(sum[:])
}

func semanticField(key string, value *tgsrlv1.SemanticValue) *tgsrlv1.SemanticField {
	if value == nil {
		return nil
	}
	return &tgsrlv1.SemanticField{
		Key:   key,
		Value: proto.Clone(value).(*tgsrlv1.SemanticValue),
	}
}

func semanticUint(value uint64) *tgsrlv1.SemanticValue {
	return &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_Uint64Value{Uint64Value: value}}
}

func semanticInt(value int64) *tgsrlv1.SemanticValue {
	return &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_Int64Value{Int64Value: value}}
}

func semanticDouble(value float64) *tgsrlv1.SemanticValue {
	return &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_DoubleValue{DoubleValue: value}}
}

func semanticBool(value bool) *tgsrlv1.SemanticValue {
	return &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_BoolValue{BoolValue: value}}
}

func semanticString(value string) *tgsrlv1.SemanticValue {
	return &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_StringValue{StringValue: value}}
}

func stableFields(fields []*tgsrlv1.SemanticField) []*tgsrlv1.SemanticField {
	filtered := make([]*tgsrlv1.SemanticField, 0, len(fields))
	for _, field := range fields {
		if field != nil {
			filtered = append(filtered, field)
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].GetKey() == filtered[j].GetKey() {
			left := ""
			right := ""
			if filtered[i].GetValue() != nil {
				left = filtered[i].GetValue().String()
			}
			if filtered[j].GetValue() != nil {
				right = filtered[j].GetValue().String()
			}
			return left < right
		}
		return filtered[i].GetKey() < filtered[j].GetKey()
	})
	return filtered
}

func stableStrings(values []string) []string {
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			filtered = append(filtered, value)
		}
	}
	sort.Strings(filtered)
	return filtered
}

func evaluationLess(left, right *tgsrlv1.ContractEvaluation) bool {
	if left.GetClauseKind() != right.GetClauseKind() {
		return left.GetClauseKind() < right.GetClauseKind()
	}
	if left.GetClauseId() != right.GetClauseId() {
		return left.GetClauseId() < right.GetClauseId()
	}
	return left.GetEvaluationId() < right.GetEvaluationId()
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func finiteSemanticValue(value *tgsrlv1.SemanticValue) bool {
	if value == nil {
		return false
	}
	if double, ok := value.GetKind().(*tgsrlv1.SemanticValue_DoubleValue); ok {
		return finite(double.DoubleValue)
	}
	if list, ok := value.GetKind().(*tgsrlv1.SemanticValue_ListValue); ok {
		for _, item := range list.ListValue.GetValues() {
			if !finiteSemanticValue(item) {
				return false
			}
		}
	}
	return true
}

func validateFactValue(path string, value *tgsrlv1.SemanticValue) error {
	switch path {
	case "batch.effective_sample_size", "batch.effective_sample_size_ratio":
		doubleValue, ok := value.GetKind().(*tgsrlv1.SemanticValue_DoubleValue)
		if !ok || !finite(doubleValue.DoubleValue) || doubleValue.DoubleValue < 0 {
			return errors.New("must be a finite non-negative double")
		}
	}
	return nil
}

func actionFor(status tgsrlv1.ContractEvaluationStatus, mode tgsrlv1.ValidityFailureMode) tgsrlv1.ContractDecisionAction {
	if status == tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED ||
		status == tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_NOT_APPLICABLE {
		return tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
	}
	switch mode {
	case tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_ABORT:
		return tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ABORT_REQUIRED
	case tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_PAUSE:
		return tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED
	default:
		return tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT
	}
}

func backpressureDirective(mode tgsrlv1.BackpressureMode) tgsrlv1.ContractDecisionAction {
	switch mode {
	case tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_BLOCK_PRODUCER:
		return tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_BLOCK_PRODUCER_REQUIRED
	case tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_SHED_OLDEST:
		return tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SHED_OLDEST_REQUIRED
	case tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_SHED_NEWEST:
		return tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SHED_NEWEST_REQUIRED
	case tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_REQUEST_SCALE_OUT:
		return tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SCALE_OUT_REQUESTED
	default:
		return tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT
	}
}

func blockingAction(action tgsrlv1.ContractDecisionAction) string {
	switch action {
	case tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ABORT_REQUIRED:
		return "ABORT"
	case tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED, tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_WAIT_FOR_SAFE_POINT:
		return "PAUSE"
	case tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_BLOCK_PRODUCER_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SHED_OLDEST_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SHED_NEWEST_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SCALE_OUT_REQUESTED:
		return "REJECT"
	default:
		return ""
	}
}

func isBlocking(eval *tgsrlv1.ContractEvaluation) bool {
	switch eval.GetObservationDisposition() {
	case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK,
		tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD:
		return true
	case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_DEGRADE,
		tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_NOT_APPLICABLE:
		return false
	}
	switch eval.GetRecommendedAction() {
	case tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ABORT_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_WAIT_FOR_SAFE_POINT,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_BLOCK_PRODUCER_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SHED_OLDEST_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SHED_NEWEST_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SCALE_OUT_REQUESTED:
		return true
	default:
		return false
	}
}

func actionPriority(action tgsrlv1.ContractDecisionAction) int {
	switch action {
	case tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ABORT_REQUIRED:
		return 3
	case tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_WAIT_FOR_SAFE_POINT:
		return 2
	case tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT:
		return 1
	case tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_BLOCK_PRODUCER_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SHED_OLDEST_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SHED_NEWEST_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SCALE_OUT_REQUESTED:
		return 1
	default:
		return 0
	}
}

func fallbackDetail(primary, fallback string) string {
	if primary != "" {
		return primary
	}
	return fallback
}

type semanticVersion struct {
	major      uint64
	minor      uint64
	patch      uint64
	prerelease []string
	build      []string
}

func normalizeVersion(value string) (string, error) {
	parsed, err := parseSemanticVersion(value)
	if err != nil {
		return "", err
	}
	result := fmt.Sprintf("%d.%d.%d", parsed.major, parsed.minor, parsed.patch)
	if len(parsed.prerelease) > 0 {
		result += "-" + strings.Join(parsed.prerelease, ".")
	}
	if len(parsed.build) > 0 {
		result += "+" + strings.Join(parsed.build, ".")
	}
	return result, nil
}

// NormalizeSemanticVersion normalizes the version syntax shared by contract
// validation and evaluation. Two-component legacy protocol versions retain
// compatibility and are normalized by appending a zero patch component.
func NormalizeSemanticVersion(value string) (string, error) {
	return normalizeVersion(value)
}

func parseSemanticVersion(value string) (semanticVersion, error) {
	if value == "" || value != strings.TrimSpace(value) {
		return semanticVersion{}, errors.New("version must be non-empty and canonical")
	}
	value = strings.TrimPrefix(strings.TrimPrefix(value, "v"), "V")
	coreAndPre, buildPart, hasBuild := strings.Cut(value, "+")
	if hasBuild && (buildPart == "" || strings.Contains(buildPart, "+")) {
		return semanticVersion{}, errors.New("version build metadata is invalid")
	}
	core, prereleasePart, hasPrerelease := strings.Cut(coreAndPre, "-")
	parts := strings.Split(core, ".")
	if len(parts) != 2 && len(parts) != 3 {
		return semanticVersion{}, errors.New("version must have 2 or 3 numeric components")
	}
	values := [3]uint64{}
	for index, part := range parts {
		if !validNumericIdentifier(part) {
			return semanticVersion{}, errors.New("version components must be canonical uint32 values")
		}
		number, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return semanticVersion{}, errors.New("version components must be canonical uint32 values")
		}
		values[index] = number
	}
	parsed := semanticVersion{major: values[0], minor: values[1], patch: values[2]}
	if hasPrerelease {
		parsed.prerelease = strings.Split(prereleasePart, ".")
		if !validVersionIdentifiers(parsed.prerelease, true) {
			return semanticVersion{}, errors.New("version prerelease is invalid")
		}
	}
	if hasBuild {
		parsed.build = strings.Split(buildPart, ".")
		if !validVersionIdentifiers(parsed.build, false) {
			return semanticVersion{}, errors.New("version build metadata is invalid")
		}
	}
	return parsed, nil
}

func validNumericIdentifier(value string) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func validVersionIdentifiers(values []string, prerelease bool) bool {
	if len(values) == 0 {
		return false
	}
	for _, value := range values {
		if value == "" {
			return false
		}
		numeric := true
		for _, character := range value {
			if (character < '0' || character > '9') && (character < 'A' || character > 'Z') && (character < 'a' || character > 'z') && character != '-' {
				return false
			}
			if character < '0' || character > '9' {
				numeric = false
			}
		}
		if prerelease && numeric && len(value) > 1 && value[0] == '0' {
			return false
		}
	}
	return true
}

func compareSemanticVersions(left, right semanticVersion) int {
	for _, pair := range [][2]uint64{{left.major, right.major}, {left.minor, right.minor}, {left.patch, right.patch}} {
		if pair[0] < pair[1] {
			return -1
		}
		if pair[0] > pair[1] {
			return 1
		}
	}
	if len(left.prerelease) == 0 && len(right.prerelease) == 0 {
		return 0
	}
	if len(left.prerelease) == 0 {
		return 1
	}
	if len(right.prerelease) == 0 {
		return -1
	}
	for index := 0; index < len(left.prerelease) && index < len(right.prerelease); index++ {
		leftPart, rightPart := left.prerelease[index], right.prerelease[index]
		if leftPart == rightPart {
			continue
		}
		leftNumeric, rightNumeric := numericPrerelease(leftPart), numericPrerelease(rightPart)
		switch {
		case leftNumeric && !rightNumeric:
			return -1
		case !leftNumeric && rightNumeric:
			return 1
		case leftNumeric && rightNumeric:
			if len(leftPart) < len(rightPart) || (len(leftPart) == len(rightPart) && leftPart < rightPart) {
				return -1
			}
			return 1
		case leftPart < rightPart:
			return -1
		default:
			return 1
		}
	}
	if len(left.prerelease) < len(right.prerelease) {
		return -1
	}
	if len(left.prerelease) > len(right.prerelease) {
		return 1
	}
	return 0
}

func numericPrerelease(value string) bool {
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return value != ""
}

func protocolCompatible(current, required string) bool {
	actual, actualErr := parseSemanticVersion(current)
	minimum, requiredErr := parseSemanticVersion(required)
	if actualErr != nil || requiredErr != nil || actual.major != minimum.major || compareSemanticVersions(actual, minimum) < 0 {
		return false
	}
	if actual.major == 0 {
		return actual.minor == minimum.minor
	}
	return true
}

// VersionsCompatible reports whether current is within required's compatible
// SemVer window and is not older than required.
func VersionsCompatible(current, required string) bool {
	return protocolCompatible(current, required)
}
