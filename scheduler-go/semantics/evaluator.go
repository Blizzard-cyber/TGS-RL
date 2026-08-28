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

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

const protocolVersion = "0.3.0"
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

	if input.SafePoint == nil {
		resolved := ResolveSafePoint(input.Snapshot, input.Intent, input.Context, safePointAnnotation)
		input.SafePoint = &resolved
	}
	obs := effectiveObservation(input.Context, input.Intent)
	facts, err := buildFacts(input, obs)
	if err != nil {
		return nil, AggregateResult{}, err
	}

	evals := make([]*tgsrlv1.ContractEvaluation, 0,
		len(input.Contract.GetValidityRules())+
			len(input.Contract.GetConditions())+
			len(input.Contract.GetVersionConstraints())+2)

	for _, rule := range input.Contract.GetValidityRules() {
		evals = append(evals, evaluateValidityRule(input, facts, rule))
	}
	for _, cond := range input.Contract.GetConditions() {
		evals = append(evals, evaluateConditionClause(input, facts, cond))
	}
	for _, constraint := range input.Contract.GetVersionConstraints() {
		evals = append(evals, evaluateVersionConstraint(input, facts, constraint))
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
		if best == nil || actionPriority(eval.GetRecommendedAction()) > actionPriority(best.GetRecommendedAction()) {
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
	values map[string]*tgsrlv1.SemanticValue
	issues map[string]factIssue
}

type factIssue struct {
	reason   string
	evidence []*tgsrlv1.SemanticField
}

func buildFacts(input Input, obs *tgsrlv1.ContractObservation) (*factRegistry, error) {
	values := map[string]*tgsrlv1.SemanticValue{
		"intent.version.current": semanticUint(input.Intent.GetVersion()),
	}
	issues := map[string]factIssue{}
	if input.SafePoint != nil && input.SafePoint.Present {
		values["runtime.safe_point"] = semanticBool(input.SafePoint.Value)
	}
	if input.SafePoint != nil && len(input.SafePoint.Evidence) > 0 {
		issues["runtime.safe_point"] = factIssue{
			reason:   "runtime safe point resolved from snapshot annotation fallback",
			evidence: stableFields(input.SafePoint.Evidence),
		}
	}

	if obs != nil {
		if obs.PolicyLag != nil {
			values["sample.policy_lag"] = semanticUint(obs.GetPolicyLag())
		}
		if obs.SampleStale != nil {
			values["sample.stale"] = semanticBool(obs.GetSampleStale())
		}
		if obs.BufferLevel != nil {
			values["buffer.level.current"] = semanticUint(obs.GetBufferLevel())
		}
		if obs.AcceptedSamples != nil {
			values["batch.accepted_samples"] = semanticUint(obs.GetAcceptedSamples())
			values["group.accepted_samples"] = semanticUint(obs.GetAcceptedSamples())
		}
		if obs.ExpectedSamples != nil {
			values["group.expected_samples"] = semanticUint(obs.GetExpectedSamples())
		}
		if obs.EffectiveSampleSize != nil {
			if !finite(obs.GetEffectiveSampleSize()) || obs.GetEffectiveSampleSize() < 0 {
				issues["batch.effective_sample_size"] = factIssue{
					reason: "effective_sample_size is invalid",
					evidence: []*tgsrlv1.SemanticField{
						semanticField("batch.effective_sample_size.invalid", semanticDouble(obs.GetEffectiveSampleSize())),
					},
				}
			} else {
				values["batch.effective_sample_size"] = semanticDouble(obs.GetEffectiveSampleSize())
			}
		}
		if obs.EffectiveSampleSizeRatio != nil {
			if !finite(obs.GetEffectiveSampleSizeRatio()) || obs.GetEffectiveSampleSizeRatio() < 0 {
				issues["batch.effective_sample_size_ratio"] = factIssue{
					reason: "effective_sample_size_ratio is invalid",
					evidence: []*tgsrlv1.SemanticField{
						semanticField("batch.effective_sample_size_ratio.invalid", semanticDouble(obs.GetEffectiveSampleSizeRatio())),
					},
				}
			} else {
				values["batch.effective_sample_size_ratio"] = semanticDouble(obs.GetEffectiveSampleSizeRatio())
			}
		}
		if input.Context.GetEvaluationTime() != nil && obs.GetOldestSampleAt() != nil {
			age := input.Context.GetEvaluationTime().AsTime().Sub(obs.GetOldestSampleAt().AsTime()).Milliseconds()
			if age >= 0 {
				values["sample.age_ms"] = semanticInt(age)
			}
		}
		for _, field := range obs.GetTypedFacts() {
			if field == nil || field.GetKey() == "" || field.GetValue() == nil {
				continue
			}
			if !finiteSemanticValue(field.GetValue()) {
				return nil, fmt.Errorf("observation typed fact %q is not finite", field.GetKey())
			}
			if existing, exists := values[field.GetKey()]; exists {
				if !proto.Equal(existing, field.GetValue()) {
					return nil, fmt.Errorf("typed fact %q conflicts with registry", field.GetKey())
				}
				continue
			}
			if existing, exists := issues[field.GetKey()]; exists {
				if len(existing.evidence) > 0 && proto.Equal(existing.evidence[0].GetValue(), field.GetValue()) {
					delete(issues, field.GetKey())
					values[field.GetKey()] = proto.Clone(field.GetValue()).(*tgsrlv1.SemanticValue)
					continue
				}
				return nil, fmt.Errorf("typed fact %q conflicts with invalid registry fact", field.GetKey())
			}
			values[field.GetKey()] = proto.Clone(field.GetValue()).(*tgsrlv1.SemanticValue)
		}
	}
	return &factRegistry{values: values, issues: issues}, nil
}

type conditionResult struct {
	status   tgsrlv1.ContractEvaluationStatus
	missing  []string
	evidence []*tgsrlv1.SemanticField
	detail   string
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
	base.RecommendedAction = actionFor(base.Status, rule.GetFailureMode())
	return finalizeEvaluation(input, base)
}

func evaluateConditionClause(input Input, facts *factRegistry, cond *tgsrlv1.Condition) *tgsrlv1.ContractEvaluation {
	base := newEvaluation(input, tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_CONDITION, cond.GetConditionId(), cond, tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_REJECT)
	result := evalCondition(facts, cond)
	applyConditionResult(base, result)
	base.Detail = result.detail
	base.RecommendedAction = actionFor(base.Status, base.GetFailureMode())
	return finalizeEvaluation(input, base)
}

func evaluateVersionConstraint(input Input, facts *factRegistry, constraint *tgsrlv1.VersionConstraint) *tgsrlv1.ContractEvaluation {
	base := newEvaluation(input, tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_VERSION_CONSTRAINT, constraint.GetComponent(), nil, tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_REJECT)
	value, ok := facts.values["intent.version.current"]
	base.Observations = append(base.Observations, semanticField("intent.version.current", value))
	if !ok || value == nil {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
		base.MissingKeys = []string{"intent.version.current"}
		base.Detail = "missing intent.version.current"
		base.RecommendedAction = actionFor(base.Status, base.GetFailureMode())
		return finalizeEvaluation(input, base)
	}
	if !strings.EqualFold(strings.TrimSpace(constraint.GetComponent()), "protocol") {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_NOT_APPLICABLE
		base.Detail = "unsupported component"
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
		return finalizeEvaluation(input, base)
	}
	required, err := normalizeVersion(constraint.GetVersion())
	if err != nil {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
		base.Detail = "invalid required protocol version"
		base.RecommendedAction = actionFor(base.Status, base.GetFailureMode())
		return finalizeEvaluation(input, base)
	}
	base.Evidence = append(base.Evidence,
		semanticField("protocol.version.current", semanticString(protocolVersion)),
		semanticField("protocol.version.required", semanticString(required)),
		semanticField("protocol.version.operator", semanticString(constraint.GetOperator().String())),
	)
	compatible := false
	switch constraint.GetOperator() {
	case tgsrlv1.VersionOperator_VERSION_OPERATOR_EXACT, tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER:
		compatible = required == protocolVersion
	case tgsrlv1.VersionOperator_VERSION_OPERATOR_COMPATIBLE:
		compatible = protocolCompatible(protocolVersion, required)
	default:
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
		base.Detail = "unknown version operator"
		base.RecommendedAction = actionFor(base.Status, base.GetFailureMode())
		return finalizeEvaluation(input, base)
	}
	if compatible {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED
		base.Detail = "protocol version compatible"
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
	} else {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED
		base.Detail = "protocol version incompatible"
		base.RecommendedAction = actionFor(base.Status, base.GetFailureMode())
	}
	return finalizeEvaluation(input, base)
}

func evaluateBackpressure(input Input, facts *factRegistry, policy *tgsrlv1.BackpressurePolicy) *tgsrlv1.ContractEvaluation {
	base := newEvaluation(input, tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_BACKPRESSURE_POLICY, "backpressure", nil, tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_REJECT)
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
		switch res.status {
		case tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED:
			combined.status = res.status
			combined.detail = "AND predicate violated"
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
		switch res.status {
		case tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED:
			combined.status = res.status
			combined.detail = "OR predicate satisfied"
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
		evidence: []*tgsrlv1.SemanticField{semanticField(cond.GetFactPath(), value)},
		detail:   "fact exists",
	}
}

func evalBinaryCondition(facts *factRegistry, cond *tgsrlv1.Condition) conditionResult {
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
	evidence := []*tgsrlv1.SemanticField{semanticField(cond.GetFactPath(), left)}
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
}

func finalizeEvaluation(input Input, eval *tgsrlv1.ContractEvaluation) *tgsrlv1.ContractEvaluation {
	eval.Observations = stableFields(eval.GetObservations())
	eval.Evidence = stableFields(eval.GetEvidence())
	eval.MissingKeys = stableStrings(eval.GetMissingKeys())
	eval.EvaluationId = evaluationID(input, eval)
	return eval
}

func evaluationID(input Input, eval *tgsrlv1.ContractEvaluation) string {
	parts := []string{
		input.Contract.GetContractId(),
		input.Contract.GetVersion(),
		eval.GetClauseKind().String(),
		eval.GetClauseId(),
		strconv.FormatUint(input.Intent.GetVersion(), 10),
		strconv.FormatUint(input.Snapshot.GetRevision(), 10),
		input.Context.GetTickKind().String(),
		strconv.FormatUint(input.Context.GetDecisionSequence(), 10),
		strconv.FormatUint(input.Context.GetObservedRevision(), 10),
		input.Context.GetCause(),
		strconv.FormatBool(input.Context.GetCompatibilityDefaultsApplied()),
	}
	if input.Context.GetEvaluationTime() != nil {
		parts = append(parts, input.Context.GetEvaluationTime().AsTime().UTC().Format("2006-01-02T15:04:05.999999999Z"))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
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
	switch eval.GetRecommendedAction() {
	case tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ABORT_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED,
		tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT:
		return true
	case tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_WAIT_FOR_SAFE_POINT:
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

func normalizeVersion(value string) (string, error) {
	if value != strings.TrimSpace(value) || value == "" {
		return "", errors.New("version must be non-empty")
	}
	value = strings.TrimPrefix(strings.TrimPrefix(value, "v"), "V")
	base, prerelease, hasPrerelease := strings.Cut(value, "-")
	parts := strings.Split(base, ".")
	if len(parts) != 2 && len(parts) != 3 {
		return "", errors.New("version must have 2 or 3 components")
	}
	normalized := make([]string, 3)
	for i, part := range parts {
		number, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return "", errors.New("version components must be numeric")
		}
		normalized[i] = strconv.FormatUint(number, 10)
	}
	if len(parts) == 2 {
		normalized[2] = "0"
	}
	result := strings.Join(normalized, ".")
	if hasPrerelease {
		result += "-" + prerelease
	}
	return result, nil
}

func protocolCompatible(current, required string) bool {
	currentBase, _, _ := strings.Cut(current, "-")
	requiredBase, _, _ := strings.Cut(required, "-")
	currentParts := strings.Split(currentBase, ".")
	requiredParts := strings.Split(requiredBase, ".")
	if len(currentParts) != 3 || len(requiredParts) != 3 {
		return false
	}
	if currentParts[0] != requiredParts[0] {
		return false
	}
	if currentParts[0] == "0" {
		return currentParts[1] == requiredParts[1]
	}
	return true
}
