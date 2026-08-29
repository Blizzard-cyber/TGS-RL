package semantics

import (
	"errors"
	"fmt"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func legacyObservationMetadata(obs *tgsrlv1.ContractObservation) (observationMetadata, error) {
	metadata := observationMetadata{source: strings.TrimSpace(obs.GetSource())}
	if obs.GetObservedAt() == nil {
		return metadata, nil
	}
	if err := obs.GetObservedAt().CheckValid(); err != nil {
		return observationMetadata{}, fmt.Errorf("observation observed_at is invalid: %w", err)
	}
	metadata.observedAt = obs.GetObservedAt().AsTime().UTC()
	metadata.present = true
	return metadata, nil
}

func explicitObservationMetadata(observedAt *timestamppb.Timestamp, source string, revision uint64) (observationMetadata, error) {
	if observedAt == nil {
		return observationMetadata{}, errors.New("observed_at is required")
	}
	if err := observedAt.CheckValid(); err != nil {
		return observationMetadata{}, fmt.Errorf("observed_at is invalid: %w", err)
	}
	if source == "" || source != strings.TrimSpace(source) {
		return observationMetadata{}, errors.New("source must be non-empty and canonical")
	}
	return observationMetadata{
		observedAt: observedAt.AsTime().UTC(),
		present:    true,
		source:     source,
		revision:   revision,
	}, nil
}

func (registry *factRegistry) register(path string, value *tgsrlv1.SemanticValue, metadata observationMetadata, origin observationOrigin) error {
	if registry == nil || path == "" || path != strings.TrimSpace(path) || value == nil {
		return errors.New("fact registration is malformed")
	}
	if !finiteSemanticValue(value) {
		return fmt.Errorf("fact %q is not finite", path)
	}
	if issue, exists := registry.issues[path]; exists {
		return fmt.Errorf("fact %q conflicts with invalid registry fact: %s", path, issue.reason)
	}
	if existing, exists := registry.values[path]; exists {
		if !proto.Equal(existing, value) {
			return fmt.Errorf("fact %q conflicts with registry", path)
		}
		existingOrigin := registry.origins[path]
		if origin == originExplicit && existingOrigin == originExplicit {
			return fmt.Errorf("observed fact %q is duplicated", path)
		}
		if origin > existingOrigin {
			registry.metadata[path] = metadata
			registry.origins[path] = origin
		}
		return nil
	}
	registry.values[path] = proto.Clone(value).(*tgsrlv1.SemanticValue)
	registry.metadata[path] = metadata
	registry.origins[path] = origin
	return nil
}

func validateObservationPolicy(policy *tgsrlv1.ObservationPolicy) error {
	if policy == nil {
		return errors.New("observation policy is required")
	}
	if maximumAge := policy.GetMaximumAge(); maximumAge != nil {
		if err := maximumAge.CheckValid(); err != nil {
			return fmt.Errorf("maximum_age is invalid: %w", err)
		}
		if maximumAge.AsDuration() <= 0 {
			return errors.New("maximum_age must be positive when present")
		}
	}
	for name, disposition := range map[string]tgsrlv1.ObservationDisposition{
		"missing": policy.GetMissing(),
		"stale":   policy.GetStale(),
	} {
		if _, known := tgsrlv1.ObservationDisposition_name[int32(disposition)]; !known || disposition == tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_UNKNOWN {
			return fmt.Errorf("%s disposition must be recognized and non-UNKNOWN", name)
		}
	}
	return nil
}

type observationState uint8

const (
	observationFresh observationState = iota
	observationMissing
	observationStale
)

func (registry *factRegistry) observationState(path string, policy *tgsrlv1.ObservationPolicy) (observationState, []*tgsrlv1.SemanticField, string) {
	value, exists := registry.values[path]
	evidence := []*tgsrlv1.SemanticField{
		semanticField("observation.fact_path", semanticString(path)),
		semanticField("observation.evaluation_time", semanticString(registry.evaluationTime.Format(time.RFC3339Nano))),
	}
	if !exists {
		if issue, hasIssue := registry.issues[path]; hasIssue {
			evidence = append(evidence, issue.evidence...)
			return observationMissing, evidence, issue.reason
		}
		return observationMissing, evidence, "required fact is missing"
	}
	evidence = append(evidence, semanticField("observation.value", value))
	metadata := registry.metadata[path]
	if metadata.source != "" {
		evidence = append(evidence, semanticField("observation.source", semanticString(metadata.source)))
	}
	evidence = append(evidence, semanticField("observation.revision", semanticUint(metadata.revision)))
	maximumAge := policy.GetMaximumAge()
	if metadata.present {
		evidence = append(evidence, semanticField("observation.observed_at", semanticString(metadata.observedAt.Format(time.RFC3339Nano))))
		age := registry.evaluationTime.Sub(metadata.observedAt)
		evidence = append(evidence, semanticField("observation.age_ms", semanticInt(age.Milliseconds())))
		if age < 0 {
			return observationStale, evidence, "fact observation time is after evaluation time"
		}
	}
	if maximumAge == nil {
		return observationFresh, evidence, "fact is present and no maximum age is configured"
	}
	evidence = append(evidence, semanticField("observation.maximum_age_ms", semanticInt(maximumAge.AsDuration().Milliseconds())))
	if !metadata.present {
		return observationStale, evidence, "fact observation time is missing"
	}
	age := registry.evaluationTime.Sub(metadata.observedAt)
	if age > maximumAge.AsDuration() {
		return observationStale, evidence, "fact observation exceeded maximum age"
	}
	return observationFresh, evidence, "fact observation is fresh"
}

// observationGuard projects freshness policy onto every clause that consumes
// the fact. Fresh facts return UNKNOWN status so the caller continues with its
// normal predicate or policy semantics.
func (registry *factRegistry) observationGuard(path string) (conditionResult, bool) {
	policy, guarded := registry.policies[path]
	if !guarded {
		return conditionResult{}, false
	}
	state, evidence, detail := registry.observationState(path, policy)
	if state == observationFresh {
		return conditionResult{evidence: evidence}, true
	}
	disposition := policy.GetStale()
	missing := []string(nil)
	if state == observationMissing {
		disposition = policy.GetMissing()
		missing = []string{path}
	}
	status := statusForObservationDisposition(disposition)
	return conditionResult{
		status:      status,
		missing:     missing,
		evidence:    evidence,
		detail:      detail,
		disposition: disposition,
	}, true
}

func evaluateCriticalFactPolicy(input Input, facts *factRegistry, critical *tgsrlv1.CriticalFactPolicy) *tgsrlv1.ContractEvaluation {
	path := critical.GetFactPath()
	base := newEvaluation(input, criticalFactClauseKind(path), path, nil, tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_REJECT)
	value, present := facts.values[path]
	if present {
		base.Observations = append(base.Observations, semanticField(path, value))
	}
	state, evidence, detail := facts.observationState(path, critical.GetObservationPolicy())
	base.Evidence = append(base.Evidence, evidence...)
	base.Detail = detail
	switch state {
	case observationFresh:
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
	case observationMissing:
		base.MissingKeys = []string{path}
		applyObservationDisposition(base, critical.GetObservationPolicy().GetMissing())
	case observationStale:
		applyObservationDisposition(base, critical.GetObservationPolicy().GetStale())
	}
	return finalizeEvaluation(input, base)
}

func criticalFactClauseKind(path string) tgsrlv1.ContractClauseKind {
	switch path {
	case "sample.age_ms", "sample.stale":
		return tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_SAMPLE_FRESHNESS
	case "sample.policy_lag":
		return tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_POLICY_LAG
	case "batch.effective_sample_size", "batch.effective_sample_size_ratio":
		return tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_EFFECTIVE_SAMPLE_SIZE
	case "batch.accepted_samples", "group.accepted_samples", "group.expected_samples", "sample.sample_count":
		return tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_SAMPLE_COVERAGE
	default:
		return tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_CONDITION
	}
}

func applyObservationDisposition(evaluation *tgsrlv1.ContractEvaluation, disposition tgsrlv1.ObservationDisposition) {
	evaluation.ObservationDisposition = disposition
	evaluation.Status = statusForObservationDisposition(disposition)
	evaluation.RecommendedAction = actionForObservationDisposition(disposition)
}

func statusForObservationDisposition(disposition tgsrlv1.ObservationDisposition) tgsrlv1.ContractEvaluationStatus {
	switch disposition {
	case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_NOT_APPLICABLE:
		return tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_NOT_APPLICABLE
	case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_DEGRADE:
		return tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
	case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD:
		return tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
	case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK:
		return tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED
	default:
		return tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
	}
}

func actionForObservationDisposition(disposition tgsrlv1.ObservationDisposition) tgsrlv1.ContractDecisionAction {
	switch disposition {
	case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_NOT_APPLICABLE,
		tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_DEGRADE:
		return tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
	case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD:
		return tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED
	default:
		return tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT
	}
}

func strongerDisposition(left, right tgsrlv1.ObservationDisposition) tgsrlv1.ObservationDisposition {
	priority := func(value tgsrlv1.ObservationDisposition) int {
		switch value {
		case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK:
			return 4
		case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD:
			return 3
		case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_DEGRADE:
			return 2
		case tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_NOT_APPLICABLE:
			return 1
		default:
			return 0
		}
	}
	if priority(right) > priority(left) {
		return right
	}
	return left
}

func invalidObservationIssue(reason string, observedAt, evaluationTime time.Time) factIssue {
	return factIssue{
		reason: reason,
		evidence: []*tgsrlv1.SemanticField{
			semanticField("observation.observed_at", semanticString(observedAt.Format(time.RFC3339Nano))),
			semanticField("observation.evaluation_time", semanticString(evaluationTime.Format(time.RFC3339Nano))),
			semanticField("observation.age_ms", semanticInt(evaluationTime.Sub(observedAt).Milliseconds())),
		},
	}
}
