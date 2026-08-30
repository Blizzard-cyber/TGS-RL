package scheduler

import (
	"math"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// generateDirectionalShareTarget converts quality and pressure signals into a
// bounded share delta. Explicit directives remain authoritative and bypass
// this generator. Producer and consumer phases react in opposite directions
// to backlog: production is throttled while consumption is strengthened.
func generateDirectionalShareTarget(input PlanningInput, current float64, targetID string) *DirectionalTarget {
	producer := producerPhase(input.Intent.GetPhaseKind())
	consumer := consumerPhase(input.Intent.GetPhaseKind())
	if (!producer && !consumer) || math.IsNaN(current) || math.IsInf(current, 0) {
		return nil
	}
	direction := 0
	reason := ""
	switch {
	case producer && input.Signals.ESSRatioPresent && input.Signals.ESSRatio < DefaultPlannerESSRatioFloor:
		direction, reason = -1, "LOW_ESS_REDUCE_PRODUCTION"
	case producer && input.Signals.SampleStalenessPresent && input.Signals.SampleStaleness:
		direction, reason = -1, "SAMPLE_STALENESS_REDUCE_PRODUCTION"
	case input.Signals.PolicyLagPresent && input.Signals.PolicyLag > DefaultPlannerPolicyLagLimit:
		if producer {
			direction, reason = -1, "POLICY_LAG_REDUCE_PRODUCTION"
		} else {
			direction, reason = 1, "POLICY_LAG_INCREASE_CONSUMPTION"
		}
	case input.Signals.BufferPressurePresent && input.Signals.BufferPressure >= BufferPressureHigh:
		if producer {
			direction, reason = -1, "BUFFER_PRESSURE_REDUCE_PRODUCTION"
		} else {
			direction, reason = 1, "BUFFER_PRESSURE_INCREASE_CONSUMPTION"
		}
	case producer && input.Signals.BufferPressurePresent && input.Signals.BufferPressure == BufferPressureLow &&
		(!input.Signals.SampleStalenessPresent || !input.Signals.SampleStaleness) &&
		(!input.Signals.PolicyLagPresent || input.Signals.PolicyLag == 0):
		direction, reason = 1, "BUFFER_HEADROOM_INCREASE_PRODUCTION"
	default:
		return nil
	}
	target := clampShare(current + float64(direction)*DefaultPlannerShareStep)
	delta := target - current
	if math.Abs(delta) < DefaultPlannerShareHysteresis {
		return nil
	}
	if !directionalObservationWindowElapsed(input, targetID) {
		return nil
	}
	return &DirectionalTarget{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, CurrentValue: current, TargetValue: target, Delta: delta, Reason: reason, ObservationWindow: DefaultPlannerObservationWindow}
}

func directionalShareSignalPresent(input PlanningInput) bool {
	producer := producerPhase(input.Intent.GetPhaseKind())
	consumer := consumerPhase(input.Intent.GetPhaseKind())
	if !producer && !consumer {
		return false
	}
	return producer && input.Signals.ESSRatioPresent && input.Signals.ESSRatio < DefaultPlannerESSRatioFloor ||
		producer && input.Signals.SampleStalenessPresent && input.Signals.SampleStaleness ||
		input.Signals.PolicyLagPresent && input.Signals.PolicyLag > DefaultPlannerPolicyLagLimit ||
		input.Signals.BufferPressurePresent && input.Signals.BufferPressure >= BufferPressureHigh ||
		producer && input.Signals.BufferPressurePresent && input.Signals.BufferPressure == BufferPressureLow &&
			(!input.Signals.SampleStalenessPresent || !input.Signals.SampleStaleness) &&
			(!input.Signals.PolicyLagPresent || input.Signals.PolicyLag == 0)
}

func directionalObservationWindowElapsed(input PlanningInput, targetID string) bool {
	if input.EvaluationContext == nil || input.EvaluationContext.GetEvaluationTime() == nil {
		return false
	}
	now := input.EvaluationContext.GetEvaluationTime().AsTime()
	for _, decision := range input.RecentDecisions {
		if decision == nil || decision.GetDecidedAt() == nil || decision.GetDecidedAt().CheckValid() != nil {
			continue
		}
		age := now.Sub(decision.GetDecidedAt().AsTime())
		if age < 0 || age >= DefaultPlannerObservationWindow {
			continue
		}
		for _, evidence := range decision.GetPlannerEvidence() {
			if evidence.GetDisposition() != tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_SELECTED || evidence.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE {
				continue
			}
			if targetID != "" && evidence.GetTargetId() != targetID {
				continue
			}
			for _, field := range evidence.GetInputs() {
				if field.GetKey() == "planner.directional.reason" {
					if _, ok := field.GetValue().GetKind().(*tgsrlv1.SemanticValue_StringValue); ok {
						return false
					}
				}
			}
		}
	}
	return true
}

func producerPhase(kind tgsrlv1.PhaseKind) bool {
	switch kind {
	case tgsrlv1.PhaseKind_PHASE_KIND_PREFILL,
		tgsrlv1.PhaseKind_PHASE_KIND_DECODE,
		tgsrlv1.PhaseKind_PHASE_KIND_TOOL_WAIT,
		tgsrlv1.PhaseKind_PHASE_KIND_KV_RESTORE:
		return true
	default:
		return false
	}
}

func consumerPhase(kind tgsrlv1.PhaseKind) bool {
	return kind == tgsrlv1.PhaseKind_PHASE_KIND_ACTOR || kind == tgsrlv1.PhaseKind_PHASE_KIND_OPTIMIZER
}

func clampShare(value float64) float64 {
	return math.Max(0, math.Min(1, value))
}

func directionalTargetEvidence(target *DirectionalTarget) []*tgsrlv1.SemanticField {
	if target == nil {
		return nil
	}
	return []*tgsrlv1.SemanticField{
		semanticDoubleField("planner.directional.current_value", target.CurrentValue),
		semanticDoubleField("planner.directional.target_value", target.TargetValue),
		semanticDoubleField("planner.directional.delta", target.Delta),
		semanticStringField("planner.directional.reason", target.Reason),
		semanticIntField("planner.directional.expected_benefit_nanos", target.ExpectedBenefitNanos),
		semanticIntField("planner.directional.recovery_cost_nanos", target.RecoveryCostNanos),
		semanticIntField("planner.directional.observation_window_nanos", target.ObservationWindow.Nanoseconds()),
	}
}

func directionalTargetForAdaptiveTarget(input PlanningInput, target adaptiveTarget) *DirectionalTarget {
	if target.sandbox == nil || target.allocation == nil {
		return nil
	}
	return generateDirectionalShareTarget(input, target.sandbox.GetShare(), target.allocation.GetAllocationId())
}
