package scheduler

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

const (
	factBufferPressure       = "planner.buffer_pressure"
	factCheckpointCapable    = "planner.checkpoint_capable"
	factBufferBelowWatermark = "planner.buffer_below_watermark"
	factRecoveryCostNanos    = "planner.recovery_cost_nanos"
	factMinimumResidencyNano = "planner.minimum_residency_nanos"
	factActionInFlight       = "planner.action_in_flight"
	factPerTickActionBudget  = "planner.per_tick_action_budget"
)

var adaptiveFactAliases = map[string][]string{
	factBufferPressure:       {factBufferPressure, "buffer.pressure"},
	factCheckpointCapable:    {factCheckpointCapable, "checkpoint.capable", "runtime.checkpoint_capable"},
	factBufferBelowWatermark: {factBufferBelowWatermark, "buffer.below_watermark"},
	factRecoveryCostNanos:    {factRecoveryCostNanos, "recovery.cost_nanos"},
	factMinimumResidencyNano: {factMinimumResidencyNano, "runtime.minimum_residency_nanos"},
	factActionInFlight:       {factActionInFlight, "action.in_flight"},
	factPerTickActionBudget:  {factPerTickActionBudget, "planner.action_budget"},
}

// normalizePlanningSignals fills only missing signals. It deliberately uses
// typed facts and structured fields; labels, annotations, status strings, and
// reward surfaces are never planner authority.
func normalizePlanningSignals(input *PlanningInput, defaultActionBudget int) error {
	if input == nil {
		return nil
	}
	signals := input.Signals
	observation := input.EvaluationContext.GetContractObservation()
	if observation == nil && input.Intent != nil {
		observation = input.Intent.GetContractObservation()
	}
	if observation != nil {
		if !signals.BufferLevelPresent && observation.BufferLevel != nil {
			signals.BufferLevel, signals.BufferLevelPresent = observation.GetBufferLevel(), true
		}
		if !signals.PolicyLagPresent && observation.PolicyLag != nil {
			signals.PolicyLag, signals.PolicyLagPresent = observation.GetPolicyLag(), true
		}
		if !signals.SampleStalenessPresent && observation.SampleStale != nil {
			signals.SampleStaleness, signals.SampleStalenessPresent = observation.GetSampleStale(), true
		}
		if !signals.ESSRatioPresent {
			switch {
			case observation.EffectiveSampleSizeRatio != nil:
				signals.ESSRatio, signals.ESSRatioPresent = observation.GetEffectiveSampleSizeRatio(), true
			case observation.EffectiveSampleSize != nil && observation.SampleCount != nil && observation.GetSampleCount() > 0:
				signals.ESSRatio = observation.GetEffectiveSampleSize() / float64(observation.GetSampleCount())
				signals.ESSRatioPresent = true
			}
		}
	}

	facts, err := collectAdaptiveFacts(input, observation)
	if err != nil {
		return err
	}
	if !signals.BufferPressurePresent {
		if value, ok := factString(facts, factBufferPressure); ok {
			pressure, valid := parseBufferPressure(value)
			if !valid {
				return fmt.Errorf("%s must be LOW, NORMAL, HIGH, or CRITICAL", factBufferPressure)
			}
			signals.BufferPressure, signals.BufferPressurePresent = pressure, true
		}
	}
	if !signals.BufferLevelPresent {
		if value, ok := genericFactUint64(observation, "buffer.level.current"); ok {
			signals.BufferLevel, signals.BufferLevelPresent = value, true
		}
	}
	if !signals.PolicyLagPresent {
		if value, ok := genericFactUint64(observation, "sample.policy_lag"); ok {
			signals.PolicyLag, signals.PolicyLagPresent = value, true
		}
	}
	if !signals.SampleStalenessPresent {
		if value, ok := genericFactBool(observation, "sample.stale"); ok {
			signals.SampleStaleness, signals.SampleStalenessPresent = value, true
		}
	}
	if !signals.ESSRatioPresent {
		if value, ok := genericFactFloat64(observation, "batch.effective_sample_size_ratio"); ok {
			signals.ESSRatio, signals.ESSRatioPresent = value, true
		}
	}
	if !signals.CheckpointCapablePresent {
		if value, ok := factBool(facts, factCheckpointCapable); ok {
			signals.CheckpointCapable, signals.CheckpointCapablePresent = value, true
		}
	}
	if !signals.BufferBelowWatermarkPresent {
		if value, ok := factBool(facts, factBufferBelowWatermark); ok {
			signals.BufferBelowWatermark, signals.BufferBelowWatermarkPresent = value, true
		}
	}
	if !signals.RecoveryCostNanosPresent {
		if value, ok := factInt64(facts, factRecoveryCostNanos); ok {
			signals.RecoveryCostNanos, signals.RecoveryCostNanosPresent = value, true
		}
	}
	if !signals.MinimumResidencyPresent {
		if value, ok := factInt64(facts, factMinimumResidencyNano); ok {
			if value < 0 {
				return fmt.Errorf("%s must be non-negative", factMinimumResidencyNano)
			}
			signals.MinimumResidency, signals.MinimumResidencyPresent = time.Duration(value), true
		}
	}
	if !signals.ActionInFlightPresent {
		if value, ok := factBool(facts, factActionInFlight); ok {
			signals.ActionInFlight, signals.ActionInFlightPresent = value, true
		} else if inFlight, present := recentActionInFlight(input.RecentDecisions); present {
			signals.ActionInFlight, signals.ActionInFlightPresent = inFlight, true
		}
	}
	if !signals.PerTickActionBudgetPresent {
		if value, ok := factUint64(facts, factPerTickActionBudget); ok {
			if value > uint64(^uint(0)>>1) {
				return fmt.Errorf("%s exceeds int capacity", factPerTickActionBudget)
			}
			signals.PerTickActionBudget, signals.PerTickActionBudgetPresent = int(value), true
		} else {
			signals.PerTickActionBudget, signals.PerTickActionBudgetPresent = defaultActionBudget, true
		}
	}

	contract := input.Intent.GetExecutionContract()
	policy := contract.GetBackpressurePolicy()
	if !signals.BufferBelowWatermarkPresent && signals.BufferLevelPresent && policy != nil {
		signals.BufferBelowWatermark = signals.BufferLevel <= policy.GetLowWatermark()
		signals.BufferBelowWatermarkPresent = true
	}
	if !signals.BufferPressurePresent && signals.BufferLevelPresent && policy != nil {
		signals.BufferPressure = bufferPressureForLevel(signals.BufferLevel, policy)
		signals.BufferPressurePresent = true
	}
	// checkpoint_restore is proto3 scalar state without presence. A true value
	// is authoritative capability evidence; false remains unknown unless a typed
	// fact or explicit PlanningSignals value supplies presence.
	if !signals.CheckpointCapablePresent && contract.GetCapabilities().GetCheckpointRestore() {
		signals.CheckpointCapable, signals.CheckpointCapablePresent = true, true
	}
	if signals.BufferPressurePresent && signals.BufferPressure > BufferPressureCritical {
		return fmt.Errorf("buffer pressure is invalid")
	}
	if signals.ESSRatioPresent && (math.IsNaN(signals.ESSRatio) || math.IsInf(signals.ESSRatio, 0) || signals.ESSRatio < 0) {
		return fmt.Errorf("ESS ratio must be finite and non-negative")
	}
	if signals.RecoveryCostNanosPresent && signals.RecoveryCostNanos < 0 {
		return fmt.Errorf("recovery cost must be non-negative")
	}
	if signals.MinimumResidencyPresent && signals.MinimumResidency < 0 {
		return fmt.Errorf("minimum residency must be non-negative")
	}
	if signals.PerTickActionBudget < 1 {
		return fmt.Errorf("per-tick action budget must be positive")
	}
	input.Signals = signals
	return nil
}

func collectAdaptiveFacts(input *PlanningInput, observation *tgsrlv1.ContractObservation) (map[string]*tgsrlv1.SemanticValue, error) {
	facts := make(map[string]*tgsrlv1.SemanticValue)
	groups := make([][]*tgsrlv1.SemanticField, 0, 3+len(input.Sandboxes))
	if observation != nil {
		groups = append(groups, observation.GetTypedFacts())
		observed := make([]*tgsrlv1.SemanticField, 0, len(observation.GetFactObservations()))
		for _, item := range observation.GetFactObservations() {
			if item != nil {
				observed = append(observed, item.GetFact())
			}
		}
		groups = append(groups, observed)
	}
	for _, sandbox := range input.Sandboxes {
		if sandbox == nil || sandbox.GetSemanticContext() == nil {
			continue
		}
		fields := append([]*tgsrlv1.SemanticField(nil), sandbox.GetSemanticContext().GetTypedFields()...)
		fields = append(fields, sandbox.GetSemanticContext().GetPayload().GetFields()...)
		groups = append(groups, fields)
	}
	for _, fields := range groups {
		for _, field := range fields {
			if field == nil || field.GetValue() == nil {
				continue
			}
			canonical, recognized := canonicalAdaptiveFact(field.GetKey())
			if !recognized {
				continue
			}
			if existing, exists := facts[canonical]; exists && !semanticValuesEqual(existing, field.GetValue()) {
				return nil, fmt.Errorf("typed fact %q conflicts across sources", canonical)
			}
			facts[canonical] = field.GetValue()
		}
	}
	return facts, nil
}

func canonicalAdaptiveFact(key string) (string, bool) {
	for canonical, aliases := range adaptiveFactAliases {
		for _, alias := range aliases {
			if key == alias {
				return canonical, true
			}
		}
	}
	return "", false
}

func semanticValuesEqual(left, right *tgsrlv1.SemanticValue) bool {
	return proto.Equal(left, right)
}

func observationSemanticValues(observation *tgsrlv1.ContractObservation, key string) []*tgsrlv1.SemanticValue {
	if observation == nil {
		return nil
	}
	result := make([]*tgsrlv1.SemanticValue, 0, 2)
	for _, field := range observation.GetTypedFacts() {
		if field != nil && field.GetKey() == key && field.GetValue() != nil {
			result = append(result, field.GetValue())
		}
	}
	for _, observed := range observation.GetFactObservations() {
		if observed != nil && observed.GetFact() != nil && observed.GetFact().GetKey() == key && observed.GetFact().GetValue() != nil {
			result = append(result, observed.GetFact().GetValue())
		}
	}
	return result
}

func genericFactUint64(observation *tgsrlv1.ContractObservation, key string) (uint64, bool) {
	values := observationSemanticValues(observation, key)
	var result uint64
	for index, value := range values {
		typed, ok := value.GetKind().(*tgsrlv1.SemanticValue_Uint64Value)
		if !ok || (index > 0 && result != typed.Uint64Value) {
			return 0, false
		}
		result = typed.Uint64Value
	}
	return result, len(values) > 0
}

func genericFactBool(observation *tgsrlv1.ContractObservation, key string) (bool, bool) {
	values := observationSemanticValues(observation, key)
	var result bool
	for index, value := range values {
		typed, ok := value.GetKind().(*tgsrlv1.SemanticValue_BoolValue)
		if !ok || (index > 0 && result != typed.BoolValue) {
			return false, false
		}
		result = typed.BoolValue
	}
	return result, len(values) > 0
}

func genericFactFloat64(observation *tgsrlv1.ContractObservation, key string) (float64, bool) {
	values := observationSemanticValues(observation, key)
	var result float64
	for index, value := range values {
		typed, ok := value.GetKind().(*tgsrlv1.SemanticValue_DoubleValue)
		if !ok || (index > 0 && result != typed.DoubleValue) {
			return 0, false
		}
		result = typed.DoubleValue
	}
	return result, len(values) > 0
}

func factString(facts map[string]*tgsrlv1.SemanticValue, key string) (string, bool) {
	value := facts[key]
	if value == nil {
		return "", false
	}
	typed, ok := value.GetKind().(*tgsrlv1.SemanticValue_StringValue)
	return typed.StringValue, ok
}

func factBool(facts map[string]*tgsrlv1.SemanticValue, key string) (bool, bool) {
	value := facts[key]
	if value == nil {
		return false, false
	}
	typed, ok := value.GetKind().(*tgsrlv1.SemanticValue_BoolValue)
	return typed.BoolValue, ok
}

func factUint64(facts map[string]*tgsrlv1.SemanticValue, key string) (uint64, bool) {
	value := facts[key]
	if value == nil {
		return 0, false
	}
	typed, ok := value.GetKind().(*tgsrlv1.SemanticValue_Uint64Value)
	return typed.Uint64Value, ok
}

func factInt64(facts map[string]*tgsrlv1.SemanticValue, key string) (int64, bool) {
	value := facts[key]
	if value == nil {
		return 0, false
	}
	switch typed := value.GetKind().(type) {
	case *tgsrlv1.SemanticValue_Int64Value:
		return typed.Int64Value, true
	case *tgsrlv1.SemanticValue_Uint64Value:
		if typed.Uint64Value <= math.MaxInt64 {
			return int64(typed.Uint64Value), true
		}
	}
	return 0, false
}

func parseBufferPressure(value string) (BufferPressure, bool) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "LOW":
		return BufferPressureLow, true
	case "NORMAL":
		return BufferPressureNormal, true
	case "HIGH":
		return BufferPressureHigh, true
	case "CRITICAL":
		return BufferPressureCritical, true
	default:
		return BufferPressureUnknown, false
	}
}

func bufferPressureForLevel(level uint64, policy *tgsrlv1.BackpressurePolicy) BufferPressure {
	if level <= policy.GetLowWatermark() {
		return BufferPressureLow
	}
	if policy.GetMaximumBufferLevel() > 0 && level > policy.GetMaximumBufferLevel() {
		return BufferPressureCritical
	}
	if level >= policy.GetHighWatermark() {
		return BufferPressureHigh
	}
	return BufferPressureNormal
}

func recentActionInFlight(decisions []*tgsrlv1.DecisionRecord) (bool, bool) {
	ordered := append([]*tgsrlv1.DecisionRecord(nil), decisions...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].GetSequence() > ordered[j].GetSequence() })
	for _, decision := range ordered {
		if decision == nil || decision.GetSelectedPlan() == nil || len(decision.GetSelectedPlan().GetActions()) == 0 {
			continue
		}
		if len(decision.GetActionResults()) == 0 {
			return true, true
		}
		for _, result := range decision.GetActionResults() {
			if result == nil || result.GetStatus() == tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_UNKNOWN {
				return true, true
			}
		}
		return false, true
	}
	return false, false
}

func applyActionBudget(proposals []PlannerProposal, budget int) {
	if budget < 1 {
		budget = DefaultPlannerActionBudget
	}
	eligible := make([]int, 0, len(proposals))
	for index := range proposals {
		if proposals[index].Plan != nil && proposals[index].Evidence.GetDisposition() != tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED {
			eligible = append(eligible, index)
		}
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		left, right := proposals[eligible[i]].Evidence, proposals[eligible[j]].Evidence
		if left.GetUtilityNanos() != right.GetUtilityNanos() {
			return left.GetUtilityNanos() > right.GetUtilityNanos()
		}
		return left.GetProposalId() < right.GetProposalId()
	})
	for position, index := range eligible {
		evidence := proposals[index].Evidence
		if position < budget {
			continue
		}
		proposals[index].Plan = nil
		evidence.Disposition = tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_DEFERRED
		evidence.Reason = "PER_TICK_ACTION_BUDGET"
	}
}

func planningSignalEvidence(signals PlanningSignals) []*tgsrlv1.SemanticField {
	fields := []*tgsrlv1.SemanticField{
		semanticBoolField("planner.signal.buffer_pressure_present", signals.BufferPressurePresent),
		semanticBoolField("planner.signal.buffer_level_present", signals.BufferLevelPresent),
		semanticBoolField("planner.signal.policy_lag_present", signals.PolicyLagPresent),
		semanticBoolField("planner.signal.sample_staleness_present", signals.SampleStalenessPresent),
		semanticBoolField("planner.signal.ess_ratio_present", signals.ESSRatioPresent),
		semanticBoolField("planner.signal.checkpoint_capable_present", signals.CheckpointCapablePresent),
		semanticBoolField("planner.signal.buffer_below_watermark_present", signals.BufferBelowWatermarkPresent),
		semanticBoolField("planner.signal.recovery_cost_present", signals.RecoveryCostNanosPresent),
		semanticBoolField("planner.signal.minimum_residency_present", signals.MinimumResidencyPresent),
		semanticBoolField("planner.signal.action_in_flight_present", signals.ActionInFlightPresent),
		semanticUintField("planner.per_tick_action_budget", uint64(signals.PerTickActionBudget)),
	}
	if signals.BufferPressurePresent {
		fields = append(fields, semanticStringField("planner.signal.buffer_pressure", signals.BufferPressure.String()))
	}
	if signals.BufferLevelPresent {
		fields = append(fields, semanticUintField("planner.signal.buffer_level", signals.BufferLevel))
	}
	if signals.PolicyLagPresent {
		fields = append(fields, semanticUintField("planner.signal.policy_lag", signals.PolicyLag))
	}
	if signals.SampleStalenessPresent {
		fields = append(fields, semanticBoolField("planner.signal.sample_staleness", signals.SampleStaleness))
	}
	if signals.ESSRatioPresent {
		fields = append(fields, semanticDoubleField("planner.signal.ess_ratio", signals.ESSRatio))
	}
	if signals.CheckpointCapablePresent {
		fields = append(fields, semanticBoolField("planner.signal.checkpoint_capable", signals.CheckpointCapable))
	}
	if signals.BufferBelowWatermarkPresent {
		fields = append(fields, semanticBoolField("planner.signal.buffer_below_watermark", signals.BufferBelowWatermark))
	}
	if signals.RecoveryCostNanosPresent {
		fields = append(fields, semanticIntField("planner.signal.recovery_cost_nanos", signals.RecoveryCostNanos))
	}
	if signals.MinimumResidencyPresent {
		fields = append(fields, semanticIntField("planner.signal.minimum_residency_nanos", signals.MinimumResidency.Nanoseconds()))
	}
	if signals.ActionInFlightPresent {
		fields = append(fields, semanticBoolField("planner.signal.action_in_flight", signals.ActionInFlight))
	}
	return fields
}
