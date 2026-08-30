package scheduler

import (
	"sort"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/semantics"
	"google.golang.org/protobuf/proto"
)

const (
	// DefaultPlannerEvidenceBudget bounds retained proposal evidence.
	DefaultPlannerEvidenceBudget = 256
	// DefaultPlannerActionBudget matches Store's one-mutation transaction
	// contract.
	DefaultPlannerActionBudget = 1
	// DefaultPlannerIdleSleepAfter is the default idle-to-sleep threshold.
	DefaultPlannerIdleSleepAfter = 5 * time.Minute
	// DefaultPlannerIdleOffloadAfter is the default idle-to-offload threshold.
	DefaultPlannerIdleOffloadAfter = 30 * time.Minute
	// Default planner sandbox ages are tier-specific so each tick consumes
	// evidence no older than its own cadence.
	DefaultPlannerFastSandboxMaximumAge   = 5 * time.Second
	DefaultPlannerMediumSandboxMaximumAge = 30 * time.Second
	DefaultPlannerSlowSandboxMaximumAge   = 2 * time.Minute
	// DefaultPlannerSandboxMaximumAge is retained as the legacy unified
	// configuration default. A configured PlannerSandboxMaximumAge still
	// overrides all three tier defaults.
	DefaultPlannerSandboxMaximumAge = DefaultPlannerSlowSandboxMaximumAge
	// DefaultPlannerSandboxFutureSkew tolerates ordinary clock skew but rejects
	// observations farther in the future.
	DefaultPlannerSandboxFutureSkew = time.Second
)

// BufferPressure is a stable, ordered pressure classification derived from
// the observed level and the execution contract watermarks.
type BufferPressure uint8

const (
	BufferPressureUnknown BufferPressure = iota
	BufferPressureLow
	BufferPressureNormal
	BufferPressureHigh
	BufferPressureCritical
)

func (p BufferPressure) String() string {
	switch p {
	case BufferPressureLow:
		return "LOW"
	case BufferPressureNormal:
		return "NORMAL"
	case BufferPressureHigh:
		return "HIGH"
	case BufferPressureCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// PlanningSignals contains only typed, replayable facts used by adaptive
// planners. Callers may populate it explicitly; missing values are
// deterministically derived from ContractObservation, Sandbox semantic
// evidence, and RecentDecisions. Explicit values always take precedence.
type PlanningSignals struct {
	BufferPressure              BufferPressure
	BufferPressurePresent       bool
	BufferLevel                 uint64
	BufferLevelPresent          bool
	PolicyLag                   uint64
	PolicyLagPresent            bool
	SampleStaleness             bool
	SampleStalenessPresent      bool
	ESSRatio                    float64
	ESSRatioPresent             bool
	CheckpointCapable           bool
	CheckpointCapablePresent    bool
	BufferBelowWatermark        bool
	BufferBelowWatermarkPresent bool
	RecoveryCostNanos           int64
	RecoveryCostNanosPresent    bool
	MinimumResidency            time.Duration
	MinimumResidencyPresent     bool
	ActionInFlight              bool
	ActionInFlightPresent       bool
	PerTickActionBudget         int
	PerTickActionBudgetPresent  bool
}

// Directives are typed, caller-owned adaptive planning goals. They are kept
// separate from observed state: a directive can request a transition, but it
// can never stand in for a missing Sandbox, Allocation, capability, or fence.
type Directives struct {
	TargetShare         *float64
	TargetPriority      *int32
	TargetResources     *tgsrlv1.ResourceVector
	ResizeResources     map[string]*tgsrlv1.ResourceVector
	ReplacementBindings map[string]*tgsrlv1.Binding
	SandboxByAllocation map[string]string
	TargetSandboxIDs    []string

	DesiredState tgsrlv1.RuntimeState
	AllowPause   bool
	AllowResume  bool
	AllowSleep   bool
	AllowOffload bool
	SleepAfter   time.Duration
	OffloadAfter time.Duration

	AllowResize   bool
	AllowScaleIn  bool
	AllowRebind   bool
	AllowRecreate bool

	MinimumUtilityNanos int64
}

// PlanningInput is the complete immutable input to adaptive planning. The
// coordinator never reads a clock, provider, Store, or package-global state.
type PlanningInput struct {
	Snapshot          *tgsrlv1.ClusterSnapshot
	Intent            *tgsrlv1.SchedulingIntent
	EvaluationContext *tgsrlv1.EvaluationContext
	Sandboxes         []*tgsrlv1.Sandbox
	RecentDecisions   []*tgsrlv1.DecisionRecord
	SafePoint         semantics.SafePointResolution
	ContractAggregate semantics.AggregateResult
	Directives        Directives
	Signals           PlanningSignals
	sandboxMaximumAge time.Duration
	sandboxFutureSkew time.Duration

	// AdmissionPlan is the already-selected legacy admission result. Keeping
	// candidate selection outside the adaptive coordinator preserves the
	// existing admission policy and byte-for-byte binding/action behavior.
	AdmissionPlan *tgsrlv1.PlacementPlan
	DecisionID    string
}

// AdaptiveEvaluationInput is the Scheduler integration form of PlanningInput.
// It is an alias so callers can use either name without conversion.
type AdaptiveEvaluationInput = PlanningInput

// PlannerProposal is one auditable alternative. A rejected proposal has no
// plan. An eligible proposal carries a complete one- or multi-action plan.
type PlannerProposal struct {
	Plan     *tgsrlv1.PlacementPlan
	Evidence *tgsrlv1.PlannerEvidence
}

// PlannerResult is the coordinator output. Evidence is a bounded projection;
// TotalProposalCount always reports the full evaluated proposal count.
type PlannerResult struct {
	Plan               *tgsrlv1.PlacementPlan
	Evidence           []*tgsrlv1.PlannerEvidence
	TotalProposalCount uint64
	EvidenceTruncated  bool
	FallbackReason     string
}

// Planner is implemented by each deterministic tier.
type Planner interface {
	Propose(PlanningInput) []PlannerProposal
}

// PlannerUtilityConfig assigns integer nanoutilities. No observed reward is
// consumed, and no floating-point value participates in proposal ordering.
type PlannerUtilityConfig struct {
	Bind        int64
	SetShare    int64
	SetPriority int64
	Resize      int64
	Release     int64
	Pause       int64
	Resume      int64
	Sleep       int64
	Offload     int64
	Rebind      int64
	Recreate    int64
	// FastSignalStep is added once for each pressure/lag/staleness/ESS
	// condition that justifies a fast mutation.
	FastSignalStep int64
	// SlowBenefit and SlowRisk are the default fixed-point benefit and risk
	// components for slow reconfiguration. RecoveryCostNanos is supplied by
	// PlanningSignals.
	SlowBenefit int64
	SlowRisk    int64
}

func DefaultPlannerUtilityConfig() PlannerUtilityConfig {
	return PlannerUtilityConfig{
		Bind: 1_000_000_000, SetShare: 900_000_000, SetPriority: 850_000_000,
		Resize: 800_000_000, Release: 780_000_000, Pause: 700_000_000,
		Resume: 950_000_000, Sleep: 650_000_000, Offload: 620_000_000,
		Rebind: 550_000_000, Recreate: 500_000_000, FastSignalStep: 25_000_000,
		SlowBenefit: 1_000_000_000, SlowRisk: 100_000_000,
	}
}

func normalizePlannerUtilityConfig(value PlannerUtilityConfig) PlannerUtilityConfig {
	defaults := DefaultPlannerUtilityConfig()
	if value == (PlannerUtilityConfig{}) {
		return defaults
	}
	if value.FastSignalStep == 0 {
		value.FastSignalStep = defaults.FastSignalStep
	}
	if value.SlowBenefit == 0 {
		value.SlowBenefit = defaults.SlowBenefit
	}
	if value.SlowRisk == 0 {
		value.SlowRisk = defaults.SlowRisk
	}
	return value
}

func clonePlanningInput(input PlanningInput) PlanningInput {
	cloned := input
	if input.Snapshot != nil {
		cloned.Snapshot = proto.Clone(input.Snapshot).(*tgsrlv1.ClusterSnapshot)
	}
	if input.Intent != nil {
		cloned.Intent = proto.Clone(input.Intent).(*tgsrlv1.SchedulingIntent)
	}
	if input.EvaluationContext != nil {
		cloned.EvaluationContext = proto.Clone(input.EvaluationContext).(*tgsrlv1.EvaluationContext)
	}
	cloned.Sandboxes = cloneMessages(input.Sandboxes)
	cloned.RecentDecisions = cloneMessages(input.RecentDecisions)
	cloned.SafePoint.Evidence = cloneMessages(input.SafePoint.Evidence)
	if input.AdmissionPlan != nil {
		cloned.AdmissionPlan = proto.Clone(input.AdmissionPlan).(*tgsrlv1.PlacementPlan)
	}
	cloned.Directives = cloneDirectives(input.Directives)
	return cloned
}

func cloneMessages[T proto.Message](values []T) []T {
	if values == nil {
		return nil
	}
	result := make([]T, 0, len(values))
	for _, value := range values {
		if any(value) == nil {
			result = append(result, value)
			continue
		}
		result = append(result, proto.Clone(value).(T))
	}
	return result
}

func cloneDirectives(input Directives) Directives {
	result := input
	if input.TargetShare != nil {
		value := *input.TargetShare
		result.TargetShare = &value
	}
	if input.TargetPriority != nil {
		value := *input.TargetPriority
		result.TargetPriority = &value
	}
	if input.TargetResources != nil {
		result.TargetResources = proto.Clone(input.TargetResources).(*tgsrlv1.ResourceVector)
	}
	result.ResizeResources = cloneResourceMap(input.ResizeResources)
	result.ReplacementBindings = make(map[string]*tgsrlv1.Binding, len(input.ReplacementBindings))
	for key, value := range input.ReplacementBindings {
		if value != nil {
			result.ReplacementBindings[key] = proto.Clone(value).(*tgsrlv1.Binding)
		}
	}
	result.SandboxByAllocation = cloneStringMap(input.SandboxByAllocation)
	result.TargetSandboxIDs = append([]string(nil), input.TargetSandboxIDs...)
	sort.Strings(result.TargetSandboxIDs)
	return result
}

func cloneResourceMap(input map[string]*tgsrlv1.ResourceVector) map[string]*tgsrlv1.ResourceVector {
	if input == nil {
		return nil
	}
	result := make(map[string]*tgsrlv1.ResourceVector, len(input))
	for key, value := range input {
		if value != nil {
			result[key] = proto.Clone(value).(*tgsrlv1.ResourceVector)
		}
	}
	return result
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
