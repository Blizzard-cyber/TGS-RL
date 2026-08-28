package policy

import (
	"fmt"
	"math"
	"sort"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scoring"
)

const invalidSelectionInput = "INVALID_SELECTION_INPUT"

// SelectionInput is the policy surface for one pending unit. Candidates is the
// authoritative engine's score-first-ranked, per-unit TopK shortlist. Policies
// may reorder only that shortlist.
type SelectionInput struct {
	UnitID     string
	Candidates []*tgsrlv1.PlacementCandidate
	TopK       int
}

// Policy chooses one candidate from a single unit's authoritative shortlist.
// Candidate enumeration, feasibility, score-first ranking, and per-unit TopK
// belong to the candidate engine.
type Policy interface {
	Name() string
	Choose(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, input SelectionInput) (selected *tgsrlv1.PlacementCandidate, fallback string)
}

// Strategy identifies a configured candidate-selection policy.
type Strategy string

const (
	StrategyScoreFirst Strategy = "score_first"
	StrategyBinpack    Strategy = "binpack"
	StrategyTraceAware Strategy = "trace_aware"
	StrategyNoOp       Strategy = "noop"
	StrategyStatic     Strategy = "static"
)

// Bundle is the runtime policy consumed by the authoritative Scheduler.
type Bundle struct {
	ID               string
	Version          string
	Strategy         Strategy
	TopK             int
	AllowPreemption  bool
	PreemptionPolicy string
	RequireSafePoint bool
}

// Validate rejects unknown or unsafe policy combinations. The scheduler owns
// zero-value compatibility and must inject its default TopK before validation.
func (b Bundle) Validate() error {
	if strings.TrimSpace(b.ID) == "" || strings.TrimSpace(b.Version) == "" {
		return fmt.Errorf("policy: id and version are required")
	}
	if b.TopK < 1 {
		return fmt.Errorf("policy: top_k must be at least 1")
	}
	switch b.Strategy {
	case StrategyScoreFirst, StrategyBinpack, StrategyTraceAware, StrategyNoOp, StrategyStatic:
	default:
		return fmt.Errorf("policy: unsupported strategy %q", b.Strategy)
	}
	if b.AllowPreemption && b.PreemptionPolicy != "low_priority_first" {
		return fmt.Errorf("policy: unsupported preemption policy %q", b.PreemptionPolicy)
	}
	return nil
}

// New builds a runtime policy from a validated bundle.
func New(bundle Bundle) (Policy, error) {
	if err := bundle.Validate(); err != nil {
		return nil, err
	}
	switch bundle.Strategy {
	case StrategyScoreFirst:
		return ScoreFirst{}, nil
	case StrategyBinpack:
		return Binpack{}, nil
	case StrategyTraceAware:
		return TraceAware{}, nil
	case StrategyNoOp:
		return NoOp{}, nil
	case StrategyStatic:
		return Static{}, nil
	default:
		return nil, fmt.Errorf("policy: unsupported strategy %q", bundle.Strategy)
	}
}

// NoOp always declines to act.
type NoOp struct{}

func (NoOp) Name() string { return "noop" }

func (NoOp) Choose(*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent, SelectionInput) (*tgsrlv1.PlacementCandidate, string) {
	return nil, "NO_OP_POLICY"
}

// Static chooses no new action but indicates existing allocations should be
// preserved by a caller that can materialize static fallback.
type Static struct{}

func (Static) Name() string { return "static" }

func (Static) Choose(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent, _ SelectionInput) (*tgsrlv1.PlacementCandidate, string) {
	if snapshot == nil || len(snapshot.GetAllocations()) == 0 {
		return nil, "STATIC_EMPTY"
	}
	return nil, "STATIC"
}

// Binpack prefers the smallest remaining headroom among feasible candidates.
type Binpack struct{}

func (Binpack) Name() string { return "binpack" }

func (Binpack) Choose(_ *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent, input SelectionInput) (*tgsrlv1.PlacementCandidate, string) {
	eligible, fallback := eligibleCandidates(input, scoring.CapacityHeadroomComponent)
	if fallback != "" {
		return nil, fallback
	}
	if len(eligible) == 0 {
		return nil, "NO_CANDIDATE"
	}
	sort.Slice(eligible, func(i, j int) bool {
		left := eligible[i].GetComponentScores()[scoring.CapacityHeadroomComponent]
		right := eligible[j].GetComponentScores()[scoring.CapacityHeadroomComponent]
		if compared := compareFloat(left, right, true); compared != 0 {
			return compared < 0
		}
		if compared := compareFloat(eligible[i].GetScore(), eligible[j].GetScore(), false); compared != 0 {
			return compared < 0
		}
		return stableCandidateLess(eligible[i], eligible[j])
	})
	return eligible[0], ""
}

// TraceAware prefers candidates with the greatest trace-affinity component.
type TraceAware struct{}

func (TraceAware) Name() string { return "trace_aware" }

func (TraceAware) Choose(_ *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent, input SelectionInput) (*tgsrlv1.PlacementCandidate, string) {
	eligible, fallback := eligibleCandidates(input, scoring.TraceAffinityComponent)
	if fallback != "" {
		return nil, fallback
	}
	if len(eligible) == 0 {
		return nil, "NO_CANDIDATE"
	}
	sort.Slice(eligible, func(i, j int) bool {
		left := eligible[i].GetComponentScores()[scoring.TraceAffinityComponent]
		right := eligible[j].GetComponentScores()[scoring.TraceAffinityComponent]
		if compared := compareFloat(left, right, false); compared != 0 {
			return compared < 0
		}
		if compared := compareFloat(eligible[i].GetScore(), eligible[j].GetScore(), false); compared != 0 {
			return compared < 0
		}
		return stableCandidateLess(eligible[i], eligible[j])
	})
	return eligible[0], ""
}

// ScoreFirst selects the highest-scored feasible candidate.
type ScoreFirst struct{}

func (ScoreFirst) Name() string { return "score_first" }

func (ScoreFirst) Choose(_ *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent, input SelectionInput) (*tgsrlv1.PlacementCandidate, string) {
	eligible, fallback := eligibleCandidates(input, "")
	if fallback != "" {
		return nil, fallback
	}
	if len(eligible) == 0 {
		return nil, "NO_CANDIDATE"
	}
	sort.Slice(eligible, func(i, j int) bool {
		if compared := compareFloat(eligible[i].GetScore(), eligible[j].GetScore(), false); compared != 0 {
			return compared < 0
		}
		return stableCandidateLess(eligible[i], eligible[j])
	})
	return eligible[0], ""
}

func eligibleCandidates(input SelectionInput, requiredComponent string) ([]*tgsrlv1.PlacementCandidate, string) {
	unitID := strings.TrimSpace(input.UnitID)
	if unitID == "" || input.TopK < 1 || len(input.Candidates) > input.TopK {
		return nil, invalidSelectionInput
	}
	out := make([]*tgsrlv1.PlacementCandidate, 0, len(input.Candidates))
	seen := make(map[string]struct{}, len(input.Candidates))
	for _, candidate := range input.Candidates {
		if !validCandidateForUnit(candidate, unitID, requiredComponent) {
			return nil, invalidSelectionInput
		}
		candidateID := candidate.GetCandidateId()
		if _, exists := seen[candidateID]; exists {
			return nil, invalidSelectionInput
		}
		seen[candidateID] = struct{}{}
		out = append(out, candidate)
	}
	return out, ""
}

func validCandidateForUnit(candidate *tgsrlv1.PlacementCandidate, unitID, requiredComponent string) bool {
	if candidate == nil || strings.TrimSpace(candidate.GetCandidateId()) == "" ||
		math.IsNaN(candidate.GetScore()) || math.IsInf(candidate.GetScore(), 0) ||
		candidate.GetPlan() == nil || len(candidate.GetPlan().GetBindings()) != 1 {
		return false
	}
	if requiredComponent != "" {
		value, exists := candidate.GetComponentScores()[requiredComponent]
		if !exists || math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	binding := candidate.GetPlan().GetBindings()[0]
	return binding != nil && binding.GetPendingUnitId() == unitID &&
		len(binding.GetDeviceIds()) == 1 && strings.TrimSpace(binding.GetDeviceIds()[0]) != ""
}

// compareFloat returns a negative value when left sorts before right. Inputs
// have already been checked as finite by validCandidateForUnit.
func compareFloat(left, right float64, ascending bool) int {
	leftNaN, rightNaN := math.IsNaN(left), math.IsNaN(right)
	switch {
	case leftNaN && rightNaN:
		return 0
	case leftNaN:
		return 1
	case rightNaN:
		return -1
	case left == right:
		return 0
	case ascending && left < right, !ascending && left > right:
		return -1
	default:
		return 1
	}
}

func stableCandidateLess(left, right *tgsrlv1.PlacementCandidate) bool {
	leftDevice, leftUnit, leftBinding := stablePlacementKeys(left)
	rightDevice, rightUnit, rightBinding := stablePlacementKeys(right)
	if leftDevice != rightDevice {
		return leftDevice < rightDevice
	}
	if leftUnit != rightUnit {
		return leftUnit < rightUnit
	}
	if left.GetCandidateId() != right.GetCandidateId() {
		return left.GetCandidateId() < right.GetCandidateId()
	}
	if left.GetPlan().GetPlanId() != right.GetPlan().GetPlanId() {
		return left.GetPlan().GetPlanId() < right.GetPlan().GetPlanId()
	}
	return leftBinding < rightBinding
}

func stablePlacementKeys(candidate *tgsrlv1.PlacementCandidate) (deviceKey, unitKey, bindingKey string) {
	binding := candidate.GetPlan().GetBindings()[0]
	return binding.GetDeviceIds()[0], binding.GetPendingUnitId(), binding.GetBindingId()
}
