package policy

import (
	"fmt"
	"sort"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/candidates"
)

// Policy orders or filters feasible candidates before action execution.
type Policy interface {
	Name() string
	Choose(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, input []candidates.EvaluatedCandidate) (selected *candidates.EvaluatedCandidate, fallback string)
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
	AllowPreemption  bool
	PreemptionPolicy string
	RequireSafePoint bool
}

// Validate rejects unknown or unsafe policy combinations.
func (b Bundle) Validate() error {
	if strings.TrimSpace(b.ID) == "" || strings.TrimSpace(b.Version) == "" {
		return fmt.Errorf("policy: id and version are required")
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

func (NoOp) Choose(*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent, []candidates.EvaluatedCandidate) (*candidates.EvaluatedCandidate, string) {
	return nil, "NO_OP_POLICY"
}

// Static chooses no new action but indicates existing allocations should be
// preserved by a caller that can materialize static fallback.
type Static struct{}

func (Static) Name() string { return "static" }

func (Static) Choose(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent, _ []candidates.EvaluatedCandidate) (*candidates.EvaluatedCandidate, string) {
	if snapshot == nil || len(snapshot.GetAllocations()) == 0 {
		return nil, "STATIC_EMPTY"
	}
	return nil, "STATIC"
}

// Binpack prefers the smallest remaining headroom among feasible candidates.
type Binpack struct{}

func (Binpack) Name() string { return "binpack" }

func (Binpack) Choose(_ *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent, input []candidates.EvaluatedCandidate) (*candidates.EvaluatedCandidate, string) {
	feasible := feasibleOnly(input)
	if len(feasible) == 0 {
		return nil, "NO_CANDIDATE"
	}
	sort.SliceStable(feasible, func(i, j int) bool {
		left := feasible[i].Candidate.GetComponentScores()["capacity_headroom"]
		right := feasible[j].Candidate.GetComponentScores()["capacity_headroom"]
		if left != right {
			return left < right
		}
		return feasible[i].Candidate.GetScore() > feasible[j].Candidate.GetScore()
	})
	return feasible[0], ""
}

// TraceAware prefers candidates whose devices match trace or job affinity.
type TraceAware struct{}

func (TraceAware) Name() string { return "trace_aware" }

func (TraceAware) Choose(_ *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, input []candidates.EvaluatedCandidate) (*candidates.EvaluatedCandidate, string) {
	feasible := feasibleOnly(input)
	if len(feasible) == 0 {
		return nil, "NO_CANDIDATE"
	}
	sort.SliceStable(feasible, func(i, j int) bool {
		left := feasible[i].Candidate.GetComponentScores()["trace_affinity"]
		right := feasible[j].Candidate.GetComponentScores()["trace_affinity"]
		if left != right {
			return left > right
		}
		if strings.TrimSpace(intent.GetTraceId()) == "" {
			return feasible[i].Candidate.GetScore() > feasible[j].Candidate.GetScore()
		}
		return feasible[i].Candidate.GetCandidateId() < feasible[j].Candidate.GetCandidateId()
	})
	return feasible[0], ""
}

// ScoreFirst selects the highest scored feasible candidate.
type ScoreFirst struct{}

func (ScoreFirst) Name() string { return "score_first" }

func (ScoreFirst) Choose(_ *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent, input []candidates.EvaluatedCandidate) (*candidates.EvaluatedCandidate, string) {
	feasible := feasibleOnly(input)
	if len(feasible) == 0 {
		return nil, "NO_CANDIDATE"
	}
	return feasible[0], ""
}

func feasibleOnly(input []candidates.EvaluatedCandidate) []*candidates.EvaluatedCandidate {
	out := make([]*candidates.EvaluatedCandidate, 0, len(input))
	for index := range input {
		if input[index].Candidate != nil {
			out = append(out, &input[index])
		}
	}
	return out
}
