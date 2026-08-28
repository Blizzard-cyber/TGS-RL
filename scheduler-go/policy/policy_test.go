package policy

import (
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/candidates"
)

func TestPoliciesChooseExpectedCandidates(t *testing.T) {
	input := []candidates.EvaluatedCandidate{
		{Candidate: &tgsrlv1.PlacementCandidate{
			CandidateId: "a",
			Score:       3.0,
			ComponentScores: map[string]float64{
				"capacity_headroom": 0.6,
				"trace_affinity":    0.1,
			},
		}},
		{Candidate: &tgsrlv1.PlacementCandidate{
			CandidateId: "b",
			Score:       2.5,
			ComponentScores: map[string]float64{
				"capacity_headroom": 0.2,
				"trace_affinity":    1.0,
			},
		}},
	}
	intent := &tgsrlv1.SchedulingIntent{TraceId: "trace-1"}

	selected, fallback := ScoreFirst{}.Choose(nil, intent, input)
	if fallback != "" || selected == nil || selected.Candidate.GetCandidateId() != "a" {
		t.Fatalf("ScoreFirst selected=%v fallback=%q", selected, fallback)
	}

	selected, fallback = Binpack{}.Choose(nil, intent, input)
	if fallback != "" || selected == nil || selected.Candidate.GetCandidateId() != "b" {
		t.Fatalf("Binpack selected=%v fallback=%q", selected, fallback)
	}

	selected, fallback = TraceAware{}.Choose(nil, intent, input)
	if fallback != "" || selected == nil || selected.Candidate.GetCandidateId() != "b" {
		t.Fatalf("TraceAware selected=%v fallback=%q", selected, fallback)
	}

	selected, fallback = NoOp{}.Choose(nil, intent, input)
	if selected != nil || fallback != "NO_OP_POLICY" {
		t.Fatalf("NoOp selected=%v fallback=%q", selected, fallback)
	}
}

func TestBundleBuildsConfiguredPolicy(t *testing.T) {
	configured, err := New(Bundle{ID: "policy-1", Version: "1", Strategy: StrategyBinpack, PreemptionPolicy: "noop"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if configured.Name() != "binpack" {
		t.Fatalf("policy name = %q, want binpack", configured.Name())
	}
	if _, err := New(Bundle{ID: "policy-1", Version: "1", Strategy: StrategyScoreFirst, AllowPreemption: true, PreemptionPolicy: "unsafe"}); err == nil {
		t.Fatal("New(unsafe preemption) error = nil")
	}
}
