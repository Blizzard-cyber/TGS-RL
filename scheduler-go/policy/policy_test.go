package policy

import (
	"math"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func TestPoliciesChooseExpectedCandidates(t *testing.T) {
	input := SelectionInput{
		UnitID: "unit-1",
		TopK:   2,
		Candidates: []*tgsrlv1.PlacementCandidate{
			testCandidate("a", "unit-1", "device-a", 3.0, 0.6, 0.1),
			testCandidate("b", "unit-1", "device-b", 2.5, 0.2, 1.0),
		},
	}

	selected, fallback := ScoreFirst{}.Choose(nil, nil, input)
	if fallback != "" || selected.GetCandidateId() != "a" {
		t.Fatalf("ScoreFirst selected=%v fallback=%q", selected, fallback)
	}

	selected, fallback = Binpack{}.Choose(nil, nil, input)
	if fallback != "" || selected.GetCandidateId() != "b" {
		t.Fatalf("Binpack selected=%v fallback=%q", selected, fallback)
	}

	selected, fallback = TraceAware{}.Choose(nil, nil, input)
	if fallback != "" || selected.GetCandidateId() != "b" {
		t.Fatalf("TraceAware selected=%v fallback=%q", selected, fallback)
	}

	selected, fallback = NoOp{}.Choose(nil, nil, input)
	if selected != nil || fallback != "NO_OP_POLICY" {
		t.Fatalf("NoOp selected=%v fallback=%q", selected, fallback)
	}
}

func TestChooseUsesOnlyEngineShortlist(t *testing.T) {
	input := SelectionInput{
		UnitID: "unit-1",
		TopK:   1,
		Candidates: []*tgsrlv1.PlacementCandidate{
			testCandidate("score-first", "unit-1", "device-b", 2, 0.5, 0.1),
		},
	}
	originalFirst := input.Candidates[0]

	selected, fallback := TraceAware{}.Choose(nil, nil, input)
	if fallback != "" || selected.GetCandidateId() != "score-first" {
		t.Fatalf("TraceAware selected=%v fallback=%q, want only shortlisted candidate", selected, fallback)
	}
	if input.Candidates[0] != originalFirst {
		t.Fatal("Choose mutated the caller's candidate order")
	}
}

func TestTopKChangesOnlyPoliciesThatReorderScoreFirstShortlist(t *testing.T) {
	scoreFirst := testCandidate("score-first", "unit-1", "device-a", 2, 0.8, 0)
	alternative := testCandidate("policy-preferred", "unit-1", "device-b", 1, 0.1, 0.3)

	for _, test := range []struct {
		name  string
		value Policy
		want1 string
		want2 string
	}{
		{name: "score first", value: ScoreFirst{}, want1: "score-first", want2: "score-first"},
		{name: "binpack", value: Binpack{}, want1: "score-first", want2: "policy-preferred"},
		{name: "trace aware", value: TraceAware{}, want1: "score-first", want2: "policy-preferred"},
	} {
		t.Run(test.name, func(t *testing.T) {
			selectedK1, fallback := test.value.Choose(nil, nil, SelectionInput{
				UnitID: "unit-1", Candidates: []*tgsrlv1.PlacementCandidate{scoreFirst}, TopK: 1,
			})
			if fallback != "" || selectedK1.GetCandidateId() != test.want1 {
				t.Fatalf("K=1 selected=%v fallback=%q, want %q", selectedK1, fallback, test.want1)
			}
			selectedK2, fallback := test.value.Choose(nil, nil, SelectionInput{
				UnitID: "unit-1", Candidates: []*tgsrlv1.PlacementCandidate{scoreFirst, alternative}, TopK: 2,
			})
			if fallback != "" || selectedK2.GetCandidateId() != test.want2 {
				t.Fatalf("K=2 selected=%v fallback=%q, want %q", selectedK2, fallback, test.want2)
			}
		})
	}
}

func TestPoliciesUseStableBusinessTieBreak(t *testing.T) {
	deviceB := testCandidate("candidate-a", "unit-1", "device-b", 5, 0.4, 0.7)
	deviceA := testCandidate("candidate-z", "unit-1", "device-a", 5, 0.4, 0.7)
	policies := []Policy{ScoreFirst{}, Binpack{}, TraceAware{}}
	for _, configured := range policies {
		for _, candidates := range [][]*tgsrlv1.PlacementCandidate{{deviceB, deviceA}, {deviceA, deviceB}} {
			selected, fallback := configured.Choose(nil, nil, SelectionInput{UnitID: "unit-1", Candidates: candidates, TopK: 2})
			if fallback != "" || selected.GetCandidateId() != "candidate-z" {
				t.Fatalf("%s selected=%v fallback=%q, want device-a candidate", configured.Name(), selected, fallback)
			}
		}
	}
}

func TestScoreFirstRejectsNonFiniteScore(t *testing.T) {
	selected, fallback := ScoreFirst{}.Choose(nil, nil, SelectionInput{
		UnitID: "unit-1",
		TopK:   2,
		Candidates: []*tgsrlv1.PlacementCandidate{
			testCandidate("nan", "unit-1", "device-a", math.NaN(), 0, 0),
			testCandidate("finite", "unit-1", "device-b", 1, 0, 0),
		},
	})
	if selected != nil || fallback != invalidSelectionInput {
		t.Fatalf("selected=%v fallback=%q, want invalid input", selected, fallback)
	}
}

func TestSelectionInputValidation(t *testing.T) {
	valid := testCandidate("valid", "unit-1", "device-a", 1, 0.7, 0.3)
	wrongUnit := testCandidate("wrong-unit", "unit-2", "device-a", 1, 0.7, 0.3)
	emptyID := testCandidate("", "unit-1", "device-a", 1, 0.7, 0.3)
	multiBinding := testCandidate("multi-binding", "unit-1", "device-a", 1, 0.7, 0.3)
	multiBinding.Plan.Bindings = append(multiBinding.Plan.Bindings, &tgsrlv1.Binding{PendingUnitId: "unit-1", DeviceIds: []string{"device-b"}})
	for name, input := range map[string]SelectionInput{
		"missing unit":        {TopK: 1},
		"invalid top-k":       {UnitID: "unit-1"},
		"wrong unit":          {UnitID: "unit-1", TopK: 1, Candidates: []*tgsrlv1.PlacementCandidate{wrongUnit}},
		"nil candidate":       {UnitID: "unit-1", TopK: 1, Candidates: []*tgsrlv1.PlacementCandidate{nil}},
		"empty candidate id":  {UnitID: "unit-1", TopK: 1, Candidates: []*tgsrlv1.PlacementCandidate{emptyID}},
		"duplicate id":        {UnitID: "unit-1", TopK: 2, Candidates: []*tgsrlv1.PlacementCandidate{valid, valid}},
		"oversized shortlist": {UnitID: "unit-1", TopK: 1, Candidates: []*tgsrlv1.PlacementCandidate{valid, testCandidate("second", "unit-1", "device-b", 0.5, 0.5, 0.1)}},
		"multiple bindings":   {UnitID: "unit-1", TopK: 1, Candidates: []*tgsrlv1.PlacementCandidate{multiBinding}},
	} {
		t.Run(name, func(t *testing.T) {
			selected, fallback := ScoreFirst{}.Choose(nil, nil, input)
			if selected != nil || fallback != invalidSelectionInput {
				t.Fatalf("selected=%v fallback=%q, want %q", selected, fallback, invalidSelectionInput)
			}
		})
	}
}

func TestPoliciesRejectMissingOrNonFiniteRequiredComponent(t *testing.T) {
	for _, configured := range []struct {
		policy    Policy
		component string
	}{
		{policy: Binpack{}, component: "capacity_headroom"},
		{policy: TraceAware{}, component: "trace_affinity"},
	} {
		t.Run(configured.policy.Name(), func(t *testing.T) {
			for _, value := range []struct {
				name   string
				mutate func(map[string]float64)
			}{
				{name: "missing", mutate: func(components map[string]float64) { delete(components, configured.component) }},
				{name: "nan", mutate: func(components map[string]float64) { components[configured.component] = math.NaN() }},
				{name: "infinite", mutate: func(components map[string]float64) { components[configured.component] = math.Inf(1) }},
			} {
				t.Run(value.name, func(t *testing.T) {
					candidate := testCandidate("candidate", "unit-1", "device-a", 1, 0.7, 0.3)
					value.mutate(candidate.ComponentScores)
					selected, fallback := configured.policy.Choose(nil, nil, SelectionInput{
						UnitID:     "unit-1",
						TopK:       1,
						Candidates: []*tgsrlv1.PlacementCandidate{candidate},
					})
					if selected != nil || fallback != invalidSelectionInput {
						t.Fatalf("selected=%v fallback=%q, want invalid input", selected, fallback)
					}
				})
			}
		})
	}
}

func TestBundleBuildsConfiguredPolicy(t *testing.T) {
	configured, err := New(Bundle{ID: "policy-1", Version: "1", Strategy: StrategyBinpack, TopK: 3, PreemptionPolicy: "noop"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if configured.Name() != "binpack" {
		t.Fatalf("policy name = %q, want binpack", configured.Name())
	}
	if _, err := New(Bundle{ID: "policy-1", Version: "1", Strategy: StrategyScoreFirst, PreemptionPolicy: "noop"}); err == nil {
		t.Fatal("New(TopK=0) error = nil")
	}
	if _, err := New(Bundle{ID: "policy-1", Version: "1", Strategy: StrategyScoreFirst, TopK: 1, AllowPreemption: true, PreemptionPolicy: "unsafe"}); err == nil {
		t.Fatal("New(unsafe preemption) error = nil")
	}
}

func testCandidate(id, unitID, deviceID string, score, headroom, trace float64) *tgsrlv1.PlacementCandidate {
	return &tgsrlv1.PlacementCandidate{
		CandidateId: id,
		Score:       score,
		ComponentScores: map[string]float64{
			"capacity_headroom": headroom,
			"share_headroom":    0.2,
			"ready":             0.1,
			"trace_affinity":    trace,
		},
		Plan: &tgsrlv1.PlacementPlan{
			PlanId: "plan-" + id,
			Bindings: []*tgsrlv1.Binding{{
				BindingId:     "binding-" + id,
				PendingUnitId: unitID,
				DeviceIds:     []string{deviceID},
			}},
		},
	}
}
