package admission

import (
	"testing"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
)

func TestEvaluateAllowsWhenQuotaFits(t *testing.T) {
	e := New()
	decision, err := e.Evaluate(&api.Bundle{
		Key: "ns/a",
		Admission: api.AdmissionSpec{
			Queue:           "train",
			QuotaGroup:      "team-a",
			Requests:        api.ResourceList{"cpu": "2", "memory": "8"},
			AllowPreemption: false,
		},
	}, QueuePolicy{
		Name:       "train",
		QuotaGroup: "team-a",
		Capacity:   api.ResourceList{"cpu": "4", "memory": "16"},
	})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if !decision.Allowed || decision.Phase != "admitted" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

func TestEvaluatePreemptsLowerPriorityVictims(t *testing.T) {
	e := New()
	decision, err := e.Evaluate(&api.Bundle{
		Key: "ns/a",
		Admission: api.AdmissionSpec{
			Queue:           "train",
			QuotaGroup:      "team-a",
			Requests:        api.ResourceList{"cpu": "8"},
			AllowPreemption: true,
		},
		Workload: api.Workload{
			Spec: api.WorkloadSpec{Priority: 100},
		},
	}, QueuePolicy{
		Name:            "train",
		QuotaGroup:      "team-a",
		Capacity:        api.ResourceList{"cpu": "2"},
		AllowPreemption: true,
		PreemptionVictims: []RunningBundle{
			{Key: "ns/low-2", Priority: 10, Resources: api.ResourceList{"cpu": "4"}},
			{Key: "ns/low-1", Priority: 5, Resources: api.ResourceList{"cpu": "2"}},
		},
	})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	if !decision.Allowed {
		t.Fatalf("expected preemption admission, got %+v", decision)
	}
	if len(decision.PreemptedKeys) != 2 {
		t.Fatalf("expected two preempted keys, got %+v", decision.PreemptedKeys)
	}
}
