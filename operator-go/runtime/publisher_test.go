package runtime

import (
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func TestBuildSandboxEventPreservesSchedulingCausality(t *testing.T) {
	binding := &tgsrlv1.Binding{BindingId: "binding-1", PendingUnitId: "unit-1", RuntimeUnitId: "unit-1", SandboxId: "sandbox-1", Generation: 3}
	decision := &tgsrlv1.DecisionRecord{DecisionId: "decision-1", Generation: 3, SelectedPlan: &tgsrlv1.PlacementPlan{PlanId: "plan-1", Bindings: []*tgsrlv1.Binding{binding}, Actions: []*tgsrlv1.Action{{ActionId: "action-1", TargetId: "unit-1", Binding: binding, IdempotencyKey: "scheduler-key"}}}, ActionResults: []*tgsrlv1.ActionResult{{ActionId: "action-1", ObservedRevision: 9}}}
	event := BuildSandboxEvent(decision, &tgsrlv1.JobRun{RunId: "run-1", JobId: "job-1"}, binding, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_RUNNING, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, "running")
	if event.GetRuntimeUnitId() != "unit-1" || event.GetDecisionId() != "decision-1" || event.GetPlanId() != "plan-1" || event.GetActionId() != "action-1" || event.GetIdempotencyKey() != "scheduler-key" || event.GetProviderRevision() != 9 {
		t.Fatalf("event causality = %+v", event)
	}
}

func TestBuildSandboxEventUsesConcreteBindingGeneration(t *testing.T) {
	binding := &tgsrlv1.Binding{
		BindingId:     "replacement-binding",
		RuntimeUnitId: "unit-1",
		SandboxId:     "sandbox-1",
		Generation:    4,
	}
	decision := &tgsrlv1.DecisionRecord{
		DecisionId: "replacement-decision",
		Generation: 3,
		SelectedPlan: &tgsrlv1.PlacementPlan{
			PlanId:   "replacement-plan",
			Bindings: []*tgsrlv1.Binding{binding},
		},
	}

	event := BuildSandboxEvent(decision, &tgsrlv1.JobRun{RunId: "run-1", JobId: "job-1"}, binding, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_RUNNING, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, "running")
	if event.GetGeneration() != binding.GetGeneration() {
		t.Fatalf("event generation = %d, want replacement binding generation %d", event.GetGeneration(), binding.GetGeneration())
	}
	if event.GetBinding().GetGeneration() != binding.GetGeneration() {
		t.Fatalf("event binding generation = %d, want %d", event.GetBinding().GetGeneration(), binding.GetGeneration())
	}
}

func TestBuildSandboxEventProjectsMutableActionStateWithPresence(t *testing.T) {
	binding := &tgsrlv1.Binding{BindingId: "binding-1", PendingUnitId: "unit-1", RuntimeUnitId: "unit-1", SandboxId: "sandbox-1", Generation: 3, Resources: &tgsrlv1.ResourceVector{AcceleratorUnits: 0.25}}
	tests := []struct {
		name        string
		action      *tgsrlv1.Action
		wantShare   *float64
		wantPrio    *int32
		wantOffload *bool
	}{
		{name: "bind", action: &tgsrlv1.Action{ActionId: "bind", ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND, Binding: binding, Priority: 7}, wantShare: float64Ptr(0.25), wantPrio: int32Ptr(7), wantOffload: boolPtr(false)},
		{name: "set share", action: &tgsrlv1.Action{ActionId: "share", ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, SandboxId: "sandbox-1", Share: 0.75}, wantShare: float64Ptr(0.75)},
		{name: "set priority", action: &tgsrlv1.Action{ActionId: "priority", ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, SandboxId: "sandbox-1", Priority: 11}, wantPrio: int32Ptr(11)},
		{name: "offload", action: &tgsrlv1.Action{ActionId: "offload", ActionType: tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD, SandboxId: "sandbox-1"}, wantOffload: boolPtr(true)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := &tgsrlv1.DecisionRecord{DecisionId: "decision-1", SelectedPlan: &tgsrlv1.PlacementPlan{PlanId: "plan-1", Actions: []*tgsrlv1.Action{test.action}}}
			event := BuildSandboxEvent(decision, &tgsrlv1.JobRun{RunId: "run-1", JobId: "job-1"}, binding, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_RUNNING, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, "observed")
			if (test.wantShare == nil) != (event.Share == nil) || test.wantShare != nil && event.GetShare() != *test.wantShare || (test.wantPrio == nil) != (event.Priority == nil) || test.wantPrio != nil && event.GetPriority() != *test.wantPrio || (test.wantOffload == nil) != (event.Offloaded == nil) || test.wantOffload != nil && event.GetOffloaded() != *test.wantOffload {
				t.Fatalf("event mutable state = share:%v priority:%v offloaded:%v", event.Share, event.Priority, event.Offloaded)
			}
		})
	}
}

func TestBuildSandboxEventLeavesCausalFieldsEmptyWhenNoActionMatches(t *testing.T) {
	binding := &tgsrlv1.Binding{BindingId: "binding-1", PendingUnitId: "unit-1", RuntimeUnitId: "unit-1", SandboxId: "sandbox-1", Generation: 3}
	decision := &tgsrlv1.DecisionRecord{
		DecisionId: "decision-1",
		Generation: 3,
		SelectedPlan: &tgsrlv1.PlacementPlan{
			PlanId: "plan-1",
			Actions: []*tgsrlv1.Action{
				{ActionId: "action-1", TargetId: "other-unit", SandboxId: "sandbox-2", IdempotencyKey: "scheduler-key"},
			},
		},
		ActionResults: []*tgsrlv1.ActionResult{{ActionId: "action-1", ObservedRevision: 9}},
	}

	event := BuildSandboxEvent(decision, &tgsrlv1.JobRun{RunId: "run-1", JobId: "job-1"}, binding, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_RUNNING, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, "running")
	if event.GetActionId() != "" || event.GetIdempotencyKey() != "" || event.GetProviderRevision() != 0 {
		t.Fatalf("event causality without action = %+v", event)
	}
}

func TestBuildSandboxEventLeavesCausalFieldsEmptyWhenDecisionIsNil(t *testing.T) {
	binding := &tgsrlv1.Binding{BindingId: "binding-1", PendingUnitId: "unit-1", RuntimeUnitId: "unit-1", SandboxId: "sandbox-1", Generation: 3}

	event := BuildSandboxEvent(nil, &tgsrlv1.JobRun{RunId: "run-1", JobId: "job-1"}, binding, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_RUNNING, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, "running")
	if event.GetActionId() != "" || event.GetIdempotencyKey() != "" || event.GetProviderRevision() != 0 {
		t.Fatalf("event causality without decision = %+v", event)
	}
	if event.GetDecisionId() != "" || event.GetPlanId() != "" {
		t.Fatalf("event decision metadata without decision = %+v", event)
	}
}

func float64Ptr(value float64) *float64 { return &value }
func int32Ptr(value int32) *int32       { return &value }
func boolPtr(value bool) *bool          { return &value }
