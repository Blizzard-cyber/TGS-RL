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
