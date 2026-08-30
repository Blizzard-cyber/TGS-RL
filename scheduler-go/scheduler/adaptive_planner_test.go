package scheduler

import (
	"bytes"
	"math"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/protection"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/semantics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAdaptivePlannersGenerateCompleteActionContracts(t *testing.T) {
	share := .75
	priority := int32(17)
	tests := []struct {
		name        string
		tick        tgsrlv1.TickKind
		state       tgsrlv1.RuntimeState
		directives  Directives
		wantAction  tgsrlv1.ActionType
		wantKind    tgsrlv1.PlannerKind
		wantPurpose tgsrlv1.PlanPurpose
	}{
		{name: "set share", tick: tgsrlv1.TickKind_TICK_KIND_FAST, state: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, directives: Directives{TargetShare: &share}, wantAction: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, wantKind: tgsrlv1.PlannerKind_PLANNER_KIND_FAST_MUTATION, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION},
		{name: "set priority", tick: tgsrlv1.TickKind_TICK_KIND_FAST, state: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, directives: Directives{TargetPriority: &priority}, wantAction: tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, wantKind: tgsrlv1.PlannerKind_PLANNER_KIND_FAST_MUTATION, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION},
		{name: "resize shrink", tick: tgsrlv1.TickKind_TICK_KIND_FAST, state: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, directives: Directives{AllowResize: true, TargetResources: &tgsrlv1.ResourceVector{CpuMillis: 50, MemoryBytes: 50, AcceleratorUnits: .125, EphemeralStorageBytes: 5, NetworkBandwidthBps: 5}}, wantAction: tgsrlv1.ActionType_ACTION_TYPE_RESIZE, wantKind: tgsrlv1.PlannerKind_PLANNER_KIND_FAST_MUTATION, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE},
		{name: "pause", tick: tgsrlv1.TickKind_TICK_KIND_MEDIUM, state: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, directives: Directives{AllowPause: true}, wantAction: tgsrlv1.ActionType_ACTION_TYPE_PAUSE, wantKind: tgsrlv1.PlannerKind_PLANNER_KIND_MEDIUM_LIFECYCLE, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION},
		{name: "resume", tick: tgsrlv1.TickKind_TICK_KIND_MEDIUM, state: tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, directives: Directives{AllowResume: true}, wantAction: tgsrlv1.ActionType_ACTION_TYPE_RESUME, wantKind: tgsrlv1.PlannerKind_PLANNER_KIND_MEDIUM_LIFECYCLE, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY},
		{name: "sleep", tick: tgsrlv1.TickKind_TICK_KIND_MEDIUM, state: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, directives: Directives{AllowSleep: true}, wantAction: tgsrlv1.ActionType_ACTION_TYPE_SLEEP, wantKind: tgsrlv1.PlannerKind_PLANNER_KIND_MEDIUM_LIFECYCLE, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION},
		{name: "offload", tick: tgsrlv1.TickKind_TICK_KIND_MEDIUM, state: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, directives: Directives{AllowOffload: true}, wantAction: tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD, wantKind: tgsrlv1.PlannerKind_PLANNER_KIND_MEDIUM_LIFECYCLE, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION},
		{name: "rebind", tick: tgsrlv1.TickKind_TICK_KIND_SLOW, state: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, directives: Directives{AllowRebind: true, ReplacementBindings: map[string]*tgsrlv1.Binding{"allocation-a": adaptiveReplacementBinding()}}, wantAction: tgsrlv1.ActionType_ACTION_TYPE_REBIND, wantKind: tgsrlv1.PlannerKind_PLANNER_KIND_SLOW_RECONFIGURATION, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE},
		{name: "recreate", tick: tgsrlv1.TickKind_TICK_KIND_SLOW, state: tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED, directives: Directives{AllowRecreate: true, ReplacementBindings: map[string]*tgsrlv1.Binding{"allocation-a": adaptiveReplacementBindingOn("device-a")}}, wantAction: tgsrlv1.ActionType_ACTION_TYPE_RECREATE, wantKind: tgsrlv1.PlannerKind_PLANNER_KIND_SLOW_RECONFIGURATION, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := adaptiveFixture(test.tick, test.state)
			input.Directives = test.directives
			result, err := NewCoordinator(Config{}).Plan(input)
			if err != nil {
				t.Fatalf("Plan() error = %v", err)
			}
			if result.Plan == nil || len(result.Plan.GetActions()) != 1 {
				t.Fatalf("Plan() = %+v, want one action; evidence=%+v", result.Plan, result.Evidence)
			}
			action := result.Plan.GetActions()[0]
			if action.GetActionType() != test.wantAction || result.Plan.GetPurpose() != test.wantPurpose {
				t.Fatalf("action/purpose = %s/%s, want %s/%s", action.GetActionType(), result.Plan.GetPurpose(), test.wantAction, test.wantPurpose)
			}
			if action.GetPlanId() != result.Plan.GetPlanId() || action.GetExpectedSnapshotRevision() != input.Snapshot.GetRevision() || action.GetExpectedGeneration() != input.Sandboxes[0].GetGeneration() || action.GetDeadline() == nil || action.GetIdempotencyKey() == "" || action.GetRollback() == nil || len(action.GetPreconditions()) == 0 || len(action.GetExpectedImpacts()) == 0 {
				t.Fatalf("incomplete action contract: %+v", action)
			}
			if err := validateActionPlan(result.Plan, input.EvaluationContext); err != nil {
				t.Fatalf("generated plan invalid: %v\nplan=%v", err, result.Plan)
			}
			selected := selectedPlannerEvidence(t, result.Evidence)
			if selected.GetPlanner() != test.wantKind || selected.GetActionType() != test.wantAction || selected.GetUtilityNanos() <= 0 || len(selected.GetInputs()) == 0 {
				t.Fatalf("selected evidence = %+v", selected)
			}
		})
	}
}

func TestFastPlannerGeneratesScaleInRelease(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	secondAllocation := proto.Clone(input.Snapshot.Allocations[0]).(*tgsrlv1.Allocation)
	secondAllocation.AllocationId = "allocation-b"
	secondAllocation.PendingUnitId = "unit-b"
	secondAllocation.RuntimeUnitId = "runtime-b"
	second := proto.Clone(input.Sandboxes[0]).(*tgsrlv1.Sandbox)
	second.SandboxId = "sandbox-b"
	second.Binding.BindingId = "binding-b"
	second.Binding.PendingUnitId = "unit-b"
	second.Binding.RuntimeUnitId = "runtime-b"
	second.Binding.SandboxId = "sandbox-b"
	input.Snapshot.Allocations = append(input.Snapshot.Allocations, secondAllocation)
	input.Sandboxes = append(input.Sandboxes, second)
	input.Intent.UnitCount = 1
	input.Directives.AllowScaleIn = true
	result, err := NewCoordinator(Config{}).Plan(input)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Plan == nil || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_RELEASE || result.Plan.GetActions()[0].GetTargetId() != "allocation-b" {
		t.Fatalf("scale-in plan = %+v", result.Plan)
	}
	if result.Plan.GetActions()[0].GetRollback().GetRestoreBinding() == nil {
		t.Fatal("release rollback lost binding before-image")
	}
}

func TestCoordinatorEvidenceIsDeterministicAndSelectedSurvivesBudget(t *testing.T) {
	share := .75
	priority := int32(11)
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Directives = Directives{TargetShare: &share, TargetPriority: &priority}
	coordinator := NewCoordinator(Config{PlannerEvidenceBudget: 1, PlannerPerTickActionBudget: 1})
	first, err := coordinator.Plan(input)
	if err != nil {
		t.Fatalf("first Plan() error = %v", err)
	}
	input.Sandboxes = append([]*tgsrlv1.Sandbox(nil), input.Sandboxes...)
	second, err := coordinator.Plan(input)
	if err != nil {
		t.Fatalf("second Plan() error = %v", err)
	}
	if first.TotalProposalCount != 2 || !first.EvidenceTruncated || len(first.Evidence) != 1 || first.Evidence[0].GetDisposition() != tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_SELECTED {
		t.Fatalf("bounded evidence = %+v total=%d truncated=%v", first.Evidence, first.TotalProposalCount, first.EvidenceTruncated)
	}
	firstRecord := &tgsrlv1.DecisionRecord{SelectedPlan: first.Plan, PlannerEvidence: first.Evidence, TotalPlannerProposalCount: first.TotalProposalCount, PlannerEvidenceTruncated: first.EvidenceTruncated}
	secondRecord := &tgsrlv1.DecisionRecord{SelectedPlan: second.Plan, PlannerEvidence: second.Evidence, TotalPlannerProposalCount: second.TotalProposalCount, PlannerEvidenceTruncated: second.EvidenceTruncated}
	marshal := proto.MarshalOptions{Deterministic: true}
	left, _ := marshal.Marshal(firstRecord)
	right, _ := marshal.Marshal(secondRecord)
	if !bytes.Equal(left, right) {
		t.Fatalf("same seed/input changed deterministic bytes\n%x\n%x", left, right)
	}
}

func TestCoordinatorRecordsTypedRejectionEvidence(t *testing.T) {
	share := 2.0
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Directives.TargetShare = &share
	result, err := NewCoordinator(Config{}).Plan(input)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Plan != nil || len(result.Evidence) != 1 {
		t.Fatalf("result = %+v", result)
	}
	evidence := result.Evidence[0]
	if evidence.GetDisposition() != tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED || evidence.GetReason() != "INVALID_TARGET_SHARE" || len(evidence.GetInputs()) == 0 {
		t.Fatalf("rejection evidence = %+v", evidence)
	}
}

func TestEvaluateAdaptiveBudgetsAdmissionActions(t *testing.T) {
	snapshot, intent := validFixture()
	context := &tgsrlv1.EvaluationContext{TickKind: tgsrlv1.TickKind_TICK_KIND_FAST, EvaluationTime: timestamppb.New(fixtureTime), DecisionSequence: 41, Cause: "test"}
	legacyPlan, legacyRecord, err := testScheduler(t, FallbackNoOp).EvaluateWithContext(snapshot, intent, context)
	if err != nil {
		t.Fatalf("EvaluateWithContext() error = %v", err)
	}
	adaptivePlan, adaptiveRecord, err := testScheduler(t, FallbackNoOp).EvaluateAdaptive(&AdaptiveEvaluationInput{Snapshot: snapshot, Intent: intent, EvaluationContext: context})
	if err != nil {
		t.Fatalf("EvaluateAdaptive() error = %v", err)
	}
	if len(legacyPlan.GetActions()) != 2 || len(adaptivePlan.GetActions()) != DefaultPlannerActionBudget {
		t.Fatalf("admission action counts legacy=%d adaptive=%d", len(legacyPlan.GetActions()), len(adaptivePlan.GetActions()))
	}
	if adaptivePlan.GetActions()[0].GetBinding().GetBindingId() != legacyPlan.GetActions()[0].GetBinding().GetBindingId() {
		t.Fatalf("adaptive admission changed the selected binding: legacy=%v adaptive=%v", legacyPlan, adaptivePlan)
	}
	if adaptiveRecord.GetScore() != legacyRecord.GetScore() || len(adaptiveRecord.GetCandidates()) != len(legacyRecord.GetCandidates()) || selectedPlannerEvidence(t, adaptiveRecord.GetPlannerEvidence()).GetPlanner() != tgsrlv1.PlannerKind_PLANNER_KIND_ADMISSION {
		t.Fatalf("adaptive admission record changed authoritative outcome: legacy=%v adaptive=%v", legacyRecord, adaptiveRecord)
	}
}

func TestEvaluateAdaptiveSelectsContractPauseOverAdmission(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Snapshot.PendingUnits = []*tgsrlv1.PendingUnit{{
		PendingUnitId: "unit-b", ExecutionId: input.Intent.GetExecutionId(), StageId: input.Intent.GetStageId(),
		IntentVersion: input.Intent.GetVersion(), JobId: input.Intent.GetJobId(),
		RequestedResources: cloneResources(input.Intent.GetResourcesPerUnit()), RequiredCapabilities: cloneCapabilities(input.Intent.GetRequiredCapabilities()),
	}}
	input.Intent.UnitCount = 2
	input.ContractAggregate = semantics.AggregateResult{Blocking: true, Action: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED}
	plan, record, err := testScheduler(t, FallbackNoOp).EvaluateAdaptive(&input)
	if err != nil {
		t.Fatalf("EvaluateAdaptive() error = %v", err)
	}
	if plan == nil || plan.GetPurpose() != tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION || len(plan.GetActions()) != 1 || plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_PAUSE {
		t.Fatalf("selected plan = %+v", plan)
	}
	if record.GetSelectedPlan() == nil || !proto.Equal(plan, record.GetSelectedPlan()) || record.GetFallback() {
		t.Fatalf("decision record = %+v", record)
	}
	if record.GetScore() != 0 {
		t.Fatalf("runtime mutation retained admission score %v", record.GetScore())
	}
}

func TestEvaluateAdaptiveNoDirectiveReturnsConvergedNonFallback(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	scheduler := testScheduler(t, FallbackNoOp)
	plan, record, err := scheduler.EvaluateAdaptive(&input)
	if err != nil {
		t.Fatalf("EvaluateAdaptive() error = %v", err)
	}
	if record.GetFallback() || record.GetFallbackReason() != "" {
		t.Fatalf("converged decision marked fallback: %+v", record)
	}
	if len(plan.GetActions()) != 0 || len(plan.GetBindings()) != 0 || len(plan.GetAffectedAllocationIds()) != 0 {
		t.Fatalf("converged plan authorizes mutation: %+v", plan)
	}
	if plan.GetPurpose() != tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION || plan.GetRollbackPolicy() != tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_NOT_REQUIRED {
		t.Fatalf("converged plan contract = %+v", plan)
	}
	if record.GetSelectedPlan() == nil || !proto.Equal(plan, record.GetSelectedPlan()) || record.GetTotalPlannerProposalCount() != 0 || len(record.GetPlannerEvidence()) != 1 || record.GetPlannerEvidence()[0].GetReason() != "CONVERGED_NO_TRIGGER" || record.GetPlannerEvidence()[0].GetDisposition() != tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_DEFERRED || record.GetPlannerEvidenceTruncated() {
		t.Fatalf("converged record = %+v", record)
	}
}

func TestEvaluateAdaptiveAlreadySatisfiedDirectiveIsConverged(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED)
	input.Directives.DesiredState = tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED
	plan, record, err := testScheduler(t, FallbackNoOp).EvaluateAdaptive(&input)
	if err != nil {
		t.Fatalf("EvaluateAdaptive() error = %v", err)
	}
	if record.GetFallback() || plan == nil || len(plan.GetActions()) != 0 {
		t.Fatalf("already-satisfied result plan=%+v record=%+v", plan, record)
	}
	if len(record.GetPlannerEvidence()) != 1 || record.GetPlannerEvidence()[0].GetReason() != "ALREADY_SATISFIED" {
		t.Fatalf("already-satisfied evidence = %+v", record.GetPlannerEvidence())
	}
}

func TestEvaluateAdaptiveRejectedProposalFallsBackWithEvidence(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	invalid := 2.0
	input.Directives.TargetShare = &invalid
	plan, record, err := testScheduler(t, FallbackNoOp).EvaluateAdaptive(&input)
	if err != nil {
		t.Fatalf("EvaluateAdaptive() error = %v", err)
	}
	if !record.GetFallback() || record.GetFallbackReason() != "NO_ELIGIBLE_PLANNER_PROPOSAL" || len(plan.GetActions()) != 0 {
		t.Fatalf("rejected result plan=%+v record=%+v", plan, record)
	}
	if record.GetTotalPlannerProposalCount() != 1 || len(record.GetPlannerEvidence()) != 1 || record.GetPlannerEvidence()[0].GetReason() != "INVALID_TARGET_SHARE" {
		t.Fatalf("rejected evidence = %+v", record.GetPlannerEvidence())
	}
}

func TestEvaluateAdaptiveMutationDoesNotMutateInputs(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Directives.AllowPause = true
	snapshotBefore := proto.Clone(input.Snapshot).(*tgsrlv1.ClusterSnapshot)
	intentBefore := proto.Clone(input.Intent).(*tgsrlv1.SchedulingIntent)
	contextBefore := proto.Clone(input.EvaluationContext).(*tgsrlv1.EvaluationContext)
	sandboxBefore := proto.Clone(input.Sandboxes[0]).(*tgsrlv1.Sandbox)

	plan, record, err := testScheduler(t, FallbackNoOp).EvaluateAdaptive(&input)
	if err != nil {
		t.Fatalf("EvaluateAdaptive() error = %v", err)
	}
	if plan == nil || len(plan.GetActions()) != 1 || plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_PAUSE || record.GetFallback() {
		t.Fatalf("adaptive mutation plan=%+v record=%+v", plan, record)
	}
	if !proto.Equal(input.Snapshot, snapshotBefore) || !proto.Equal(input.Intent, intentBefore) || !proto.Equal(input.EvaluationContext, contextBefore) || !proto.Equal(input.Sandboxes[0], sandboxBefore) {
		t.Fatal("EvaluateAdaptive mutated caller input")
	}
}

func TestEvaluateAdaptiveMutationIsByteStable(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Directives.AllowPause = true
	firstPlan, firstRecord, err := testScheduler(t, FallbackNoOp).EvaluateAdaptive(&input)
	if err != nil {
		t.Fatalf("first EvaluateAdaptive() error = %v", err)
	}
	secondPlan, secondRecord, err := testScheduler(t, FallbackNoOp).EvaluateAdaptive(&input)
	if err != nil {
		t.Fatalf("second EvaluateAdaptive() error = %v", err)
	}
	marshal := proto.MarshalOptions{Deterministic: true}
	firstPlanBytes, _ := marshal.Marshal(firstPlan)
	secondPlanBytes, _ := marshal.Marshal(secondPlan)
	firstRecordBytes, _ := marshal.Marshal(firstRecord)
	secondRecordBytes, _ := marshal.Marshal(secondRecord)
	if !bytes.Equal(firstPlanBytes, secondPlanBytes) || !bytes.Equal(firstRecordBytes, secondRecordBytes) {
		t.Fatalf("EvaluateAdaptive is not byte-stable for fixed explicit inputs")
	}
}

func TestParseCompatibilityDirectivesIsStrict(t *testing.T) {
	share := .75
	got, err := parseCompatibilityDirectives(map[string]string{
		compatPlannerTargetShare: "0.75", compatPlannerAllowOffload: "true", compatPlannerSleepAfter: "5m",
	})
	if err != nil {
		t.Fatalf("parseCompatibilityDirectives() error = %v", err)
	}
	if got.TargetShare == nil || *got.TargetShare != share || !got.AllowOffload || got.SleepAfter != 5*time.Minute {
		t.Fatalf("directives = %+v", got)
	}
	for _, labels := range []map[string]string{
		{compatPlannerTargetShare: "NaN"},
		{compatPlannerAllowOffload: "TRUE"},
		{compatPlannerSleepAfter: "0s"},
		{compatPlannerTargetState: "stopped"},
	} {
		if _, err := parseCompatibilityDirectives(labels); err == nil {
			t.Fatalf("malformed labels %+v unexpectedly accepted", labels)
		}
	}
}

func TestAdaptivePlannerDoesNotGeneratePreemption(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_SLOW, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Directives.AllowRebind = true
	input.Directives.ReplacementBindings = map[string]*tgsrlv1.Binding{"allocation-a": adaptiveReplacementBinding()}
	result, err := NewCoordinator(Config{}).Plan(input)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Plan == nil {
		t.Fatalf("Plan() produced no ordinary mutation: %+v", result)
	}
	if result.Plan.GetPurpose() == tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION {
		t.Fatalf("adaptive planner generated preemption: %+v", result.Plan)
	}
	for _, action := range result.Plan.GetActions() {
		if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND || action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_RELEASE {
			t.Fatalf("unexpected replacement/preemption action: %+v", action)
		}
	}
}

func TestWrongTickDirectiveRecordsActionPurpose(t *testing.T) {
	tests := []struct {
		name        string
		tick        tgsrlv1.TickKind
		planner     Planner
		directives  Directives
		action      tgsrlv1.ActionType
		wantPurpose tgsrlv1.PlanPurpose
	}{
		{name: "resize", tick: tgsrlv1.TickKind_TICK_KIND_MEDIUM, planner: FastMutationPlanner{}, directives: Directives{AllowResize: true}, action: tgsrlv1.ActionType_ACTION_TYPE_RESIZE, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE},
		{name: "rebind", tick: tgsrlv1.TickKind_TICK_KIND_FAST, planner: SlowReconfigurationPlanner{}, directives: Directives{AllowRebind: true}, action: tgsrlv1.ActionType_ACTION_TYPE_REBIND, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE},
		{name: "resume", tick: tgsrlv1.TickKind_TICK_KIND_FAST, planner: MediumLifecyclePlanner{}, directives: Directives{AllowResume: true}, action: tgsrlv1.ActionType_ACTION_TYPE_RESUME, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY},
		{name: "recreate", tick: tgsrlv1.TickKind_TICK_KIND_FAST, planner: SlowReconfigurationPlanner{}, directives: Directives{AllowRecreate: true}, action: tgsrlv1.ActionType_ACTION_TYPE_RECREATE, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY},
		{name: "pause remains reconciliation", tick: tgsrlv1.TickKind_TICK_KIND_FAST, planner: MediumLifecyclePlanner{}, directives: Directives{AllowPause: true}, action: tgsrlv1.ActionType_ACTION_TYPE_PAUSE, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION},
		{name: "release remains reconciliation", tick: tgsrlv1.TickKind_TICK_KIND_MEDIUM, planner: FastMutationPlanner{}, directives: Directives{AllowScaleIn: true}, action: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, wantPurpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := adaptiveFixture(test.tick, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
			input.Directives = test.directives
			proposals := test.planner.Propose(input)
			if len(proposals) != 1 || proposals[0].Plan != nil || proposals[0].Evidence.GetDisposition() != tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_DEFERRED || proposals[0].Evidence.GetReason() != "TICK_NOT_ELIGIBLE" {
				t.Fatalf("wrong-tick proposals = %+v", proposals)
			}
			if got := proposals[0].Evidence.GetActionType(); got != test.action {
				t.Fatalf("wrong-tick action = %s, want %s", got, test.action)
			}
			if got := proposals[0].Evidence.GetPurpose(); got != test.wantPurpose {
				t.Fatalf("wrong-tick purpose = %s, want %s", got, test.wantPurpose)
			}
		})
	}
}

func TestSlowPlannerCompletesExplicitReplacementBindingIdentity(t *testing.T) {
	tests := []struct {
		name       string
		state      tgsrlv1.RuntimeState
		action     tgsrlv1.ActionType
		deviceID   string
		bindingID  string
		directives Directives
	}{
		{name: "rebind", state: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, action: tgsrlv1.ActionType_ACTION_TYPE_REBIND, deviceID: "device-b", directives: Directives{AllowRebind: true}},
		{name: "recreate", state: tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED, action: tgsrlv1.ActionType_ACTION_TYPE_RECREATE, deviceID: "device-a", bindingID: "caller-supplied-id", directives: Directives{AllowRecreate: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_SLOW, test.state)
			override := &tgsrlv1.Binding{BindingId: test.bindingID, DeviceIds: []string{test.deviceID}, Resources: cloneResources(input.Snapshot.GetAllocations()[0].GetResources())}
			test.directives.ReplacementBindings = map[string]*tgsrlv1.Binding{"allocation-a": override}
			input.Directives = test.directives

			first, err := NewCoordinator(Config{}).Plan(input)
			if err != nil || first.Plan == nil || len(first.Plan.GetActions()) != 1 {
				t.Fatalf("first Plan() result=%+v err=%v", first, err)
			}
			second, err := NewCoordinator(Config{}).Plan(input)
			if err != nil || second.Plan == nil || len(second.Plan.GetActions()) != 1 {
				t.Fatalf("second Plan() result=%+v err=%v", second, err)
			}

			action := first.Plan.GetActions()[0]
			binding := action.GetBinding()
			current := input.Sandboxes[0].GetBinding()
			allocation := input.Snapshot.GetAllocations()[0]
			if action.GetActionType() != test.action || binding == nil {
				t.Fatalf("replacement action = %+v, want %s with binding", action, test.action)
			}
			if len(first.Plan.GetBindings()) != 1 || !proto.Equal(first.Plan.GetBindings()[0], binding) {
				t.Fatalf("replacement desired-state binding = %+v, want action binding", first.Plan.GetBindings())
			}
			if binding.GetBindingId() == "" || binding.GetBindingId() == current.GetBindingId() || binding.GetBindingId() == override.GetBindingId() {
				t.Fatalf("replacement binding_id = %q, current=%q caller=%q", binding.GetBindingId(), current.GetBindingId(), override.GetBindingId())
			}
			if binding.GetBindingId() != second.Plan.GetActions()[0].GetBinding().GetBindingId() {
				t.Fatalf("replacement binding_id is not stable: first=%q second=%q", binding.GetBindingId(), second.Plan.GetActions()[0].GetBinding().GetBindingId())
			}
			if binding.GetPendingUnitId() != allocation.GetPendingUnitId() || binding.GetSandboxId() != input.Sandboxes[0].GetSandboxId() || binding.GetRuntimeUnitId() != allocation.GetRuntimeUnitId() || binding.GetGeneration() != input.Sandboxes[0].GetGeneration()+1 {
				t.Fatalf("replacement identity incomplete or mismatched: %+v", binding)
			}
			if len(binding.GetDeviceIds()) != 1 || binding.GetDeviceIds()[0] != test.deviceID || !proto.Equal(binding.GetResources(), override.GetResources()) {
				t.Fatalf("replacement overrides not applied: %+v", binding)
			}
			if override.GetBindingId() != test.bindingID || override.GetPendingUnitId() != "" || override.GetSandboxId() != "" || override.GetRuntimeUnitId() != "" || override.GetGeneration() != 0 {
				t.Fatalf("planner mutated caller replacement: %+v", override)
			}
		})
	}
}

func TestSlowPlannerRejectsConflictingOrMissingReplacementIdentity(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*PlanningInput, *tgsrlv1.Binding)
		wantReason string
	}{
		{name: "directive identity conflicts", mutate: func(_ *PlanningInput, replacement *tgsrlv1.Binding) { replacement.PendingUnitId = "other-unit" }, wantReason: "REPLACEMENT_IDENTITY_MISMATCH"},
		{name: "current binding id missing", mutate: func(input *PlanningInput, _ *tgsrlv1.Binding) { input.Sandboxes[0].Binding.BindingId = "" }, wantReason: "INVALID_CURRENT_BINDING_IDENTITY"},
		{name: "resource override conflicts with allocation", mutate: func(_ *PlanningInput, replacement *tgsrlv1.Binding) { replacement.Resources.CpuMillis++ }, wantReason: "REPLACEMENT_RESOURCE_MISMATCH"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_SLOW, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
			replacement := &tgsrlv1.Binding{DeviceIds: []string{"device-b"}, Resources: cloneResources(input.Snapshot.GetAllocations()[0].GetResources())}
			input.Directives = Directives{AllowRebind: true, ReplacementBindings: map[string]*tgsrlv1.Binding{"allocation-a": replacement}}
			test.mutate(&input, replacement)

			result, err := NewCoordinator(Config{}).Plan(input)
			if err != nil {
				t.Fatalf("Plan() error = %v", err)
			}
			if result.Plan != nil || len(result.Evidence) != 1 || result.Evidence[0].GetReason() != test.wantReason || result.Evidence[0].GetDisposition() != tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED {
				t.Fatalf("result=%+v, want rejected %s evidence", result, test.wantReason)
			}
		})
	}
}

func TestFastPlannerAutoTriggersSharePriorityResizeAndScaleIn(t *testing.T) {
	t.Run("share", func(t *testing.T) {
		input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
		result, err := NewCoordinator(Config{}).Plan(input)
		if err != nil || result.Plan == nil || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE {
			t.Fatalf("auto share result=%+v err=%v", result, err)
		}
	})
	t.Run("priority", func(t *testing.T) {
		input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
		input.Sandboxes[0].Share = input.Intent.GetResourcesPerUnit().GetAcceleratorUnits()
		result, err := NewCoordinator(Config{}).Plan(input)
		if err != nil || result.Plan == nil || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY {
			t.Fatalf("auto priority result=%+v err=%v", result, err)
		}
	})
	t.Run("resize", func(t *testing.T) {
		input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
		input.Sandboxes[0].Share = input.Intent.GetResourcesPerUnit().GetAcceleratorUnits()
		input.Sandboxes[0].Priority = input.Intent.GetPriority()
		input.Snapshot.Allocations[0].Resources.CpuMillis = 50
		result, err := NewCoordinator(Config{}).Plan(input)
		if err != nil || result.Plan == nil || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_RESIZE {
			t.Fatalf("auto resize result=%+v err=%v", result, err)
		}
	})
	t.Run("scale in one deterministic victim per tick", func(t *testing.T) {
		input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
		input.Sandboxes[0].Share = input.Intent.GetResourcesPerUnit().GetAcceleratorUnits()
		input.Sandboxes[0].Priority = input.Intent.GetPriority()
		for _, suffix := range []string{"b", "c"} {
			allocation := proto.Clone(input.Snapshot.Allocations[0]).(*tgsrlv1.Allocation)
			allocation.AllocationId = "allocation-" + suffix
			allocation.PendingUnitId = "unit-" + suffix
			allocation.RuntimeUnitId = "runtime-" + suffix
			sandbox := proto.Clone(input.Sandboxes[0]).(*tgsrlv1.Sandbox)
			sandbox.SandboxId = "sandbox-" + suffix
			sandbox.Binding.BindingId = "binding-" + suffix
			sandbox.Binding.PendingUnitId = allocation.PendingUnitId
			sandbox.Binding.RuntimeUnitId = allocation.RuntimeUnitId
			sandbox.Binding.SandboxId = sandbox.SandboxId
			input.Snapshot.Allocations = append(input.Snapshot.Allocations, allocation)
			input.Sandboxes = append(input.Sandboxes, sandbox)
		}
		result, err := NewCoordinator(Config{}).Plan(input)
		if err != nil || result.Plan == nil || len(result.Plan.GetActions()) != 1 {
			t.Fatalf("auto scale-in result=%+v err=%v", result, err)
		}
		action := result.Plan.GetActions()[0]
		if action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_RELEASE || action.GetTargetId() != "allocation-c" {
			t.Fatalf("scale-in action=%+v, want deterministic allocation-c release", action)
		}
	})
}

func TestMediumPlannerAutoTriggersContractIdleAndRecentResume(t *testing.T) {
	t.Run("contract pause", func(t *testing.T) {
		input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
		input.ContractAggregate = semantics.AggregateResult{Blocking: true, Action: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED}
		result, err := NewCoordinator(Config{}).Plan(input)
		if err != nil || result.Plan == nil || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_PAUSE {
			t.Fatalf("contract pause result=%+v err=%v", result, err)
		}
	})
	t.Run("idle sleep", func(t *testing.T) {
		input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
		input.Intent.PhaseKind = tgsrlv1.PhaseKind_PHASE_KIND_IDLE
		input.Sandboxes[0].StateChangedAt = timestamppb.New(fixtureTime.Add(-10 * time.Minute))
		result, err := NewCoordinator(Config{}).Plan(input)
		if err != nil || result.Plan == nil || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_SLEEP {
			t.Fatalf("idle result=%+v err=%v", result, err)
		}
	})
	t.Run("idle offload threshold", func(t *testing.T) {
		input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
		input.Intent.PhaseKind = tgsrlv1.PhaseKind_PHASE_KIND_IDLE
		input.Sandboxes[0].StateChangedAt = timestamppb.New(fixtureTime.Add(-45 * time.Minute))
		result, err := NewCoordinator(Config{}).Plan(input)
		if err != nil || result.Plan == nil || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD {
			t.Fatalf("idle offload result=%+v err=%v", result, err)
		}
		if selectedPlannerEvidence(t, result.Evidence).GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD {
			t.Fatalf("idle offload evidence=%+v", result.Evidence)
		}
	})
	t.Run("recent resume", func(t *testing.T) {
		input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED)
		input.RecentDecisions = []*tgsrlv1.DecisionRecord{{
			SelectedPlan:  &tgsrlv1.PlacementPlan{Actions: []*tgsrlv1.Action{{ActionId: "pause-a", ActionType: tgsrlv1.ActionType_ACTION_TYPE_PAUSE, TargetId: "allocation-a", ExpectedGeneration: 3}}},
			ActionResults: []*tgsrlv1.ActionResult{{ActionId: "pause-a", Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED}},
		}}
		result, err := NewCoordinator(Config{}).Plan(input)
		if err != nil || result.Plan == nil || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_RESUME {
			t.Fatalf("resume result=%+v err=%v", result, err)
		}
	})
}

func TestSlowPlannerAutoTriggersRebindAndRecreate(t *testing.T) {
	t.Run("unhealthy rebind", func(t *testing.T) {
		input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_SLOW, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
		input.Snapshot.Devices[1].Health = tgsrlv1.DeviceHealth_DEVICE_HEALTH_UNAVAILABLE
		result, err := NewCoordinator(Config{}).Plan(input)
		if err != nil || result.Plan == nil || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_REBIND {
			t.Fatalf("rebind result=%+v err=%v", result, err)
		}
		if result.Plan.GetPurpose() != tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY {
			t.Fatalf("unhealthy-device rebind purpose = %s, want recovery", result.Plan.GetPurpose())
		}
	})
	t.Run("failed recreate", func(t *testing.T) {
		input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_SLOW, tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED)
		result, err := NewCoordinator(Config{}).Plan(input)
		if err != nil || result.Plan == nil || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_RECREATE {
			t.Fatalf("recreate result=%+v err=%v", result, err)
		}
	})
}

func TestPlanningSignalsDeriveOnlyFromStructuredEvidence(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Intent.ExecutionContract.BackpressurePolicy.LowWatermark = 4
	input.Intent.ExecutionContract.BackpressurePolicy.HighWatermark = 8
	input.Intent.ExecutionContract.BackpressurePolicy.MaximumBufferLevel = 12
	input.EvaluationContext.ContractObservation = &tgsrlv1.ContractObservation{
		BufferLevel:              proto.Uint64(9),
		PolicyLag:                proto.Uint64(3),
		SampleStale:              proto.Bool(true),
		EffectiveSampleSizeRatio: proto.Float64(.5),
		TypedFacts: []*tgsrlv1.SemanticField{
			semanticBoolField(factCheckpointCapable, true),
			semanticIntField(factRecoveryCostNanos, 25),
			semanticIntField(factMinimumResidencyNano, int64(30*time.Second)),
		},
	}
	input.Sandboxes[0].SemanticContext = &tgsrlv1.SemanticEnvelope{TypedFields: []*tgsrlv1.SemanticField{semanticBoolField(factActionInFlight, false)}}

	if err := normalizePlanningSignals(&input, DefaultPlannerActionBudget); err != nil {
		t.Fatalf("normalizePlanningSignals() error = %v", err)
	}
	got := input.Signals
	if !got.BufferLevelPresent || got.BufferLevel != 9 || !got.BufferPressurePresent || got.BufferPressure != BufferPressureHigh || !got.PolicyLagPresent || got.PolicyLag != 3 || !got.SampleStalenessPresent || !got.SampleStaleness || !got.ESSRatioPresent || got.ESSRatio != .5 {
		t.Fatalf("observation signals = %+v", got)
	}
	if !got.CheckpointCapablePresent || !got.CheckpointCapable || !got.BufferBelowWatermarkPresent || got.BufferBelowWatermark || !got.RecoveryCostNanosPresent || got.RecoveryCostNanos != 25 || !got.MinimumResidencyPresent || got.MinimumResidency != 30*time.Second || !got.ActionInFlightPresent || got.ActionInFlight || got.PerTickActionBudget != DefaultPlannerActionBudget {
		t.Fatalf("derived signals = %+v", got)
	}
}

func TestFastSignalsAdjustUtilityWithoutReward(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.EvaluationContext.ContractObservation = &tgsrlv1.ContractObservation{PolicyLag: proto.Uint64(2), SampleStale: proto.Bool(true), EffectiveSampleSizeRatio: proto.Float64(.25)}
	input.Intent.SemanticContext = &tgsrlv1.SemanticEnvelope{TypedFields: []*tgsrlv1.SemanticField{semanticDoubleField("reward", 1e12)}}
	result, err := NewCoordinator(Config{PlannerPerTickActionBudget: 1}).Plan(input)
	if err != nil || result.Plan == nil {
		t.Fatalf("Plan() result=%+v err=%v", result, err)
	}
	evidence := selectedPlannerEvidence(t, result.Evidence)
	want := DefaultPlannerUtilityConfig().SetShare + 3*DefaultPlannerUtilityConfig().FastSignalStep
	if evidence.GetUtilityNanos() != want {
		t.Fatalf("utility=%d, want %d; reward must not participate", evidence.GetUtilityNanos(), want)
	}
	if !hasSemanticInput(evidence, "planner.directional.reason") || !hasSemanticInput(evidence, "planner.directional.delta") {
		t.Fatalf("directional target evidence=%+v", evidence.GetInputs())
	}
}

func TestDirectionalTargetsRespectProducerConsumerSemantics(t *testing.T) {
	tests := []struct {
		name       string
		phase      tgsrlv1.PhaseKind
		signals    PlanningSignals
		wantTarget float64
		wantReason string
	}{
		{name: "producer pressure reduces share", phase: tgsrlv1.PhaseKind_PHASE_KIND_DECODE, signals: PlanningSignals{BufferPressure: BufferPressureHigh, BufferPressurePresent: true}, wantTarget: .4, wantReason: "BUFFER_PRESSURE_REDUCE_PRODUCTION"},
		{name: "consumer pressure increases share", phase: tgsrlv1.PhaseKind_PHASE_KIND_OPTIMIZER, signals: PlanningSignals{BufferPressure: BufferPressureHigh, BufferPressurePresent: true}, wantTarget: .6, wantReason: "BUFFER_PRESSURE_INCREASE_CONSUMPTION"},
		{name: "producer policy lag reduces share", phase: tgsrlv1.PhaseKind_PHASE_KIND_DECODE, signals: PlanningSignals{PolicyLag: DefaultPlannerPolicyLagLimit + 1, PolicyLagPresent: true}, wantTarget: .4, wantReason: "POLICY_LAG_REDUCE_PRODUCTION"},
		{name: "consumer policy lag increases share", phase: tgsrlv1.PhaseKind_PHASE_KIND_ACTOR, signals: PlanningSignals{PolicyLag: DefaultPlannerPolicyLagLimit + 1, PolicyLagPresent: true}, wantTarget: .6, wantReason: "POLICY_LAG_INCREASE_CONSUMPTION"},
		{name: "staleness only reduces producer", phase: tgsrlv1.PhaseKind_PHASE_KIND_DECODE, signals: PlanningSignals{SampleStaleness: true, SampleStalenessPresent: true}, wantTarget: .4, wantReason: "SAMPLE_STALENESS_REDUCE_PRODUCTION"},
		{name: "low ESS only reduces producer", phase: tgsrlv1.PhaseKind_PHASE_KIND_DECODE, signals: PlanningSignals{ESSRatio: .25, ESSRatioPresent: true}, wantTarget: .4, wantReason: "LOW_ESS_REDUCE_PRODUCTION"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
			input.Intent.PhaseKind = test.phase
			input.Signals = test.signals
			target := generateDirectionalShareTarget(input, .5, "allocation-a")
			if target == nil || math.Abs(target.TargetValue-test.wantTarget) > 1e-9 || target.Reason != test.wantReason {
				t.Fatalf("target = %+v, want value=%v reason=%s", target, test.wantTarget, test.wantReason)
			}
		})
	}
}

func TestDirectionalTargetsAreBoundedAndMonotonic(t *testing.T) {
	producer := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	producer.Intent.PhaseKind = tgsrlv1.PhaseKind_PHASE_KIND_DECODE
	producer.Signals = PlanningSignals{BufferPressure: BufferPressureCritical, BufferPressurePresent: true}
	for _, current := range []float64{0, .01, .5, 1} {
		target := generateDirectionalShareTarget(producer, current, "allocation-a")
		if current <= DefaultPlannerShareHysteresis {
			if target != nil {
				t.Fatalf("boundary target = %+v for current=%v", target, current)
			}
			continue
		}
		if target == nil || target.TargetValue < 0 || target.TargetValue > 1 || target.TargetValue >= current {
			t.Fatalf("producer target = %+v for current=%v", target, current)
		}
	}
	consumer := producer
	consumer.Intent = proto.Clone(producer.Intent).(*tgsrlv1.SchedulingIntent)
	consumer.Intent.PhaseKind = tgsrlv1.PhaseKind_PHASE_KIND_OPTIMIZER
	for _, current := range []float64{0, .5, .99, 1} {
		target := generateDirectionalShareTarget(consumer, current, "allocation-a")
		if current >= 1-DefaultPlannerShareHysteresis {
			if target != nil {
				t.Fatalf("boundary target = %+v for current=%v", target, current)
			}
			continue
		}
		if target == nil || target.TargetValue < current || target.TargetValue > 1 {
			t.Fatalf("consumer target = %+v for current=%v", target, current)
		}
	}
}

func TestESSDoesNotIncreaseUnrelatedActionUtility(t *testing.T) {
	priority := int32(17)
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Directives.TargetPriority = &priority
	input.Signals = PlanningSignals{ESSRatio: .1, ESSRatioPresent: true}
	result, err := NewCoordinator(Config{}).Plan(input)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	evidence := selectedPlannerEvidence(t, result.Evidence)
	if evidence.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY || evidence.GetUtilityNanos() != DefaultPlannerUtilityConfig().SetPriority {
		t.Fatalf("priority evidence = %+v", evidence)
	}
}

func TestDirectionalTargetIsDeterministic(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Intent.PhaseKind = tgsrlv1.PhaseKind_PHASE_KIND_DECODE
	input.Signals = PlanningSignals{PolicyLag: 7, PolicyLagPresent: true, SampleStaleness: true, SampleStalenessPresent: true}
	first, err := NewCoordinator(Config{}).Plan(input)
	if err != nil {
		t.Fatalf("first Plan() error = %v", err)
	}
	second, err := NewCoordinator(Config{}).Plan(input)
	if err != nil {
		t.Fatalf("second Plan() error = %v", err)
	}
	marshal := proto.MarshalOptions{Deterministic: true}
	left, _ := marshal.Marshal(first.Plan)
	right, _ := marshal.Marshal(second.Plan)
	if !bytes.Equal(left, right) {
		t.Fatalf("directional plan changed for identical input\n%x\n%x", left, right)
	}
}

func TestDirectionalTargetWaitsForObservationWindow(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Intent.PhaseKind = tgsrlv1.PhaseKind_PHASE_KIND_DECODE
	input.Signals = PlanningSignals{BufferPressure: BufferPressureHigh, BufferPressurePresent: true}
	input.Sandboxes[0].Priority = input.Intent.GetPriority()
	input.Snapshot.Allocations[0].Resources = cloneResources(input.Intent.GetResourcesPerUnit())
	input.RecentDecisions = []*tgsrlv1.DecisionRecord{{
		DecidedAt: timestamppb.New(fixtureTime.Add(-time.Second)),
		PlannerEvidence: []*tgsrlv1.PlannerEvidence{{
			ActionType:  tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE,
			TargetId:    "allocation-a",
			Disposition: tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_SELECTED,
			Inputs:      []*tgsrlv1.SemanticField{semanticStringField("planner.directional.reason", "BUFFER_PRESSURE_REDUCE_PRODUCTION")},
		}},
	}}
	if target := generateDirectionalShareTarget(input, .5, "allocation-a"); target != nil {
		t.Fatalf("target escaped observation window: %+v", target)
	}
	result, err := NewCoordinator(Config{}).Plan(input)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Plan != nil {
		t.Fatalf("intent reconciliation bypassed observation window: %+v", result.Plan)
	}
	input.RecentDecisions[0].DecidedAt = timestamppb.New(fixtureTime.Add(-DefaultPlannerObservationWindow))
	if target := generateDirectionalShareTarget(input, .5, "allocation-a"); target == nil {
		t.Fatal("target remained blocked after observation window")
	}
}

func TestConsumerQualitySignalsDoNotGenerateOrRewardUnrelatedActions(t *testing.T) {
	priority := int32(17)
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Intent.PhaseKind = tgsrlv1.PhaseKind_PHASE_KIND_OPTIMIZER
	input.Directives.TargetPriority = &priority
	input.Signals = PlanningSignals{ESSRatio: .1, ESSRatioPresent: true, SampleStaleness: true, SampleStalenessPresent: true}
	result, err := NewCoordinator(Config{}).Plan(input)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Plan == nil || len(result.Plan.GetActions()) != 1 || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY {
		t.Fatalf("consumer quality signal generated unrelated action: %+v", result.Plan)
	}
	if selectedPlannerEvidence(t, result.Evidence).GetUtilityNanos() != DefaultPlannerUtilityConfig().SetPriority {
		t.Fatalf("consumer quality signal changed priority utility: %+v", result.Evidence)
	}
}

func TestMediumPlannerGuardsStructuredSignals(t *testing.T) {
	tests := []struct {
		name       string
		signals    PlanningSignals
		wantReason string
	}{
		{name: "all missing", signals: PlanningSignals{}, wantReason: "CHECKPOINT_CAPABILITY_UNKNOWN"},
		{name: "missing checkpoint", signals: PlanningSignals{ActionInFlightPresent: true, BufferBelowWatermark: true, BufferBelowWatermarkPresent: true, MinimumResidencyPresent: true}, wantReason: "CHECKPOINT_CAPABILITY_UNKNOWN"},
		{name: "checkpoint disabled", signals: PlanningSignals{ActionInFlightPresent: true, CheckpointCapablePresent: true, BufferBelowWatermark: true, BufferBelowWatermarkPresent: true, MinimumResidencyPresent: true}, wantReason: "CHECKPOINT_CAPABILITY_REQUIRED"},
		{name: "watermark unknown", signals: PlanningSignals{ActionInFlightPresent: true, CheckpointCapable: true, CheckpointCapablePresent: true, MinimumResidencyPresent: true}, wantReason: "BUFFER_WATERMARK_UNKNOWN"},
		{name: "above watermark", signals: PlanningSignals{ActionInFlightPresent: true, CheckpointCapable: true, CheckpointCapablePresent: true, BufferBelowWatermarkPresent: true, MinimumResidencyPresent: true}, wantReason: "BUFFER_ABOVE_WATERMARK"},
		{name: "residency", signals: PlanningSignals{ActionInFlightPresent: true, CheckpointCapable: true, CheckpointCapablePresent: true, BufferBelowWatermark: true, BufferBelowWatermarkPresent: true, MinimumResidency: 2 * time.Minute, MinimumResidencyPresent: true}, wantReason: "MINIMUM_RESIDENCY_NOT_MET"},
		{name: "in flight", signals: PlanningSignals{CheckpointCapable: true, CheckpointCapablePresent: true, BufferBelowWatermark: true, BufferBelowWatermarkPresent: true, MinimumResidencyPresent: true, ActionInFlight: true, ActionInFlightPresent: true}, wantReason: "ACTION_IN_FLIGHT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
			input.Directives.AllowSleep = true
			input.Signals = test.signals
			result, err := NewCoordinator(Config{}).Plan(input)
			if err != nil || result.Plan != nil || len(result.Evidence) != 1 || result.Evidence[0].GetReason() != test.wantReason {
				t.Fatalf("result=%+v err=%v, want %s", result, err, test.wantReason)
			}
		})
	}
}

func TestAdaptiveTiersRejectStaleSandboxObservations(t *testing.T) {
	tests := []struct {
		name       string
		tick       tgsrlv1.TickKind
		state      tgsrlv1.RuntimeState
		configure  func(*PlanningInput)
		wantReason string
	}{
		{name: "fast stale", tick: tgsrlv1.TickKind_TICK_KIND_FAST, state: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, configure: func(input *PlanningInput) {
			input.Directives.TargetShare = proto.Float64(.75)
			input.Sandboxes[0].LastConfirmedAt = timestamppb.New(fixtureTime.Add(-time.Minute - time.Nanosecond))
		}, wantReason: "STALE_SANDBOX_OBSERVATION"},
		{name: "medium missing timestamp", tick: tgsrlv1.TickKind_TICK_KIND_MEDIUM, state: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, configure: func(input *PlanningInput) {
			input.Directives.AllowSleep = true
			input.Sandboxes[0].ObservedAt = nil
			input.Sandboxes[0].LastConfirmedAt = nil
		}, wantReason: "MISSING_SANDBOX_CONFIRMATION_TIME"},
		{name: "slow future timestamp", tick: tgsrlv1.TickKind_TICK_KIND_SLOW, state: tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED, configure: func(input *PlanningInput) {
			input.Sandboxes[0].LastConfirmedAt = timestamppb.New(fixtureTime.Add(time.Second + time.Nanosecond))
		}, wantReason: "SANDBOX_OBSERVATION_FROM_FUTURE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := adaptiveFixture(test.tick, test.state)
			test.configure(&input)
			result, err := NewCoordinator(Config{PlannerSandboxMaximumAge: time.Minute}).Plan(input)
			if err != nil || result.Plan != nil || len(result.Evidence) != 1 || result.Evidence[0].GetReason() != test.wantReason {
				t.Fatalf("result=%+v err=%v, want %s", result, err, test.wantReason)
			}
		})
	}
}

func TestAdaptiveFreshnessUsesTierDefaultsAndInclusiveBoundary(t *testing.T) {
	share := .75
	for _, test := range []struct {
		name       string
		tick       tgsrlv1.TickKind
		maximumAge time.Duration
		state      tgsrlv1.RuntimeState
		configure  func(*PlanningInput)
	}{
		{name: "fast", tick: tgsrlv1.TickKind_TICK_KIND_FAST, maximumAge: 5 * time.Second, state: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, configure: func(input *PlanningInput) { input.Directives.TargetShare = &share }},
		{name: "medium", tick: tgsrlv1.TickKind_TICK_KIND_MEDIUM, maximumAge: 30 * time.Second, state: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, configure: func(input *PlanningInput) { input.Directives.AllowPause = true }},
		{name: "slow", tick: tgsrlv1.TickKind_TICK_KIND_SLOW, maximumAge: 2 * time.Minute, state: tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED, configure: func(input *PlanningInput) {
			input.Directives.AllowRecreate = true
			input.Directives.ReplacementBindings = map[string]*tgsrlv1.Binding{"allocation-a": adaptiveReplacementBindingOn("device-a")}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, delta := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
				input := adaptiveFixture(test.tick, test.state)
				test.configure(&input)
				input.Sandboxes[0].LastConfirmedAt = timestamppb.New(fixtureTime.Add(-test.maximumAge - delta))
				result, err := NewCoordinator(Config{}).Plan(input)
				if err != nil {
					t.Fatalf("delta %v: Plan() error = %v", delta, err)
				}
				wantPlan := delta <= 0
				if (result.Plan != nil) != wantPlan {
					t.Fatalf("delta %v: plan=%v evidence=%+v, want plan=%v", delta, result.Plan, result.Evidence, wantPlan)
				}
			}
		})
	}
}

func TestAdaptiveFreshnessAllowsOneSecondFutureSkew(t *testing.T) {
	share := .75
	for _, offset := range []time.Duration{time.Second, time.Second + time.Nanosecond} {
		input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
		input.Directives.TargetShare = &share
		input.Sandboxes[0].LastConfirmedAt = timestamppb.New(fixtureTime.Add(offset))
		result, err := NewCoordinator(Config{}).Plan(input)
		if err != nil {
			t.Fatalf("offset %v: Plan() error = %v", offset, err)
		}
		if offset == time.Second && result.Plan == nil {
			t.Fatalf("offset %v rejected: %+v", offset, result.Evidence)
		}
		if offset > time.Second && (result.Plan != nil || len(result.Evidence) != 1 || result.Evidence[0].GetReason() != "SANDBOX_OBSERVATION_FROM_FUTURE") {
			t.Fatalf("offset %v result=%+v, want future rejection", offset, result)
		}
	}
}

func TestAdaptiveIdleAndResidencyUseStateChangedAt(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Directives.AllowSleep = true
	input.Directives.SleepAfter = 5 * time.Minute
	input.Signals.MinimumResidency = 5 * time.Minute
	input.Sandboxes[0].ObservedAt = timestamppb.New(fixtureTime.Add(-time.Second))
	input.Sandboxes[0].LastConfirmedAt = timestamppb.New(fixtureTime)
	input.Sandboxes[0].StateChangedAt = timestamppb.New(fixtureTime.Add(-5 * time.Minute))
	result, err := NewCoordinator(Config{}).Plan(input)
	if err != nil || result.Plan == nil {
		t.Fatalf("Plan() result=%+v err=%v, want eligible from state_changed_at", result, err)
	}
}

func TestSlowPlannerUsesBenefitCostRiskFixedPointUtility(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_SLOW, tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED)
	input.Signals = PlanningSignals{CheckpointCapable: true, CheckpointCapablePresent: true, RecoveryCostNanos: 300_000_000, RecoveryCostNanosPresent: true}
	result, err := NewCoordinator(Config{}).Plan(input)
	if err != nil || result.Plan == nil {
		t.Fatalf("Plan() result=%+v err=%v", result, err)
	}
	evidence := selectedPlannerEvidence(t, result.Evidence)
	wantBenefit := DefaultPlannerUtilityConfig().Recreate + DefaultPlannerUtilityConfig().SlowBenefit
	wantUtility := wantBenefit - input.Signals.RecoveryCostNanos
	if evidence.GetUtilityNanos() != wantUtility {
		t.Fatalf("utility=%d, want %d", evidence.GetUtilityNanos(), wantUtility)
	}
	for key, want := range map[string]int64{
		"planner.benefit_base_nanos":         wantBenefit,
		"planner.cost_recovery_nanos":        input.Signals.RecoveryCostNanos,
		"planner.risk_reconfiguration_nanos": 0,
	} {
		if got, ok := semanticIntInput(evidence, key); !ok || got != want {
			t.Fatalf("%s=%d present=%v, want %d; inputs=%+v", key, got, ok, want, evidence.GetInputs())
		}
	}
}

func TestSlowPlannerUsesTargetSpecificRecoveryCost(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_SLOW, tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED)
	input.Signals = PlanningSignals{
		CheckpointCapable: true, CheckpointCapablePresent: true,
		RecoveryCostNanos: 900_000_000, RecoveryCostNanosPresent: true,
		RecoveryCostNanosByTarget: map[string]int64{"allocation-a": 100_000_000},
	}
	result, err := NewCoordinator(Config{}).Plan(input)
	if err != nil || result.Plan == nil {
		t.Fatalf("Plan() result=%+v err=%v", result, err)
	}
	evidence := selectedPlannerEvidence(t, result.Evidence)
	if got, ok := semanticIntInput(evidence, "planner.cost_recovery_nanos"); !ok || got != 100_000_000 {
		t.Fatalf("target recovery cost=%d present=%v evidence=%+v", got, ok, evidence.GetInputs())
	}
}

func TestPerTickActionBudgetDefersLowerUtilityProposals(t *testing.T) {
	share := .75
	priority := int32(17)
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Directives = Directives{TargetShare: &share, TargetPriority: &priority}
	result, err := NewCoordinator(Config{PlannerPerTickActionBudget: 1}).Plan(input)
	if err != nil || result.Plan == nil || len(result.Plan.GetActions()) != 1 || result.TotalProposalCount != 2 {
		t.Fatalf("Plan() result=%+v err=%v", result, err)
	}
	var budgetDeferred bool
	for _, evidence := range result.Evidence {
		if evidence.GetReason() == "PER_TICK_ACTION_BUDGET" && evidence.GetDisposition() == tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_DEFERRED {
			budgetDeferred = true
		}
	}
	if !budgetDeferred {
		t.Fatalf("missing action-budget evidence: %+v", result.Evidence)
	}
}

func TestObservedActionBudgetCannotExceedPolicyLimit(t *testing.T) {
	share := .75
	priority := int32(17)
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Directives = Directives{TargetShare: &share, TargetPriority: &priority}
	input.Signals.PerTickActionBudget = 20
	input.Signals.PerTickActionBudgetPresent = true
	result, err := NewCoordinator(Config{PlannerBudget: PlannerBudgetConfig{MaxActions: 1}}).Plan(input)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Plan == nil || len(result.Plan.GetActions()) != 1 {
		t.Fatalf("policy action budget was exceeded: %+v", result.Plan)
	}
}

func TestCoordinatorSelectsCompatibleActionsWithinFinalBudget(t *testing.T) {
	share := .75
	priority := int32(17)
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.Directives = Directives{TargetShare: &share, TargetPriority: &priority}
	result, err := NewCoordinator(Config{PlannerBudget: PlannerBudgetConfig{MaxActions: 2, MaxAffectedSandboxes: 1}}).Plan(input)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Plan == nil || len(result.Plan.GetActions()) != 2 {
		t.Fatalf("Plan() = %+v, want two compatible actions", result.Plan)
	}
	if got := []tgsrlv1.ActionType{result.Plan.GetActions()[0].GetActionType(), result.Plan.GetActions()[1].GetActionType()}; got[0] != tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE || got[1] != tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY {
		t.Fatalf("selected actions = %v", got)
	}
	if result.Plan.GetRollbackPolicy() != tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION {
		t.Fatalf("rollback policy = %s", result.Plan.GetRollbackPolicy())
	}
	selectedCount := 0
	for _, evidence := range result.Evidence {
		if evidence.GetDisposition() == tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_SELECTED {
			selectedCount++
		}
	}
	if selectedCount != 2 {
		t.Fatalf("selected evidence count = %d, want 2", selectedCount)
	}
	for _, evidence := range result.Evidence {
		for _, key := range []string{"planner.budget.max_actions", "planner.budget.max_affected_sandboxes", "planner.budget.max_gpu_reconfigurations", "planner.budget.max_recovery_cost_nanos", "planner.budget.l4_disabled"} {
			if !hasSemanticInput(evidence, key) {
				t.Fatalf("missing budget field %s: %+v", key, evidence.GetInputs())
			}
		}
	}
	if err := validateActionPlan(result.Plan, input.EvaluationContext); err != nil {
		t.Fatalf("arbitrated plan invalid: %v", err)
	}
}

func TestAdmissionActionBudgetAppliesToFinalActionCount(t *testing.T) {
	snapshot, intent := validFixture()
	ctx := &tgsrlv1.EvaluationContext{TickKind: tgsrlv1.TickKind_TICK_KIND_FAST, EvaluationTime: timestamppb.New(fixtureTime), DecisionSequence: 41, Cause: "test"}
	admission, _, err := testScheduler(t, FallbackNoOp).EvaluateWithContext(snapshot, intent, ctx)
	if err != nil {
		t.Fatalf("EvaluateWithContext() error = %v", err)
	}
	result, err := NewCoordinator(Config{PlannerPerTickActionBudget: 1}).Plan(PlanningInput{
		Snapshot: snapshot, Intent: intent, EvaluationContext: ctx, AdmissionPlan: admission, DecisionID: admission.GetDecisionId(),
	})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Plan == nil || len(result.Plan.GetActions()) != 1 || len(result.Plan.GetBindings()) != 1 {
		t.Fatalf("budgeted admission plan = %+v", result.Plan)
	}
	if result.TotalProposalCount != 2 {
		t.Fatalf("proposal count = %d, want 2", result.TotalProposalCount)
	}
	deferred := 0
	for _, evidence := range result.Evidence {
		if evidence.GetReason() == "PER_TICK_ACTION_BUDGET" {
			deferred++
		}
	}
	if deferred != 1 {
		t.Fatalf("budget-deferred evidence = %d, want 1; evidence=%+v", deferred, result.Evidence)
	}
}

func TestContractPausePreemptsAdmission(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	input.ContractAggregate = semantics.AggregateResult{Blocking: true, Action: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED}
	binding := &tgsrlv1.Binding{BindingId: "admission-binding", PendingUnitId: "pending-b", RuntimeUnitId: "runtime-b", DeviceIds: []string{"device-b"}, Resources: cloneResources(input.Intent.GetResourcesPerUnit()), SandboxId: "sandbox-b", Generation: 1}
	input.AdmissionPlan = makePlan(input.DecisionID, "admission-plan", input.Snapshot, input.Intent, fixtureTime, []*tgsrlv1.Binding{binding}, false, input.EvaluationContext.GetTickKind())
	result, err := NewCoordinator(Config{}).Plan(input)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Plan == nil || result.Plan.GetPurpose() != tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION || len(result.Plan.GetActions()) != 1 || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_PAUSE {
		t.Fatalf("contract arbitration plan = %+v", result.Plan)
	}
	for _, evidence := range result.Evidence {
		if evidence.GetPlanner() == tgsrlv1.PlannerKind_PLANNER_KIND_ADMISSION && evidence.GetDisposition() != tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_REJECTED {
			t.Fatalf("admission was not rejected by blocking contract: %+v", evidence)
		}
	}
}

func TestContractPauseFailsClosedWhenBudgetCannotCoverEveryTarget(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_MEDIUM, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	secondAllocation := proto.Clone(input.Snapshot.Allocations[0]).(*tgsrlv1.Allocation)
	secondAllocation.AllocationId = "allocation-b"
	secondAllocation.PendingUnitId = "unit-b"
	secondAllocation.RuntimeUnitId = "runtime-b"
	secondSandbox := proto.Clone(input.Sandboxes[0]).(*tgsrlv1.Sandbox)
	secondSandbox.SandboxId = "sandbox-b"
	secondSandbox.Binding.BindingId = "binding-b"
	secondSandbox.Binding.PendingUnitId = secondAllocation.GetPendingUnitId()
	secondSandbox.Binding.RuntimeUnitId = secondAllocation.GetRuntimeUnitId()
	secondSandbox.Binding.SandboxId = secondSandbox.GetSandboxId()
	input.Snapshot.Allocations = append(input.Snapshot.Allocations, secondAllocation)
	input.Sandboxes = append(input.Sandboxes, secondSandbox)
	input.Intent.UnitCount = 2
	input.ContractAggregate = semantics.AggregateResult{Blocking: true, Action: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED}
	result, err := NewCoordinator(Config{PlannerPerTickActionBudget: 1}).Plan(input)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Plan != nil || result.FallbackReason != "NO_ELIGIBLE_PLANNER_PROPOSAL" {
		t.Fatalf("partial contract action set escaped: %+v", result)
	}
	for _, evidence := range result.Evidence {
		if evidence.GetDisposition() == tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_SELECTED {
			t.Fatalf("partial contract proposal remained selected: %+v", evidence)
		}
	}
}

func TestRecoveryPreemptsAdmission(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_SLOW, tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED)
	binding := &tgsrlv1.Binding{BindingId: "admission-binding", PendingUnitId: "pending-b", RuntimeUnitId: "runtime-b", DeviceIds: []string{"device-b"}, Resources: cloneResources(input.Intent.GetResourcesPerUnit()), SandboxId: "sandbox-b", Generation: 1}
	input.AdmissionPlan = makePlan(input.DecisionID, "admission-plan", input.Snapshot, input.Intent, fixtureTime, []*tgsrlv1.Binding{binding}, false, input.EvaluationContext.GetTickKind())
	result, err := NewCoordinator(Config{}).Plan(input)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Plan == nil || result.Plan.GetPurpose() != tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY || result.Plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_RECREATE {
		t.Fatalf("recovery arbitration plan = %+v", result.Plan)
	}
}

func TestPlannerBudgetConstrainsL4AndRecoveryCost(t *testing.T) {
	for _, test := range []struct {
		name   string
		budget PlannerBudgetConfig
		reason string
	}{
		{name: "L4 disabled", budget: PlannerBudgetConfig{MaxActions: 1, MaxAffectedSandboxes: 1, MaxGPUReconfigurations: 1, MaxRecoveryCostNanos: math.MaxInt64, DisableL4: true}, reason: "L4_DISABLED_BY_BUDGET"},
		{name: "recovery cost budget", budget: PlannerBudgetConfig{MaxActions: 1, MaxAffectedSandboxes: 1, MaxGPUReconfigurations: 1, MaxRecoveryCostNanos: 10}, reason: "RECOVERY_COST_BUDGET"},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_SLOW, tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED)
			input.Signals.RecoveryCostNanos = 20
			input.Signals.RecoveryCostNanosPresent = true
			coordinator := NewCoordinator(Config{PlannerBudget: test.budget})
			result, err := coordinator.Plan(input)
			if err != nil {
				t.Fatalf("Plan() error = %v", err)
			}
			if result.Plan != nil {
				t.Fatalf("budget allowed L4 plan: %+v", result.Plan)
			}
			found := false
			for _, evidence := range result.Evidence {
				if evidence.GetReason() == test.reason {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing %s evidence: %+v", test.reason, result.Evidence)
			}
		})
	}
}

func TestArbitrationEnforcesGPUReconfigurationBudget(t *testing.T) {
	input := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_SLOW, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	proposal := func(id, target string) PlannerProposal {
		plan := &tgsrlv1.PlacementPlan{
			PlanId: id, Purpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY,
			Actions: []*tgsrlv1.Action{{ActionId: id + "-action", ActionType: tgsrlv1.ActionType_ACTION_TYPE_RECREATE, TargetId: target, SandboxId: target}},
		}
		return PlannerProposal{Plan: plan, Evidence: plannerEvidence(input, tgsrlv1.PlannerKind_PLANNER_KIND_SLOW_RECONFIGURATION, plan.GetPurpose(), tgsrlv1.ActionType_ACTION_TYPE_RECREATE, target, 100, tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_DEFERRED, "ELIGIBLE")}
	}
	proposals := []PlannerProposal{proposal("plan-a", "sandbox-a"), proposal("plan-b", "sandbox-b")}
	selected, _ := arbitrateProposals(input, proposals, PlannerBudgetConfig{MaxActions: 2, MaxAffectedSandboxes: 2, MaxGPUReconfigurations: 1, MaxRecoveryCostNanos: math.MaxInt64})
	if len(selected) != 1 {
		t.Fatalf("selected proposals = %v, want one", selected)
	}
	if proposals[1].Evidence.GetReason() != "GPU_RECONFIGURATION_BUDGET" {
		t.Fatalf("deferred reason = %q", proposals[1].Evidence.GetReason())
	}
}

func TestSignedUtilitySaturatesDeterministically(t *testing.T) {
	if got := saturatingUtilityAdd(math.MaxInt64-1, 2); got != math.MaxInt64 {
		t.Fatalf("positive overflow=%d", got)
	}
	if got := saturatingUtilityAdd(math.MinInt64+1, -2); got != math.MinInt64 {
		t.Fatalf("negative overflow=%d", got)
	}
	if got := saturatingSubtract(-1, math.MinInt64); got != math.MaxInt64 {
		t.Fatalf("subtraction overflow=%d", got)
	}
}

func TestRecentActionInFlightUsesOnlyNonTerminalResults(t *testing.T) {
	decision := func(results ...*tgsrlv1.ActionResult) *tgsrlv1.DecisionRecord {
		return &tgsrlv1.DecisionRecord{Sequence: 1, SelectedPlan: &tgsrlv1.PlacementPlan{Actions: []*tgsrlv1.Action{{ActionId: "action-a"}}}, ActionResults: results}
	}
	for _, test := range []struct {
		name        string
		decisions   []*tgsrlv1.DecisionRecord
		wantValue   bool
		wantPresent bool
	}{
		{name: "missing terminal result is in flight", decisions: []*tgsrlv1.DecisionRecord{decision()}, wantValue: true, wantPresent: true},
		{name: "succeeded is terminal", decisions: []*tgsrlv1.DecisionRecord{decision(&tgsrlv1.ActionResult{Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED})}, wantPresent: true},
		{name: "rolled back is terminal", decisions: []*tgsrlv1.DecisionRecord{decision(&tgsrlv1.ActionResult{Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK})}, wantPresent: true},
		{name: "unknown is in flight", decisions: []*tgsrlv1.DecisionRecord{decision(&tgsrlv1.ActionResult{Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_UNKNOWN})}, wantValue: true, wantPresent: true},
		{name: "nil is in flight", decisions: []*tgsrlv1.DecisionRecord{decision(nil)}, wantValue: true, wantPresent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, present := recentActionInFlight(test.decisions)
			if got != test.wantValue || present != test.wantPresent {
				t.Fatalf("recentActionInFlight()=(%v,%v), want (%v,%v)", got, present, test.wantValue, test.wantPresent)
			}
		})
	}
}

func TestAdaptiveProtectionRejectionCarriesGuardEvidence(t *testing.T) {
	clock := protection.ClockFunc(func() time.Time { return fixtureTime })
	guard := protection.NewGuard(protection.Config{Enabled: true, Cooldown: time.Minute, Hysteresis: .25, MaxActionsPerWindow: 2, Window: time.Hour, BreakerThreshold: 2, BreakerResetAfter: time.Hour}, clock)
	scheduler, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), Sequence: SequenceFunc(func() uint64 { return 9 }), Guard: guard})
	if err != nil {
		t.Fatal(err)
	}
	guardKey := "execution/stage"
	if decision := guard.CommitN(guardKey, .5, 1); !decision.Allowed {
		t.Fatalf("seed guard state=%+v", decision)
	}
	second := adaptiveFixture(tgsrlv1.TickKind_TICK_KIND_FAST, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING)
	second.Directives.TargetShare = proto.Float64(.75)
	_, record, err := scheduler.EvaluateAdaptive(&second)
	if err != nil || !record.GetFallback() || record.GetFallbackReason() != "PROTECTION_COOLDOWN" {
		t.Fatalf("second record=%+v err=%v", record, err)
	}
	evidence := record.GetPlannerEvidence()[0]
	if evidence.GetReason() != "PROTECTION_COOLDOWN" {
		t.Fatalf("guard reason evidence=%+v", evidence)
	}
	for _, key := range []string{"planner.guard.cooldown_nanos", "planner.guard.hysteresis", "planner.guard.max_actions_per_window", "planner.guard.reason"} {
		if !hasSemanticInput(evidence, key) {
			t.Fatalf("missing %s in guard evidence: %+v", key, evidence.GetInputs())
		}
	}
}

func hasSemanticInput(evidence *tgsrlv1.PlannerEvidence, key string) bool {
	for _, field := range evidence.GetInputs() {
		if field.GetKey() == key {
			return true
		}
	}
	return false
}

func semanticIntInput(evidence *tgsrlv1.PlannerEvidence, key string) (int64, bool) {
	for _, field := range evidence.GetInputs() {
		if field.GetKey() == key {
			value, ok := field.GetValue().GetKind().(*tgsrlv1.SemanticValue_Int64Value)
			if ok {
				return value.Int64Value, true
			}
		}
	}
	return 0, false
}

func adaptiveFixture(tick tgsrlv1.TickKind, state tgsrlv1.RuntimeState) PlanningInput {
	snapshot, intent := validFixture()
	resources := proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector)
	intent.UnitCount = 1
	intent.RequiredCapabilities.SupportedActions = nil
	for _, device := range snapshot.Devices {
		device.Capabilities.SupportedActions = []string{"bind", "release", "set_share", "set_priority", "resize", "pause", "resume", "sleep", "offload", "rebind", "recreate"}
	}
	snapshot.PendingUnits = nil
	snapshot.Allocations = []*tgsrlv1.Allocation{{AllocationId: "allocation-a", ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(), IntentVersion: intent.GetVersion(), JobId: intent.GetJobId(), PendingUnitId: "unit-a", RuntimeUnitId: "runtime-a", DeviceIds: []string{"device-a"}, Resources: resources, State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE, Generation: 3}}
	binding := &tgsrlv1.Binding{BindingId: "binding-a", PendingUnitId: "unit-a", RuntimeUnitId: "runtime-a", DeviceIds: []string{"device-a"}, Resources: proto.Clone(resources).(*tgsrlv1.ResourceVector), SandboxId: "sandbox-a", Generation: 3}
	sandbox := &tgsrlv1.Sandbox{SandboxId: "sandbox-a", State: state, Generation: 3, Binding: binding, Share: .5, Priority: 5, SafePoint: true, ObservedAt: timestamppb.New(fixtureTime.Add(-time.Minute)), StateChangedAt: timestamppb.New(fixtureTime.Add(-time.Minute)), LastConfirmedAt: timestamppb.New(fixtureTime)}
	signals := PlanningSignals{}
	if tick == tgsrlv1.TickKind_TICK_KIND_MEDIUM {
		signals = PlanningSignals{
			CheckpointCapable: true, CheckpointCapablePresent: true,
			BufferBelowWatermark: true, BufferBelowWatermarkPresent: true,
			MinimumResidencyPresent: true,
			ActionInFlightPresent:   true,
		}
	}
	return PlanningInput{Snapshot: snapshot, Intent: intent, EvaluationContext: &tgsrlv1.EvaluationContext{TickKind: tick, EvaluationTime: timestamppb.New(fixtureTime), DecisionSequence: 9, Cause: "adaptive-test"}, Sandboxes: []*tgsrlv1.Sandbox{sandbox}, SafePoint: semanticsSafePoint(true), Signals: signals, DecisionID: "decision-adaptive"}
}

func semanticsSafePoint(value bool) semantics.SafePointResolution {
	return semantics.SafePointResolution{Present: true, Value: value}
}

func adaptiveReplacementBinding() *tgsrlv1.Binding {
	return adaptiveReplacementBindingOn("device-b")
}

func adaptiveReplacementBindingOn(deviceID string) *tgsrlv1.Binding {
	return &tgsrlv1.Binding{BindingId: "replacement-a", PendingUnitId: "unit-a", RuntimeUnitId: "runtime-a", DeviceIds: []string{deviceID}, Resources: &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 100, AcceleratorUnits: .25, EphemeralStorageBytes: 10, NetworkBandwidthBps: 10}, SandboxId: "sandbox-a", Generation: 4}
}

func selectedPlannerEvidence(t *testing.T, evidence []*tgsrlv1.PlannerEvidence) *tgsrlv1.PlannerEvidence {
	t.Helper()
	for _, item := range evidence {
		if item.GetDisposition() == tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_SELECTED {
			return item
		}
	}
	t.Fatalf("no selected planner evidence: %+v", evidence)
	return nil
}
