package scheduler

import (
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/policy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/preemption"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/protection"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func int32ptr(v int32) *int32 { return &v }

func TestConfiguredPolicyAndProtectionChecksDoNotMutatePureEvaluate(t *testing.T) {
	snapshot, intent := validFixture()
	guard := protection.NewGuard(protection.Config{Enabled: true, Cooldown: time.Minute, MaxActionsPerWindow: 4, Window: time.Hour, BreakerThreshold: 2, BreakerResetAfter: time.Hour}, protection.ClockFunc(func() time.Time { return fixtureTime }))
	evaluator, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), Policy: policy.Bundle{ID: "configured", Version: "1", Strategy: policy.StrategyScoreFirst, PreemptionPolicy: "noop"}, Guard: guard})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() || len(plan.GetActions()) != 2 {
		t.Fatalf("first Evaluate() plan=%v decision=%v error=%v", plan, decision, err)
	}
	plan, decision, err = evaluator.Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() || len(plan.GetActions()) != 2 {
		t.Fatalf("second Evaluate() plan=%v decision=%v error=%v", plan, decision, err)
	}
}

func TestConfiguredNoOpPolicyFallsBack(t *testing.T) {
	snapshot, intent := validFixture()
	evaluator, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), Policy: policy.Bundle{ID: "noop", Version: "1", Strategy: policy.StrategyNoOp, PreemptionPolicy: "noop"}})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || !decision.GetFallback() || decision.GetFallbackReason() != "POLICY_NO_OP_POLICY" || len(plan.GetActions()) != 0 {
		t.Fatalf("Evaluate() plan=%v decision=%v error=%v", plan, decision, err)
	}
}

func TestConfiguredBinpackPolicyChangesSelectedDevice(t *testing.T) {
	snapshot, intent := validFixture()
	snapshot.Devices[0].Allocatable.CpuMillis = 900
	snapshot.Devices[1].Allocatable.CpuMillis = 400
	intent.UnitCount = 1
	snapshot.PendingUnits = snapshot.PendingUnits[:1]
	evaluator, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), Policy: policy.Bundle{ID: "binpack", Version: "1", Strategy: policy.StrategyBinpack, TopK: 2, PreemptionPolicy: "noop"}})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() || firstDeviceID(plan) != "device-a" {
		t.Fatalf("Evaluate(binpack) device=%q decision=%v error=%v", firstDeviceID(plan), decision, err)
	}
}

func TestConfiguredBinpackPolicyCannotSelectOutsideTopK(t *testing.T) {
	snapshot, intent := validFixture()
	snapshot.Devices[0].Allocatable.CpuMillis = 900
	snapshot.Devices[1].Allocatable.CpuMillis = 400
	intent.UnitCount = 1
	snapshot.PendingUnits = snapshot.PendingUnits[:1]
	evaluator, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), Policy: policy.Bundle{ID: "binpack", Version: "1", Strategy: policy.StrategyBinpack, TopK: 1, PreemptionPolicy: "noop"}})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() || firstDeviceID(plan) != "device-b" {
		t.Fatalf("Evaluate(binpack, top_k=1) device=%q decision=%v error=%v", firstDeviceID(plan), decision, err)
	}
}

func TestEvaluateWithContextUsesExplicitDeterministicInputs(t *testing.T) {
	snapshot, intent := validFixture()
	explicitTime := fixtureTime.Add(2 * time.Minute)
	evaluationContext := &tgsrlv1.EvaluationContext{
		TickKind:         tgsrlv1.TickKind_TICK_KIND_SLOW,
		EvaluationTime:   timestamppb.New(explicitTime),
		DecisionSequence: 42,
	}
	evaluator := testScheduler(t, FallbackNoOp)

	plan, decision, err := evaluator.EvaluateWithContext(snapshot, intent, evaluationContext)
	if err != nil {
		t.Fatalf("EvaluateWithContext() error = %v", err)
	}
	if decision.GetSequence() != 42 || !decision.GetDecidedAt().AsTime().Equal(explicitTime) {
		t.Fatalf("decision sequence/time = (%d, %s), want (42, %s)", decision.GetSequence(), decision.GetDecidedAt().AsTime(), explicitTime)
	}
	if decision.GetTickKind() != tgsrlv1.TickKind_TICK_KIND_SLOW {
		t.Fatalf("decision tick_kind = %s, want SLOW", decision.GetTickKind())
	}
	if decision.GetEvaluationContext() == nil || decision.GetEvaluationContext().GetCompatibilityDefaultsApplied() {
		t.Fatalf("evaluation_context = %+v, want explicit context without compatibility defaults", decision.GetEvaluationContext())
	}
	for _, action := range plan.GetActions() {
		if action.GetTickKind() != tgsrlv1.TickKind_TICK_KIND_SLOW {
			t.Fatalf("action tick_kind = %s, want SLOW", action.GetTickKind())
		}
	}
}

func TestEvaluateWithContextUsesOneResolvedSafePointFact(t *testing.T) {
	snapshot, intent := validFixture()
	intent.UnitCount = 1
	intent.ExecutionContract.CommitPolicy.RequireSafePoint = true
	intent.ExecutionContract.ContractId, _ = CanonicalContractID(intent.ExecutionContract)
	snapshot.PendingUnits = snapshot.PendingUnits[:1]
	snapshot.Annotations[SafePointAnnotation] = "false"
	safePoint := true
	ctx := &tgsrlv1.EvaluationContext{
		TickKind:         tgsrlv1.TickKind_TICK_KIND_FAST,
		EvaluationTime:   timestamppb.New(fixtureTime),
		DecisionSequence: 9,
		ContractObservation: &tgsrlv1.ContractObservation{
			SafePoint: &safePoint,
		},
	}

	plan, decision, err := testScheduler(t, FallbackNoOp).EvaluateWithContext(snapshot, intent, ctx)
	if err != nil || decision.GetFallback() {
		t.Fatalf("EvaluateWithContext() plan=%v decision=%v error=%v", plan, decision, err)
	}
	for _, action := range plan.GetActions() {
		if !action.GetRequiresSafePoint() {
			t.Fatalf("action %q does not preserve commit safe-point requirement", action.GetActionId())
		}
	}
}

func TestEvaluateWithCustomSafePointAnnotation(t *testing.T) {
	const customKey = "example.test/safe-point"
	tests := []struct {
		name       string
		annotation map[string]string
		wantAllow  bool
	}{
		{name: "true allows", annotation: map[string]string{customKey: "true"}, wantAllow: true},
		{name: "false blocks", annotation: map[string]string{customKey: "false"}},
		{name: "missing blocks", annotation: map[string]string{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot, intent := validFixture()
			intent.UnitCount = 1
			intent.ExecutionContract.CommitPolicy.RequireSafePoint = true
			intent.ExecutionContract.ContractId, _ = CanonicalContractID(intent.ExecutionContract)
			snapshot.PendingUnits = snapshot.PendingUnits[:1]
			snapshot.Annotations = test.annotation
			evaluator, err := New(Config{
				Fallback:            FallbackNoOp,
				Clock:               ClockFunc(func() time.Time { return fixtureTime }),
				SafePointAnnotation: customKey,
			})
			if err != nil {
				t.Fatal(err)
			}
			plan, decision, err := evaluator.Evaluate(snapshot, intent)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if test.wantAllow {
				if decision.GetFallback() || len(plan.GetActions()) == 0 {
					t.Fatalf("decision=%v plan=%v, want actionable decision", decision, plan)
				}
				return
			}
			if !decision.GetFallback() || decision.GetFallbackReason() != FallbackReasonSafePoint || len(plan.GetActions()) != 0 {
				t.Fatalf("fallback=(%v, %q) actions=%d, want SAFE_POINT_REQUIRED with no actions", decision.GetFallback(), decision.GetFallbackReason(), len(plan.GetActions()))
			}
		})
	}
}

func TestEvaluateContextSafePointOverridesCustomAnnotationWithoutMutatingContext(t *testing.T) {
	const customKey = "example.test/safe-point"
	snapshot, intent := validFixture()
	intent.UnitCount = 1
	intent.ExecutionContract.CommitPolicy.RequireSafePoint = true
	intent.ExecutionContract.ContractId, _ = CanonicalContractID(intent.ExecutionContract)
	snapshot.PendingUnits = snapshot.PendingUnits[:1]
	snapshot.Annotations = map[string]string{customKey: "false"}
	safePoint := true
	ctx := &tgsrlv1.EvaluationContext{
		TickKind:         tgsrlv1.TickKind_TICK_KIND_FAST,
		EvaluationTime:   timestamppb.New(fixtureTime),
		DecisionSequence: 10,
		ContractObservation: &tgsrlv1.ContractObservation{
			EventId:   "context-safe-point",
			SafePoint: &safePoint,
		},
	}
	wantContext := proto.Clone(ctx).(*tgsrlv1.EvaluationContext)
	evaluator, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), SafePointAnnotation: customKey})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.EvaluateWithContext(snapshot, intent, ctx)
	if err != nil || decision.GetFallback() || len(plan.GetActions()) == 0 {
		t.Fatalf("EvaluateWithContext() plan=%v decision=%v error=%v", plan, decision, err)
	}
	if !proto.Equal(ctx, wantContext) || !proto.Equal(decision.GetEvaluationContext(), wantContext) {
		t.Fatalf("evaluation context mutated: input=%v recorded=%v want=%v", ctx, decision.GetEvaluationContext(), wantContext)
	}
}

func TestConfiguredTraceAwarePolicySelectsTraceMatchedDevice(t *testing.T) {
	snapshot, intent := validFixture()
	intent.UnitCount = 1
	intent.TraceId = "trace-123"
	snapshot.PendingUnits = snapshot.PendingUnits[:1]
	snapshot.Devices[0].Labels = map[string]string{"trace_id": "other-trace"}
	snapshot.Devices[1].Labels = map[string]string{"trace_id": "trace-123"}

	evaluator, err := New(Config{
		Fallback: FallbackNoOp,
		Clock:    ClockFunc(func() time.Time { return fixtureTime }),
		Policy:   policy.Bundle{ID: "trace-aware", Version: "1", Strategy: policy.StrategyTraceAware, TopK: 2, PreemptionPolicy: "noop"},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() {
		t.Fatalf("Evaluate(trace-aware) plan=%v decision=%v error=%v", plan, decision, err)
	}
	if got := firstDeviceID(plan); got != "device-a" {
		t.Fatalf("trace-aware selected device = %q, want device-a", got)
	}
	selected := selectedCandidateForBinding(t, decision, plan.GetBindings()[0])
	if got := selected.GetComponentScores()["trace_affinity"]; got <= 0 {
		t.Fatalf("selected candidate trace_affinity = %f, want > 0", got)
	}
}

func TestPreemptionUnsafeFallsBackWithoutActions(t *testing.T) {
	snapshot, intent := preemptionFixture(t)
	snapshot.Annotations[SafePointAnnotation] = "false"
	evaluator, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), Policy: policy.Bundle{ID: "preempt", Version: "1", Strategy: policy.StrategyScoreFirst, AllowPreemption: true, PreemptionPolicy: "low_priority_first", RequireSafePoint: true}, Preemption: preemption.LowPriorityFirst{}})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || !decision.GetFallback() || decision.GetFallbackReason() != preemption.FallbackUnsafe || len(plan.GetActions()) != 0 {
		t.Fatalf("Evaluate() plan=%v decision=%v error=%v", plan, decision, err)
	}
}

func TestPreemptionBuildsDeterministicTransactionalPlan(t *testing.T) {
	snapshot, intent := preemptionFixture(t)
	evaluator, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), Policy: policy.Bundle{ID: "preempt", Version: "1", Strategy: policy.StrategyScoreFirst, AllowPreemption: true, PreemptionPolicy: "low_priority_first", RequireSafePoint: true}, Preemption: preemption.LowPriorityFirst{}})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() {
		t.Fatalf("Evaluate() plan=%v decision=%v error=%v", plan, decision, err)
	}
	if plan.GetPurpose() != tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION || len(plan.GetActions()) != 2 {
		t.Fatalf("preemption plan = %+v", plan)
	}
	if plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_RELEASE || plan.GetActions()[0].GetTargetId() != "victim-allocation" {
		t.Fatalf("release action = %+v", plan.GetActions()[0])
	}
	if plan.GetActions()[1].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND {
		t.Fatalf("bind action = %+v", plan.GetActions()[1])
	}
	if got := plan.GetCapabilityRequirements(); len(got) != 4 {
		t.Fatalf("capability requirements = %+v", got)
	}
	for index, action := range plan.GetActions() {
		if action.GetTickKind() != decision.GetTickKind() {
			t.Fatalf("action %d tick_kind = %s, want %s", index, action.GetTickKind(), decision.GetTickKind())
		}
		if !action.GetRequiresSafePoint() {
			t.Fatalf("action %d requires_safe_point = false, want true", index)
		}
	}
	if err := actionpolicy.ValidatePlan(plan, decision.GetTickKind(), actionpolicy.ValidationOptions{}); err != nil {
		t.Fatalf("actionpolicy.ValidatePlan() error = %v", err)
	}
	if err := validatePreemptionPlan(plan, &tgsrlv1.EvaluationContext{TickKind: decision.GetTickKind()}); err != nil {
		t.Fatalf("validatePreemptionPlan() error = %v", err)
	}
}

func TestPreemptionFailsClosedWithoutRuntimeIdentityForVictimSandbox(t *testing.T) {
	snapshot, intent := preemptionFixture(t)
	snapshot.Allocations[0].RuntimeUnitId = ""
	evaluator, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), Policy: policy.Bundle{ID: "preempt", Version: "1", Strategy: policy.StrategyScoreFirst, AllowPreemption: true, PreemptionPolicy: "low_priority_first", RequireSafePoint: true}, Preemption: preemption.LowPriorityFirst{}})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || !decision.GetFallback() || decision.GetFallbackReason() != preemption.FallbackNotExpressible || len(plan.GetActions()) != 0 {
		t.Fatalf("Evaluate() plan=%v decision=%v error=%v", plan, decision, err)
	}
}

func TestPreemptionAggregatesVictimCapacityOnOneDeviceDeterministically(t *testing.T) {
	snapshot, intent := preemptionFixture(t)
	intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1 << 30, AcceleratorUnits: .25}
	snapshot.PendingUnits[0].RequestedResources = proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector)
	first := proto.Clone(snapshot.Allocations[0]).(*tgsrlv1.Allocation)
	first.AllocationId = "victim-b"
	first.Priority = int32ptr(1)
	first.DeviceIds = []string{"device-a"}
	first.Resources = &tgsrlv1.ResourceVector{CpuMillis: 500, MemoryBytes: 1 << 30, AcceleratorUnits: .25}
	second := proto.Clone(snapshot.Allocations[0]).(*tgsrlv1.Allocation)
	second.AllocationId = "victim-a"
	second.Priority = int32ptr(1)
	second.DeviceIds = []string{"device-a"}
	second.Resources = &tgsrlv1.ResourceVector{CpuMillis: 500, MemoryBytes: 1 << 30, AcceleratorUnits: .25}
	deviceA := proto.Clone(snapshot.Devices[0]).(*tgsrlv1.Device)
	deviceA.DeviceId = "device-a"
	deviceA.Capacity = &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 2 << 30, AcceleratorUnits: .5}
	deviceA.Allocatable = &tgsrlv1.ResourceVector{}
	snapshot.Devices = append(snapshot.Devices, deviceA)
	snapshot.Allocations = []*tgsrlv1.Allocation{first, second}
	evaluator, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), Policy: policy.Bundle{ID: "preempt", Version: "1", Strategy: policy.StrategyScoreFirst, AllowPreemption: true, PreemptionPolicy: "low_priority_first", RequireSafePoint: true}, Preemption: preemption.LowPriorityFirst{}})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() {
		t.Fatalf("Evaluate() plan=%v decision=%v error=%v", plan, decision, err)
	}
	if got := []string{plan.GetActions()[0].GetTargetId(), plan.GetActions()[1].GetTargetId()}; got[0] != "victim-a" || got[1] != "victim-b" {
		t.Fatalf("release order = %v", got)
	}
	if bind := plan.GetActions()[2]; bind.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND || bind.GetBinding().GetDeviceIds()[0] != "device-a" {
		t.Fatalf("replacement bind = %+v", bind)
	}
}

func TestPreemptionUsesExistingHeadroomWithoutReleasingOtherDevices(t *testing.T) {
	snapshot, intent := preemptionFixture(t)
	intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 100, AcceleratorUnits: .25, EphemeralStorageBytes: 10, NetworkBandwidthBps: 10}
	snapshot.PendingUnits[0].RequestedResources = proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector)
	target := snapshot.Allocations[0]
	target.AllocationId = "target-victim"
	target.Priority = int32ptr(2)
	target.DeviceIds = []string{"device-a"}
	target.Resources = &tgsrlv1.ResourceVector{CpuMillis: 500, MemoryBytes: 100, AcceleratorUnits: .25, EphemeralStorageBytes: 10, NetworkBandwidthBps: 10}
	snapshot.Devices[0].DeviceId = "device-a"
	snapshot.Devices[0].Allocatable = &tgsrlv1.ResourceVector{CpuMillis: 500}
	unrelatedDevice := proto.Clone(snapshot.Devices[0]).(*tgsrlv1.Device)
	unrelatedDevice.DeviceId = "device-b"
	unrelatedDevice.Allocatable = &tgsrlv1.ResourceVector{}
	snapshot.Devices = append(snapshot.Devices, unrelatedDevice)
	unrelated := proto.Clone(target).(*tgsrlv1.Allocation)
	unrelated.AllocationId = "unrelated-victim"
	unrelated.BindingId = "unrelated-binding"
	unrelated.SandboxId = "unrelated-sandbox"
	unrelated.RuntimeUnitId = "unrelated-runtime"
	unrelated.PendingUnitId = "unrelated-pending"
	unrelated.Priority = int32ptr(1)
	unrelated.DeviceIds = []string{"device-b"}
	unrelated.Resources = &tgsrlv1.ResourceVector{CpuMillis: 100}
	snapshot.Allocations = []*tgsrlv1.Allocation{unrelated, target}

	evaluator, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), Policy: policy.Bundle{ID: "preempt", Version: "1", Strategy: policy.StrategyScoreFirst, AllowPreemption: true, PreemptionPolicy: "low_priority_first", RequireSafePoint: true}, Preemption: preemption.LowPriorityFirst{}})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() {
		t.Fatalf("Evaluate() plan=%v decision=%v error=%v", plan, decision, err)
	}
	if len(plan.GetActions()) != 2 || plan.GetActions()[0].GetTargetId() != target.GetAllocationId() || plan.GetActions()[1].GetBinding().GetDeviceIds()[0] != "device-a" {
		t.Fatalf("preemption actions = %+v, want only target-device victim then replacement", plan.GetActions())
	}
}

func preemptionFixture(t *testing.T) (*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent) {
	t.Helper()
	snapshot, intent := validFixture()
	intent.UnitCount = 1
	intent.Priority = 100
	snapshot.PendingUnits = snapshot.PendingUnits[:1]
	unit := snapshot.PendingUnits[0]
	unit.Priority = intent.GetPriority()
	unit.RuntimeUnitId = "runtime-new"
	unit.RequestedResources = proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector)
	device := snapshot.Devices[0]
	snapshot.Devices = snapshot.Devices[:1]
	device.Allocatable = &tgsrlv1.ResourceVector{}
	device.Capabilities.SupportedActions = []string{"bind", "release"}
	allocation := &tgsrlv1.Allocation{AllocationId: "victim-allocation", ExecutionId: "old-execution", StageId: "old-stage", IntentVersion: 1, JobId: "old-job", PendingUnitId: "victim-unit", DeviceIds: []string{device.GetDeviceId()}, Resources: proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector), State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE, RuntimeUnitId: "runtime-victim", SandboxId: "sandbox-victim", BindingId: "binding-victim", Generation: 2, Priority: int32ptr(1)}
	snapshot.Allocations = []*tgsrlv1.Allocation{allocation}
	snapshot.PendingUnits = append(snapshot.PendingUnits, &tgsrlv1.PendingUnit{PendingUnitId: "victim-unit", ExecutionId: "old-execution", StageId: "old-stage", IntentVersion: 1, JobId: "old-job", RequestedResources: proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector), Priority: 1})
	if !strings.Contains(strings.Join(device.Capabilities.SupportedActions, ","), "release") {
		t.Fatal("fixture lacks release capability")
	}
	return snapshot, intent
}
