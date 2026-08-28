package scheduler

import (
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/policy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/preemption"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/protection"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestConfiguredPolicyAndProtectionApplyInAuthoritativeEvaluate(t *testing.T) {
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
	if err != nil || !decision.GetFallback() || decision.GetFallbackReason() != "PROTECTION_COOLDOWN" || len(plan.GetActions()) != 0 {
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

func TestPreemptionFallsBackWhenAtomicReplacementCannotBeExpressed(t *testing.T) {
	snapshot, intent := preemptionFixture(t)
	evaluator, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), Policy: policy.Bundle{ID: "preempt", Version: "1", Strategy: policy.StrategyScoreFirst, AllowPreemption: true, PreemptionPolicy: "low_priority_first", RequireSafePoint: true}, Preemption: preemption.LowPriorityFirst{}})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || !decision.GetFallback() || decision.GetFallbackReason() != preemption.FallbackNotExpressible || len(plan.GetActions()) != 0 {
		t.Fatalf("Evaluate() plan=%v decision=%v error=%v", plan, decision, err)
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
	unit.RequestedResources = proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector)
	device := snapshot.Devices[0]
	snapshot.Devices = snapshot.Devices[:1]
	device.Allocatable = &tgsrlv1.ResourceVector{}
	device.Capabilities.SupportedActions = []string{"bind", "release"}
	allocation := &tgsrlv1.Allocation{AllocationId: "victim-allocation", ExecutionId: "old-execution", StageId: "old-stage", IntentVersion: 1, JobId: "old-job", PendingUnitId: "victim-unit", DeviceIds: []string{device.GetDeviceId()}, Resources: proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector), State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE, RuntimeUnitId: "runtime-victim", Generation: 2}
	snapshot.Allocations = []*tgsrlv1.Allocation{allocation}
	snapshot.PendingUnits = append(snapshot.PendingUnits, &tgsrlv1.PendingUnit{PendingUnitId: "victim-unit", ExecutionId: "old-execution", StageId: "old-stage", IntentVersion: 1, JobId: "old-job", RequestedResources: proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector), Priority: 1})
	if !strings.Contains(strings.Join(device.Capabilities.SupportedActions, ","), "release") {
		t.Fatal("fixture lacks release capability")
	}
	return snapshot, intent
}
