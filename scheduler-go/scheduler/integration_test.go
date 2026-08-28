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
	evaluator, err := New(Config{Fallback: FallbackNoOp, Clock: ClockFunc(func() time.Time { return fixtureTime }), Policy: policy.Bundle{ID: "binpack", Version: "1", Strategy: policy.StrategyBinpack, PreemptionPolicy: "noop"}})
	if err != nil {
		t.Fatal(err)
	}
	plan, decision, err := evaluator.Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() || firstDeviceID(plan) != "device-a" {
		t.Fatalf("Evaluate(binpack) device=%q decision=%v error=%v", firstDeviceID(plan), decision, err)
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
