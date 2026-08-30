package preemption

import (
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func int32ptr(v int32) *int32 { return &v }

func TestLowPriorityFirstPick(t *testing.T) {
	snapshot := &tgsrlv1.ClusterSnapshot{
		Allocations: []*tgsrlv1.Allocation{
			{AllocationId: "a2", PendingUnitId: "old-2", IntentVersion: 10, State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE, Priority: int32ptr(10), DeviceIds: []string{"device-b"}},
			{AllocationId: "a1", PendingUnitId: "old-1", IntentVersion: 1, State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE, Priority: int32ptr(1), DeviceIds: []string{"device-a"}},
			{AllocationId: "released", IntentVersion: 0, State: tgsrlv1.AllocationState_ALLOCATION_STATE_RELEASED},
		},
	}
	unit := &tgsrlv1.PendingUnit{Priority: 100}
	victims := LowPriorityFirst{}.Pick(snapshot, unit)
	if len(victims) != 2 {
		t.Fatalf("Pick() victims = %d, want 2 active allocations", len(victims))
	}
	if victims[0].Allocation.GetAllocationId() != "a1" {
		t.Fatalf("first victim = %q, want a1", victims[0].Allocation.GetAllocationId())
	}
	got := NoOp{}.Pick(snapshot, unit)
	if len(got) != 0 {
		t.Fatalf("NoOp.Pick() = %v, want no victims", got)
	}
}

func TestCanReleaseRequiresCompleteProtocolEvidence(t *testing.T) {
	snapshot := &tgsrlv1.ClusterSnapshot{Devices: []*tgsrlv1.Device{{DeviceId: "device-1", Capabilities: &tgsrlv1.CapabilitySet{SupportedActions: []string{"release"}}}}}
	victim := Victim{Allocation: &tgsrlv1.Allocation{AllocationId: "allocation-1", PendingUnitId: "unit-1", RuntimeUnitId: "runtime-1", SandboxId: "sandbox-1", BindingId: "binding-1", Generation: 2, Priority: int32ptr(1), DeviceIds: []string{"device-1"}}}
	if reason, ok := CanRelease(snapshot, victim, true); !ok || reason != "" {
		t.Fatalf("CanRelease() = (%q, %v), want safe", reason, ok)
	}
	victim.Allocation.Generation = 0
	if reason, ok := CanRelease(snapshot, victim, true); ok || reason != FallbackNotExpressible {
		t.Fatalf("CanRelease(missing generation) = (%q, %v)", reason, ok)
	}
}

func TestLowPriorityFirstUsesAllocationPriorityAndStableDeviceTieBreak(t *testing.T) {
	snapshot := &tgsrlv1.ClusterSnapshot{
		Allocations: []*tgsrlv1.Allocation{
			{AllocationId: "victim-z", State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE, Priority: int32ptr(1), DeviceIds: []string{"device-z"}},
			{AllocationId: "victim-a2", State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE, Priority: int32ptr(1), DeviceIds: []string{"device-a"}},
			{AllocationId: "victim-a1", State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE, Priority: int32ptr(1), DeviceIds: []string{"device-a"}},
			{AllocationId: "higher", State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE, Priority: int32ptr(50), DeviceIds: []string{"device-b"}},
		},
	}
	unit := &tgsrlv1.PendingUnit{Priority: 100}
	victims := LowPriorityFirst{}.Pick(snapshot, unit)
	if got := []string{victims[0].Allocation.GetAllocationId(), victims[1].Allocation.GetAllocationId(), victims[2].Allocation.GetAllocationId()}; got[0] != "victim-a1" || got[1] != "victim-a2" || got[2] != "victim-z" {
		t.Fatalf("victim order = %v", got)
	}
}

func TestLowPriorityFirstSkipsAllocationsWithoutExplicitPriority(t *testing.T) {
	snapshot := &tgsrlv1.ClusterSnapshot{Allocations: []*tgsrlv1.Allocation{
		{AllocationId: "legacy", State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE, DeviceIds: []string{"device-a"}},
		{AllocationId: "explicit", State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE, Priority: int32ptr(1), DeviceIds: []string{"device-a"}},
	}}
	victims := LowPriorityFirst{}.Pick(snapshot, &tgsrlv1.PendingUnit{Priority: 100})
	if len(victims) != 1 || victims[0].Allocation.GetAllocationId() != "explicit" {
		t.Fatalf("Pick() victims = %+v, want only explicitly prioritized allocation", victims)
	}
}
