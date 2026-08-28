package preemption

import (
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func TestLowPriorityFirstPick(t *testing.T) {
	snapshot := &tgsrlv1.ClusterSnapshot{
		Allocations: []*tgsrlv1.Allocation{
			{AllocationId: "a2", PendingUnitId: "old-2", IntentVersion: 10, State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE},
			{AllocationId: "a1", PendingUnitId: "old-1", IntentVersion: 1, State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE},
			{AllocationId: "released", IntentVersion: 0, State: tgsrlv1.AllocationState_ALLOCATION_STATE_RELEASED},
		},
		PendingUnits: []*tgsrlv1.PendingUnit{{PendingUnitId: "old-1", Priority: 1}, {PendingUnitId: "old-2", Priority: 10}},
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
	victim := Victim{Allocation: &tgsrlv1.Allocation{AllocationId: "allocation-1", PendingUnitId: "unit-1", RuntimeUnitId: "runtime-1", Generation: 2, DeviceIds: []string{"device-1"}}}
	if reason, ok := CanRelease(snapshot, victim, true); !ok || reason != "" {
		t.Fatalf("CanRelease() = (%q, %v), want safe", reason, ok)
	}
	victim.Allocation.Generation = 0
	if reason, ok := CanRelease(snapshot, victim, true); ok || reason != FallbackNotExpressible {
		t.Fatalf("CanRelease(missing generation) = (%q, %v)", reason, ok)
	}
}
