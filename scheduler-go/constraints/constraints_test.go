package constraints

import (
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDefaultConstraints(t *testing.T) {
	unit := &tgsrlv1.PendingUnit{
		PendingUnitId:      "u1",
		RequestedResources: &tgsrlv1.ResourceVector{CpuMillis: 500, MemoryBytes: 1024, AcceleratorUnits: 0.25},
		RequiredCapabilities: &tgsrlv1.CapabilitySet{
			Names: []string{"logical-cpu"},
		},
	}
	device := &tgsrlv1.Device{
		DeviceId:    "d1",
		Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
		Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 2048, AcceleratorUnits: 1},
		Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 2048, AcceleratorUnits: 1},
		Capabilities: &tgsrlv1.CapabilitySet{
			Names: []string{"logical-cpu"},
		},
	}
	ctx := Context{Unit: unit, Device: device}
	for _, check := range Default() {
		if err := check.Check(ctx); err != nil {
			t.Fatalf("%s.Check() error = %v", check.Name(), err)
		}
	}

	device.Health = tgsrlv1.DeviceHealth_DEVICE_HEALTH_DRAINING
	err := ReadyDevice{}.Check(ctx)
	if err == nil {
		t.Fatal("ReadyDevice.Check() unexpectedly succeeded for draining device")
	}

	device.Health = tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY
	device.Allocatable.CpuMillis = 100
	err = SufficientResources{}.Check(ctx)
	if err == nil {
		t.Fatal("SufficientResources.Check() unexpectedly succeeded for undersized device")
	}

	device.Allocatable.CpuMillis = 1000
	device.Capabilities.Names = []string{"other"}
	err = CapabilityMatch{}.Check(ctx)
	if err == nil {
		t.Fatal("CapabilityMatch.Check() unexpectedly succeeded for mismatched capability")
	}
}

func TestStableUnitOrderAndScores(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	units := []*tgsrlv1.PendingUnit{
		{PendingUnitId: "b", Priority: 1, QueuedAt: timestamppb.New(now.Add(time.Second))},
		{PendingUnitId: "a", Priority: 5, QueuedAt: timestamppb.New(now)},
		{PendingUnitId: "c", Priority: 5, QueuedAt: timestamppb.New(now.Add(time.Second))},
	}
	ordered := StableUnitOrder(units)
	if got := []string{ordered[0].GetPendingUnitId(), ordered[1].GetPendingUnitId(), ordered[2].GetPendingUnitId()}; got[0] != "a" || got[1] != "c" || got[2] != "b" {
		t.Fatalf("StableUnitOrder() = %v, want [a c b]", got)
	}

	device := &tgsrlv1.Device{
		Capacity: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 2000, AcceleratorUnits: 2},
	}
	unit := &tgsrlv1.PendingUnit{
		RequestedResources: &tgsrlv1.ResourceVector{CpuMillis: 500, MemoryBytes: 1000, AcceleratorUnits: 1},
	}
	if got := Headroom(device, unit); got <= 0 || got >= 1 {
		t.Fatalf("Headroom() = %f, want normalized fraction in (0,1)", got)
	}
	snapshot := &tgsrlv1.ClusterSnapshot{
		Allocations: []*tgsrlv1.Allocation{
			{AllocationId: "a1", DeviceIds: []string{"d1"}, State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE},
			{AllocationId: "a2", DeviceIds: []string{"d1"}, State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE},
		},
	}
	if got := ShareScore(snapshot, "d1"); got != 1.0/3.0 {
		t.Fatalf("ShareScore() = %f, want %f", got, 1.0/3.0)
	}
}
