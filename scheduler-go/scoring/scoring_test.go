package scoring

import (
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func TestWeightedScore(t *testing.T) {
	snapshot := &tgsrlv1.ClusterSnapshot{
		Allocations: []*tgsrlv1.Allocation{
			{AllocationId: "a1", DeviceIds: []string{"d1"}, State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE},
		},
	}
	intent := &tgsrlv1.SchedulingIntent{TraceId: "trace-1", JobId: "job-1"}
	unit := &tgsrlv1.PendingUnit{
		Priority:           20,
		RequestedResources: &tgsrlv1.ResourceVector{CpuMillis: 250, MemoryBytes: 256, AcceleratorUnits: 0.5},
	}
	device := &tgsrlv1.Device{
		DeviceId:    "d1",
		Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 2},
		Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 2},
		Labels:      map[string]string{"trace": "trace-1", "job": "job-1"},
	}

	components := Default().Score(snapshot, intent, unit, device)
	if components["capacity_headroom"] <= 0 {
		t.Fatalf("capacity_headroom = %f, want > 0", components["capacity_headroom"])
	}
	if components["share_headroom"] <= 0 {
		t.Fatalf("share_headroom = %f, want > 0", components["share_headroom"])
	}
	if components["priority"] <= 0 {
		t.Fatalf("priority = %f, want > 0", components["priority"])
	}
	if components["trace_affinity"] <= 0 {
		t.Fatalf("trace_affinity = %f, want > 0", components["trace_affinity"])
	}
}
