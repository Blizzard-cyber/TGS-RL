package candidates

import (
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/constraints"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scoring"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTopKEvaluateBuildsCandidatesAndRejections(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	intent := &tgsrlv1.SchedulingIntent{
		ExecutionId:          "exec",
		StageId:              "stage",
		Version:              1,
		IdempotencyKey:       "intent-key",
		PolicyVersion:        "policy-1",
		RequiredCapabilities: &tgsrlv1.CapabilitySet{Names: []string{"logical-cpu"}},
		ValidUntil:           timestamppb.New(now.Add(time.Minute)),
	}
	snapshot := &tgsrlv1.ClusterSnapshot{
		SnapshotId: "snapshot-1",
		Revision:   7,
		Devices: []*tgsrlv1.Device{
			{
				DeviceId:    "good",
				Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
				Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
				Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
				Capabilities: &tgsrlv1.CapabilitySet{
					Names: []string{"logical-cpu"},
				},
			},
			{
				DeviceId:    "bad",
				Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_DRAINING,
				Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
				Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
				Capabilities: &tgsrlv1.CapabilitySet{
					Names: []string{"logical-cpu"},
				},
			},
		},
		PendingUnits: []*tgsrlv1.PendingUnit{{
			PendingUnitId:        "unit-1",
			ExecutionId:          "exec",
			StageId:              "stage",
			IntentVersion:        1,
			Priority:             10,
			QueuedAt:             timestamppb.New(now),
			RequestedResources:   &tgsrlv1.ResourceVector{CpuMillis: 250, MemoryBytes: 256, AcceleratorUnits: 0.5},
			RequiredCapabilities: &tgsrlv1.CapabilitySet{Names: []string{"logical-cpu"}},
		}},
	}

	got := (TopK{Limit: 4}).Evaluate(snapshot, intent, constraints.Default(), func(snapshot *tgsrlv1.ClusterSnapshot, unit *tgsrlv1.PendingUnit, device *tgsrlv1.Device) map[string]float64 {
		return scoring.Default().Score(snapshot, intent, unit, device)
	})
	if len(got) != 2 {
		t.Fatalf("Evaluate() returned %d entries, want 2", len(got))
	}
	var candidateCount, rejectionCount int
	for _, item := range got {
		if item.Candidate != nil {
			candidateCount++
			if item.Candidate.GetPlan() == nil || len(item.Candidate.GetPlan().GetActions()) != 1 {
				t.Fatalf("candidate plan = %+v, want one action", item.Candidate.GetPlan())
			}
		}
		if item.Rejection != nil {
			rejectionCount++
		}
	}
	if candidateCount != 1 || rejectionCount != 1 {
		t.Fatalf("candidateCount=%d rejectionCount=%d, want 1/1", candidateCount, rejectionCount)
	}
}
