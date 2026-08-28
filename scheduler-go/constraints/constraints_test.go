package constraints

import (
	"math"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestResourceLessOrEqualCoversEveryDimension(t *testing.T) {
	available := &tgsrlv1.ResourceVector{
		CpuMillis:             1000,
		MemoryBytes:           2000,
		AcceleratorUnits:      0.75,
		EphemeralStorageBytes: 3000,
		NetworkBandwidthBps:   4000,
	}
	request := &tgsrlv1.ResourceVector{
		CpuMillis:             1000,
		MemoryBytes:           2000,
		AcceleratorUnits:      0.75,
		EphemeralStorageBytes: 3000,
		NetworkBandwidthBps:   4000,
	}
	if !ResourceLessOrEqual(request, available) {
		t.Fatal("equal resource vectors should fit")
	}

	for name, mutate := range map[string]func(*tgsrlv1.ResourceVector){
		"cpu":         func(r *tgsrlv1.ResourceVector) { r.CpuMillis++ },
		"memory":      func(r *tgsrlv1.ResourceVector) { r.MemoryBytes++ },
		"accelerator": func(r *tgsrlv1.ResourceVector) { r.AcceleratorUnits += 0.01 },
		"storage":     func(r *tgsrlv1.ResourceVector) { r.EphemeralStorageBytes++ },
		"network":     func(r *tgsrlv1.ResourceVector) { r.NetworkBandwidthBps++ },
	} {
		t.Run(name, func(t *testing.T) {
			over := proto.Clone(request).(*tgsrlv1.ResourceVector)
			mutate(over)
			if ResourceLessOrEqual(over, available) {
				t.Fatal("oversized request unexpectedly fit")
			}
		})
	}
}

func TestSumFitsRejectsOverflowAndInvalidShare(t *testing.T) {
	max := ^uint64(0)
	if SumFits(
		&tgsrlv1.ResourceVector{CpuMillis: max},
		&tgsrlv1.ResourceVector{CpuMillis: 1},
		&tgsrlv1.ResourceVector{CpuMillis: max},
	) {
		t.Fatal("overflowing sum unexpectedly fit")
	}
	if AcceleratorShareFits(0.8, 0.3) {
		t.Fatal("aggregate accelerator share above one unexpectedly fit")
	}
	if AcceleratorShareFits(math.NaN(), 0.1) {
		t.Fatal("NaN accelerator share unexpectedly fit")
	}
	if !AcceleratorShareFits(0.75, 0.25) {
		t.Fatal("aggregate accelerator share equal to one should fit")
	}
}

func TestResourceLessOrEqualRejectsInvalidAcceleratorValues(t *testing.T) {
	available := &tgsrlv1.ResourceVector{AcceleratorUnits: 1}
	for name, request := range map[string]*tgsrlv1.ResourceVector{
		"negative": {AcceleratorUnits: -0.1},
		"nan":      {AcceleratorUnits: math.NaN()},
		"infinite": {AcceleratorUnits: math.Inf(1)},
	} {
		t.Run(name, func(t *testing.T) {
			if ResourceLessOrEqual(request, available) {
				t.Fatal("invalid accelerator request unexpectedly fit")
			}
		})
	}
}

func TestStableUnitOrder(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	units := []*tgsrlv1.PendingUnit{
		nil,
		{PendingUnitId: "b", Priority: 1, QueuedAt: timestamppb.New(now.Add(time.Second))},
		{PendingUnitId: "a", Priority: 5, QueuedAt: timestamppb.New(now)},
		{PendingUnitId: "c", Priority: 5, QueuedAt: timestamppb.New(now.Add(time.Second))},
	}
	originalFirst := units[0]
	ordered := StableUnitOrder(units)
	if got := []string{ordered[0].GetPendingUnitId(), ordered[1].GetPendingUnitId(), ordered[2].GetPendingUnitId()}; got[0] != "a" || got[1] != "c" || got[2] != "b" {
		t.Fatalf("StableUnitOrder() = %v, want [a c b nil]", got)
	}
	if ordered[3] != nil {
		t.Fatalf("StableUnitOrder() final unit = %v, want nil", ordered[3])
	}
	if units[0] != originalFirst {
		t.Fatal("StableUnitOrder mutated input slice")
	}
}
