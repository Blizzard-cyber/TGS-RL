package preemption

import (
	"math"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

const (
	FallbackDisabled       = "PREEMPTION_DISABLED"
	FallbackUnsafe         = "PREEMPTION_UNSAFE"
	FallbackNotExpressible = "PREEMPTION_NOT_EXPRESSIBLE"
	FallbackInsufficient   = "PREEMPTION_INSUFFICIENT"
)

// Victim describes an active allocation that may be released to free capacity.
type Victim struct {
	Allocation *tgsrlv1.Allocation
	Pressure   float64
}

// Strategy proposes preemption victims.
type Strategy interface {
	Name() string
	Pick(snapshot *tgsrlv1.ClusterSnapshot, unit *tgsrlv1.PendingUnit) []Victim
}

// NoOp never preempts existing allocations.
type NoOp struct{}

func (NoOp) Name() string { return "noop" }
func (NoOp) Pick(*tgsrlv1.ClusterSnapshot, *tgsrlv1.PendingUnit) []Victim {
	return nil
}

// LowPriorityFirst picks lower-priority allocations first using available
// allocation metadata only.
type LowPriorityFirst struct{}

func (LowPriorityFirst) Name() string { return "low_priority_first" }

func (LowPriorityFirst) Pick(snapshot *tgsrlv1.ClusterSnapshot, unit *tgsrlv1.PendingUnit) []Victim {
	if snapshot == nil || unit == nil {
		return nil
	}
	victims := make([]Victim, 0, len(snapshot.GetAllocations()))
	for _, allocation := range snapshot.GetAllocations() {
		if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
			continue
		}
		if allocationPriority(snapshot, allocation) >= unit.GetPriority() {
			continue
		}
		// The current Allocation DTO does not carry authoritative workload
		// priority. Treat only metadata-complete, explicitly lower-priority
		// allocations as candidates; unknown priority is handled conservatively.
		pressure := float64(allocationPriority(snapshot, allocation))
		victims = append(victims, Victim{Allocation: allocation, Pressure: pressure})
	}
	sort.SliceStable(victims, func(i, j int) bool {
		if victims[i].Pressure != victims[j].Pressure {
			return victims[i].Pressure < victims[j].Pressure
		}
		return victims[i].Allocation.GetAllocationId() < victims[j].Allocation.GetAllocationId()
	})
	return victims
}

func allocationPriority(snapshot *tgsrlv1.ClusterSnapshot, allocation *tgsrlv1.Allocation) int32 {
	if snapshot == nil || allocation == nil {
		return 0
	}
	for _, unit := range snapshot.GetPendingUnits() {
		if unit.GetPendingUnitId() == allocation.GetPendingUnitId() {
			return unit.GetPriority()
		}
	}
	return math.MaxInt32
}

// CanRelease verifies that a victim can be represented as an explicit,
// generation-fenced release action in the current protocol.
func CanRelease(snapshot *tgsrlv1.ClusterSnapshot, victim Victim, safePoint bool) (string, bool) {
	allocation := victim.Allocation
	if !safePoint {
		return FallbackUnsafe, false
	}
	if allocation == nil || allocation.GetAllocationId() == "" || allocation.GetPendingUnitId() == "" || allocation.GetRuntimeUnitId() == "" || allocation.GetGeneration() == 0 || len(allocation.GetDeviceIds()) == 0 {
		return FallbackNotExpressible, false
	}
	for _, deviceID := range allocation.GetDeviceIds() {
		for _, device := range snapshot.GetDevices() {
			if device.GetDeviceId() == deviceID && supportsRelease(device.GetCapabilities()) {
				return "", true
			}
		}
	}
	return FallbackNotExpressible, false
}

func supportsRelease(capabilities *tgsrlv1.CapabilitySet) bool {
	if capabilities == nil {
		return false
	}
	for _, action := range capabilities.GetSupportedActions() {
		if action == "release" {
			return true
		}
	}
	return false
}
