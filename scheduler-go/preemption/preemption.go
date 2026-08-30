package preemption

import (
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
		if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE || allocation.Priority == nil {
			continue
		}
		if allocation.GetPriority() >= unit.GetPriority() {
			continue
		}
		pressure := float64(allocation.GetPriority())
		victims = append(victims, Victim{Allocation: allocation, Pressure: pressure})
	}
	sort.SliceStable(victims, func(i, j int) bool {
		if victims[i].Pressure != victims[j].Pressure {
			return victims[i].Pressure < victims[j].Pressure
		}
		leftDevices := append([]string(nil), victims[i].Allocation.GetDeviceIds()...)
		rightDevices := append([]string(nil), victims[j].Allocation.GetDeviceIds()...)
		sort.Strings(leftDevices)
		sort.Strings(rightDevices)
		for index := 0; index < len(leftDevices) && index < len(rightDevices); index++ {
			if leftDevices[index] != rightDevices[index] {
				return leftDevices[index] < rightDevices[index]
			}
		}
		if len(leftDevices) != len(rightDevices) {
			return len(leftDevices) < len(rightDevices)
		}
		return victims[i].Allocation.GetAllocationId() < victims[j].Allocation.GetAllocationId()
	})
	return victims
}

// CanRelease verifies that a victim can be represented as an explicit,
// generation-fenced release action in the current protocol.
func CanRelease(snapshot *tgsrlv1.ClusterSnapshot, victim Victim, safePoint bool) (string, bool) {
	allocation := victim.Allocation
	if !safePoint {
		return FallbackUnsafe, false
	}
	if allocation == nil || allocation.Priority == nil || allocation.GetAllocationId() == "" || allocation.GetPendingUnitId() == "" || allocation.GetRuntimeUnitId() == "" || allocation.GetSandboxId() == "" || allocation.GetBindingId() == "" || allocation.GetGeneration() == 0 || len(allocation.GetDeviceIds()) != 1 {
		return FallbackNotExpressible, false
	}
	for _, device := range snapshot.GetDevices() {
		if device.GetDeviceId() == allocation.GetDeviceIds()[0] && supportsRelease(device.GetCapabilities()) {
			return "", true
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
