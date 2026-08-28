// Package constraints contains stateless primitives shared by the
// authoritative candidate engine. It intentionally does not expose a default
// evaluator; the engine owns readiness, capability, safe-point, allocation,
// and per-unit resource-ledger semantics.
package constraints

import (
	"math"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// ResourceLessOrEqual reports whether every request dimension fits within the
// available vector. A nil request is empty; a nil available vector fits only
// an empty request. Invalid accelerator values fail closed.
func ResourceLessOrEqual(request, available *tgsrlv1.ResourceVector) bool {
	if request != nil && (invalidFloat(request.GetAcceleratorUnits()) || request.GetAcceleratorUnits() < 0) {
		return false
	}
	if available != nil && (invalidFloat(available.GetAcceleratorUnits()) || available.GetAcceleratorUnits() < 0) {
		return false
	}
	if request == nil {
		return true
	}
	if available == nil {
		return ResourceIsZero(request)
	}
	return request.GetCpuMillis() <= available.GetCpuMillis() &&
		request.GetMemoryBytes() <= available.GetMemoryBytes() &&
		request.GetAcceleratorUnits() <= available.GetAcceleratorUnits() &&
		request.GetEphemeralStorageBytes() <= available.GetEphemeralStorageBytes() &&
		request.GetNetworkBandwidthBps() <= available.GetNetworkBandwidthBps()
}

// SumFits reports whether used+request fits every capacity dimension without
// unsigned overflow. Invalid accelerator values fail closed.
func SumFits(used, request, capacity *tgsrlv1.ResourceVector) bool {
	if used == nil {
		used = &tgsrlv1.ResourceVector{}
	}
	if request == nil {
		request = &tgsrlv1.ResourceVector{}
	}
	if capacity == nil {
		return ResourceIsZero(used) && ResourceIsZero(request)
	}
	return uintSumFits(used.GetCpuMillis(), request.GetCpuMillis(), capacity.GetCpuMillis()) &&
		uintSumFits(used.GetMemoryBytes(), request.GetMemoryBytes(), capacity.GetMemoryBytes()) &&
		floatSumFits(used.GetAcceleratorUnits(), request.GetAcceleratorUnits(), capacity.GetAcceleratorUnits()) &&
		uintSumFits(used.GetEphemeralStorageBytes(), request.GetEphemeralStorageBytes(), capacity.GetEphemeralStorageBytes()) &&
		uintSumFits(used.GetNetworkBandwidthBps(), request.GetNetworkBandwidthBps(), capacity.GetNetworkBandwidthBps())
}

// AcceleratorShareFits applies the scheduler's aggregate accelerator-share
// invariant: active share plus the request must not exceed one.
func AcceleratorShareFits(used, request float64) bool {
	return floatSumFits(used, request, 1)
}

// ResourceIsZero reports whether all resource dimensions are zero. Invalid
// accelerator values are not considered zero.
func ResourceIsZero(resources *tgsrlv1.ResourceVector) bool {
	return resources == nil || (resources.GetCpuMillis() == 0 &&
		resources.GetMemoryBytes() == 0 &&
		resources.GetAcceleratorUnits() == 0 &&
		resources.GetEphemeralStorageBytes() == 0 &&
		resources.GetNetworkBandwidthBps() == 0)
}

// StableUnitOrder returns a deterministic copy ordered by priority descending,
// queue time ascending, then pending-unit ID ascending. Nil units sort last.
func StableUnitOrder(units []*tgsrlv1.PendingUnit) []*tgsrlv1.PendingUnit {
	out := append([]*tgsrlv1.PendingUnit(nil), units...)
	sort.SliceStable(out, func(i, j int) bool {
		left, right := out[i], out[j]
		if left == nil || right == nil {
			return left != nil
		}
		if left.GetPriority() != right.GetPriority() {
			return left.GetPriority() > right.GetPriority()
		}
		leftQueued, rightQueued := left.GetQueuedAt(), right.GetQueuedAt()
		if leftQueued == nil || rightQueued == nil {
			if leftQueued == nil && rightQueued != nil {
				return false
			}
			if leftQueued != nil {
				return true
			}
		} else if !leftQueued.AsTime().Equal(rightQueued.AsTime()) {
			return leftQueued.AsTime().Before(rightQueued.AsTime())
		}
		return left.GetPendingUnitId() < right.GetPendingUnitId()
	})
	return out
}

func uintSumFits(left, right, limit uint64) bool {
	return left <= limit && right <= limit-left
}

func floatSumFits(left, right, limit float64) bool {
	if invalidFloat(left) || invalidFloat(right) || invalidFloat(limit) || left < 0 || right < 0 || limit < 0 {
		return false
	}
	return left <= limit && right <= limit-left
}

func invalidFloat(value float64) bool {
	return math.IsNaN(value) || math.IsInf(value, 0)
}
