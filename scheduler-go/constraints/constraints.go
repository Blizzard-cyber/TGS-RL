package constraints

import (
	"fmt"
	"math"
	"sort"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// Context is the immutable candidate-evaluation surface passed to constraints.
type Context struct {
	Snapshot *tgsrlv1.ClusterSnapshot
	Intent   *tgsrlv1.SchedulingIntent
	Unit     *tgsrlv1.PendingUnit
	Device   *tgsrlv1.Device
}

// Constraint rejects infeasible unit/device pairs.
type Constraint interface {
	Name() string
	Check(Context) error
}

// Reason classifies a constraint failure with a stable proto rejection code.
type Reason struct {
	Code   tgsrlv1.CandidateRejectionReason
	Detail string
}

func (r *Reason) Error() string { return r.Detail }

// Compose runs constraints in order and returns the first rejection.
func Compose(constraints ...Constraint) Constraint {
	return composed(constraints)
}

type composed []Constraint

func (c composed) Name() string { return "compose" }

func (c composed) Check(ctx Context) error {
	for _, constraint := range c {
		if constraint == nil {
			continue
		}
		if err := constraint.Check(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Default returns conservative production-ready constraints.
func Default() []Constraint {
	return []Constraint{
		ReadyDevice{},
		SufficientResources{},
		CapabilityMatch{},
	}
}

// ReadyDevice requires the device to be ready.
type ReadyDevice struct{}

func (ReadyDevice) Name() string { return "ready_device" }

func (ReadyDevice) Check(ctx Context) error {
	if ctx.Device == nil || ctx.Device.GetHealth() != tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY {
		return &Reason{
			Code:   tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VALIDITY_RULE,
			Detail: "device is not READY",
		}
	}
	return nil
}

// SufficientResources rejects devices without enough allocatable resources.
type SufficientResources struct{}

func (SufficientResources) Name() string { return "sufficient_resources" }

func (SufficientResources) Check(ctx Context) error {
	if ctx.Device == nil || ctx.Unit == nil {
		return &Reason{
			Code:   tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES,
			Detail: "device or pending unit is missing",
		}
	}
	if ctx.Unit.GetRequestedResources().GetCpuMillis() > ctx.Device.GetAllocatable().GetCpuMillis() ||
		ctx.Unit.GetRequestedResources().GetMemoryBytes() > ctx.Device.GetAllocatable().GetMemoryBytes() ||
		ctx.Unit.GetRequestedResources().GetEphemeralStorageBytes() > ctx.Device.GetAllocatable().GetEphemeralStorageBytes() ||
		ctx.Unit.GetRequestedResources().GetNetworkBandwidthBps() > ctx.Device.GetAllocatable().GetNetworkBandwidthBps() ||
		ctx.Unit.GetRequestedResources().GetAcceleratorUnits() > ctx.Device.GetAllocatable().GetAcceleratorUnits() {
		return &Reason{
			Code:   tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES,
			Detail: "device allocatable resources are insufficient",
		}
	}
	return nil
}

// CapabilityMatch requires all named capabilities from the unit to be present.
type CapabilityMatch struct{}

func (CapabilityMatch) Name() string { return "capability_match" }

func (CapabilityMatch) Check(ctx Context) error {
	if ctx.Device == nil || ctx.Unit == nil {
		return &Reason{
			Code:   tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH,
			Detail: "device or pending unit is missing",
		}
	}
	deviceNames := make(map[string]struct{}, len(ctx.Device.GetCapabilities().GetNames()))
	for _, name := range ctx.Device.GetCapabilities().GetNames() {
		deviceNames[strings.ToLower(name)] = struct{}{}
	}
	for _, required := range ctx.Unit.GetRequiredCapabilities().GetNames() {
		if _, ok := deviceNames[strings.ToLower(required)]; !ok {
			return &Reason{
				Code:   tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH,
				Detail: fmt.Sprintf("device lacks capability %q", required),
			}
		}
	}
	return nil
}

// Headroom reports normalized remaining capacity after placing the unit.
func Headroom(device *tgsrlv1.Device, unit *tgsrlv1.PendingUnit) float64 {
	if device == nil || unit == nil {
		return 0
	}
	capacity := device.GetCapacity()
	requested := unit.GetRequestedResources()
	if capacity.GetCpuMillis() == 0 || capacity.GetMemoryBytes() == 0 {
		return 0
	}
	cpu := 1 - float64(requested.GetCpuMillis())/float64(capacity.GetCpuMillis())
	mem := 1 - float64(requested.GetMemoryBytes())/float64(capacity.GetMemoryBytes())
	acc := 1.0
	if capacity.GetAcceleratorUnits() > 0 {
		acc = 1 - requested.GetAcceleratorUnits()/capacity.GetAcceleratorUnits()
	}
	return roundNonNegative((cpu + mem + acc) / 3)
}

// ShareScore penalizes devices with more active allocations.
func ShareScore(snapshot *tgsrlv1.ClusterSnapshot, deviceID string) float64 {
	if snapshot == nil || deviceID == "" {
		return 0
	}
	var total int
	for _, allocation := range snapshot.GetAllocations() {
		for _, current := range allocation.GetDeviceIds() {
			if current == deviceID && allocation.GetState() == tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
				total++
			}
		}
	}
	return 1 / float64(total+1)
}

// StableUnitOrder returns a deterministic pending-unit order.
func StableUnitOrder(units []*tgsrlv1.PendingUnit) []*tgsrlv1.PendingUnit {
	out := append([]*tgsrlv1.PendingUnit(nil), units...)
	sort.SliceStable(out, func(i, j int) bool {
		left, right := out[i], out[j]
		if left.GetPriority() != right.GetPriority() {
			return left.GetPriority() > right.GetPriority()
		}
		if !left.GetQueuedAt().AsTime().Equal(right.GetQueuedAt().AsTime()) {
			return left.GetQueuedAt().AsTime().Before(right.GetQueuedAt().AsTime())
		}
		return left.GetPendingUnitId() < right.GetPendingUnitId()
	})
	return out
}

func roundNonNegative(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0
	}
	return math.Round(value*1_000_000) / 1_000_000
}
