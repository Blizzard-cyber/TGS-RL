package candidates

import (
	"container/heap"
	"math"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/constraints"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scoring"
	"google.golang.org/protobuf/proto"
)

const scorePrecision = 1_000_000_000.0

type deviceResources struct {
	capacity      *tgsrlv1.ResourceVector
	allocatable   *tgsrlv1.ResourceVector
	used          *tgsrlv1.ResourceVector
	overcommitted bool
}

type deviceState struct {
	device     *tgsrlv1.Device
	resources  *deviceResources
	evaluation pairEvaluation
}

func buildDeviceStates(snapshot *tgsrlv1.ClusterSnapshot, devices []*tgsrlv1.Device, reclaimedAllocationIDs map[string]struct{}) []*deviceState {
	states := make([]*deviceState, 0, len(devices))
	byID := make(map[string]*deviceState, len(devices))
	for _, device := range devices {
		state := &deviceState{
			device: device,
			resources: &deviceResources{
				capacity:    cloneResources(device.GetCapacity()),
				allocatable: cloneResources(device.GetAllocatable()),
				used:        &tgsrlv1.ResourceVector{},
			},
		}
		states = append(states, state)
		byID[device.GetDeviceId()] = state
	}

	allocations := append([]*tgsrlv1.Allocation(nil), snapshot.GetAllocations()...)
	sort.SliceStable(allocations, func(i, j int) bool {
		return allocations[i].GetAllocationId() < allocations[j].GetAllocationId()
	})
	for _, allocation := range allocations {
		if allocation == nil || (allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING && allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE) {
			continue
		}
		if _, reclaimed := reclaimedAllocationIDs[allocation.GetAllocationId()]; reclaimed {
			// Reclaimed resources are already absent from used below. Return them
			// to the snapshot's remaining-resource ledger exactly once.
			state := byID[allocation.GetDeviceIds()[0]]
			if state != nil {
				addResourcesCapped(state.resources.allocatable, allocation.GetResources(), state.resources.capacity)
			}
			continue
		}
		for _, deviceID := range allocation.GetDeviceIds() {
			state := byID[deviceID]
			if state == nil {
				continue
			}
			if !constraints.SumFits(state.resources.used, allocation.GetResources(), state.resources.capacity) ||
				!constraints.AcceleratorShareFits(state.resources.used.GetAcceleratorUnits(), allocation.GetResources().GetAcceleratorUnits()) {
				state.resources.overcommitted = true
			}
			addResources(state.resources.used, allocation.GetResources())
		}
	}
	return states
}

func addResourcesCapped(target, value, capacity *tgsrlv1.ResourceVector) {
	if target == nil || value == nil {
		return
	}
	target.CpuMillis = saturatingAddCapped(target.GetCpuMillis(), value.GetCpuMillis(), capacity.GetCpuMillis())
	target.MemoryBytes = saturatingAddCapped(target.GetMemoryBytes(), value.GetMemoryBytes(), capacity.GetMemoryBytes())
	target.AcceleratorUnits = math.Min(capacity.GetAcceleratorUnits(), target.GetAcceleratorUnits()+value.GetAcceleratorUnits())
	target.EphemeralStorageBytes = saturatingAddCapped(target.GetEphemeralStorageBytes(), value.GetEphemeralStorageBytes(), capacity.GetEphemeralStorageBytes())
	target.NetworkBandwidthBps = saturatingAddCapped(target.GetNetworkBandwidthBps(), value.GetNetworkBandwidthBps(), capacity.GetNetworkBandwidthBps())
}

func saturatingAddCapped(left, right, limit uint64) uint64 {
	if left >= limit || right > limit-left {
		return limit
	}
	return left + right
}

func cloneResources(resources *tgsrlv1.ResourceVector) *tgsrlv1.ResourceVector {
	if resources == nil {
		return &tgsrlv1.ResourceVector{}
	}
	return proto.Clone(resources).(*tgsrlv1.ResourceVector)
}

func cloneOptionalResources(resources *tgsrlv1.ResourceVector) *tgsrlv1.ResourceVector {
	if resources == nil {
		return nil
	}
	return proto.Clone(resources).(*tgsrlv1.ResourceVector)
}

func cloneCapabilities(capabilities *tgsrlv1.CapabilitySet) *tgsrlv1.CapabilitySet {
	if capabilities == nil {
		return nil
	}
	return proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)
}

func addResources(target, value *tgsrlv1.ResourceVector) {
	if target == nil || value == nil {
		return
	}
	target.CpuMillis = saturatingUintAdd(target.GetCpuMillis(), value.GetCpuMillis())
	target.MemoryBytes = saturatingUintAdd(target.GetMemoryBytes(), value.GetMemoryBytes())
	target.AcceleratorUnits += value.GetAcceleratorUnits()
	target.EphemeralStorageBytes = saturatingUintAdd(target.GetEphemeralStorageBytes(), value.GetEphemeralStorageBytes())
	target.NetworkBandwidthBps = saturatingUintAdd(target.GetNetworkBandwidthBps(), value.GetNetworkBandwidthBps())
}

func consume(resources *deviceResources, request *tgsrlv1.ResourceVector) {
	if resources == nil || request == nil {
		return
	}
	resources.allocatable.CpuMillis -= request.GetCpuMillis()
	resources.allocatable.MemoryBytes -= request.GetMemoryBytes()
	resources.allocatable.AcceleratorUnits = math.Max(0, resources.allocatable.GetAcceleratorUnits()-request.GetAcceleratorUnits())
	resources.allocatable.EphemeralStorageBytes -= request.GetEphemeralStorageBytes()
	resources.allocatable.NetworkBandwidthBps -= request.GetNetworkBandwidthBps()
	addResources(resources.used, request)
}

type scoreValues struct {
	capacityHeadroom float64
	shareHeadroom    float64
	ready            float64
	traceAffinity    float64
	total            float64
}

func (s scoreValues) components() map[string]float64 {
	return scoring.Components(s.capacityHeadroom, s.shareHeadroom, s.ready, s.traceAffinity)
}

func scorePair(intent *tgsrlv1.SchedulingIntent, device *tgsrlv1.Device, resources *deviceResources, request *tgsrlv1.ResourceVector) scoreValues {
	ratioSum := 0.0
	ratioCount := 0
	addUintRatio := func(requested, available uint64) {
		if requested > 0 && available > 0 {
			ratioSum += float64(available-requested) / float64(available)
			ratioCount++
		}
	}
	addUintRatio(request.GetCpuMillis(), resources.allocatable.GetCpuMillis())
	addUintRatio(request.GetMemoryBytes(), resources.allocatable.GetMemoryBytes())
	addUintRatio(request.GetEphemeralStorageBytes(), resources.allocatable.GetEphemeralStorageBytes())
	addUintRatio(request.GetNetworkBandwidthBps(), resources.allocatable.GetNetworkBandwidthBps())
	if request.GetAcceleratorUnits() > 0 && resources.allocatable.GetAcceleratorUnits() > 0 {
		ratioSum += (resources.allocatable.GetAcceleratorUnits() - request.GetAcceleratorUnits()) / resources.allocatable.GetAcceleratorUnits()
		ratioCount++
	}
	headroom := 1.0
	if ratioCount > 0 {
		headroom = ratioSum / float64(ratioCount)
	}

	values := scoreValues{
		capacityHeadroom: roundScore(headroom * 0.70),
		shareHeadroom:    roundScore(math.Max(0, 1-resources.used.GetAcceleratorUnits()-request.GetAcceleratorUnits()) * 0.20),
		ready:            0.10,
		traceAffinity:    roundScore(scoring.TraceAffinity(intent, device) * 0.30),
	}
	values.total = roundScore(values.capacityHeadroom + values.shareHeadroom + values.ready + values.traceAffinity)
	return values
}

func roundScore(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return math.Round(value*scorePrecision) / scorePrecision
}

type deviceFrontier []*deviceState

func (f deviceFrontier) Len() int { return len(f) }

func (f deviceFrontier) Less(i, j int) bool {
	return compareRank(
		f[i].evaluation.score.total, f[i].device.GetDeviceId(), "", "",
		f[j].evaluation.score.total, f[j].device.GetDeviceId(), "", "",
	) < 0
}

func (f deviceFrontier) Swap(i, j int) { f[i], f[j] = f[j], f[i] }

func (f *deviceFrontier) Push(value any) { *f = append(*f, value.(*deviceState)) }

func (f *deviceFrontier) Pop() any {
	old := *f
	last := old[len(old)-1]
	old[len(old)-1] = nil
	*f = old[:len(old)-1]
	return last
}

var _ heap.Interface = (*deviceFrontier)(nil)
