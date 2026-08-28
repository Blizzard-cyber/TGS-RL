package scheduler

import (
	"math"
	"sort"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

const scorePrecision = 1_000_000_000.0

type deviceResources struct {
	capacity      *tgsrlv1.ResourceVector
	allocatable   *tgsrlv1.ResourceVector
	used          *tgsrlv1.ResourceVector
	overcommitted bool
}

func buildDeviceResources(snapshot *tgsrlv1.ClusterSnapshot, devices []*tgsrlv1.Device) map[string]*deviceResources {
	result := make(map[string]*deviceResources, len(devices))
	for _, device := range devices {
		result[device.GetDeviceId()] = &deviceResources{
			capacity:    cloneResources(device.GetCapacity()),
			allocatable: cloneResources(device.GetAllocatable()),
			used:        &tgsrlv1.ResourceVector{},
		}
	}
	allocations := append([]*tgsrlv1.Allocation(nil), snapshot.GetAllocations()...)
	sort.Slice(allocations, func(i, j int) bool { return allocations[i].GetAllocationId() < allocations[j].GetAllocationId() })
	for _, allocation := range allocations {
		if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING && allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
			continue
		}
		for _, deviceID := range allocation.GetDeviceIds() {
			device := result[deviceID]
			if !sumFits(device.used, allocation.GetResources(), device.capacity) ||
				device.used.GetAcceleratorUnits()+allocation.GetResources().GetAcceleratorUnits() > 1 {
				device.overcommitted = true
			}
			addResources(device.used, allocation.GetResources())
		}
	}
	return result
}

func resourceLessOrEqual(left, right *tgsrlv1.ResourceVector) bool {
	if left == nil {
		return true
	}
	if right == nil {
		return resourceIsZero(left)
	}
	return left.GetCpuMillis() <= right.GetCpuMillis() &&
		left.GetMemoryBytes() <= right.GetMemoryBytes() &&
		left.GetAcceleratorUnits() <= right.GetAcceleratorUnits() &&
		left.GetEphemeralStorageBytes() <= right.GetEphemeralStorageBytes() &&
		left.GetNetworkBandwidthBps() <= right.GetNetworkBandwidthBps()
}

func sumFits(left, right, limit *tgsrlv1.ResourceVector) bool {
	if left == nil {
		left = &tgsrlv1.ResourceVector{}
	}
	if right == nil {
		right = &tgsrlv1.ResourceVector{}
	}
	if limit == nil {
		return resourceIsZero(left) && resourceIsZero(right)
	}
	return uintSumFits(left.GetCpuMillis(), right.GetCpuMillis(), limit.GetCpuMillis()) &&
		uintSumFits(left.GetMemoryBytes(), right.GetMemoryBytes(), limit.GetMemoryBytes()) &&
		floatSumFits(left.GetAcceleratorUnits(), right.GetAcceleratorUnits(), limit.GetAcceleratorUnits()) &&
		uintSumFits(left.GetEphemeralStorageBytes(), right.GetEphemeralStorageBytes(), limit.GetEphemeralStorageBytes()) &&
		uintSumFits(left.GetNetworkBandwidthBps(), right.GetNetworkBandwidthBps(), limit.GetNetworkBandwidthBps())
}

func uintSumFits(left, right, limit uint64) bool {
	return left <= limit && right <= limit-left
}

func floatSumFits(left, right, limit float64) bool {
	if math.IsNaN(left) || math.IsNaN(right) || math.IsNaN(limit) || math.IsInf(left, 0) || math.IsInf(right, 0) || math.IsInf(limit, 0) {
		return false
	}
	return left <= limit && right <= limit-left
}

func resourceIsZero(resources *tgsrlv1.ResourceVector) bool {
	return resources == nil || (resources.GetCpuMillis() == 0 && resources.GetMemoryBytes() == 0 && resources.GetAcceleratorUnits() == 0 && resources.GetEphemeralStorageBytes() == 0 && resources.GetNetworkBandwidthBps() == 0)
}

func cloneResources(resources *tgsrlv1.ResourceVector) *tgsrlv1.ResourceVector {
	if resources == nil {
		return &tgsrlv1.ResourceVector{}
	}
	return proto.Clone(resources).(*tgsrlv1.ResourceVector)
}

func addResources(target, value *tgsrlv1.ResourceVector) {
	if target == nil || value == nil {
		return
	}
	target.CpuMillis = saturatingAdd(target.GetCpuMillis(), value.GetCpuMillis())
	target.MemoryBytes = saturatingAdd(target.GetMemoryBytes(), value.GetMemoryBytes())
	target.AcceleratorUnits += value.GetAcceleratorUnits()
	target.EphemeralStorageBytes = saturatingAdd(target.GetEphemeralStorageBytes(), value.GetEphemeralStorageBytes())
	target.NetworkBandwidthBps = saturatingAdd(target.GetNetworkBandwidthBps(), value.GetNetworkBandwidthBps())
}

func saturatingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

func consume(resources *deviceResources, request *tgsrlv1.ResourceVector) {
	resources.allocatable.CpuMillis -= request.GetCpuMillis()
	resources.allocatable.MemoryBytes -= request.GetMemoryBytes()
	resources.allocatable.AcceleratorUnits = math.Max(0, resources.allocatable.GetAcceleratorUnits()-request.GetAcceleratorUnits())
	resources.allocatable.EphemeralStorageBytes -= request.GetEphemeralStorageBytes()
	resources.allocatable.NetworkBandwidthBps -= request.GetNetworkBandwidthBps()
	addResources(resources.used, request)
}

func scoreCandidate(resources *deviceResources, request *tgsrlv1.ResourceVector) (map[string]float64, float64) {
	capacity, share, ready, total := scoreCandidateValues(resources, request)
	return map[string]float64{
		"capacity_headroom": capacity,
		"share_headroom":    share,
		"ready":             ready,
	}, total
}

func scoreCandidateValue(resources *deviceResources, request *tgsrlv1.ResourceVector) float64 {
	_, _, _, total := scoreCandidateValues(resources, request)
	return total
}

func scoreCandidateValues(resources *deviceResources, request *tgsrlv1.ResourceVector) (float64, float64, float64, float64) {
	ratioSum := 0.0
	ratioCount := 0
	addRatio := func(requested uint64, available uint64) {
		if requested > 0 && available > 0 {
			ratioSum += float64(available-requested) / float64(available)
			ratioCount++
		}
	}
	addRatio(request.GetCpuMillis(), resources.allocatable.GetCpuMillis())
	addRatio(request.GetMemoryBytes(), resources.allocatable.GetMemoryBytes())
	addRatio(request.GetEphemeralStorageBytes(), resources.allocatable.GetEphemeralStorageBytes())
	addRatio(request.GetNetworkBandwidthBps(), resources.allocatable.GetNetworkBandwidthBps())
	if request.GetAcceleratorUnits() > 0 && resources.allocatable.GetAcceleratorUnits() > 0 {
		ratioSum += (resources.allocatable.GetAcceleratorUnits() - request.GetAcceleratorUnits()) / resources.allocatable.GetAcceleratorUnits()
		ratioCount++
	}
	headroom := 1.0
	if ratioCount > 0 {
		headroom = ratioSum / float64(ratioCount)
	}
	shareHeadroom := math.Max(0, 1-resources.used.GetAcceleratorUnits()-request.GetAcceleratorUnits())
	capacityComponent := roundScore(headroom * 0.70)
	shareComponent := roundScore(shareHeadroom * 0.20)
	readyComponent := 0.10
	return capacityComponent, shareComponent, readyComponent, roundScore(capacityComponent + shareComponent + readyComponent)
}

func roundScore(value float64) float64 {
	return math.Round(value*scorePrecision) / scorePrecision
}

func staticBindings(decisionID string, snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) []*tgsrlv1.Binding {
	allocations := append([]*tgsrlv1.Allocation(nil), snapshot.GetAllocations()...)
	sort.Slice(allocations, func(i, j int) bool { return allocations[i].GetAllocationId() < allocations[j].GetAllocationId() })
	bindings := make([]*tgsrlv1.Binding, 0)
	for _, allocation := range allocations {
		if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE ||
			allocation.GetExecutionId() != intent.GetExecutionId() ||
			allocation.GetStageId() != intent.GetStageId() ||
			allocation.GetIntentVersion() != intent.GetVersion() {
			continue
		}
		deviceIDs := append([]string(nil), allocation.GetDeviceIds()...)
		sort.Strings(deviceIDs)
		bindings = append(bindings, &tgsrlv1.Binding{
			BindingId:     stableID("static-binding", decisionID, allocation.GetAllocationId()),
			PendingUnitId: allocation.GetPendingUnitId(),
			DeviceIds:     deviceIDs,
			Resources:     cloneResources(allocation.GetResources()),
		})
	}
	return bindings
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func containsNormalized(values []string, expected string) bool {
	expected = normalizeAction(expected)
	for _, value := range values {
		if normalizeAction(value) == expected {
			return true
		}
	}
	return false
}

func normalizeAction(value string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(value)), "-", "_")
}
