package scheduler

import (
	"math"
	"sort"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/constraints"
	"google.golang.org/protobuf/proto"
)

const scorePrecision = 1_000_000_000.0

func resourceLessOrEqual(left, right *tgsrlv1.ResourceVector) bool {
	return constraints.ResourceLessOrEqual(left, right)
}

func resourceIsZero(resources *tgsrlv1.ResourceVector) bool {
	return constraints.ResourceIsZero(resources)
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
