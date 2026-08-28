package scheduler

import (
	"fmt"
	"sort"
	"strconv"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

func workUnits(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) ([]workUnit, string, string) {
	matching := make([]*tgsrlv1.PendingUnit, 0)
	newer, older := uint64(0), uint64(0)
	for _, unit := range snapshot.GetPendingUnits() {
		if unit.GetExecutionId() != intent.GetExecutionId() || unit.GetStageId() != intent.GetStageId() {
			continue
		}
		if unit.GetIntentVersion() > intent.GetVersion() {
			if newer == 0 || unit.GetIntentVersion() < newer {
				newer = unit.GetIntentVersion()
			}
			continue
		}
		if unit.GetIntentVersion() < intent.GetVersion() {
			if unit.GetIntentVersion() > older {
				older = unit.GetIntentVersion()
			}
			continue
		}
		if unit.GetJobId() != "" && unit.GetJobId() != intent.GetJobId() {
			continue
		}
		matching = append(matching, unit)
	}
	if newer != 0 {
		return nil, FallbackReasonStaleIntent, fmt.Sprintf("pending unit version %d is newer than intent version %d", newer, intent.GetVersion())
	}
	if older != 0 {
		return nil, FallbackReasonRevision, fmt.Sprintf("pending unit version %d is older than intent version %d", older, intent.GetVersion())
	}
	sort.Slice(matching, func(i, j int) bool { return matching[i].GetPendingUnitId() < matching[j].GetPendingUnitId() })

	committed := uint32(0)
	for _, allocation := range snapshot.GetAllocations() {
		if allocation.GetExecutionId() == intent.GetExecutionId() && allocation.GetStageId() == intent.GetStageId() &&
			(allocation.GetState() == tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING || allocation.GetState() == tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE) {
			committed++
		}
	}
	if committed > intent.GetUnitCount() || uint64(committed)+uint64(len(matching)) > uint64(intent.GetUnitCount()) {
		return nil, FallbackReasonPendingCount, fmt.Sprintf("committed (%d) plus pending (%d) exceeds unit_count (%d)", committed, len(matching), intent.GetUnitCount())
	}
	remaining := intent.GetUnitCount() - committed
	if len(matching) > 0 && uint32(len(matching)) != remaining {
		return nil, FallbackReasonPendingCount, fmt.Sprintf("pending count %d does not equal remaining unit_count %d", len(matching), remaining)
	}
	units := make([]workUnit, 0, remaining)
	for _, unit := range matching {
		if !proto.Equal(unit.GetRequestedResources(), intent.GetResourcesPerUnit()) {
			return nil, FallbackReasonRevision, fmt.Sprintf("pending unit %s resources do not match intent version %d", unit.GetPendingUnitId(), intent.GetVersion())
		}
		if !capabilityRequirementEqual(unit.GetRequiredCapabilities(), intent.GetRequiredCapabilities()) {
			return nil, FallbackReasonRevision, fmt.Sprintf("pending unit %s capabilities do not match intent version %d", unit.GetPendingUnitId(), intent.GetVersion())
		}
		units = append(units, workUnit{id: unit.GetPendingUnitId(), pending: proto.Clone(unit).(*tgsrlv1.PendingUnit)})
	}
	if len(matching) == 0 {
		for index := uint32(0); index < remaining; index++ {
			units = append(units, workUnit{id: stableID("pending", intent.GetExecutionId(), intent.GetStageId(), strconv.FormatUint(intent.GetVersion(), 10), strconv.FormatUint(uint64(index), 10))})
		}
	}
	return units, "", ""
}

func capabilityRequirementEqual(left, right *tgsrlv1.CapabilitySet) bool {
	if left == nil {
		left = &tgsrlv1.CapabilitySet{}
	}
	if right == nil {
		right = &tgsrlv1.CapabilitySet{}
	}
	left = proto.Clone(left).(*tgsrlv1.CapabilitySet)
	right = proto.Clone(right).(*tgsrlv1.CapabilitySet)
	for _, capabilities := range []*tgsrlv1.CapabilitySet{left, right} {
		sort.Strings(capabilities.Names)
		sort.Strings(capabilities.Algorithms)
		sort.Strings(capabilities.RolloutModes)
		sort.Strings(capabilities.SupportedActions)
	}
	return proto.Equal(left, right)
}

func contractRequiresSafePoint(contract *tgsrlv1.ExecutionContract) bool {
	if contract == nil {
		return false
	}
	return contract.GetCommitPolicy().GetRequireSafePoint()
}
