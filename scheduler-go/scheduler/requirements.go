package scheduler

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

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
		units = append(units, workUnit{id: unit.GetPendingUnitId()})
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

func candidateRejection(candidateID string, device *tgsrlv1.Device, resources *deviceResources, request *tgsrlv1.ResourceVector, required *tgsrlv1.CapabilitySet, safePoint bool) *tgsrlv1.CandidateRejection {
	return candidateRejectionWithCapabilityDetail(candidateID, device, resources, request, safePoint, capabilityMismatch(device.GetCapabilities(), required))
}

func candidateRejectionWithCapabilityDetail(candidateID string, device *tgsrlv1.Device, resources *deviceResources, request *tgsrlv1.ResourceVector, safePoint bool, capabilityDetail string) *tgsrlv1.CandidateRejection {
	if device.GetHealth() != tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY {
		return rejection(candidateID, tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VALIDITY_RULE, "device is not READY")
	}
	if !safePoint {
		return rejection(candidateID, tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VALIDITY_RULE, "execution contract requires a safe point not present in snapshot annotations")
	}
	if resources.overcommitted {
		return rejection(candidateID, tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, "existing active allocations exceed device capacity or aggregate share")
	}
	if capabilityDetail != "" {
		return rejection(candidateID, tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH, capabilityDetail)
	}
	if !resourceLessOrEqual(request, resources.allocatable) {
		return rejection(candidateID, tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, "request exceeds allocatable resources")
	}
	share := resources.used.GetAcceleratorUnits() + request.GetAcceleratorUnits()
	if math.IsInf(share, 0) || math.IsNaN(share) || share > 1 {
		return rejection(candidateID, tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, "aggregate active accelerator share plus request exceeds 1")
	}
	if !sumFits(resources.used, request, resources.capacity) {
		return rejection(candidateID, tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, "active allocations plus request exceed device capacity")
	}
	return nil
}

func capabilityMismatch(available, required *tgsrlv1.CapabilitySet) string {
	if available == nil {
		return "device has no measured CapabilitySet"
	}
	if strings.TrimSpace(available.GetSource()) == "" || available.GetRevision() == 0 || available.GetMeasuredAt() == nil {
		return "device CapabilitySet lacks source, revision, or measured_at evidence"
	}
	if err := available.GetMeasuredAt().CheckValid(); err != nil {
		return "device CapabilitySet measured_at is invalid"
	}
	if !containsNormalized(available.GetSupportedActions(), "bind") {
		return "device does not advertise the bind action"
	}
	if required == nil {
		return ""
	}
	if required.GetSource() != "" && available.GetSource() != required.GetSource() {
		return "capability source mismatch"
	}
	if available.GetRevision() < required.GetRevision() {
		return "capability revision is older than required"
	}
	if required.GetMeasuredAt() != nil && available.GetMeasuredAt().AsTime().Before(required.GetMeasuredAt().AsTime()) {
		return "capability measurement is older than required"
	}
	if missing := missingStrings(available.GetNames(), required.GetNames(), false); len(missing) > 0 {
		return "missing capability names: " + strings.Join(missing, ",")
	}
	if missing := missingStrings(available.GetAlgorithms(), required.GetAlgorithms(), false); len(missing) > 0 {
		return "missing declared algorithm capabilities: " + strings.Join(missing, ",")
	}
	if missing := missingStrings(available.GetRolloutModes(), required.GetRolloutModes(), false); len(missing) > 0 {
		return "missing declared rollout capabilities: " + strings.Join(missing, ",")
	}
	if missing := missingStrings(available.GetSupportedActions(), required.GetSupportedActions(), true); len(missing) > 0 {
		return "missing supported actions: " + strings.Join(missing, ",")
	}
	attributeKeys := make([]string, 0, len(required.GetAttributes()))
	for key := range required.GetAttributes() {
		attributeKeys = append(attributeKeys, key)
	}
	sort.Strings(attributeKeys)
	for _, key := range attributeKeys {
		if available.GetAttributes()[key] != required.GetAttributes()[key] {
			return "capability attribute mismatch: " + key
		}
	}
	limitKeys := make([]string, 0, len(required.GetLimits()))
	for key := range required.GetLimits() {
		limitKeys = append(limitKeys, key)
	}
	sort.Strings(limitKeys)
	for _, key := range limitKeys {
		availableValue, exists := available.GetLimits()[key]
		if !exists || availableValue < required.GetLimits()[key] {
			return "capability limit below requirement: " + key
		}
	}
	return ""
}

func missingStrings(available, required []string, normalized bool) []string {
	set := make(map[string]struct{}, len(available))
	for _, value := range available {
		if normalized {
			value = normalizeAction(value)
		}
		set[value] = struct{}{}
	}
	missing := make([]string, 0)
	for _, value := range required {
		lookup := value
		if normalized {
			lookup = normalizeAction(value)
		}
		if _, exists := set[lookup]; !exists {
			missing = append(missing, value)
		}
	}
	sort.Strings(missing)
	return missing
}

func contractRequiresSafePoint(contract *tgsrlv1.ExecutionContract) bool {
	if contract == nil {
		return false
	}
	return contract.GetSafePointPolicy().GetEnabled() || contract.GetCommitPolicy().GetRequireSafePoint()
}

func snapshotAtSafePoint(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, configuredKey string) bool {
	keys := []string{
		configuredKey + "/" + intent.GetExecutionId() + "/" + intent.GetStageId(),
		configuredKey,
		"safe_point/" + intent.GetExecutionId() + "/" + intent.GetStageId(),
		"safe_point",
	}
	for _, key := range keys {
		if raw, exists := snapshot.GetAnnotations()[key]; exists {
			value, err := strconv.ParseBool(strings.TrimSpace(raw))
			return err == nil && value
		}
	}
	return false
}

func rejection(id string, reason tgsrlv1.CandidateRejectionReason, detail string) *tgsrlv1.CandidateRejection {
	return &tgsrlv1.CandidateRejection{CandidateId: id, Reason: reason, Detail: detail}
}
