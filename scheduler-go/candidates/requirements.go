package candidates

import (
	"math"
	"sort"
	"strconv"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/constraints"
)

type pairEvaluation struct {
	feasible bool
	reason   tgsrlv1.CandidateRejectionReason
	detail   string
	score    scoreValues
}

func evaluatePair(intent *tgsrlv1.SchedulingIntent, device *tgsrlv1.Device, resources *deviceResources, request *tgsrlv1.ResourceVector, required *tgsrlv1.CapabilitySet, decision DecisionMetadata) pairEvaluation {
	if device.GetHealth() != tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY {
		return rejected(tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VALIDITY_RULE, "device is not READY")
	}
	if decision.RequiresSafePoint && !decision.SafePoint {
		return rejected(tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VALIDITY_RULE, "execution contract requires a safe point not present in the evaluated context")
	}
	if resources == nil || resources.overcommitted {
		return rejected(tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, "existing pending or active allocations exceed device capacity or aggregate share")
	}
	if detail := capabilityMismatch(device.GetCapabilities(), required); detail != "" {
		return rejected(tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH, detail)
	}
	if !constraints.ResourceLessOrEqual(request, resources.allocatable) {
		return rejected(tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, "request exceeds allocatable resources")
	}
	if !constraints.AcceleratorShareFits(resources.used.GetAcceleratorUnits(), request.GetAcceleratorUnits()) {
		return rejected(tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, "aggregate pending and active accelerator share plus request exceeds 1")
	}
	if !constraints.SumFits(resources.used, request, resources.capacity) {
		return rejected(tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, "pending and active allocations plus request exceed device capacity")
	}
	return pairEvaluation{feasible: true, score: scorePair(intent, device, resources, request)}
}

func rejected(reason tgsrlv1.CandidateRejectionReason, detail string) pairEvaluation {
	return pairEvaluation{reason: reason, detail: detail}
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
	if detail := capabilityEvidenceMismatch(available); detail != "" {
		return detail
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
	if required.GetMeasuredAt() != nil {
		if err := required.GetMeasuredAt().CheckValid(); err != nil {
			return "required CapabilitySet measured_at is invalid"
		}
		if available.GetMeasuredAt().AsTime().Before(required.GetMeasuredAt().AsTime()) {
			return "capability measurement is older than required"
		}
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

	attributeKeys := sortedStringKeys(required.GetAttributes())
	for _, key := range attributeKeys {
		availableValue, exists := available.GetAttributes()[key]
		if !exists || availableValue != required.GetAttributes()[key] {
			return "capability attribute mismatch: " + key
		}
	}
	limitKeys := sortedFloatKeys(required.GetLimits())
	for _, key := range limitKeys {
		requiredValue := required.GetLimits()[key]
		availableValue, exists := available.GetLimits()[key]
		if !exists || invalidLimit(requiredValue) || invalidLimit(availableValue) || availableValue < requiredValue {
			return "capability limit below requirement: " + key
		}
	}
	return ""
}

func capabilityEvidenceMismatch(available *tgsrlv1.CapabilitySet) string {
	for index, evidence := range available.GetEvidence() {
		prefix := "capability evidence " + strconv.Itoa(index)
		// A nil repeated message is represented as an empty message by a
		// protobuf clone. Treat both forms as the same malformed entry.
		if evidence == nil || isEmptyCapabilityEvidence(evidence) {
			return prefix + " is nil"
		}
		if strings.TrimSpace(evidence.GetEvidenceId()) == "" ||
			strings.TrimSpace(evidence.GetSource()) == "" ||
			evidence.GetRevision() == 0 ||
			evidence.GetObservedAt() == nil ||
			strings.TrimSpace(evidence.GetCollector()) == "" {
			return prefix + " lacks evidence_id, source, revision, observed_at, or collector"
		}
		if err := evidence.GetObservedAt().CheckValid(); err != nil {
			return prefix + " observed_at is invalid"
		}
		if evidence.GetSource() != available.GetSource() {
			return prefix + " source does not match CapabilitySet source"
		}
		if evidence.GetRevision() < available.GetRevision() {
			return prefix + " revision is older than CapabilitySet revision"
		}
		if evidence.GetObservedAt().AsTime().Before(available.GetMeasuredAt().AsTime()) {
			return prefix + " observed_at is older than CapabilitySet measured_at"
		}
	}
	return ""
}

func isEmptyCapabilityEvidence(evidence *tgsrlv1.CapabilityEvidence) bool {
	return evidence != nil &&
		evidence.GetEvidenceId() == "" &&
		evidence.GetSource() == "" &&
		evidence.GetRevision() == 0 &&
		evidence.GetObservedAt() == nil &&
		evidence.GetCollector() == "" &&
		evidence.GetDetail() == "" &&
		len(evidence.GetAttributes()) == 0
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

func sortedStringKeys(input map[string]string) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedFloatKeys(input map[string]float64) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func invalidLimit(value float64) bool {
	return math.IsNaN(value) || math.IsInf(value, 0) || value < 0
}
