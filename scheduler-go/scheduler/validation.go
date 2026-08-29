package scheduler

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/semantics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	contractVersionPattern = regexp.MustCompile(`^1\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$`)
	factPathPattern        = regexp.MustCompile(`^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$`)
)

var componentKindAliases = map[string]tgsrlv1.ComponentKind{
	"protocol":          tgsrlv1.ComponentKind_COMPONENT_KIND_PROTOCOL,
	"scheduler":         tgsrlv1.ComponentKind_COMPONENT_KIND_SCHEDULER,
	"runtime":           tgsrlv1.ComponentKind_COMPONENT_KIND_RUNTIME,
	"operator":          tgsrlv1.ComponentKind_COMPONENT_KIND_OPERATOR,
	"provider":          tgsrlv1.ComponentKind_COMPONENT_KIND_PROVIDER,
	"framework_adapter": tgsrlv1.ComponentKind_COMPONENT_KIND_FRAMEWORK_ADAPTER,
	"rollout_engine":    tgsrlv1.ComponentKind_COMPONENT_KIND_ROLLOUT_ENGINE,
	"trainer":           tgsrlv1.ComponentKind_COMPONENT_KIND_TRAINER,
	"cuda_driver":       tgsrlv1.ComponentKind_COMPONENT_KIND_CUDA_DRIVER,
	"execution_backend": tgsrlv1.ComponentKind_COMPONENT_KIND_EXECUTION_BACKEND,
}

// ProtocolVersion is the normalized protocol release understood by this
// scheduler. Both "0.3" and "v0.3" normalize to this value.
const ProtocolVersion = semantics.ProtocolVersion

func validateSnapshot(snapshot *tgsrlv1.ClusterSnapshot) error {
	if strings.TrimSpace(snapshot.GetSnapshotId()) == "" {
		return invalid("snapshot.snapshot_id", "must not be empty")
	}
	if snapshot.GetRevision() == 0 {
		return invalid("snapshot.revision", "must be greater than zero")
	}
	if observed := snapshot.GetObservedAt(); observed != nil {
		if err := observed.CheckValid(); err != nil {
			return invalid("snapshot.observed_at", err.Error())
		}
	}
	for _, key := range []string{"snapshot_revision", "tgsrl.io/snapshot-revision"} {
		if raw, ok := snapshot.GetAnnotations()[key]; ok {
			revision, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
			if err != nil || revision != snapshot.GetRevision() {
				return invalid("snapshot.annotations["+key+"]", "must equal snapshot.revision")
			}
		}
	}

	deviceIDs := make(map[string]struct{}, len(snapshot.GetDevices()))
	for i, device := range snapshot.GetDevices() {
		prefix := fmt.Sprintf("snapshot.devices[%d]", i)
		if device == nil {
			return invalid(prefix, "must not be nil")
		}
		id := strings.TrimSpace(device.GetDeviceId())
		if id == "" {
			return invalid(prefix+".device_id", "must not be empty")
		}
		if _, duplicate := deviceIDs[id]; duplicate {
			return invalid(prefix+".device_id", "must be unique")
		}
		deviceIDs[id] = struct{}{}
		if err := validateResourceVector(prefix+".capacity", device.GetCapacity()); err != nil {
			return err
		}
		if err := validateResourceVector(prefix+".allocatable", device.GetAllocatable()); err != nil {
			return err
		}
		if !resourceLessOrEqual(device.GetAllocatable(), device.GetCapacity()) {
			return invalid(prefix+".allocatable", "must not exceed capacity in any dimension")
		}
		if err := validateCapabilityShape(prefix+".capabilities", device.GetCapabilities()); err != nil {
			return err
		}
	}

	allocationIDs := make(map[string]struct{}, len(snapshot.GetAllocations()))
	for i, allocation := range snapshot.GetAllocations() {
		prefix := fmt.Sprintf("snapshot.allocations[%d]", i)
		if allocation == nil {
			return invalid(prefix, "must not be nil")
		}
		if strings.TrimSpace(allocation.GetAllocationId()) == "" {
			return invalid(prefix+".allocation_id", "must not be empty")
		}
		if _, duplicate := allocationIDs[allocation.GetAllocationId()]; duplicate {
			return invalid(prefix+".allocation_id", "must be unique")
		}
		allocationIDs[allocation.GetAllocationId()] = struct{}{}
		if err := validateResourceVector(prefix+".resources", allocation.GetResources()); err != nil {
			return err
		}
		if allocation.GetState() == tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
			if allocation.GetIntentVersion() == 0 {
				return invalid(prefix+".intent_version", "active allocation version must be greater than zero")
			}
			if len(allocation.GetDeviceIds()) == 0 {
				return invalid(prefix+".device_ids", "active allocation must reference at least one device")
			}
			seen := make(map[string]struct{}, len(allocation.GetDeviceIds()))
			for _, deviceID := range allocation.GetDeviceIds() {
				if _, exists := deviceIDs[deviceID]; !exists {
					return invalid(prefix+".device_ids", "active allocation references an unknown device")
				}
				if _, duplicate := seen[deviceID]; duplicate {
					return invalid(prefix+".device_ids", "must not contain duplicates")
				}
				seen[deviceID] = struct{}{}
			}
		}
	}

	pendingIDs := make(map[string]struct{}, len(snapshot.GetPendingUnits()))
	for i, unit := range snapshot.GetPendingUnits() {
		prefix := fmt.Sprintf("snapshot.pending_units[%d]", i)
		if unit == nil {
			return invalid(prefix, "must not be nil")
		}
		if strings.TrimSpace(unit.GetPendingUnitId()) == "" {
			return invalid(prefix+".pending_unit_id", "must not be empty")
		}
		if _, duplicate := pendingIDs[unit.GetPendingUnitId()]; duplicate {
			return invalid(prefix+".pending_unit_id", "must be unique")
		}
		pendingIDs[unit.GetPendingUnitId()] = struct{}{}
		if strings.TrimSpace(unit.GetExecutionId()) == "" || strings.TrimSpace(unit.GetStageId()) == "" {
			return invalid(prefix, "execution_id and stage_id must not be empty")
		}
		if unit.GetIntentVersion() == 0 {
			return invalid(prefix+".intent_version", "must be greater than zero")
		}
		if err := validateResourceVector(prefix+".requested_resources", unit.GetRequestedResources()); err != nil {
			return err
		}
		if err := validateCapabilityShape(prefix+".required_capabilities", unit.GetRequiredCapabilities()); err != nil {
			return err
		}
	}
	return nil
}

func validateIntent(intent *tgsrlv1.SchedulingIntent) error {
	requiredStrings := []struct {
		field string
		value string
	}{
		{"intent.execution_id", intent.GetExecutionId()},
		{"intent.stage_id", intent.GetStageId()},
		{"intent.job_id", intent.GetJobId()},
		{"intent.idempotency_key", intent.GetIdempotencyKey()},
		{"intent.policy_version", intent.GetPolicyVersion()},
		{"intent.queue", intent.GetQueue()},
	}
	for _, required := range requiredStrings {
		if strings.TrimSpace(required.value) == "" {
			return invalid(required.field, "must not be empty")
		}
	}
	if intent.GetVersion() == 0 {
		return invalid("intent.version", "must be greater than zero")
	}
	if intent.GetUnitCount() == 0 {
		return invalid("intent.unit_count", "must be greater than zero")
	}
	if _, ok := tgsrlv1.RolloutMode_name[int32(intent.GetRolloutMode())]; !ok ||
		intent.GetRolloutMode() == tgsrlv1.RolloutMode_ROLLOUT_MODE_UNKNOWN {
		return invalid("intent.rollout_mode", "must not be UNKNOWN")
	}
	if _, ok := tgsrlv1.PhaseKind_name[int32(intent.GetPhaseKind())]; !ok ||
		intent.GetPhaseKind() == tgsrlv1.PhaseKind_PHASE_KIND_UNKNOWN {
		return invalid("intent.phase_kind", "must not be UNKNOWN")
	}
	if intent.GetSubmittedAt() == nil || intent.GetValidUntil() == nil || intent.GetTtl() == nil {
		return invalid("intent.validity", "submitted_at, valid_until, and ttl are required")
	}
	if err := intent.GetSubmittedAt().CheckValid(); err != nil {
		return invalid("intent.submitted_at", err.Error())
	}
	if err := intent.GetValidUntil().CheckValid(); err != nil {
		return invalid("intent.valid_until", err.Error())
	}
	if err := intent.GetTtl().CheckValid(); err != nil {
		return invalid("intent.ttl", err.Error())
	}
	if intent.GetTtl().AsDuration() <= 0 {
		return invalid("intent.ttl", "must be positive")
	}
	if !intent.GetSubmittedAt().AsTime().Add(intent.GetTtl().AsDuration()).Equal(intent.GetValidUntil().AsTime()) {
		return invalid("intent.valid_until", "must equal submitted_at plus ttl")
	}
	if err := validateResourceVector("intent.resources_per_unit", intent.GetResourcesPerUnit()); err != nil {
		return err
	}
	if resourceIsZero(intent.GetResourcesPerUnit()) {
		return invalid("intent.resources_per_unit", "at least one dimension must be positive")
	}
	if err := validateCapabilityShape("intent.required_capabilities", intent.GetRequiredCapabilities()); err != nil {
		return err
	}
	if err := validateExecutionContract(intent.GetExecutionContract()); err != nil {
		return err
	}
	if err := validateContractObservation("intent.contract_observation", intent.GetContractObservation()); err != nil {
		return err
	}
	var contractStage *tgsrlv1.Phase
	for _, phase := range intent.GetExecutionContract().GetPhaseGraph().GetPhases() {
		if phase.GetPhaseId() == intent.GetStageId() {
			contractStage = phase
			break
		}
	}
	if contractStage == nil {
		return invalid("intent.stage_id", "must reference an execution-contract phase")
	}
	if contractStage.GetKind() != intent.GetPhaseKind() {
		return invalid("intent.phase_kind", "must match the execution-contract phase")
	}
	for _, key := range sortedKeys(intent.GetPreferences()) {
		value := intent.GetPreferences()[key]
		if strings.TrimSpace(key) == "" || math.IsNaN(value) || math.IsInf(value, 0) {
			return invalid("intent.preferences", "keys must be non-empty and values finite")
		}
	}
	for _, key := range sortedKeys(intent.GetLabels()) {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if normalized == "device_id" || normalized == "device_ids" || normalized == "physical_device_id" {
			return invalid("intent.labels", "physical device selectors are forbidden")
		}
	}
	wantKey := IntentIdempotencyKey(intent.GetExecutionId(), intent.GetStageId(), intent.GetVersion())
	if intent.GetIdempotencyKey() != wantKey {
		return invalid("intent.idempotency_key", "does not match intent identity")
	}
	return nil
}

// IntentIdempotencyKey returns the canonical key for one immutable intent.
func IntentIdempotencyKey(executionID, stageID string, version uint64) string {
	material := executionID + "\x00" + stageID + "\x00" + strconv.FormatUint(version, 10)
	digest := sha256.Sum256([]byte(material))
	return "intent-sha256-" + hex.EncodeToString(digest[:])
}

// ValidateIntent validates every scheduler-required field without evaluating
// it against a particular cluster snapshot.
func ValidateIntent(intent *tgsrlv1.SchedulingIntent) error {
	if intent == nil {
		return invalid("intent", "must not be nil")
	}
	return validateIntent(proto.Clone(intent).(*tgsrlv1.SchedulingIntent))
}

func validateExecutionContract(contract *tgsrlv1.ExecutionContract) error {
	if contract == nil {
		return invalid("intent.execution_contract", "must be present")
	}
	if strings.TrimSpace(contract.GetContractId()) == "" {
		return invalid("intent.execution_contract.contract_id", "must not be empty")
	}
	if !contractVersionPattern.MatchString(contract.GetVersion()) {
		return invalid("intent.execution_contract.version", "must use supported major version 1 as major.minor.patch")
	}
	if contract.GetPhaseGraph() == nil || contract.GetCommitPolicy() == nil ||
		contract.GetBackpressurePolicy() == nil || contract.GetSafePointPolicy() == nil ||
		contract.GetCapabilities() == nil {
		return invalid("intent.execution_contract", "required policy and capability messages are missing")
	}
	phases := make(map[string]*tgsrlv1.Phase, len(contract.GetPhaseGraph().GetPhases()))
	indegree := make(map[string]int, len(contract.GetPhaseGraph().GetPhases()))
	adjacency := make(map[string][]string, len(contract.GetPhaseGraph().GetPhases()))
	for _, phase := range contract.GetPhaseGraph().GetPhases() {
		_, knownKind := tgsrlv1.PhaseKind_name[int32(phase.GetKind())]
		if phase == nil || !canonicalNonEmpty(phase.GetPhaseId()) ||
			strings.TrimSpace(phase.GetDisplayName()) == "" ||
			!knownKind || phase.GetKind() == tgsrlv1.PhaseKind_PHASE_KIND_UNKNOWN ||
			phase.GetParallelism() == 0 || phase.GetMaxAttempts() == 0 {
			return invalid("intent.execution_contract.phase_graph", "contains an invalid phase")
		}
		for key := range phase.GetLabels() {
			if !canonicalNonEmpty(key) {
				return invalid("intent.execution_contract.phase_graph", "phase label keys must be non-empty and have no surrounding whitespace")
			}
		}
		if _, exists := phases[phase.GetPhaseId()]; exists {
			return invalid("intent.execution_contract.phase_graph", "phase IDs must be unique")
		}
		phases[phase.GetPhaseId()] = phase
		indegree[phase.GetPhaseId()] = 0
	}
	if len(phases) == 0 {
		return invalid("intent.execution_contract.phase_graph", "must contain phases")
	}
	for _, edge := range contract.GetPhaseGraph().GetEdges() {
		if edge == nil || phases[edge.GetFromPhaseId()] == nil ||
			phases[edge.GetToPhaseId()] == nil || edge.GetFromPhaseId() == edge.GetToPhaseId() {
			return invalid("intent.execution_contract.phase_graph", "contains an invalid edge")
		}
		adjacency[edge.GetFromPhaseId()] = append(adjacency[edge.GetFromPhaseId()], edge.GetToPhaseId())
		indegree[edge.GetToPhaseId()]++
	}
	edgeSet := make(map[string]struct{}, len(contract.GetPhaseGraph().GetEdges()))
	for _, edge := range contract.GetPhaseGraph().GetEdges() {
		key := edge.GetFromPhaseId() + "\x00" + edge.GetToPhaseId()
		if _, duplicate := edgeSet[key]; duplicate {
			return invalid("intent.execution_contract.phase_graph", "edges must be unique")
		}
		edgeSet[key] = struct{}{}
	}
	entries := make(map[string]struct{}, len(contract.GetPhaseGraph().GetEntryPhaseIds()))
	for _, entry := range contract.GetPhaseGraph().GetEntryPhaseIds() {
		if !canonicalNonEmpty(entry) || phases[entry] == nil || indegree[entry] != 0 {
			return invalid("intent.execution_contract.phase_graph.entry_phase_ids", "must identify graph roots")
		}
		if _, duplicate := entries[entry]; duplicate {
			return invalid("intent.execution_contract.phase_graph.entry_phase_ids", "must be unique")
		}
		entries[entry] = struct{}{}
	}
	if len(entries) == 0 {
		return invalid("intent.execution_contract.phase_graph.entry_phase_ids", "must not be empty")
	}
	for phaseID, degree := range indegree {
		_, declared := entries[phaseID]
		if (degree == 0) != declared {
			return invalid("intent.execution_contract.phase_graph.entry_phase_ids", "must exactly identify graph roots")
		}
	}
	queue := make([]string, 0, len(entries))
	for entry := range entries {
		queue = append(queue, entry)
	}
	visited := 0
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		visited++
		for _, next := range adjacency[current] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if visited != len(phases) {
		return invalid("intent.execution_contract.phase_graph", "must be acyclic and reachable")
	}
	if len(contract.GetValidityRules()) == 0 ||
		contract.GetCommitPolicy().GetMode() == tgsrlv1.CommitMode_COMMIT_MODE_UNKNOWN ||
		contract.GetBackpressurePolicy().GetMode() == tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_UNKNOWN ||
		contract.GetBackpressurePolicy().GetMaximumBufferLevel() == 0 {
		return invalid("intent.execution_contract", "requires validity, commit, and backpressure policies")
	}
	ruleIDs := make(map[string]struct{}, len(contract.GetValidityRules()))
	for _, rule := range contract.GetValidityRules() {
		_, knownFailureMode := tgsrlv1.ValidityFailureMode_name[int32(rule.GetFailureMode())]
		if rule == nil || !canonicalNonEmpty(rule.GetRuleId()) ||
			strings.TrimSpace(rule.GetDescription()) == "" || strings.TrimSpace(rule.GetExpression()) == "" ||
			!knownFailureMode || rule.GetFailureMode() == tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_UNKNOWN {
			return invalid("intent.execution_contract.validity_rules", "contains an invalid rule")
		}
		if _, duplicate := ruleIDs[rule.GetRuleId()]; duplicate {
			return invalid("intent.execution_contract.validity_rules", "rule IDs must be unique")
		}
		ruleIDs[rule.GetRuleId()] = struct{}{}
	}
	_, knownCommitMode := tgsrlv1.CommitMode_name[int32(contract.GetCommitPolicy().GetMode())]
	if !knownCommitMode || contract.GetCommitPolicy().GetMinimumSuccessfulUnits() == 0 ||
		!positiveDuration(contract.GetCommitPolicy().GetCommitTimeout()) {
		return invalid("intent.execution_contract.commit_policy", "requires minimum units and a positive timeout")
	}
	_, knownBackpressureMode := tgsrlv1.BackpressureMode_name[int32(contract.GetBackpressurePolicy().GetMode())]
	if !knownBackpressureMode || !positiveDuration(contract.GetBackpressurePolicy().GetStallTimeout()) ||
		contract.GetBackpressurePolicy().GetLowWatermark() > contract.GetBackpressurePolicy().GetHighWatermark() ||
		contract.GetBackpressurePolicy().GetHighWatermark() > contract.GetBackpressurePolicy().GetMaximumBufferLevel() {
		return invalid("intent.execution_contract.backpressure_policy", "has invalid watermarks or timeout")
	}
	safePoint := contract.GetSafePointPolicy()
	if contract.GetCommitPolicy().GetRequireSafePoint() && !safePoint.GetEnabled() {
		return invalid("intent.execution_contract.safe_point_policy", "must be enabled when commit requires it")
	}
	if safePoint.GetEnabled() {
		_, knownTrigger := tgsrlv1.SafePointTrigger_name[int32(safePoint.GetTrigger())]
		if !knownTrigger || safePoint.GetTrigger() == tgsrlv1.SafePointTrigger_SAFE_POINT_TRIGGER_UNKNOWN ||
			!positiveDuration(safePoint.GetMaximumWait()) {
			return invalid("intent.execution_contract.safe_point_policy", "requires trigger and positive maximum_wait")
		}
		requiredPhases := make(map[string]struct{}, len(safePoint.GetRequiredPhaseIds()))
		for _, phaseID := range safePoint.GetRequiredPhaseIds() {
			if !canonicalNonEmpty(phaseID) || phases[phaseID] == nil {
				return invalid("intent.execution_contract.safe_point_policy.required_phase_ids", "references an unknown phase")
			}
			if _, duplicate := requiredPhases[phaseID]; duplicate {
				return invalid("intent.execution_contract.safe_point_policy.required_phase_ids", "must be unique")
			}
			requiredPhases[phaseID] = struct{}{}
		}
		if safePoint.GetTrigger() == tgsrlv1.SafePointTrigger_SAFE_POINT_TRIGGER_INTERVAL && !positiveDuration(safePoint.GetInterval()) {
			return invalid("intent.execution_contract.safe_point_policy.interval", "must be positive for interval trigger")
		}
		if safePoint.GetTrigger() != tgsrlv1.SafePointTrigger_SAFE_POINT_TRIGGER_INTERVAL && safePoint.GetInterval() != nil {
			if err := safePoint.GetInterval().CheckValid(); err != nil {
				return invalid("intent.execution_contract.safe_point_policy.interval", err.Error())
			}
			if safePoint.GetInterval().AsDuration() != 0 {
				return invalid("intent.execution_contract.safe_point_policy.interval", "may only be set for interval trigger")
			}
		}
	} else if safePoint.GetTrigger() != tgsrlv1.SafePointTrigger_SAFE_POINT_TRIGGER_UNKNOWN ||
		safePoint.GetInterval() != nil || safePoint.GetMaximumWait() != nil ||
		len(safePoint.GetRequiredPhaseIds()) > 0 {
		return invalid("intent.execution_contract.safe_point_policy", "disabled policy must not carry active fields")
	}
	protocolFound := false
	components := make(map[componentVersionIdentity]struct{}, len(contract.GetVersionConstraints()))
	for index, constraint := range contract.GetVersionConstraints() {
		field := fmt.Sprintf("intent.execution_contract.version_constraints[%d]", index)
		if constraint == nil {
			return invalid(field, "must not be nil")
		}
		_, knownOperator := tgsrlv1.VersionOperator_name[int32(constraint.GetOperator())]
		if !knownOperator || constraint.GetOperator() == tgsrlv1.VersionOperator_VERSION_OPERATOR_UNKNOWN {
			return invalid(field+".operator", "must not be UNKNOWN")
		}
		if constraint.GetOperator() == tgsrlv1.VersionOperator_VERSION_OPERATOR_EXACT {
			if !canonicalNonEmpty(constraint.GetVersion()) {
				return invalid(field+".version", "must be non-empty and canonical")
			}
		} else if _, err := normalizeVersion(constraint.GetVersion()); err != nil {
			return invalid(field+".version", err.Error())
		}
		if constraint.GetSource() != "" && !canonicalNonEmpty(constraint.GetSource()) {
			return invalid(field+".source", "must be canonical when present")
		}
		identity, legacy, err := versionConstraintIdentity(constraint)
		if err != nil {
			return invalid(field+".component", err.Error())
		}
		if _, duplicate := components[identity]; duplicate {
			return invalid(field+".component", "kind and name identity must be unique ignoring ASCII case")
		}
		components[identity] = struct{}{}
		if identity.kind == tgsrlv1.ComponentKind_COMPONENT_KIND_PROTOCOL {
			protocolFound = true
		}
		if constraint.GetObservationPolicy() != nil {
			if err := validateObservationPolicy(field+".observation_policy", constraint.GetObservationPolicy()); err != nil {
				return err
			}
		} else if !legacy || identity.kind != tgsrlv1.ComponentKind_COMPONENT_KIND_PROTOCOL {
			return invalid(field+".observation_policy", "must be present for typed or non-protocol constraints")
		}
	}
	if !protocolFound {
		return invalid("intent.execution_contract.version_constraints", "protocol constraint is required")
	}
	criticalFacts := make(map[string]struct{}, len(contract.GetCriticalFactPolicies()))
	for index, critical := range contract.GetCriticalFactPolicies() {
		field := fmt.Sprintf("intent.execution_contract.critical_fact_policies[%d]", index)
		if critical == nil {
			return invalid(field, "must not be nil")
		}
		path := critical.GetFactPath()
		if !canonicalFactPath(path) {
			return invalid(field+".fact_path", "must use canonical dotted syntax without surrounding whitespace")
		}
		if _, duplicate := criticalFacts[path]; duplicate {
			return invalid(field+".fact_path", "must be unique")
		}
		criticalFacts[path] = struct{}{}
		if err := validateObservationPolicy(field+".observation_policy", critical.GetObservationPolicy()); err != nil {
			return err
		}
	}
	seenExtensions := make(map[string]struct{}, len(contract.GetCapabilities().GetExtensions()))
	for _, extension := range contract.GetCapabilities().GetExtensions() {
		if !canonicalNonEmpty(extension) {
			return invalid("intent.execution_contract.capabilities.extensions", "must be non-empty and have no surrounding whitespace")
		}
		if _, duplicate := seenExtensions[extension]; duplicate {
			return invalid("intent.execution_contract.capabilities.extensions", "must be unique")
		}
		seenExtensions[extension] = struct{}{}
	}
	wantID, err := CanonicalContractID(contract)
	if err != nil {
		return invalid("intent.execution_contract.contract_id", err.Error())
	}
	if contract.GetContractId() != wantID {
		return invalid("intent.execution_contract.contract_id", "does not match canonical contract content")
	}
	return nil
}

func positiveDuration(duration *durationpb.Duration) bool {
	return duration != nil && duration.CheckValid() == nil && duration.AsDuration() > 0
}

type componentVersionIdentity struct {
	kind tgsrlv1.ComponentKind
	name string
}

func versionConstraintIdentity(constraint *tgsrlv1.VersionConstraint) (componentVersionIdentity, bool, error) {
	if constraint == nil || !canonicalNonEmpty(constraint.GetComponent()) {
		return componentVersionIdentity{}, false, errors.New("component must be non-empty and canonical")
	}
	name := strings.ToLower(constraint.GetComponent())
	aliasKind, alias := componentKindForAlias(name)
	kind := constraint.GetComponentKind()
	if kind == tgsrlv1.ComponentKind_COMPONENT_KIND_UNKNOWN {
		if !alias {
			return componentVersionIdentity{}, false, errors.New("legacy component must use a recognized canonical alias")
		}
		return componentVersionIdentity{kind: aliasKind, name: componentKindAlias(aliasKind)}, true, nil
	}
	if _, known := tgsrlv1.ComponentKind_name[int32(kind)]; !known {
		return componentVersionIdentity{}, false, errors.New("component_kind must be recognized")
	}
	if alias && aliasKind != kind {
		return componentVersionIdentity{}, false, errors.New("component alias conflicts with component_kind")
	}
	if alias {
		name = componentKindAlias(kind)
	}
	return componentVersionIdentity{kind: kind, name: name}, false, nil
}

func componentKindForAlias(value string) (tgsrlv1.ComponentKind, bool) {
	canonical := strings.ReplaceAll(strings.ToLower(value), "-", "_")
	kind, ok := componentKindAliases[canonical]
	return kind, ok
}

func componentKindAlias(kind tgsrlv1.ComponentKind) string {
	for alias, candidate := range componentKindAliases {
		if candidate == kind {
			return alias
		}
	}
	return ""
}

func validateObservationPolicy(field string, policy *tgsrlv1.ObservationPolicy) error {
	if policy == nil {
		return invalid(field, "must be present")
	}
	if maximumAge := policy.GetMaximumAge(); maximumAge != nil {
		if err := maximumAge.CheckValid(); err != nil {
			return invalid(field+".maximum_age", err.Error())
		}
		if maximumAge.AsDuration() <= 0 {
			return invalid(field+".maximum_age", "must be positive when present")
		}
	}
	for _, disposition := range []struct {
		name  string
		value tgsrlv1.ObservationDisposition
	}{
		{"missing", policy.GetMissing()},
		{"stale", policy.GetStale()},
	} {
		if _, known := tgsrlv1.ObservationDisposition_name[int32(disposition.value)]; !known ||
			disposition.value == tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_UNKNOWN {
			return invalid(field+"."+disposition.name, "must be recognized and non-UNKNOWN")
		}
	}
	return nil
}

func canonicalFactPath(value string) bool {
	return canonicalNonEmpty(value) && factPathPattern.MatchString(value)
}

// CanonicalContractID hashes every wire-visible contract field except the ID.
func CanonicalContractID(contract *tgsrlv1.ExecutionContract) (string, error) {
	if contract == nil {
		return "", errors.New("contract is nil")
	}
	canonical := proto.Clone(contract).(*tgsrlv1.ExecutionContract)
	canonical.ContractId = ""
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(wire)
	return "contract-sha256-" + hex.EncodeToString(digest[:]), nil
}

// NormalizeProtocolVersion normalizes supported two- or three-component
// protocol spellings, including an optional leading v.
func NormalizeProtocolVersion(value string) (string, error) {
	return normalizeVersion(value)
}

func normalizeVersion(value string) (string, error) {
	return semantics.NormalizeSemanticVersion(value)
}

func canonicalNonEmpty(value string) bool {
	return value != "" && value == strings.TrimSpace(value)
}

func protocolCompatible(current, required string) bool {
	return semantics.VersionsCompatible(current, required)
}

func validateResourceVector(field string, resources *tgsrlv1.ResourceVector) error {
	if resources == nil {
		return invalid(field, "must be present")
	}
	if math.IsNaN(resources.GetAcceleratorUnits()) || math.IsInf(resources.GetAcceleratorUnits(), 0) || resources.GetAcceleratorUnits() < 0 {
		return invalid(field+".accelerator_units", "must be finite and non-negative")
	}
	return nil
}

func validateCapabilityShape(field string, capabilities *tgsrlv1.CapabilitySet) error {
	if capabilities == nil {
		return nil
	}
	if err := uniqueNonEmpty(field+".names", capabilities.GetNames()); err != nil {
		return err
	}
	if err := uniqueNonEmpty(field+".algorithms", capabilities.GetAlgorithms()); err != nil {
		return err
	}
	if err := uniqueNonEmpty(field+".rollout_modes", capabilities.GetRolloutModes()); err != nil {
		return err
	}
	if err := uniqueNonEmpty(field+".supported_actions", capabilities.GetSupportedActions()); err != nil {
		return err
	}
	for _, key := range sortedKeys(capabilities.GetAttributes()) {
		value := capabilities.GetAttributes()[key]
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			return invalid(field+".attributes", "keys and values must be non-empty")
		}
	}
	for _, key := range sortedKeys(capabilities.GetLimits()) {
		value := capabilities.GetLimits()[key]
		if strings.TrimSpace(key) == "" || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return invalid(field+".limits", "keys must be non-empty and values finite and non-negative")
		}
	}
	if capabilities.GetMeasuredAt() != nil {
		if err := capabilities.GetMeasuredAt().CheckValid(); err != nil {
			return invalid(field+".measured_at", err.Error())
		}
	}
	if err := validateComponentVersions(field+".component_versions", capabilities.GetComponentVersions()); err != nil {
		return err
	}
	return nil
}

func validateContractObservation(field string, observation *tgsrlv1.ContractObservation) error {
	if observation == nil {
		return nil
	}
	if observation.GetSource() != "" && !canonicalNonEmpty(observation.GetSource()) {
		return invalid(field+".source", "must be canonical when present")
	}
	for _, value := range []struct {
		name  string
		value string
	}{
		{"event_id", observation.GetEventId()},
		{"phase_id", observation.GetPhaseId()},
		{"policy_version", observation.GetPolicyVersion()},
	} {
		if value.value != "" && !canonicalNonEmpty(value.value) {
			return invalid(field+"."+value.name, "must be canonical when present")
		}
	}
	for _, timestamp := range []struct {
		name  string
		value *timestamppb.Timestamp
	}{
		{"observed_at", observation.GetObservedAt()},
		{"oldest_sample_at", observation.GetOldestSampleAt()},
		{"backpressure_started_at", observation.GetBackpressureStartedAt()},
	} {
		if timestamp.value != nil {
			if err := timestamp.value.CheckValid(); err != nil {
				return invalid(field+"."+timestamp.name, err.Error())
			}
		}
	}
	observedFacts := make(map[string]struct{}, len(observation.GetFactObservations()))
	for index, observed := range observation.GetFactObservations() {
		factField := fmt.Sprintf("%s.fact_observations[%d]", field, index)
		if observed == nil {
			return invalid(factField, "must not be nil")
		}
		if err := validateSemanticFact(factField+".fact", observed.GetFact()); err != nil {
			return err
		}
		if observed.GetObservedAt() == nil {
			return invalid(factField+".observed_at", "must be present")
		}
		if err := observed.GetObservedAt().CheckValid(); err != nil {
			return invalid(factField+".observed_at", err.Error())
		}
		if !canonicalNonEmpty(observed.GetSource()) {
			return invalid(factField+".source", "must be non-empty and canonical")
		}
		key := observed.GetFact().GetKey()
		if _, duplicate := observedFacts[key]; duplicate {
			return invalid(factField+".fact.key", "must be unique")
		}
		observedFacts[key] = struct{}{}
	}
	if err := validateComponentVersions(field+".component_versions", observation.GetComponentVersions()); err != nil {
		return err
	}
	return nil
}

func validateSemanticFact(field string, fact *tgsrlv1.SemanticField) error {
	if fact == nil {
		return invalid(field, "must not be nil")
	}
	if !canonicalFactPath(fact.GetKey()) {
		return invalid(field+".key", "must use canonical dotted syntax without surrounding whitespace")
	}
	if fact.GetValue() == nil || fact.GetValue().GetKind() == nil {
		return invalid(field+".value", "must set a typed value")
	}
	if !finiteSemanticValue(fact.GetValue()) {
		return invalid(field+".value", "must contain only finite numeric values")
	}
	return nil
}

func finiteSemanticValue(value *tgsrlv1.SemanticValue) bool {
	if value == nil || value.GetKind() == nil {
		return false
	}
	switch kind := value.GetKind().(type) {
	case *tgsrlv1.SemanticValue_DoubleValue:
		return !math.IsNaN(kind.DoubleValue) && !math.IsInf(kind.DoubleValue, 0)
	case *tgsrlv1.SemanticValue_ObjectValue:
		if kind.ObjectValue == nil {
			return false
		}
		seen := make(map[string]struct{}, len(kind.ObjectValue.GetFields()))
		for _, field := range kind.ObjectValue.GetFields() {
			if field == nil || !canonicalNonEmpty(field.GetKey()) || !finiteSemanticValue(field.GetValue()) {
				return false
			}
			if _, duplicate := seen[field.GetKey()]; duplicate {
				return false
			}
			seen[field.GetKey()] = struct{}{}
		}
	case *tgsrlv1.SemanticValue_ListValue:
		if kind.ListValue == nil {
			return false
		}
		for _, item := range kind.ListValue.GetValues() {
			if !finiteSemanticValue(item) {
				return false
			}
		}
	}
	return true
}

func validateComponentVersions(field string, versions []*tgsrlv1.ComponentVersion) error {
	seen := make(map[componentVersionIdentity]struct{}, len(versions))
	for index, version := range versions {
		versionField := fmt.Sprintf("%s[%d]", field, index)
		if version == nil {
			return invalid(versionField, "must not be nil")
		}
		kind := version.GetKind()
		if _, known := tgsrlv1.ComponentKind_name[int32(kind)]; !known || kind == tgsrlv1.ComponentKind_COMPONENT_KIND_UNKNOWN {
			return invalid(versionField+".kind", "must be recognized and non-UNKNOWN")
		}
		if !canonicalNonEmpty(version.GetName()) {
			return invalid(versionField+".name", "must be non-empty and canonical")
		}
		if !canonicalNonEmpty(version.GetVersion()) {
			return invalid(versionField+".version", "must be non-empty and canonical")
		}
		if version.GetObservedAt() == nil {
			return invalid(versionField+".observed_at", "must be present")
		}
		if err := version.GetObservedAt().CheckValid(); err != nil {
			return invalid(versionField+".observed_at", err.Error())
		}
		if !canonicalNonEmpty(version.GetSource()) {
			return invalid(versionField+".source", "must be non-empty and canonical")
		}
		for _, key := range sortedKeys(version.GetAttributes()) {
			if !canonicalNonEmpty(key) || !canonicalNonEmpty(version.GetAttributes()[key]) {
				return invalid(versionField+".attributes", "keys and values must be non-empty and canonical")
			}
		}
		identity, err := observedComponentIdentity(kind, version.GetName())
		if err != nil {
			return invalid(versionField+".name", err.Error())
		}
		if _, duplicate := seen[identity]; duplicate {
			return invalid(versionField, "kind and name identity must be unique ignoring ASCII case")
		}
		seen[identity] = struct{}{}
	}
	return nil
}

func observedComponentIdentity(kind tgsrlv1.ComponentKind, name string) (componentVersionIdentity, error) {
	canonicalName := strings.ToLower(name)
	aliasKind, alias := componentKindForAlias(canonicalName)
	if alias && aliasKind != kind {
		return componentVersionIdentity{}, errors.New("component alias conflicts with kind")
	}
	if alias {
		canonicalName = componentKindAlias(kind)
	}
	return componentVersionIdentity{kind: kind, name: canonicalName}, nil
}

func uniqueNonEmpty(field string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return invalid(field, "values must be non-empty")
		}
		if _, duplicate := seen[value]; duplicate {
			return invalid(field, "values must be unique")
		}
		seen[value] = struct{}{}
	}
	return nil
}

func invalid(field, reason string) error {
	return &ValidationError{Field: field, Reason: reason}
}
