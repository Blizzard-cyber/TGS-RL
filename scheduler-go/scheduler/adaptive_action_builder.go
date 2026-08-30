package scheduler

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type adaptiveTarget struct {
	allocation *tgsrlv1.Allocation
	sandbox    *tgsrlv1.Sandbox
}

type adaptiveActionSpec struct {
	actionType        tgsrlv1.ActionType
	target            adaptiveTarget
	binding           *tgsrlv1.Binding
	share             float64
	priority          int32
	requiresSafePoint bool
}

func buildAdaptivePlan(input PlanningInput, kind tgsrlv1.PlannerKind, purpose tgsrlv1.PlanPurpose, specs []adaptiveActionSpec) (*tgsrlv1.PlacementPlan, string) {
	if input.Snapshot == nil || input.Intent == nil || input.EvaluationContext == nil || len(specs) == 0 {
		return nil, "INCOMPLETE_PLANNING_INPUT"
	}
	if input.Snapshot.GetRevision() == 0 || input.EvaluationContext.GetTickKind() == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		return nil, "MISSING_FENCE"
	}
	ordered := append([]adaptiveActionSpec(nil), specs...)
	sort.Slice(ordered, func(i, j int) bool {
		left, right := ordered[i], ordered[j]
		if left.target.allocation.GetAllocationId() != right.target.allocation.GetAllocationId() {
			return left.target.allocation.GetAllocationId() < right.target.allocation.GetAllocationId()
		}
		return left.actionType < right.actionType
	})
	identity := []string{input.DecisionID, kind.String(), purpose.String(), strconv.FormatUint(input.Snapshot.GetRevision(), 10), strconv.FormatUint(input.EvaluationContext.GetDecisionSequence(), 10), strconv.FormatUint(input.Intent.GetDeterministicSeed(), 10)}
	for _, spec := range ordered {
		identity = append(identity, spec.actionType.String(), spec.target.allocation.GetAllocationId(), spec.target.sandbox.GetSandboxId(), strconv.FormatUint(spec.target.sandbox.GetGeneration(), 10), strconv.FormatFloat(spec.share, 'g', -1, 64), strconv.FormatInt(int64(spec.priority), 10), bindingIdentity(spec.binding))
	}
	planID := stableID("adaptive-plan", identity...)
	if input.EvaluationContext.GetEvaluationTime() == nil || input.Intent.GetValidUntil() == nil {
		return nil, "MISSING_DEADLINE"
	}
	plan := &tgsrlv1.PlacementPlan{
		PlanId: planID, ExecutionId: input.Intent.GetExecutionId(), StageId: input.Intent.GetStageId(),
		IntentVersion: input.Intent.GetVersion(), SnapshotRevision: input.Snapshot.GetRevision(),
		CreatedAt: proto.Clone(input.EvaluationContext.GetEvaluationTime()).(*timestamppb.Timestamp),
		ExpiresAt: proto.Clone(input.Intent.GetValidUntil()).(*timestamppb.Timestamp), DecisionId: input.DecisionID,
		RunId: input.Intent.GetRunId(), TraceId: input.Intent.GetTraceId(), DataKind: input.Intent.GetDataKind(),
		SemanticContext: cloneSemanticEnvelope(input.Intent.GetSemanticContext()), Generation: input.Intent.GetGeneration(),
		Cursor: input.Intent.GetCursor(), JobId: input.Intent.GetJobId(), Purpose: purpose,
		RollbackPolicy:        tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION,
		AffectedAllocationIds: sortedAffectedIDs(ordered),
	}
	plan.CapabilityRequirements = actionpolicy.StableCapabilityRequirements(
		actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK),
	)
	if len(ordered) > 1 {
		plan.CapabilityRequirements = actionpolicy.StableCapabilityRequirements(append(plan.CapabilityRequirements,
			actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION),
		)...)
	}
	for index, spec := range ordered {
		if spec.target.allocation == nil || spec.target.sandbox == nil || spec.target.sandbox.GetGeneration() == 0 {
			return nil, "MISSING_TARGET_STATE"
		}
		definition, ok := actionpolicy.DefinitionForAction(spec.actionType)
		if !ok {
			return nil, "UNKNOWN_ACTION"
		}
		order := uint32(index + 1)
		action := &tgsrlv1.Action{
			ActionId:   stableID("adaptive-action", planID, spec.actionType.String(), spec.target.allocation.GetAllocationId(), strconv.FormatUint(uint64(order), 10)),
			ActionType: spec.actionType, Level: definition.Level, TargetId: spec.target.allocation.GetAllocationId(),
			Share: spec.share, Priority: spec.priority, Rollback: compensationFor(spec, definition), Order: order,
			PlanId: planID, SandboxId: spec.target.sandbox.GetSandboxId(), ExpectedGeneration: spec.target.sandbox.GetGeneration(),
			ExpectedSnapshotRevision: input.Snapshot.GetRevision(), RequiredCapabilities: actionCapabilities(input.Intent.GetRequiredCapabilities(), definition.CapabilityName),
			RequiresSafePoint: spec.requiresSafePoint, Deadline: proto.Clone(input.Intent.GetValidUntil()).(*timestamppb.Timestamp),
			IdempotencyKey:  stableID("adaptive-action-idempotency", input.Intent.GetIdempotencyKey(), planID, spec.actionType.String(), spec.target.allocation.GetAllocationId(), strconv.FormatUint(uint64(order), 10)),
			TickKind:        input.EvaluationContext.GetTickKind(),
			Preconditions:   actionpolicy.RequiredPreconditions(spec.actionType, spec.requiresSafePoint, true),
			ExpectedImpacts: append([]tgsrlv1.ExpectedImpact(nil), definition.ExpectedImpacts...),
		}
		if spec.binding != nil {
			action.Binding = proto.Clone(spec.binding).(*tgsrlv1.Binding)
			if spec.actionType == tgsrlv1.ActionType_ACTION_TYPE_REBIND || spec.actionType == tgsrlv1.ActionType_ACTION_TYPE_RECREATE {
				// Plan bindings are desired workload deltas consumed by the
				// Operator after the Provider commits the resource mutation.
				plan.Bindings = append(plan.Bindings, proto.Clone(spec.binding).(*tgsrlv1.Binding))
			}
		}
		plan.Actions = append(plan.Actions, action)
	}
	return plan, ""
}

func sortedAffectedIDs(specs []adaptiveActionSpec) []string {
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		seen[spec.target.allocation.GetAllocationId()] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func compensationFor(spec adaptiveActionSpec, definition actionpolicy.Definition) *tgsrlv1.Rollback {
	rollback := &tgsrlv1.Rollback{
		ActionType: definition.CompensationType, TargetId: spec.target.sandbox.GetSandboxId(),
		Reason: "restore the observed sandbox before-image",
	}
	if definition.CompensationNeedsBinding && spec.target.sandbox.GetBinding() != nil {
		rollback.RestoreBinding = proto.Clone(spec.target.sandbox.GetBinding()).(*tgsrlv1.Binding)
	}
	return rollback
}

func providerSupports(input PlanningInput, allocation *tgsrlv1.Allocation, actionType tgsrlv1.ActionType) (bool, string) {
	definition, ok := actionpolicy.DefinitionForAction(actionType)
	if !ok {
		return false, "UNKNOWN_ACTION"
	}
	devices := make(map[string]*tgsrlv1.Device, len(input.Snapshot.GetDevices()))
	for _, device := range input.Snapshot.GetDevices() {
		if device != nil {
			devices[device.GetDeviceId()] = device
		}
	}
	if allocation == nil || len(allocation.GetDeviceIds()) == 0 {
		return false, "MISSING_DEVICE_EVIDENCE"
	}
	for _, deviceID := range allocation.GetDeviceIds() {
		device := devices[deviceID]
		if device == nil {
			return false, "MISSING_DEVICE_EVIDENCE"
		}
		if !containsNormalized(device.GetCapabilities().GetSupportedActions(), definition.CapabilityName) {
			return false, "CAPABILITY_NOT_ADVERTISED"
		}
	}
	return true, ""
}

func activeAdaptiveTargets(input PlanningInput) ([]adaptiveTarget, []string) {
	targets, missing, _ := adaptiveTargets(input, false)
	return targets, missing
}

func activeAdaptiveTargetsWithFreshness(input PlanningInput) ([]adaptiveTarget, []string, map[string]string) {
	return adaptiveTargets(input, true)
}

func adaptiveTargets(input PlanningInput, enforceFreshness bool) ([]adaptiveTarget, []string, map[string]string) {
	allocations := make([]*tgsrlv1.Allocation, 0)
	for _, allocation := range input.Snapshot.GetAllocations() {
		if allocation != nil && allocation.GetState() == tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE &&
			allocation.GetExecutionId() == input.Intent.GetExecutionId() && allocation.GetStageId() == input.Intent.GetStageId() && allocation.GetIntentVersion() == input.Intent.GetVersion() {
			allocations = append(allocations, allocation)
		}
	}
	sort.Slice(allocations, func(i, j int) bool { return allocations[i].GetAllocationId() < allocations[j].GetAllocationId() })
	sandboxes := make(map[string]*tgsrlv1.Sandbox, len(input.Sandboxes))
	duplicateSandboxIDs := make(map[string]struct{})
	for _, sandbox := range input.Sandboxes {
		if sandbox != nil {
			if _, exists := sandboxes[sandbox.GetSandboxId()]; exists {
				duplicateSandboxIDs[sandbox.GetSandboxId()] = struct{}{}
				continue
			}
			sandboxes[sandbox.GetSandboxId()] = sandbox
		}
	}
	targetFilter := make(map[string]struct{}, len(input.Directives.TargetSandboxIDs))
	for _, id := range input.Directives.TargetSandboxIDs {
		targetFilter[id] = struct{}{}
	}
	result := make([]adaptiveTarget, 0, len(allocations))
	missing := make([]string, 0)
	stale := make(map[string]string)
	matchedTargets := make(map[string]struct{}, len(targetFilter))
	for _, allocation := range allocations {
		sandboxID := input.Directives.SandboxByAllocation[allocation.GetAllocationId()]
		var sandbox *tgsrlv1.Sandbox
		if sandboxID != "" {
			if _, duplicate := duplicateSandboxIDs[sandboxID]; !duplicate {
				sandbox = sandboxes[sandboxID]
			}
		} else {
			sandbox = uniquelyMatchingSandbox(allocation, input.Sandboxes)
		}
		if sandbox == nil || sandbox.GetGeneration() == 0 || sandbox.GetBinding() == nil || !sandboxMatchesAllocation(sandbox, allocation) {
			missing = append(missing, allocation.GetAllocationId())
			continue
		}
		if freshnessReason := sandboxFreshnessReason(input, sandbox); enforceFreshness && freshnessReason != "" {
			stale[allocation.GetAllocationId()] = freshnessReason
			continue
		}
		if len(targetFilter) > 0 {
			if _, selected := targetFilter[sandbox.GetSandboxId()]; !selected {
				continue
			}
			matchedTargets[sandbox.GetSandboxId()] = struct{}{}
		}
		result = append(result, adaptiveTarget{allocation: allocation, sandbox: sandbox})
	}
	for _, sandboxID := range input.Directives.TargetSandboxIDs {
		if _, matched := matchedTargets[sandboxID]; !matched {
			missing = append(missing, sandboxID)
		}
	}
	sort.Strings(missing)
	return result, missing, stale
}

func sandboxFreshnessReason(input PlanningInput, sandbox *tgsrlv1.Sandbox) string {
	confirmedAt := effectiveSandboxLastConfirmedAt(sandbox)
	if confirmedAt == nil || confirmedAt.CheckValid() != nil {
		return "MISSING_SANDBOX_CONFIRMATION_TIME"
	}
	if input.EvaluationContext == nil || input.EvaluationContext.GetEvaluationTime() == nil {
		return "MISSING_EVALUATION_TIME"
	}
	age := input.EvaluationContext.GetEvaluationTime().AsTime().Sub(confirmedAt.AsTime())
	futureSkew := input.sandboxFutureSkew
	if futureSkew <= 0 {
		futureSkew = DefaultPlannerSandboxFutureSkew
	}
	if age < -futureSkew {
		return "SANDBOX_OBSERVATION_FROM_FUTURE"
	}
	if age < 0 {
		age = 0
	}
	maximumAge := input.sandboxMaximumAge
	if maximumAge <= 0 {
		maximumAge = defaultSandboxMaximumAge(input.EvaluationContext.GetTickKind())
	}
	if age > maximumAge {
		return "STALE_SANDBOX_OBSERVATION"
	}
	return ""
}

func defaultSandboxMaximumAge(tick tgsrlv1.TickKind) time.Duration {
	switch tick {
	case tgsrlv1.TickKind_TICK_KIND_FAST:
		return DefaultPlannerFastSandboxMaximumAge
	case tgsrlv1.TickKind_TICK_KIND_MEDIUM:
		return DefaultPlannerMediumSandboxMaximumAge
	case tgsrlv1.TickKind_TICK_KIND_SLOW:
		return DefaultPlannerSlowSandboxMaximumAge
	default:
		return DefaultPlannerSandboxMaximumAge
	}
}

func effectiveSandboxLastConfirmedAt(sandbox *tgsrlv1.Sandbox) *timestamppb.Timestamp {
	if sandbox == nil {
		return nil
	}
	if sandbox.GetLastConfirmedAt() != nil {
		return sandbox.GetLastConfirmedAt()
	}
	return sandbox.GetObservedAt()
}

func effectiveSandboxStateChangedAt(sandbox *tgsrlv1.Sandbox) *timestamppb.Timestamp {
	if sandbox == nil {
		return nil
	}
	if sandbox.GetStateChangedAt() != nil {
		return sandbox.GetStateChangedAt()
	}
	return sandbox.GetObservedAt()
}

func uniquelyMatchingSandbox(allocation *tgsrlv1.Allocation, sandboxes []*tgsrlv1.Sandbox) *tgsrlv1.Sandbox {
	var match *tgsrlv1.Sandbox
	for _, sandbox := range sandboxes {
		if sandbox == nil || sandbox.GetBinding() == nil {
			continue
		}
		if !sandboxMatchesAllocation(sandbox, allocation) {
			continue
		}
		if match != nil {
			return nil
		}
		match = sandbox
	}
	return match
}

func sandboxMatchesAllocation(sandbox *tgsrlv1.Sandbox, allocation *tgsrlv1.Allocation) bool {
	if sandbox == nil || allocation == nil || sandbox.GetBinding() == nil || sandbox.GetGeneration() != allocation.GetGeneration() {
		return false
	}
	binding := sandbox.GetBinding()
	if allocation.GetPendingUnitId() == "" || binding.GetPendingUnitId() != allocation.GetPendingUnitId() {
		return false
	}
	if allocation.GetRuntimeUnitId() != "" && binding.GetRuntimeUnitId() != allocation.GetRuntimeUnitId() {
		return false
	}
	left, right := append([]string(nil), binding.GetDeviceIds()...), append([]string(nil), allocation.GetDeviceIds()...)
	sort.Strings(left)
	sort.Strings(right)
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func plannerEvidence(input PlanningInput, kind tgsrlv1.PlannerKind, purpose tgsrlv1.PlanPurpose, actionType tgsrlv1.ActionType, target string, utility int64, disposition tgsrlv1.PlannerDisposition, reason string, fields ...*tgsrlv1.SemanticField) *tgsrlv1.PlannerEvidence {
	base := []*tgsrlv1.SemanticField{
		semanticUintField("planner.snapshot_revision", input.Snapshot.GetRevision()),
		semanticUintField("planner.deterministic_seed", input.Intent.GetDeterministicSeed()),
		semanticUintField("planner.decision_sequence", input.EvaluationContext.GetDecisionSequence()),
		semanticUintField("planner.observed_revision", input.EvaluationContext.GetObservedRevision()),
		semanticUintField("planner.recent_decision_count", uint64(len(input.RecentDecisions))),
		semanticIntField("planner.tick_kind", int64(input.EvaluationContext.GetTickKind())),
		semanticStringField("planner.trigger_cause", input.EvaluationContext.GetCause()),
		semanticBoolField("planner.safe_point_present", input.SafePoint.Present),
		semanticBoolField("planner.safe_point", input.SafePoint.Value),
		semanticBoolField("planner.contract_blocking", input.ContractAggregate.Blocking),
		semanticIntField("planner.utility_nanos", utility),
	}
	base = append(base, planningSignalEvidence(input.Signals)...)
	if input.EvaluationContext.GetEvaluationTime() != nil {
		base = append(base, semanticIntField("planner.evaluation_time_unix_nanos", input.EvaluationContext.GetEvaluationTime().AsTime().UnixNano()))
	}
	base = append(base, fields...)
	sort.Slice(base, func(i, j int) bool { return base[i].GetKey() < base[j].GetKey() })
	return &tgsrlv1.PlannerEvidence{
		ProposalId: stableID("proposal", input.DecisionID, kind.String(), purpose.String(), actionType.String(), target, strconv.FormatInt(utility, 10), strconv.FormatUint(input.Snapshot.GetRevision(), 10), strconv.FormatUint(input.EvaluationContext.GetDecisionSequence(), 10), strconv.FormatUint(input.Intent.GetDeterministicSeed(), 10), semanticFieldsIdentity(base)),
		Planner:    kind, Purpose: purpose, ActionType: actionType, TargetId: target, UtilityNanos: utility, Disposition: disposition, Reason: reason, Inputs: base,
	}
}

func bindingIdentity(binding *tgsrlv1.Binding) string {
	if binding == nil {
		return ""
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(binding)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func semanticFieldsIdentity(fields []*tgsrlv1.SemanticField) string {
	object := &tgsrlv1.SemanticObject{Fields: fields}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(object)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func semanticStringField(key, value string) *tgsrlv1.SemanticField {
	return &tgsrlv1.SemanticField{Key: key, Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_StringValue{StringValue: value}}}
}
func semanticIntField(key string, value int64) *tgsrlv1.SemanticField {
	return &tgsrlv1.SemanticField{Key: key, Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_Int64Value{Int64Value: value}}}
}
func semanticUintField(key string, value uint64) *tgsrlv1.SemanticField {
	return &tgsrlv1.SemanticField{Key: key, Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_Uint64Value{Uint64Value: value}}}
}
func semanticBoolField(key string, value bool) *tgsrlv1.SemanticField {
	return &tgsrlv1.SemanticField{Key: key, Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_BoolValue{BoolValue: value}}}
}
func semanticDoubleField(key string, value float64) *tgsrlv1.SemanticField {
	return &tgsrlv1.SemanticField{Key: key, Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_DoubleValue{DoubleValue: value}}}
}

func multiplyUtility(value int64, count int) int64 {
	if count <= 0 || value == 0 {
		return 0
	}
	if value > 0 && int64(count) > (int64(^uint64(0)>>1)/value) {
		return int64(^uint64(0) >> 1)
	}
	if value < 0 && value < (-int64(^uint64(0)>>1)-1)/int64(count) {
		return -int64(^uint64(0)>>1) - 1
	}
	return value * int64(count)
}

func formatResourceVector(value *tgsrlv1.ResourceVector) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("cpu=%d,memory=%d,accelerator=%s,storage=%d,network=%d", value.GetCpuMillis(), value.GetMemoryBytes(), strconv.FormatFloat(value.GetAcceleratorUnits(), 'g', -1, 64), value.GetEphemeralStorageBytes(), value.GetNetworkBandwidthBps())
}
