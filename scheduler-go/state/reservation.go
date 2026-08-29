package state

import (
	"errors"
	"fmt"
	"math"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ValidatePlan verifies the optimistic-concurrency inputs captured by plan.
// The snapshot revision and latest still-valid intent version must both match.
func (s *Store) ValidatePlan(plan *tgsrlv1.PlacementPlan) error {
	if plan == nil {
		return errors.New("state: nil placement plan")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.validatePlanLocked(plan)
}

func (s *Store) validatePlanLocked(plan *tgsrlv1.PlacementPlan) error {
	if plan.GetSnapshotRevision() != s.snapshot.GetRevision() {
		return fmt.Errorf("%w: plan has %d, current revision is %d", ErrPlanRevisionConflict, plan.GetSnapshotRevision(), s.snapshot.GetRevision())
	}
	key := intentKey{executionID: plan.GetExecutionId(), stageID: plan.GetStageId()}
	intent, ok := s.intents[key]
	if !ok {
		return fmt.Errorf("%w: %s/%s", ErrIntentNotFound, plan.GetExecutionId(), plan.GetStageId())
	}
	if !intent.GetValidUntil().AsTime().After(s.clock.Now()) {
		return fmt.Errorf("%w: %s/%s version %d", ErrIntentExpired, plan.GetExecutionId(), plan.GetStageId(), intent.GetVersion())
	}
	if plan.GetIntentVersion() != intent.GetVersion() {
		return fmt.Errorf("%w: plan has %d, current intent version is %d", ErrPlanIntentConflict, plan.GetIntentVersion(), intent.GetVersion())
	}
	return nil
}

// ReservePlan atomically fences a plan, consumes its pending units and
// allocatable resources, and creates pending allocations. Provider execution
// happens only after this authoritative reservation succeeds.
func (s *Store) ReservePlan(plan *tgsrlv1.PlacementPlan) (*tgsrlv1.ClusterSnapshot, error) {
	if plan == nil || plan.GetPlanId() == "" {
		return nil, fmt.Errorf("%w: plan_id is required", ErrInvalidIntent)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.reservations[plan.GetPlanId()]; ok {
		if proto.Equal(existing.plan, plan) {
			return cloneSnapshot(s.snapshot), ErrPlanAlreadyReserved
		}
		return nil, fmt.Errorf("%w: plan_id %q has different content", ErrPlanAlreadyReserved, plan.GetPlanId())
	}
	if err := s.validatePlanLocked(plan); err != nil {
		return nil, err
	}
	purpose, _ := effectivePlanPurpose(plan)
	if isMutationPurpose(purpose) {
		return s.reserveMutationPlanLocked(plan)
	}
	if len(plan.GetBindings()) == 0 {
		return nil, fmt.Errorf("%w: admission plans require bindings", ErrInvalidIntent)
	}
	if s.snapshot.GetRevision() == math.MaxUint64 {
		return nil, ErrRevisionExhausted
	}

	working := cloneSnapshot(s.snapshot)
	devices := make(map[string]*tgsrlv1.Device, len(working.GetDevices()))
	for _, device := range working.GetDevices() {
		devices[device.GetDeviceId()] = device
	}
	pending := make(map[string]*tgsrlv1.PendingUnit, len(working.GetPendingUnits()))
	for _, unit := range working.GetPendingUnits() {
		pending[unit.GetPendingUnitId()] = unit
	}
	reservation := &planReservation{
		plan:           clonePlan(plan),
		beforeSnapshot: cloneSnapshot(s.snapshot),
	}
	seenUnits := make(map[string]struct{}, len(plan.GetBindings()))
	seenAllocations := make(map[string]struct{}, len(plan.GetBindings()))
	for _, binding := range plan.GetBindings() {
		if binding == nil || binding.GetBindingId() == "" || binding.GetPendingUnitId() == "" || binding.GetResources() == nil || len(binding.GetDeviceIds()) != 1 {
			return nil, fmt.Errorf("%w: each binding requires IDs, resources, and one logical device", ErrInvalidIntent)
		}
		if _, duplicate := seenUnits[binding.GetPendingUnitId()]; duplicate {
			return nil, fmt.Errorf("%w: pending unit %q is bound more than once", ErrInvalidIntent, binding.GetPendingUnitId())
		}
		seenUnits[binding.GetPendingUnitId()] = struct{}{}
		unit, ok := pending[binding.GetPendingUnitId()]
		if !ok || unit.GetExecutionId() != plan.GetExecutionId() || unit.GetStageId() != plan.GetStageId() || unit.GetIntentVersion() != plan.GetIntentVersion() {
			return nil, fmt.Errorf("%w: %q", ErrPendingUnitNotFound, binding.GetPendingUnitId())
		}
		if !proto.Equal(unit.GetRequestedResources(), binding.GetResources()) {
			return nil, fmt.Errorf("%w: binding resources differ from pending request", ErrInvalidIntent)
		}
		device := devices[binding.GetDeviceIds()[0]]
		if device == nil || device.GetHealth() != tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY {
			return nil, fmt.Errorf("%w: device %q is unavailable", ErrInsufficientResource, binding.GetDeviceIds()[0])
		}
		if !resourcesFit(binding.GetResources(), device.GetAllocatable()) {
			return nil, fmt.Errorf("%w: device %q", ErrInsufficientResource, device.GetDeviceId())
		}
		subtractResources(device.Allocatable, binding.GetResources())
		allocationID := allocationID(binding.GetBindingId())
		if _, duplicate := seenAllocations[allocationID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate binding_id %q", ErrInvalidIntent, binding.GetBindingId())
		}
		seenAllocations[allocationID] = struct{}{}
		reservation.pendingUnits = append(reservation.pendingUnits, clonePendingUnit(unit))
		reservation.allocationIDs = append(reservation.allocationIDs, allocationID)
		actionID := actionIDForBinding(plan, binding.GetBindingId())
		working.Allocations = append(working.Allocations, &tgsrlv1.Allocation{
			AllocationId:  allocationID,
			ExecutionId:   plan.GetExecutionId(),
			StageId:       plan.GetStageId(),
			IntentVersion: plan.GetIntentVersion(),
			JobId:         unit.GetJobId(),
			PendingUnitId: unit.GetPendingUnitId(),
			DeviceIds:     append([]string(nil), binding.GetDeviceIds()...),
			Resources:     cloneResourceVector(binding.GetResources()),
			State:         tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING,
			CreatedAt:     timestamppb.New(s.clock.Now()),
			ExpiresAt:     cloneTimestamp(plan.GetExpiresAt()),
			RunId:         firstNonEmpty(plan.GetRunId(), unit.GetRunId()),
			TraceId:       firstNonEmpty(plan.GetTraceId(), unit.GetTraceId()),
			DataKind:      firstKnownDataKind(plan.GetDataKind(), unit.GetDataKind()),
			Generation:    binding.GetGeneration(),
			RuntimeUnitId: firstNonEmpty(binding.GetRuntimeUnitId(), unit.GetRuntimeUnitId()),
			DecisionId:    plan.GetDecisionId(),
			PlanId:        plan.GetPlanId(),
			ActionId:      actionID,
		})
		delete(pending, binding.GetPendingUnitId())
	}
	working.PendingUnits = working.PendingUnits[:0]
	for _, unit := range pending {
		working.PendingUnits = append(working.PendingUnits, clonePendingUnit(unit))
	}
	sortPendingUnits(working.PendingUnits)
	s.commitSnapshotLocked(working)
	s.reservations[plan.GetPlanId()] = reservation
	return cloneSnapshot(s.snapshot), nil
}

func actionIDForBinding(plan *tgsrlv1.PlacementPlan, bindingID string) string {
	if plan == nil || bindingID == "" {
		return ""
	}
	for _, action := range plan.GetActions() {
		if action != nil && action.GetBinding().GetBindingId() == bindingID {
			return action.GetActionId()
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func firstKnownDataKind(values ...tgsrlv1.DataKind) tgsrlv1.DataKind {
	for _, value := range values {
		if value != tgsrlv1.DataKind_DATA_KIND_UNKNOWN {
			return value
		}
	}
	return tgsrlv1.DataKind_DATA_KIND_UNKNOWN
}

// FinalizePlan is the no-result convenience path. Success confirms every
// reservation; failure conservatively retains it because no per-action
// compensation evidence is available. It is idempotent.
func (s *Store) FinalizePlan(plan *tgsrlv1.PlacementPlan, succeeded bool) (*tgsrlv1.ClusterSnapshot, error) {
	return s.FinalizePlanResults(plan, succeeded, nil)
}

// FinalizePlanResults converges an executed reservation from per-action
// results. After a failed plan, only bindings whose actions may have left a
// forward side effect remain allocated. An incomplete rollback marks the
// retained allocation failed so callers can distinguish it from confirmed
// active capacity. Missing or contradictory result evidence is handled
// conservatively by retaining the affected binding.
func (s *Store) FinalizePlanResults(plan *tgsrlv1.PlacementPlan, succeeded bool, results []*tgsrlv1.ActionResult) (*tgsrlv1.ClusterSnapshot, error) {
	if plan == nil {
		return nil, fmt.Errorf("%w: nil plan", ErrPlanNotReserved)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	reservation, ok := s.reservations[plan.GetPlanId()]
	if !ok || !proto.Equal(reservation.plan, plan) {
		return nil, fmt.Errorf("%w: %q", ErrPlanNotReserved, plan.GetPlanId())
	}
	purpose, _ := effectivePlanPurpose(plan)
	if isMutationPurpose(purpose) {
		outcome := mutationOutcome(plan, reservation, succeeded, results)
		if reservation.finalized {
			if reservation.succeeded != succeeded || !equalStrings(reservation.retainedAllocationIDs, outcome.retainedIDs) {
				return nil, fmt.Errorf("%w: plan %q finalized with different outcome", ErrPlanAlreadyReserved, plan.GetPlanId())
			}
			return cloneSnapshot(s.snapshot), nil
		}
		snapshot, retainedIDs, err := finalizeMutationPlanResultsLocked(s, plan, reservation, succeeded, results)
		if err != nil {
			return nil, err
		}
		reservation.finalized = true
		reservation.succeeded = succeeded
		reservation.retainedAllocationIDs = retainedIDs
		return snapshot, nil
	}
	retained, degraded := retainedAllocations(plan, succeeded, results)
	retainedIDs := sortedSetValues(retained)
	if reservation.finalized {
		if reservation.succeeded != succeeded || !equalStrings(reservation.retainedAllocationIDs, retainedIDs) {
			return nil, fmt.Errorf("%w: plan %q finalized with different outcome", ErrPlanAlreadyReserved, plan.GetPlanId())
		}
		return cloneSnapshot(s.snapshot), nil
	}
	if s.snapshot.GetRevision() == math.MaxUint64 {
		return nil, ErrRevisionExhausted
	}
	working := cloneSnapshot(s.snapshot)
	allocationSet := make(map[string]struct{}, len(reservation.allocationIDs))
	for _, allocationID := range reservation.allocationIDs {
		allocationSet[allocationID] = struct{}{}
	}
	kept := working.Allocations[:0]
	for _, allocation := range working.GetAllocations() {
		if _, reserved := allocationSet[allocation.GetAllocationId()]; !reserved {
			kept = append(kept, allocation)
			continue
		}
		if _, retain := retained[allocation.GetAllocationId()]; retain {
			if _, uncertain := degraded[allocation.GetAllocationId()]; uncertain {
				allocation.State = tgsrlv1.AllocationState_ALLOCATION_STATE_FAILED
			} else {
				allocation.State = tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE
			}
			kept = append(kept, allocation)
			continue
		}
		for _, deviceID := range allocation.GetDeviceIds() {
			if device := findDevice(working.GetDevices(), deviceID); device != nil {
				addResourcesCapped(device.Allocatable, allocation.GetResources(), device.GetCapacity())
			}
		}
	}
	working.Allocations = kept
	if !succeeded {
		retainedPending := make(map[string]struct{}, len(retained))
		for _, binding := range reservation.plan.GetBindings() {
			if _, keep := retained[allocationID(binding.GetBindingId())]; keep {
				retainedPending[binding.GetPendingUnitId()] = struct{}{}
			}
		}
		for _, unit := range reservation.pendingUnits {
			if _, keep := retainedPending[unit.GetPendingUnitId()]; keep {
				continue
			}
			working.PendingUnits = append(working.PendingUnits, clonePendingUnit(unit))
		}
		sortPendingUnits(working.PendingUnits)
	}
	s.commitSnapshotLocked(working)
	reservation.finalized = true
	reservation.succeeded = succeeded
	reservation.retainedAllocationIDs = retainedIDs
	return cloneSnapshot(s.snapshot), nil
}

func retainedAllocations(plan *tgsrlv1.PlacementPlan, succeeded bool, results []*tgsrlv1.ActionResult) (map[string]struct{}, map[string]struct{}) {
	retained := make(map[string]struct{}, len(plan.GetBindings()))
	degraded := make(map[string]struct{}, len(plan.GetBindings()))
	if succeeded {
		retainAllBindings(retained, plan)
		return retained, degraded
	}
	if len(results) == 0 {
		retainAllBindings(retained, plan)
		return retained, degraded
	}

	resultByAction := make(map[string]*tgsrlv1.ActionResult, len(results))
	planActions := make(map[string]struct{}, len(plan.GetActions()))
	for _, action := range plan.GetActions() {
		if action != nil && action.GetActionId() != "" {
			planActions[action.GetActionId()] = struct{}{}
		}
	}
	malformedEvidence := false
	for _, result := range results {
		if result == nil || result.GetActionId() == "" {
			malformedEvidence = true
			continue
		}
		if _, duplicate := resultByAction[result.GetActionId()]; duplicate {
			malformedEvidence = true
		}
		if _, expected := planActions[result.GetActionId()]; !expected {
			malformedEvidence = true
		}
		resultByAction[result.GetActionId()] = result
	}
	coveredBindings := make(map[string]struct{}, len(plan.GetBindings()))
	for _, action := range plan.GetActions() {
		if action == nil || action.GetBinding() == nil || action.GetBinding().GetBindingId() == "" {
			continue
		}
		bindingID := action.GetBinding().GetBindingId()
		coveredBindings[bindingID] = struct{}{}
		result, ok := resultByAction[action.GetActionId()]
		if !ok {
			malformedEvidence = true
			continue
		}
		rollbackIncomplete := result.GetRollbackAttempted() &&
			result.GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK
		mutationMayRemain := rollbackIncomplete ||
			(result.GetStatus() == tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED && !result.GetRollbackAttempted())
		if mutationMayRemain {
			id := allocationID(bindingID)
			retained[id] = struct{}{}
			if rollbackIncomplete {
				degraded[id] = struct{}{}
			}
		}
	}
	for _, binding := range plan.GetBindings() {
		if _, covered := coveredBindings[binding.GetBindingId()]; !covered {
			malformedEvidence = true
		}
	}
	if malformedEvidence {
		// Unknown provider evidence must never release capacity that may still
		// be occupied. Retaining the whole reservation is intentionally safe.
		retainAllBindings(retained, plan)
	}
	return retained, degraded
}

func isMutationPurpose(purpose tgsrlv1.PlanPurpose) bool {
	switch purpose {
	case tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE,
		tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION,
		tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECOVERY,
		tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION:
		return true
	default:
		return false
	}
}

func retainAllBindings(retained map[string]struct{}, plan *tgsrlv1.PlacementPlan) {
	for _, binding := range plan.GetBindings() {
		retained[allocationID(binding.GetBindingId())] = struct{}{}
	}
}

func sortedSetValues(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func equalStrings(left, right []string) bool {
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

func resourcesFit(requested, available *tgsrlv1.ResourceVector) bool {
	if requested == nil || available == nil || math.IsNaN(requested.GetAcceleratorUnits()) || math.IsInf(requested.GetAcceleratorUnits(), 0) {
		return false
	}
	return requested.GetCpuMillis() <= available.GetCpuMillis() &&
		requested.GetMemoryBytes() <= available.GetMemoryBytes() &&
		requested.GetAcceleratorUnits() <= available.GetAcceleratorUnits()+1e-12 &&
		requested.GetEphemeralStorageBytes() <= available.GetEphemeralStorageBytes() &&
		requested.GetNetworkBandwidthBps() <= available.GetNetworkBandwidthBps()
}

func subtractResources(available, requested *tgsrlv1.ResourceVector) {
	available.CpuMillis -= requested.GetCpuMillis()
	available.MemoryBytes -= requested.GetMemoryBytes()
	available.AcceleratorUnits -= requested.GetAcceleratorUnits()
	available.EphemeralStorageBytes -= requested.GetEphemeralStorageBytes()
	available.NetworkBandwidthBps -= requested.GetNetworkBandwidthBps()
}

func addResourcesCapped(available, returned, capacity *tgsrlv1.ResourceVector) {
	available.CpuMillis = cappedAdd(available.GetCpuMillis(), returned.GetCpuMillis(), capacity.GetCpuMillis())
	available.MemoryBytes = cappedAdd(available.GetMemoryBytes(), returned.GetMemoryBytes(), capacity.GetMemoryBytes())
	available.AcceleratorUnits = math.Min(capacity.GetAcceleratorUnits(), available.GetAcceleratorUnits()+returned.GetAcceleratorUnits())
	available.EphemeralStorageBytes = cappedAdd(available.GetEphemeralStorageBytes(), returned.GetEphemeralStorageBytes(), capacity.GetEphemeralStorageBytes())
	available.NetworkBandwidthBps = cappedAdd(available.GetNetworkBandwidthBps(), returned.GetNetworkBandwidthBps(), capacity.GetNetworkBandwidthBps())
}

func cappedAdd(left, right, limit uint64) uint64 {
	if left >= limit || right > limit-left {
		return limit
	}
	return left + right
}

func findDevice(devices []*tgsrlv1.Device, deviceID string) *tgsrlv1.Device {
	for _, device := range devices {
		if device.GetDeviceId() == deviceID {
			return device
		}
	}
	return nil
}

func allocationID(bindingID string) string {
	return stableID("allocation", bindingID)
}
