package state

import (
	"fmt"
	"math"
	"sort"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Store) reserveMutationPlanLocked(
	plan *tgsrlv1.PlacementPlan,
) (*tgsrlv1.ClusterSnapshot, error) {
	if plan.GetPurpose() == tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION {
		return s.reservePreemptionPlanLocked(plan)
	}
	if len(plan.GetActions()) == 0 {
		return nil, fmt.Errorf("%w: mutation plans require at least one action", ErrInvalidIntent)
	}
	if len(plan.GetActions()) > 1 {
		return s.reserveLifecyclePlanLocked(plan)
	}
	action := plan.GetActions()[0]
	if action == nil || action.GetActionId() == "" {
		return nil, fmt.Errorf("%w: mutation action is required", ErrInvalidIntent)
	}
	affected, err := affectedAllocationsByID(s.snapshot.GetAllocations(), plan.GetAffectedAllocationIds())
	if err != nil {
		return nil, err
	}
	if err := ensureMutationTargetsUnlocked(s.reservations, plan.GetPlanId(), plan.GetAffectedAllocationIds()); err != nil {
		return nil, err
	}
	if s.snapshot.GetRevision() == math.MaxUint64 {
		return nil, ErrRevisionExhausted
	}

	working := cloneSnapshot(s.snapshot)
	reservation := &planReservation{
		plan:              clonePlan(plan),
		beforeSnapshot:    cloneSnapshot(s.snapshot),
		deviceAllocatable: make(map[string]*tgsrlv1.ResourceVector),
	}
	if err := applyMutationReservation(
		working,
		plan,
		action,
		affected,
		reservation,
		s.clock.Now(),
	); err != nil {
		return nil, err
	}
	if !proto.Equal(working, s.snapshot) {
		s.commitSnapshotLocked(working)
	}
	s.reservations[plan.GetPlanId()] = reservation
	return cloneSnapshot(s.snapshot), nil
}

func (s *Store) reserveLifecyclePlanLocked(plan *tgsrlv1.PlacementPlan) (*tgsrlv1.ClusterSnapshot, error) {
	affected, err := affectedAllocationsByID(s.snapshot.GetAllocations(), plan.GetAffectedAllocationIds())
	if err != nil {
		return nil, err
	}
	if err := ensureMutationTargetsUnlocked(s.reservations, plan.GetPlanId(), plan.GetAffectedAllocationIds()); err != nil {
		return nil, err
	}
	mutationsByAllocation := make(map[string][]tgsrlv1.ActionType, len(plan.GetActions()))
	allocationIDs := make(map[string]struct{}, len(plan.GetActions()))
	for _, action := range plan.GetActions() {
		if action == nil || !mergeableLifecycleAction(action.GetActionType()) {
			return nil, fmt.Errorf("%w: multi-action mutation contains exclusive action %s", ErrInvalidIntent, action.GetActionType())
		}
		allocation, err := singleAffectedAllocation(action, affected)
		if err != nil {
			return nil, err
		}
		for _, existing := range mutationsByAllocation[allocation.GetAllocationId()] {
			if !compatibleLifecycleMutations(existing, action.GetActionType()) {
				return nil, fmt.Errorf("%w: conflicting mutation for allocation %q", ErrInvalidIntent, allocation.GetAllocationId())
			}
		}
		mutationsByAllocation[allocation.GetAllocationId()] = append(mutationsByAllocation[allocation.GetAllocationId()], action.GetActionType())
		allocationIDs[allocation.GetAllocationId()] = struct{}{}
	}
	reservation := &planReservation{plan: clonePlan(plan), beforeSnapshot: cloneSnapshot(s.snapshot)}
	for allocationID := range allocationIDs {
		reservation.allocationIDs = append(reservation.allocationIDs, allocationID)
	}
	sort.Strings(reservation.allocationIDs)
	s.reservations[plan.GetPlanId()] = reservation
	return cloneSnapshot(s.snapshot), nil
}

func mergeableLifecycleAction(actionType tgsrlv1.ActionType) bool {
	switch actionType {
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE,
		tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY,
		tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
		tgsrlv1.ActionType_ACTION_TYPE_RESUME,
		tgsrlv1.ActionType_ACTION_TYPE_SLEEP,
		tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		return true
	default:
		return false
	}
}

func compatibleLifecycleMutations(left, right tgsrlv1.ActionType) bool {
	return left == tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE && right == tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY ||
		left == tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY && right == tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE
}

func applyMutationReservation(
	working *tgsrlv1.ClusterSnapshot,
	plan *tgsrlv1.PlacementPlan,
	action *tgsrlv1.Action,
	affected map[string]*tgsrlv1.Allocation,
	reservation *planReservation,
	nowTimestamp time.Time,
) error {
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		return reserveReleaseAction(working, plan, action, affected, reservation)
	case tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
		tgsrlv1.ActionType_ACTION_TYPE_RESUME,
		tgsrlv1.ActionType_ACTION_TYPE_SLEEP,
		tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD,
		tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE,
		tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY:
		return reserveLifecycleOnlyAction(working, action, affected, reservation)
	case tgsrlv1.ActionType_ACTION_TYPE_RESIZE:
		return reserveResizeAction(working, plan, action, affected, reservation)
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND:
		return reserveRebindAction(working, plan, action, affected, reservation, nowTimestamp)
	case tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		return reserveRecreateAction(working, plan, action, affected, reservation, nowTimestamp)
	default:
		return fmt.Errorf("%w: unsupported mutation action %s", ErrInvalidIntent, action.GetActionType())
	}
}

func reserveReleaseAction(
	working *tgsrlv1.ClusterSnapshot,
	plan *tgsrlv1.PlacementPlan,
	action *tgsrlv1.Action,
	affected map[string]*tgsrlv1.Allocation,
	reservation *planReservation,
) error {
	allocation, err := singleAffectedAllocation(action, affected)
	if err != nil {
		return err
	}
	if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
		return fmt.Errorf("%w: allocation %q is not active", ErrInvalidIntent, allocation.GetAllocationId())
	}
	mutable, err := findAllocationMutable(working, allocation.GetAllocationId())
	if err != nil {
		return err
	}
	mutable.State = tgsrlv1.AllocationState_ALLOCATION_STATE_RELEASING
	reservation.allocationIDs = append(reservation.allocationIDs, allocation.GetAllocationId())
	if plan.GetPurpose() == tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION {
		reservation.retainedAllocationIDs = append(reservation.retainedAllocationIDs, allocation.GetAllocationId())
	}
	return nil
}

func reserveLifecycleOnlyAction(
	working *tgsrlv1.ClusterSnapshot,
	action *tgsrlv1.Action,
	affected map[string]*tgsrlv1.Allocation,
	reservation *planReservation,
) error {
	_ = working
	allocation, err := singleAffectedAllocation(action, affected)
	if err != nil {
		return err
	}
	if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
		return fmt.Errorf("%w: allocation %q is not active", ErrInvalidIntent, allocation.GetAllocationId())
	}
	reservation.allocationIDs = append(reservation.allocationIDs, allocation.GetAllocationId())
	return nil
}

func reserveResizeAction(
	working *tgsrlv1.ClusterSnapshot,
	plan *tgsrlv1.PlacementPlan,
	action *tgsrlv1.Action,
	affected map[string]*tgsrlv1.Allocation,
	reservation *planReservation,
) error {
	allocation, err := singleAffectedAllocation(action, affected)
	if err != nil {
		return err
	}
	if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
		return fmt.Errorf("%w: allocation %q is not active", ErrInvalidIntent, allocation.GetAllocationId())
	}
	if action.GetBinding() == nil || action.GetBinding().GetResources() == nil {
		return fmt.Errorf("%w: resize action requires binding resources", ErrInvalidIntent)
	}
	target := action.GetBinding().GetResources()
	current := allocation.GetResources()
	if current == nil {
		return fmt.Errorf("%w: allocation %q has no resources", ErrInvalidIntent, allocation.GetAllocationId())
	}
	mutable, err := findAllocationMutable(working, allocation.GetAllocationId())
	if err != nil {
		return err
	}
	if resourcesEqual(target, current) {
		reservation.allocationIDs = append(reservation.allocationIDs, allocation.GetAllocationId())
		return nil
	}
	rememberDeviceAllocatable(reservation, allocation.GetDeviceIds())
	consumed := positiveResourceDelta(target, current)
	returned := positiveResourceDelta(current, target)
	for _, deviceID := range mutable.GetDeviceIds() {
		device := findDevice(working.GetDevices(), deviceID)
		if device == nil {
			return fmt.Errorf("%w: device %q is unavailable", ErrInsufficientResource, deviceID)
		}
		if !resourcesFit(consumed, device.GetAllocatable()) {
			return fmt.Errorf("%w: device %q", ErrInsufficientResource, deviceID)
		}
	}
	for _, deviceID := range mutable.GetDeviceIds() {
		device := findDevice(working.GetDevices(), deviceID)
		subtractResources(device.Allocatable, consumed)
		addResourcesCapped(device.Allocatable, returned, device.GetCapacity())
	}
	mutable.Resources = cloneResourceVector(target)
	mutable.IntentVersion = plan.GetIntentVersion()
	reservation.allocationIDs = append(reservation.allocationIDs, allocation.GetAllocationId())
	return nil
}

func reserveRebindAction(
	working *tgsrlv1.ClusterSnapshot,
	plan *tgsrlv1.PlacementPlan,
	action *tgsrlv1.Action,
	affected map[string]*tgsrlv1.Allocation,
	reservation *planReservation,
	nowTimestamp time.Time,
) error {
	return reserveReplacementAction(working, plan, action, affected, reservation, nowTimestamp, false)
}

func reserveRecreateAction(
	working *tgsrlv1.ClusterSnapshot,
	plan *tgsrlv1.PlacementPlan,
	action *tgsrlv1.Action,
	affected map[string]*tgsrlv1.Allocation,
	reservation *planReservation,
	nowTimestamp time.Time,
) error {
	return reserveReplacementAction(working, plan, action, affected, reservation, nowTimestamp, true)
}

func reserveReplacementAction(
	working *tgsrlv1.ClusterSnapshot,
	plan *tgsrlv1.PlacementPlan,
	action *tgsrlv1.Action,
	affected map[string]*tgsrlv1.Allocation,
	reservation *planReservation,
	nowTimestamp time.Time,
	sameDevice bool,
) error {
	allocation, err := singleAffectedAllocation(action, affected)
	if err != nil {
		return err
	}
	if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
		return fmt.Errorf("%w: allocation %q is not active", ErrInvalidIntent, allocation.GetAllocationId())
	}
	binding := action.GetBinding()
	if binding == nil || binding.GetResources() == nil || len(binding.GetDeviceIds()) == 0 {
		return fmt.Errorf("%w: %s action requires binding target devices/resources", ErrInvalidIntent, action.GetActionType())
	}
	if !proto.Equal(binding.GetResources(), allocation.GetResources()) {
		return fmt.Errorf("%w: %s must reserve full existing allocation resources", ErrInvalidIntent, action.GetActionType())
	}
	rememberDeviceAllocatable(reservation, allocation.GetDeviceIds())
	// A rebind changes two capacity ledgers: the replacement reservation
	// consumes the target device during prepare, and commit later returns the
	// source capacity. Record both touched ledgers for persistence/debugging;
	// rollback reverses only this transaction's delta rather than replacing a
	// device's allocatable vector with this stale before-image.
	rememberDeviceAllocatable(reservation, binding.GetDeviceIds())
	if sameDevice && !equalStrings(binding.GetDeviceIds(), allocation.GetDeviceIds()) {
		return fmt.Errorf("%w: recreate must target the existing device set", ErrInvalidIntent)
	}
	if !sameDevice {
		for _, deviceID := range binding.GetDeviceIds() {
			device := findDevice(working.GetDevices(), deviceID)
			if device == nil || device.GetHealth() != tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY {
				return fmt.Errorf("%w: device %q is unavailable", ErrInsufficientResource, deviceID)
			}
			if !resourcesFit(binding.GetResources(), device.GetAllocatable()) {
				return fmt.Errorf("%w: device %q", ErrInsufficientResource, deviceID)
			}
		}
		for _, deviceID := range binding.GetDeviceIds() {
			device := findDevice(working.GetDevices(), deviceID)
			subtractResources(device.Allocatable, binding.GetResources())
		}
	}
	newAllocationID := allocationID(binding.GetBindingId())
	if _, err := findAllocationMutable(working, newAllocationID); err == nil {
		return fmt.Errorf("%w: duplicate binding_id %q", ErrInvalidIntent, binding.GetBindingId())
	}
	working.Allocations = append(working.Allocations, buildReplacementAllocation(plan, action, allocation, binding, newAllocationID, nowTimestamp, sameDevice))
	reservation.allocationIDs = append(
		reservation.allocationIDs,
		allocation.GetAllocationId(),
		newAllocationID,
	)
	return nil
}

func cloneOptionalInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func int32Pointer(value int32) *int32 { return &value }

func consumeReplacementPendingUnit(
	pending []*tgsrlv1.PendingUnit,
	plan *tgsrlv1.PlacementPlan,
	binding *tgsrlv1.Binding,
) (*tgsrlv1.PendingUnit, []*tgsrlv1.PendingUnit, error) {
	remaining := make([]*tgsrlv1.PendingUnit, 0, len(pending))
	var matched *tgsrlv1.PendingUnit
	for _, unit := range pending {
		if matched == nil &&
			unit.GetPendingUnitId() == binding.GetPendingUnitId() &&
			unit.GetExecutionId() == plan.GetExecutionId() &&
			unit.GetStageId() == plan.GetStageId() &&
			unit.GetIntentVersion() == plan.GetIntentVersion() {
			if !proto.Equal(unit.GetRequestedResources(), binding.GetResources()) {
				return nil, nil, fmt.Errorf("%w: replacement binding resources differ from pending request", ErrInvalidIntent)
			}
			matched = clonePendingUnit(unit)
			continue
		}
		remaining = append(remaining, unit)
	}
	if matched == nil {
		return nil, nil, fmt.Errorf("%w: %q", ErrPendingUnitNotFound, binding.GetPendingUnitId())
	}
	return matched, remaining, nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func addResources(available, returned *tgsrlv1.ResourceVector) {
	if available == nil || returned == nil {
		return
	}
	available.CpuMillis += returned.GetCpuMillis()
	available.MemoryBytes += returned.GetMemoryBytes()
	available.AcceleratorUnits += returned.GetAcceleratorUnits()
	available.EphemeralStorageBytes += returned.GetEphemeralStorageBytes()
	available.NetworkBandwidthBps += returned.GetNetworkBandwidthBps()
}

func buildReplacementAllocation(
	plan *tgsrlv1.PlacementPlan,
	action *tgsrlv1.Action,
	source *tgsrlv1.Allocation,
	binding *tgsrlv1.Binding,
	allocationID string,
	nowTimestamp time.Time,
	recreate bool,
) *tgsrlv1.Allocation {
	generation := binding.GetGeneration()
	runtimeUnitID := source.GetRuntimeUnitId()
	if recreate {
		runtimeUnitID = binding.GetRuntimeUnitId()
	}
	return &tgsrlv1.Allocation{
		AllocationId:  allocationID,
		ExecutionId:   source.GetExecutionId(),
		StageId:       source.GetStageId(),
		IntentVersion: plan.GetIntentVersion(),
		JobId:         source.GetJobId(),
		PendingUnitId: source.GetPendingUnitId(),
		DeviceIds:     append([]string(nil), binding.GetDeviceIds()...),
		Resources:     cloneResourceVector(binding.GetResources()),
		State:         tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING,
		CreatedAt:     timestamppb.New(nowTimestamp),
		ExpiresAt:     cloneTimestamp(plan.GetExpiresAt()),
		RunId:         source.GetRunId(),
		TraceId:       source.GetTraceId(),
		DataKind:      source.GetDataKind(),
		Generation:    generation,
		RuntimeUnitId: runtimeUnitID,
		DecisionId:    plan.GetDecisionId(),
		PlanId:        plan.GetPlanId(),
		ActionId:      action.GetActionId(),
		Priority:      cloneOptionalInt32(source.Priority),
		SandboxId:     binding.GetSandboxId(),
		BindingId:     binding.GetBindingId(),
	}
}

func affectedAllocationsByID(
	allocations []*tgsrlv1.Allocation,
	ids []string,
) (map[string]*tgsrlv1.Allocation, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: affected_allocation_ids are required", ErrInvalidIntent)
	}
	items := make(map[string]*tgsrlv1.Allocation, len(ids))
	for _, id := range ids {
		found := false
		for _, allocation := range allocations {
			if allocation.GetAllocationId() == id {
				if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE &&
					allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING &&
					allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_RELEASING {
					return nil, fmt.Errorf("%w: allocation %q is not reservable", ErrInvalidIntent, id)
				}
				items[id] = proto.Clone(allocation).(*tgsrlv1.Allocation)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: affected allocation %q", ErrPendingUnitNotFound, id)
		}
	}
	return items, nil
}

func ensureMutationTargetsUnlocked(
	reservations map[string]*planReservation,
	planID string,
	affected []string,
) error {
	locked := make(map[string]string)
	for existingPlanID, reservation := range reservations {
		if existingPlanID == planID || reservation == nil || reservation.finalized {
			continue
		}
		for _, allocationID := range reservation.allocationIDs {
			locked[allocationID] = existingPlanID
		}
	}
	for _, allocationID := range affected {
		if holder, ok := locked[allocationID]; ok {
			return fmt.Errorf("%w: target %q already reserved by %s", ErrPlanAlreadyReserved, allocationID, holder)
		}
	}
	return nil
}

func singleAffectedAllocation(
	action *tgsrlv1.Action,
	affected map[string]*tgsrlv1.Allocation,
) (*tgsrlv1.Allocation, error) {
	targetID := action.GetTargetId()
	if targetID == "" && len(affected) == 1 {
		for _, allocation := range affected {
			return allocation, nil
		}
	}
	allocation := affected[targetID]
	if allocation == nil {
		return nil, fmt.Errorf("%w: target allocation %q", ErrPendingUnitNotFound, targetID)
	}
	return allocation, nil
}

func findAllocationMutable(
	snapshot *tgsrlv1.ClusterSnapshot,
	allocationID string,
) (*tgsrlv1.Allocation, error) {
	for _, allocation := range snapshot.GetAllocations() {
		if allocation.GetAllocationId() == allocationID {
			return allocation, nil
		}
	}
	return nil, fmt.Errorf("%w: allocation %q", ErrPendingUnitNotFound, allocationID)
}

// positiveResourceDelta returns max(left-right, 0) for every resource
// dimension. Resize may grow one dimension while shrinking another, so an
// unsigned whole-vector subtraction is never safe here.
func positiveResourceDelta(left, right *tgsrlv1.ResourceVector) *tgsrlv1.ResourceVector {
	return &tgsrlv1.ResourceVector{
		CpuMillis:             positiveUint64Delta(left.GetCpuMillis(), right.GetCpuMillis()),
		MemoryBytes:           positiveUint64Delta(left.GetMemoryBytes(), right.GetMemoryBytes()),
		AcceleratorUnits:      math.Max(0, left.GetAcceleratorUnits()-right.GetAcceleratorUnits()),
		EphemeralStorageBytes: positiveUint64Delta(left.GetEphemeralStorageBytes(), right.GetEphemeralStorageBytes()),
		NetworkBandwidthBps:   positiveUint64Delta(left.GetNetworkBandwidthBps(), right.GetNetworkBandwidthBps()),
	}
}

func positiveUint64Delta(left, right uint64) uint64 {
	if left <= right {
		return 0
	}
	return left - right
}

func resourcesEqual(left, right *tgsrlv1.ResourceVector) bool {
	return proto.Equal(left, right)
}

func removeAllocationsByID(
	allocations []*tgsrlv1.Allocation,
	ids []string,
) []*tgsrlv1.Allocation {
	if len(ids) == 0 {
		return allocations
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	kept := allocations[:0]
	for _, allocation := range allocations {
		if _, remove := set[allocation.GetAllocationId()]; remove {
			continue
		}
		kept = append(kept, allocation)
	}
	return kept
}

func restoreMutationStateFromBeforeImage(
	working *tgsrlv1.ClusterSnapshot,
	plan *tgsrlv1.PlacementPlan,
	reservation *planReservation,
) error {
	if reservation == nil || plan == nil || len(plan.GetActions()) == 0 {
		return fmt.Errorf("%w: mutation reservation is incomplete", ErrPlanNotReserved)
	}
	before := reservation.beforeSnapshot
	if before == nil {
		return fmt.Errorf("%w: mutation reservation has no before-image", ErrPlanNotReserved)
	}
	if len(plan.GetActions()) > 1 {
		for _, action := range plan.GetActions() {
			if action == nil || !mergeableLifecycleAction(action.GetActionType()) {
				return fmt.Errorf("%w: multi-action mutation contains exclusive action %s", ErrInvalidIntent, action.GetActionType())
			}
		}
		return nil
	}
	beforeByID := make(map[string]*tgsrlv1.Allocation, len(before.GetAllocations()))
	for _, allocation := range before.GetAllocations() {
		beforeByID[allocation.GetAllocationId()] = allocation
	}
	action := plan.GetActions()[0]
	targetID := action.GetTargetId()
	if targetID == "" && len(reservation.allocationIDs) > 0 {
		targetID = reservation.allocationIDs[0]
	}
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		previous := beforeByID[targetID]
		current, err := findAllocationMutable(working, targetID)
		if previous == nil || err != nil {
			return fmt.Errorf("%w: release target %q is unavailable for rollback", ErrPlanNotReserved, targetID)
		}
		current.State = previous.GetState()
	case tgsrlv1.ActionType_ACTION_TYPE_RESIZE:
		previous := beforeByID[targetID]
		current, err := findAllocationMutable(working, targetID)
		if previous == nil || err != nil {
			return fmt.Errorf("%w: resize target %q is unavailable for rollback", ErrPlanNotReserved, targetID)
		}
		target := action.GetBinding().GetResources()
		preparedConsumption := positiveResourceDelta(target, previous.GetResources())
		preparedReturn := positiveResourceDelta(previous.GetResources(), target)
		if err := reverseDeviceResourceDelta(working, previous.GetDeviceIds(), preparedConsumption, preparedReturn); err != nil {
			return err
		}
		// Resize prepare owns only these allocation fields. Preserve any other
		// observation written while provider execution was in flight.
		current.Resources = cloneResourceVector(previous.GetResources())
		current.IntentVersion = previous.GetIntentVersion()
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND:
		binding := action.GetBinding()
		if err := reverseDeviceResourceDelta(working, binding.GetDeviceIds(), binding.GetResources(), nil); err != nil {
			return err
		}
		if len(reservation.allocationIDs) >= 2 {
			working.Allocations = removeAllocationsByID(working.GetAllocations(), reservation.allocationIDs[1:])
		}
	case tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		if len(reservation.allocationIDs) >= 2 {
			working.Allocations = removeAllocationsByID(working.GetAllocations(), reservation.allocationIDs[1:])
		}
	case tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
		tgsrlv1.ActionType_ACTION_TYPE_RESUME,
		tgsrlv1.ActionType_ACTION_TYPE_SLEEP,
		tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD,
		tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE,
		tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY:
		// Lifecycle-only prepare does not change authoritative snapshot fields.
	default:
		return fmt.Errorf("%w: unsupported mutation rollback %s", ErrInvalidIntent, action.GetActionType())
	}
	restorePendingUnitsFromReservation(working, reservation.pendingUnits)
	return nil
}

// reverseDeviceResourceDelta undoes the prepare-time resource adjustment.
// preparedConsumption was subtracted during prepare and preparedReturn was
// added. Applying the inverse to the current value preserves changes committed
// by other transactions while provider execution was in flight.
func reverseDeviceResourceDelta(
	working *tgsrlv1.ClusterSnapshot,
	deviceIDs []string,
	preparedConsumption *tgsrlv1.ResourceVector,
	preparedReturn *tgsrlv1.ResourceVector,
) error {
	if preparedConsumption == nil {
		preparedConsumption = &tgsrlv1.ResourceVector{}
	}
	if preparedReturn == nil {
		preparedReturn = &tgsrlv1.ResourceVector{}
	}
	for _, deviceID := range deviceIDs {
		device := findDevice(working.GetDevices(), deviceID)
		if device == nil {
			return fmt.Errorf("%w: rollback device %q is unavailable", ErrInsufficientResource, deviceID)
		}
		if !resourcesFit(preparedReturn, device.GetAllocatable()) {
			return fmt.Errorf("%w: rollback delta on device %q", ErrInsufficientResource, deviceID)
		}
	}
	for _, deviceID := range deviceIDs {
		device := findDevice(working.GetDevices(), deviceID)
		subtractResources(device.Allocatable, preparedReturn)
		addResourcesCapped(device.Allocatable, preparedConsumption, device.GetCapacity())
	}
	return nil
}

func successfulMutationRetainedIDs(
	plan *tgsrlv1.PlacementPlan,
	reservation *planReservation,
) []string {
	if reservation == nil {
		return nil
	}
	switch plan.GetActions()[0].GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		return nil
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND,
		tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		if len(reservation.allocationIDs) == 2 {
			return []string{reservation.allocationIDs[1]}
		}
	}
	return append([]string(nil), reservation.allocationIDs...)
}

func failedMutationRetainedIDs(
	plan *tgsrlv1.PlacementPlan,
	reservation *planReservation,
	results []*tgsrlv1.ActionResult,
) ([]string, bool) {
	if reservation == nil || len(plan.GetActions()) == 0 {
		return nil, false
	}
	action := plan.GetActions()[0]
	if len(results) != 1 || results[0] == nil || results[0].GetActionId() != action.GetActionId() {
		return append([]string(nil), reservation.allocationIDs...), true
	}
	result := results[0]
	rollbackIncomplete := result.GetRollbackAttempted() &&
		result.GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK
	mutationMayRemain := rollbackIncomplete ||
		(!result.GetRollbackAttempted() &&
			result.GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED &&
			result.GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SKIPPED &&
			result.GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK)
	if !mutationMayRemain {
		return nil, false
	}
	if rollbackIncomplete {
		return append([]string(nil), reservation.allocationIDs...), true
	}
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		return append([]string(nil), reservation.allocationIDs...), false
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND,
		tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		if len(reservation.allocationIDs) == 2 {
			return []string{reservation.allocationIDs[1]}, false
		}
	}
	return append([]string(nil), reservation.allocationIDs...), false
}

type mutationFinalizationOutcome struct {
	restore     bool
	retainedIDs []string
}

func mutationOutcome(
	plan *tgsrlv1.PlacementPlan,
	reservation *planReservation,
	succeeded bool,
	results []*tgsrlv1.ActionResult,
) mutationFinalizationOutcome {
	if succeeded {
		return mutationFinalizationOutcome{
			retainedIDs: sortedStrings(successfulMutationRetainedIDs(plan, reservation)),
		}
	}
	if mutationResultConfirmsRestoration(plan, results) {
		return mutationFinalizationOutcome{restore: true}
	}
	retainedIDs, _ := failedMutationRetainedIDs(plan, reservation, results)
	return mutationFinalizationOutcome{retainedIDs: sortedStrings(retainedIDs)}
}

// A failed or skipped single action did not commit a forward effect. Providers
// likewise explicitly confirm compensation with ROLLED_BACK. In both cases the
// state reservation can be reversed. Unknown/malformed evidence, a successful
// uncompensated action, or an incomplete rollback may still have a live side
// effect and must remain retained as FAILED.
func mutationResultConfirmsRestoration(
	plan *tgsrlv1.PlacementPlan,
	results []*tgsrlv1.ActionResult,
) bool {
	if plan == nil || len(plan.GetActions()) == 0 || len(results) != len(plan.GetActions()) {
		return false
	}
	byID := make(map[string]*tgsrlv1.ActionResult, len(results))
	for _, result := range results {
		if result == nil || result.GetActionId() == "" {
			return false
		}
		if _, duplicate := byID[result.GetActionId()]; duplicate {
			return false
		}
		byID[result.GetActionId()] = result
	}
	for _, action := range plan.GetActions() {
		result := byID[action.GetActionId()]
		if result == nil {
			return false
		}
		if result.GetRollbackAttempted() {
			if result.GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK {
				return false
			}
			continue
		}
		switch result.GetStatus() {
		case tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
			tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SKIPPED,
			tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK:
		default:
			return false
		}
	}
	return true
}

func sortedStrings(items []string) []string {
	cloned := append([]string(nil), items...)
	sort.Strings(cloned)
	return cloned
}

func markAllocationsFailed(
	snapshot *tgsrlv1.ClusterSnapshot,
	allocationIDs []string,
) {
	for _, allocationID := range allocationIDs {
		allocation, err := findAllocationMutable(snapshot, allocationID)
		if err != nil {
			continue
		}
		allocation.State = tgsrlv1.AllocationState_ALLOCATION_STATE_FAILED
	}
}

func finalizeMutationPlanResultsLocked(
	store *Store,
	plan *tgsrlv1.PlacementPlan,
	reservation *planReservation,
	succeeded bool,
	results []*tgsrlv1.ActionResult,
) (*tgsrlv1.ClusterSnapshot, []string, error) {
	if reservation == nil || len(plan.GetActions()) == 0 {
		return nil, nil, fmt.Errorf("%w: mutation reservation is incomplete", ErrPlanNotReserved)
	}
	action := plan.GetActions()[0]
	working := cloneSnapshot(store.snapshot)
	outcome := mutationOutcome(plan, reservation, succeeded, results)
	switch {
	case succeeded:
		switch action.GetActionType() {
		case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
			for _, allocationID := range reservation.allocationIDs {
				allocation, err := findAllocationMutable(working, allocationID)
				if err != nil {
					continue
				}
				for _, deviceID := range allocation.GetDeviceIds() {
					if device := findDevice(working.GetDevices(), deviceID); device != nil {
						addResourcesCapped(device.Allocatable, allocation.GetResources(), device.GetCapacity())
					}
				}
			}
			working.Allocations = removeAllocationsByID(working.GetAllocations(), reservation.allocationIDs)
		case tgsrlv1.ActionType_ACTION_TYPE_REBIND:
			if len(reservation.allocationIDs) >= 2 {
				sourceID := reservation.allocationIDs[0]
				targetID := reservation.allocationIDs[1]
				source, err := findAllocationMutable(working, sourceID)
				if err == nil {
					for _, deviceID := range source.GetDeviceIds() {
						if device := findDevice(working.GetDevices(), deviceID); device != nil {
							addResourcesCapped(device.Allocatable, source.GetResources(), device.GetCapacity())
						}
					}
				}
				working.Allocations = removeAllocationsByID(working.GetAllocations(), []string{sourceID})
				if target, err := findAllocationMutable(working, targetID); err == nil {
					target.State = tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE
				}
			}
		case tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
			if len(reservation.allocationIDs) >= 2 {
				sourceID := reservation.allocationIDs[0]
				targetID := reservation.allocationIDs[1]
				working.Allocations = removeAllocationsByID(working.GetAllocations(), []string{sourceID})
				if target, err := findAllocationMutable(working, targetID); err == nil {
					target.State = tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE
				}
			}
		default:
			for _, allocationID := range reservation.allocationIDs {
				if allocation, err := findAllocationMutable(working, allocationID); err == nil {
					allocation.State = tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE
				}
			}
		}
	case outcome.restore:
		if err := restoreMutationStateFromBeforeImage(working, plan, reservation); err != nil {
			return nil, nil, err
		}
	default:
		markAllocationsFailed(working, outcome.retainedIDs)
	}
	if store.snapshot.GetRevision() == math.MaxUint64 && !proto.Equal(working, store.snapshot) {
		return nil, nil, ErrRevisionExhausted
	}
	if !proto.Equal(working, store.snapshot) {
		store.commitSnapshotLocked(working)
	}
	return cloneSnapshot(store.snapshot), outcome.retainedIDs, nil
}

func rememberDeviceAllocatable(reservation *planReservation, deviceIDs []string) {
	if reservation == nil || reservation.beforeSnapshot == nil || len(deviceIDs) == 0 {
		return
	}
	if reservation.deviceAllocatable == nil {
		reservation.deviceAllocatable = make(map[string]*tgsrlv1.ResourceVector)
	}
	for _, deviceID := range deviceIDs {
		if _, exists := reservation.deviceAllocatable[deviceID]; exists {
			continue
		}
		device := findDevice(reservation.beforeSnapshot.GetDevices(), deviceID)
		if device == nil {
			continue
		}
		reservation.deviceAllocatable[deviceID] = cloneResourceVector(device.GetAllocatable())
	}
}

func restorePendingUnitsFromReservation(
	working *tgsrlv1.ClusterSnapshot,
	units []*tgsrlv1.PendingUnit,
) {
	if len(units) == 0 {
		return
	}
	existing := make(map[string]struct{}, len(working.GetPendingUnits()))
	for _, unit := range working.GetPendingUnits() {
		existing[unit.GetPendingUnitId()] = struct{}{}
	}
	for _, unit := range units {
		if _, ok := existing[unit.GetPendingUnitId()]; ok {
			continue
		}
		working.PendingUnits = append(working.PendingUnits, clonePendingUnit(unit))
	}
	sortPendingUnits(working.PendingUnits)
}
