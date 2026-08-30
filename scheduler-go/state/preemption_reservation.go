package state

import (
	"fmt"
	"math"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Store) reservePreemptionPlanLocked(
	plan *tgsrlv1.PlacementPlan,
) (*tgsrlv1.ClusterSnapshot, error) {
	replacementBinding, bindAction, releaseActions, err := preemptionPlanParts(plan)
	if err != nil {
		return nil, err
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
	if err := reservePreemptionReplacement(
		working,
		plan,
		replacementBinding,
		bindAction,
		releaseActions,
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

func preemptionPlanParts(plan *tgsrlv1.PlacementPlan) (*tgsrlv1.Binding, *tgsrlv1.Action, []*tgsrlv1.Action, error) {
	if plan == nil {
		return nil, nil, nil, fmt.Errorf("%w: preemption plan is required", ErrInvalidIntent)
	}
	if len(plan.GetBindings()) != 1 {
		return nil, nil, nil, fmt.Errorf("%w: preemption requires exactly one replacement binding", ErrInvalidIntent)
	}
	replacement := plan.GetBindings()[0]
	if replacement == nil || replacement.GetBindingId() == "" || replacement.GetPendingUnitId() == "" || replacement.GetResources() == nil || len(replacement.GetDeviceIds()) != 1 {
		return nil, nil, nil, fmt.Errorf("%w: preemption replacement binding requires one device, pending_unit_id, and resources", ErrInvalidIntent)
	}
	var bindAction *tgsrlv1.Action
	releaseActions := make([]*tgsrlv1.Action, 0, len(plan.GetActions()))
	for _, action := range plan.GetActions() {
		if action == nil || action.GetActionId() == "" {
			return nil, nil, nil, fmt.Errorf("%w: preemption action is required", ErrInvalidIntent)
		}
		switch action.GetActionType() {
		case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
			releaseActions = append(releaseActions, action)
		case tgsrlv1.ActionType_ACTION_TYPE_BIND:
			if bindAction != nil {
				return nil, nil, nil, fmt.Errorf("%w: preemption requires exactly one bind action", ErrInvalidIntent)
			}
			bindAction = action
		default:
			return nil, nil, nil, fmt.Errorf("%w: unsupported preemption action %s", ErrInvalidIntent, action.GetActionType())
		}
	}
	if len(releaseActions) == 0 || bindAction == nil {
		return nil, nil, nil, fmt.Errorf("%w: preemption requires release victims and one replacement bind", ErrInvalidIntent)
	}
	if bindAction.GetBinding() == nil || !proto.Equal(bindAction.GetBinding(), replacement) ||
		bindAction.GetTargetId() != replacement.GetPendingUnitId() ||
		bindAction.GetSandboxId() != replacement.GetSandboxId() ||
		bindAction.GetExpectedGeneration() != replacement.GetGeneration() {
		return nil, nil, nil, fmt.Errorf("%w: preemption bind action must match replacement binding", ErrInvalidIntent)
	}
	if replacement.GetSandboxId() == "" || replacement.GetRuntimeUnitId() == "" || replacement.GetGeneration() == 0 {
		return nil, nil, nil, fmt.Errorf("%w: preemption replacement binding requires complete runtime identity", ErrInvalidIntent)
	}
	return replacement, bindAction, releaseActions, nil
}

func reservePreemptionReplacement(
	working *tgsrlv1.ClusterSnapshot,
	plan *tgsrlv1.PlacementPlan,
	replacementBinding *tgsrlv1.Binding,
	bindAction *tgsrlv1.Action,
	releaseActions []*tgsrlv1.Action,
	affected map[string]*tgsrlv1.Allocation,
	reservation *planReservation,
	nowTimestamp time.Time,
) error {
	releasedOnTarget := &tgsrlv1.ResourceVector{}
	releasedVictims := make(map[string]struct{}, len(releaseActions))
	for _, action := range releaseActions {
		victim, err := singleAffectedAllocation(action, affected)
		if err != nil {
			return err
		}
		if victim.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
			return fmt.Errorf("%w: allocation %q is not active", ErrInvalidIntent, victim.GetAllocationId())
		}
		if err := validatePreemptionReleaseIdentity(action, victim); err != nil {
			return err
		}
		if _, duplicate := releasedVictims[victim.GetAllocationId()]; duplicate {
			return fmt.Errorf("%w: duplicate preemption victim %q", ErrInvalidIntent, victim.GetAllocationId())
		}
		releasedVictims[victim.GetAllocationId()] = struct{}{}
		mutable, err := findAllocationMutable(working, victim.GetAllocationId())
		if err != nil {
			return err
		}
		mutable.State = tgsrlv1.AllocationState_ALLOCATION_STATE_RELEASING
		reservation.allocationIDs = append(reservation.allocationIDs, victim.GetAllocationId())
		if containsString(victim.GetDeviceIds(), replacementBinding.GetDeviceIds()[0]) {
			addResources(releasedOnTarget, victim.GetResources())
		}
	}

	pending, remaining, err := consumeReplacementPendingUnit(working.GetPendingUnits(), plan, replacementBinding)
	if err != nil {
		return err
	}
	reservation.pendingUnits = append(reservation.pendingUnits, clonePendingUnit(pending))
	working.PendingUnits = remaining
	sortPendingUnits(working.PendingUnits)

	targetDeviceID := replacementBinding.GetDeviceIds()[0]
	targetDevice := findDevice(working.GetDevices(), targetDeviceID)
	if targetDevice == nil || targetDevice.GetHealth() != tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY {
		return fmt.Errorf("%w: device %q is unavailable", ErrInsufficientResource, targetDeviceID)
	}
	extraConsumption := positiveResourceDelta(replacementBinding.GetResources(), releasedOnTarget)
	if !resourcesFit(extraConsumption, targetDevice.GetAllocatable()) {
		return fmt.Errorf("%w: device %q", ErrInsufficientResource, targetDeviceID)
	}
	rememberDeviceAllocatable(reservation, replacementBinding.GetDeviceIds())
	subtractResources(targetDevice.Allocatable, extraConsumption)

	replacementAllocationID := allocationID(replacementBinding.GetBindingId())
	if _, err := findAllocationMutable(working, replacementAllocationID); err == nil {
		return fmt.Errorf("%w: duplicate binding_id %q", ErrInvalidIntent, replacementBinding.GetBindingId())
	}
	working.Allocations = append(working.Allocations, buildPreemptionReplacementAllocation(plan, bindAction, replacementBinding, pending, replacementAllocationID, nowTimestamp))
	reservation.allocationIDs = append(reservation.allocationIDs, replacementAllocationID)
	return nil
}

func validatePreemptionReleaseIdentity(action *tgsrlv1.Action, victim *tgsrlv1.Allocation) error {
	if action == nil || victim == nil {
		return fmt.Errorf("%w: preemption release identity is incomplete", ErrInvalidIntent)
	}
	want := &tgsrlv1.Binding{
		BindingId:     victim.GetBindingId(),
		PendingUnitId: victim.GetPendingUnitId(),
		DeviceIds:     append([]string(nil), victim.GetDeviceIds()...),
		Resources:     cloneResourceVector(victim.GetResources()),
		SandboxId:     victim.GetSandboxId(),
		Generation:    victim.GetGeneration(),
		RuntimeUnitId: victim.GetRuntimeUnitId(),
	}
	rollback := action.GetRollback()
	if want.GetBindingId() == "" || want.GetPendingUnitId() == "" || want.GetSandboxId() == "" || want.GetRuntimeUnitId() == "" || want.GetGeneration() == 0 || len(want.GetDeviceIds()) == 0 || want.GetResources() == nil ||
		action.GetSandboxId() != want.GetSandboxId() || action.GetExpectedGeneration() != want.GetGeneration() || !proto.Equal(action.GetBinding(), want) ||
		rollback == nil || rollback.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND || rollback.GetTargetId() != want.GetSandboxId() || !proto.Equal(rollback.GetRestoreBinding(), want) {
		return fmt.Errorf("%w: preemption release %q does not match victim allocation %q", ErrInvalidIntent, action.GetActionId(), victim.GetAllocationId())
	}
	return nil
}

func buildPreemptionReplacementAllocation(
	plan *tgsrlv1.PlacementPlan,
	action *tgsrlv1.Action,
	binding *tgsrlv1.Binding,
	unit *tgsrlv1.PendingUnit,
	allocationID string,
	nowTimestamp time.Time,
) *tgsrlv1.Allocation {
	return &tgsrlv1.Allocation{
		AllocationId:  allocationID,
		ExecutionId:   plan.GetExecutionId(),
		StageId:       plan.GetStageId(),
		IntentVersion: plan.GetIntentVersion(),
		JobId:         firstNonEmpty(plan.GetJobId(), unit.GetJobId()),
		PendingUnitId: unit.GetPendingUnitId(),
		DeviceIds:     append([]string(nil), binding.GetDeviceIds()...),
		Resources:     cloneResourceVector(binding.GetResources()),
		State:         tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING,
		CreatedAt:     timestamppb.New(nowTimestamp),
		ExpiresAt:     cloneTimestamp(plan.GetExpiresAt()),
		RunId:         firstNonEmpty(plan.GetRunId(), unit.GetRunId()),
		TraceId:       firstNonEmpty(plan.GetTraceId(), unit.GetTraceId()),
		DataKind:      firstKnownDataKind(plan.GetDataKind(), unit.GetDataKind()),
		Generation:    binding.GetGeneration(),
		RuntimeUnitId: firstNonEmpty(binding.GetRuntimeUnitId(), unit.GetRuntimeUnitId()),
		DecisionId:    plan.GetDecisionId(),
		PlanId:        plan.GetPlanId(),
		ActionId:      action.GetActionId(),
		Priority:      int32Pointer(unit.GetPriority()),
		SandboxId:     binding.GetSandboxId(),
		BindingId:     binding.GetBindingId(),
	}
}

func finalizePreemptionPlanResultsLocked(
	store *Store,
	plan *tgsrlv1.PlacementPlan,
	reservation *planReservation,
	succeeded bool,
	results []*tgsrlv1.ActionResult,
) (*tgsrlv1.ClusterSnapshot, []string, error) {
	if store == nil || plan == nil || reservation == nil {
		return nil, nil, fmt.Errorf("%w: preemption reservation is incomplete", ErrPlanNotReserved)
	}
	replacementBinding, _, releaseActions, err := preemptionPlanParts(plan)
	if err != nil {
		return nil, nil, err
	}
	outcome := preemptionOutcome(plan, reservation, succeeded, results)
	if reservation.finalized {
		if reservation.succeeded != succeeded || !equalStrings(reservation.retainedAllocationIDs, outcome.retainedIDs) {
			return nil, nil, fmt.Errorf("%w: plan %q finalized with different outcome", ErrPlanAlreadyReserved, plan.GetPlanId())
		}
		return cloneSnapshot(store.snapshot), outcome.retainedIDs, nil
	}
	working := cloneSnapshot(store.snapshot)
	switch {
	case succeeded:
		if err := commitPreemptionReplacement(working, plan, replacementBinding, releaseActions, reservation); err != nil {
			return nil, nil, err
		}
	case outcome.restore:
		if err := abortPreemptionReplacement(working, plan, replacementBinding, releaseActions, reservation); err != nil {
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

func commitPreemptionReplacement(
	working *tgsrlv1.ClusterSnapshot,
	plan *tgsrlv1.PlacementPlan,
	replacementBinding *tgsrlv1.Binding,
	releaseActions []*tgsrlv1.Action,
	reservation *planReservation,
) error {
	_ = plan
	replacementID := allocationID(replacementBinding.GetBindingId())
	replacement, err := findAllocationMutable(working, replacementID)
	if err != nil {
		return err
	}
	replacement.State = tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE
	targetDeviceID := ""
	if len(replacementBinding.GetDeviceIds()) > 0 {
		targetDeviceID = replacementBinding.GetDeviceIds()[0]
	}
	releasedOnTarget := &tgsrlv1.ResourceVector{}
	for _, action := range releaseActions {
		victimID := action.GetTargetId()
		victim, err := findAllocationMutable(working, victimID)
		if err != nil {
			return err
		}
		for _, deviceID := range victim.GetDeviceIds() {
			if device := findDevice(working.GetDevices(), deviceID); device != nil {
				if deviceID == targetDeviceID {
					addResources(releasedOnTarget, victim.GetResources())
					continue
				}
				addResourcesCapped(device.Allocatable, victim.GetResources(), device.GetCapacity())
			}
		}
	}
	if targetDeviceID != "" {
		targetDevice := findDevice(working.GetDevices(), targetDeviceID)
		if targetDevice == nil {
			return fmt.Errorf("%w: device %q is unavailable", ErrInsufficientResource, targetDeviceID)
		}
		returnedOnTarget := positiveResourceDelta(releasedOnTarget, replacementBinding.GetResources())
		addResourcesCapped(targetDevice.Allocatable, returnedOnTarget, targetDevice.GetCapacity())
	}
	victimIDs := make([]string, 0, len(releaseActions))
	for _, action := range releaseActions {
		victimIDs = append(victimIDs, action.GetTargetId())
	}
	working.Allocations = removeAllocationsByID(working.GetAllocations(), victimIDs)
	return nil
}

func abortPreemptionReplacement(
	working *tgsrlv1.ClusterSnapshot,
	plan *tgsrlv1.PlacementPlan,
	replacementBinding *tgsrlv1.Binding,
	releaseActions []*tgsrlv1.Action,
	reservation *planReservation,
) error {
	releasedOnTarget := &tgsrlv1.ResourceVector{}
	for _, action := range releaseActions {
		victimID := action.GetTargetId()
		previous, err := findAllocationInSnapshot(reservation.beforeSnapshot, victimID)
		if err != nil {
			return err
		}
		current, err := findAllocationMutable(working, victimID)
		if err != nil {
			return err
		}
		current.State = previous.GetState()
		current.IntentVersion = previous.GetIntentVersion()
		if containsString(previous.GetDeviceIds(), replacementBinding.GetDeviceIds()[0]) {
			addResources(releasedOnTarget, previous.GetResources())
		}
	}
	extraConsumption := positiveResourceDelta(replacementBinding.GetResources(), releasedOnTarget)
	if err := reverseDeviceResourceDelta(working, replacementBinding.GetDeviceIds(), extraConsumption, nil); err != nil {
		return err
	}
	working.Allocations = removeAllocationsByID(working.GetAllocations(), []string{allocationID(replacementBinding.GetBindingId())})
	restorePendingUnitsFromReservation(working, reservation.pendingUnits)
	return nil
}

func preemptionOutcome(
	plan *tgsrlv1.PlacementPlan,
	reservation *planReservation,
	succeeded bool,
	results []*tgsrlv1.ActionResult,
) mutationFinalizationOutcome {
	if succeeded {
		replacementBinding, _, _, err := preemptionPlanParts(plan)
		if err != nil {
			return mutationFinalizationOutcome{retainedIDs: sortedStrings(successfulMutationRetainedIDs(plan, reservation))}
		}
		return mutationFinalizationOutcome{retainedIDs: []string{allocationID(replacementBinding.GetBindingId())}}
	}
	if preemptionResultsConfirmRestoration(plan, results) {
		return mutationFinalizationOutcome{restore: true}
	}
	retained := append([]string(nil), reservation.allocationIDs...)
	return mutationFinalizationOutcome{retainedIDs: sortedStrings(retained)}
}

func preemptionResultsConfirmRestoration(plan *tgsrlv1.PlacementPlan, results []*tgsrlv1.ActionResult) bool {
	if plan == nil || len(plan.GetActions()) == 0 || len(results) != len(plan.GetActions()) {
		return false
	}
	byID := make(map[string]*tgsrlv1.ActionResult, len(results))
	for _, result := range results {
		if result == nil || result.GetActionId() == "" {
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

func findAllocationInSnapshot(snapshot *tgsrlv1.ClusterSnapshot, allocationID string) (*tgsrlv1.Allocation, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("%w: allocation %q", ErrPendingUnitNotFound, allocationID)
	}
	for _, allocation := range snapshot.GetAllocations() {
		if allocation.GetAllocationId() == allocationID {
			return allocation, nil
		}
	}
	return nil, fmt.Errorf("%w: allocation %q", ErrPendingUnitNotFound, allocationID)
}
