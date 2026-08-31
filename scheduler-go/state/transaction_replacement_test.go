package state

import (
	"context"
	"errors"
	"sync"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

func setupPreemptionTransactionStore(
	t *testing.T,
) (*Store, *fakeClock, *tgsrlv1.SchedulingIntent, *tgsrlv1.ClusterSnapshot, *tgsrlv1.PendingUnit, *tgsrlv1.Allocation, *tgsrlv1.Allocation) {
	t.Helper()
	store, clock := newTestStore(t)
	intent := testIntent(clock, 1, "preemption-intent")
	intent.UnitCount = 3
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if _, err := store.MutateResources(snapshot.GetRevision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.Devices = []*tgsrlv1.Device{
			{
				DeviceId: "device-1",
				Health:   tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
				Capacity: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096},
				Allocatable: &tgsrlv1.ResourceVector{
					CpuMillis:   1000,
					MemoryBytes: 4096,
				},
			},
			{
				DeviceId: "device-2",
				Health:   tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
				Capacity: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096},
				Allocatable: &tgsrlv1.ResourceVector{
					CpuMillis:   1000,
					MemoryBytes: 4096,
				},
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("MutateResources() error = %v", err)
	}
	snapshot, err = store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if len(snapshot.GetPendingUnits()) != 3 {
		t.Fatalf("pending units = %d, want 3", len(snapshot.GetPendingUnits()))
	}
	victimA := admissionPlanForPendingUnit(intent, snapshot, snapshot.GetPendingUnits()[0], "victim-a-binding", "device-1")
	if _, err := store.ReservePlan(victimA); err != nil {
		t.Fatalf("ReservePlan(victimA) error = %v", err)
	}
	if _, err := store.FinalizePlan(victimA, true); err != nil {
		t.Fatalf("FinalizePlan(victimA) error = %v", err)
	}
	snapshot, _ = store.GetSnapshot(context.Background(), 0, true)
	victimB := admissionPlanForPendingUnit(intent, snapshot, snapshot.GetPendingUnits()[0], "victim-b-binding", "device-2")
	if _, err := store.ReservePlan(victimB); err != nil {
		t.Fatalf("ReservePlan(victimB) error = %v", err)
	}
	if _, err := store.FinalizePlan(victimB, true); err != nil {
		t.Fatalf("FinalizePlan(victimB) error = %v", err)
	}
	snapshot, err = store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot(final seed) error = %v", err)
	}
	if len(snapshot.GetAllocations()) != 2 || len(snapshot.GetPendingUnits()) != 1 {
		t.Fatalf("seeded snapshot = %+v, want 2 active allocations and 1 pending", snapshot)
	}
	return store, clock, intent, snapshot, proto.Clone(snapshot.GetPendingUnits()[0]).(*tgsrlv1.PendingUnit), proto.Clone(snapshot.GetAllocations()[0]).(*tgsrlv1.Allocation), proto.Clone(snapshot.GetAllocations()[1]).(*tgsrlv1.Allocation)
}

func admissionPlanForPendingUnit(intent *tgsrlv1.SchedulingIntent, snapshot *tgsrlv1.ClusterSnapshot, pending *tgsrlv1.PendingUnit, bindingID, deviceID string) *tgsrlv1.PlacementPlan {
	return &tgsrlv1.PlacementPlan{
		PlanId:           stableID("admission", bindingID),
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		Purpose:          tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION,
		Bindings: []*tgsrlv1.Binding{{
			BindingId:     bindingID,
			PendingUnitId: pending.GetPendingUnitId(),
			DeviceIds:     []string{deviceID},
			Resources:     cloneResourceVector(pending.GetRequestedResources()),
			Generation:    1,
			RuntimeUnitId: bindingID + "-runtime",
			SandboxId:     bindingID + "-sandbox",
		}},
		Actions: []*tgsrlv1.Action{{
			ActionId:   stableID("bind-action", bindingID),
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND,
			Binding: &tgsrlv1.Binding{
				BindingId:     bindingID,
				PendingUnitId: pending.GetPendingUnitId(),
				DeviceIds:     []string{deviceID},
				Resources:     cloneResourceVector(pending.GetRequestedResources()),
				Generation:    1,
				RuntimeUnitId: bindingID + "-runtime",
				SandboxId:     bindingID + "-sandbox",
			},
		}},
	}
}

func preemptionReplacementPlan(snapshot *tgsrlv1.ClusterSnapshot, pending *tgsrlv1.PendingUnit, victims ...*tgsrlv1.Allocation) *tgsrlv1.PlacementPlan {
	planID := stableID("preemption-plan", pending.GetPendingUnitId())
	replacementBinding := &tgsrlv1.Binding{
		BindingId:     "replacement-binding",
		PendingUnitId: pending.GetPendingUnitId(),
		DeviceIds:     []string{"device-1"},
		Resources:     cloneResourceVector(pending.GetRequestedResources()),
		Generation:    7,
		RuntimeUnitId: "replacement-runtime",
		SandboxId:     "replacement-sandbox",
	}
	plan := &tgsrlv1.PlacementPlan{
		PlanId:           planID,
		ExecutionId:      pending.GetExecutionId(),
		StageId:          pending.GetStageId(),
		IntentVersion:    pending.GetIntentVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		Purpose:          tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION,
		RollbackPolicy:   tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION,
		Bindings:         []*tgsrlv1.Binding{proto.Clone(replacementBinding).(*tgsrlv1.Binding)},
	}
	for index, victim := range victims {
		restoreBinding := &tgsrlv1.Binding{BindingId: victim.GetBindingId(), PendingUnitId: victim.GetPendingUnitId(), DeviceIds: append([]string(nil), victim.GetDeviceIds()...), Resources: cloneResourceVector(victim.GetResources()), Generation: victim.GetGeneration(), RuntimeUnitId: victim.GetRuntimeUnitId(), SandboxId: victim.GetSandboxId()}
		plan.Actions = append(plan.Actions, &tgsrlv1.Action{
			ActionId:           stableID("preemption-release", victim.GetAllocationId()),
			ActionType:         tgsrlv1.ActionType_ACTION_TYPE_RELEASE,
			TargetId:           victim.GetAllocationId(),
			Binding:            proto.Clone(restoreBinding).(*tgsrlv1.Binding),
			Rollback:           &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND, TargetId: victim.GetSandboxId(), RestoreBinding: proto.Clone(restoreBinding).(*tgsrlv1.Binding)},
			Order:              uint32(index + 1),
			SandboxId:          victim.GetSandboxId(),
			ExpectedGeneration: victim.GetGeneration(),
		})
		plan.AffectedAllocationIds = append(plan.AffectedAllocationIds, victim.GetAllocationId())
	}
	plan.Actions = append(plan.Actions, &tgsrlv1.Action{
		ActionId:           stableID("preemption-bind", replacementBinding.GetBindingId()),
		ActionType:         tgsrlv1.ActionType_ACTION_TYPE_BIND,
		TargetId:           replacementBinding.GetPendingUnitId(),
		Binding:            proto.Clone(replacementBinding).(*tgsrlv1.Binding),
		Rollback:           &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, TargetId: replacementBinding.GetBindingId()},
		Order:              uint32(len(plan.Actions) + 1),
		SandboxId:          replacementBinding.GetSandboxId(),
		ExpectedGeneration: replacementBinding.GetGeneration(),
	})
	return plan
}

func TestReserveTransactionPreemptionRejectsIdentityMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*tgsrlv1.PlacementPlan)
	}{
		{name: "replacement binding mismatch", mutate: func(plan *tgsrlv1.PlacementPlan) {
			plan.Actions[len(plan.Actions)-1].Binding.RuntimeUnitId = "other-runtime"
		}},
		{name: "replacement generation mismatch", mutate: func(plan *tgsrlv1.PlacementPlan) { plan.Actions[len(plan.Actions)-1].ExpectedGeneration++ }},
		{name: "victim sandbox mismatch", mutate: func(plan *tgsrlv1.PlacementPlan) { plan.Actions[0].SandboxId = "other-sandbox" }},
		{name: "victim generation mismatch", mutate: func(plan *tgsrlv1.PlacementPlan) { plan.Actions[0].ExpectedGeneration++ }},
		{name: "victim binding mismatch", mutate: func(plan *tgsrlv1.PlacementPlan) { plan.Actions[0].Binding.BindingId = "other-binding" }},
		{name: "victim rollback mismatch", mutate: func(plan *tgsrlv1.PlacementPlan) {
			plan.Actions[0].Rollback.RestoreBinding.RuntimeUnitId = "other-runtime"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, _, _, snapshot, pending, victimA, _ := setupPreemptionTransactionStore(t)
			plan := preemptionReplacementPlan(snapshot, pending, victimA)
			test.mutate(plan)
			record, err := store.CreateTransaction(plan, 1)
			if err != nil {
				t.Fatalf("CreateTransaction() error = %v", err)
			}
			if _, _, err := store.ReserveTransaction(record.TransactionID, record.Generation); !errors.Is(err, ErrInvalidIntent) {
				t.Fatalf("ReserveTransaction() error = %v, want ErrInvalidIntent", err)
			}
		})
	}
}

func TestReserveTransactionPreemptionPrepareIsAtomic(t *testing.T) {
	store, _, _, snapshot, pending, victimA, victimB := setupPreemptionTransactionStore(t)
	plan := preemptionReplacementPlan(snapshot, pending, victimA, victimB)
	record, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction() error = %v", err)
	}
	record, reserved, err := store.ReserveTransaction(record.TransactionID, 1)
	if err != nil {
		t.Fatalf("ReserveTransaction() error = %v", err)
	}
	if record.State != TransactionStateReserved || record.Generation != 2 || !record.ReservationHeld {
		t.Fatalf("reserved record = %+v", record)
	}
	if got := store.Revision(); got != snapshot.GetRevision()+1 {
		t.Fatalf("revision after preemption prepare = %d, want %d", got, snapshot.GetRevision()+1)
	}
	for _, victimID := range []string{victimA.GetAllocationId(), victimB.GetAllocationId()} {
		allocation, err := findAllocationMutable(reserved, victimID)
		if err != nil {
			t.Fatalf("findAllocationMutable(victim) error = %v", err)
		}
		if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_RELEASING {
			t.Fatalf("victim %s state = %s, want RELEASING", victimID, allocation.GetState())
		}
	}
	replacementID := allocationID(plan.GetBindings()[0].GetBindingId())
	replacement, err := findAllocationMutable(reserved, replacementID)
	if err != nil {
		t.Fatalf("findAllocationMutable(replacement) error = %v", err)
	}
	if replacement.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING || replacement.GetGeneration() != plan.GetBindings()[0].GetGeneration() {
		t.Fatalf("replacement allocation = %+v", replacement)
	}
	if got := findDevice(reserved.GetDevices(), "device-1").GetAllocatable().GetCpuMillis(); got != 500 {
		t.Fatalf("device-1 allocatable CPU after prepare = %d, want 500", got)
	}
}

func TestReserveTransactionPreemptionBlocksThirdPartyCapacityGrab(t *testing.T) {
	store, _, _, snapshot, pending, victimA, victimB := setupPreemptionTransactionStore(t)
	plan := preemptionReplacementPlan(snapshot, pending, victimA, victimB)
	record, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction() error = %v", err)
	}
	if _, _, err := store.ReserveTransaction(record.TransactionID, 1); err != nil {
		t.Fatalf("ReserveTransaction(preemption) error = %v", err)
	}
	otherPending := &tgsrlv1.PendingUnit{
		PendingUnitId:      "other-pending",
		ExecutionId:        pending.GetExecutionId(),
		StageId:            pending.GetStageId(),
		IntentVersion:      pending.GetIntentVersion(),
		RequestedResources: &tgsrlv1.ResourceVector{CpuMillis: 600, MemoryBytes: 1024},
	}
	if _, err := store.MutateResources(store.Revision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.PendingUnits = append(working.PendingUnits, otherPending)
		sortPendingUnits(working.PendingUnits)
		return nil
	}); err != nil {
		t.Fatalf("MutateResources(add pending) error = %v", err)
	}
	current, _ := store.GetSnapshot(context.Background(), 0, true)
	admission := &tgsrlv1.PlacementPlan{
		PlanId:           "third-party-admission",
		ExecutionId:      otherPending.GetExecutionId(),
		StageId:          otherPending.GetStageId(),
		IntentVersion:    otherPending.GetIntentVersion(),
		SnapshotRevision: current.GetRevision(),
		Purpose:          tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION,
		Bindings: []*tgsrlv1.Binding{{
			BindingId:     "third-party-binding",
			PendingUnitId: otherPending.GetPendingUnitId(),
			DeviceIds:     []string{"device-1"},
			Resources:     cloneResourceVector(otherPending.GetRequestedResources()),
		}},
		Actions: []*tgsrlv1.Action{{ActionId: "third-party-bind", ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND, Binding: &tgsrlv1.Binding{BindingId: "third-party-binding", PendingUnitId: otherPending.GetPendingUnitId(), DeviceIds: []string{"device-1"}, Resources: cloneResourceVector(otherPending.GetRequestedResources())}}},
	}
	if _, err := store.ReservePlan(admission); !errors.Is(err, ErrInsufficientResource) {
		t.Fatalf("ReservePlan(third-party) error = %v, want ErrInsufficientResource", err)
	}
}

func TestFinalizeTransactionPreemptionCommitActivatesReplacement(t *testing.T) {
	store, _, _, snapshot, pending, victimA, victimB := setupPreemptionTransactionStore(t)
	plan := preemptionReplacementPlan(snapshot, pending, victimA, victimB)
	record, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction() error = %v", err)
	}
	record, _, err = store.ReserveTransaction(record.TransactionID, 1)
	if err != nil {
		t.Fatalf("ReserveTransaction() error = %v", err)
	}
	results := []*tgsrlv1.ActionResult{
		{ActionId: plan.GetActions()[0].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED},
		{ActionId: plan.GetActions()[1].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED},
		{ActionId: plan.GetActions()[2].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED},
	}
	record, final, err := store.FinalizeTransaction(TransactionFinalizeRequest{
		TransactionID:      record.TransactionID,
		ExpectedGeneration: 2,
		Succeeded:          true,
		FinalState:         TransactionStateCommitted,
		Results:            results,
	})
	if err != nil {
		t.Fatalf("FinalizeTransaction(commit) error = %v", err)
	}
	if record.State != TransactionStateCommitted || !record.Terminal || record.ReservationHeld {
		t.Fatalf("final record = %+v", record)
	}
	if len(final.GetAllocations()) != 1 {
		t.Fatalf("final allocations = %+v, want only replacement", final.GetAllocations())
	}
	replacement := final.GetAllocations()[0]
	if replacement.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE || replacement.GetGeneration() != plan.GetBindings()[0].GetGeneration() {
		t.Fatalf("replacement after commit = %+v", replacement)
	}
	if got := findDevice(final.GetDevices(), "device-1").GetAllocatable().GetCpuMillis(); got != 500 {
		t.Fatalf("device-1 allocatable CPU after commit = %d, want 500", got)
	}
	if got := findDevice(final.GetDevices(), "device-2").GetAllocatable().GetCpuMillis(); got != 1000 {
		t.Fatalf("device-2 allocatable CPU after commit = %d, want 1000", got)
	}
	revision := final.GetRevision()
	repeated, err := store.FinalizePlanResults(plan, true, results)
	if err != nil {
		t.Fatalf("FinalizePlanResults(idempotent transaction outcome) error = %v", err)
	}
	if repeated.GetRevision() != revision || !proto.Equal(repeated, final) {
		t.Fatalf("repeated finalization changed snapshot: before=%+v after=%+v", final, repeated)
	}
}

func TestFinalizeTransactionPreemptionCommitReturnsOnlyTargetExcessRelease(t *testing.T) {
	store, _, _, _, pending, victimA, _ := setupPreemptionTransactionStore(t)
	replacementResources := &tgsrlv1.ResourceVector{CpuMillis: 300, MemoryBytes: 1024}
	var err error
	if _, err = store.MutateResources(store.Revision(), func(working *tgsrlv1.ClusterSnapshot) error {
		for _, unit := range working.GetPendingUnits() {
			if unit.GetPendingUnitId() == pending.GetPendingUnitId() {
				unit.RequestedResources = cloneResourceVector(replacementResources)
				return nil
			}
		}
		return errors.New("replacement pending unit not found")
	}); err != nil {
		t.Fatalf("MutateResources(update replacement pending) error = %v", err)
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot(updated) error = %v", err)
	}
	pending = proto.Clone(snapshot.GetPendingUnits()[0]).(*tgsrlv1.PendingUnit)
	plan := preemptionReplacementPlan(snapshot, pending, victimA)
	plan.Bindings[0].Resources = cloneResourceVector(replacementResources)
	plan.Actions[len(plan.Actions)-1].Binding.Resources = cloneResourceVector(plan.Bindings[0].GetResources())
	record, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction() error = %v", err)
	}
	record, reserved, err := store.ReserveTransaction(record.TransactionID, 1)
	if err != nil {
		t.Fatalf("ReserveTransaction() error = %v", err)
	}
	if got := findDevice(reserved.GetDevices(), "device-1").GetAllocatable().GetCpuMillis(); got != 500 {
		t.Fatalf("device-1 allocatable CPU after prepare = %d, want 500", got)
	}
	_, final, err := store.FinalizeTransaction(TransactionFinalizeRequest{
		TransactionID:      record.TransactionID,
		ExpectedGeneration: 2,
		Succeeded:          true,
		FinalState:         TransactionStateCommitted,
		Results: []*tgsrlv1.ActionResult{
			{ActionId: plan.GetActions()[0].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED},
			{ActionId: plan.GetActions()[1].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED},
		},
	})
	if err != nil {
		t.Fatalf("FinalizeTransaction(commit excess return) error = %v", err)
	}
	if got := findDevice(final.GetDevices(), "device-1").GetAllocatable().GetCpuMillis(); got != 700 {
		t.Fatalf("device-1 allocatable CPU after commit = %d, want 700", got)
	}
}

func TestFinalizeTransactionPreemptionAbortPreservesUnrelatedUpdates(t *testing.T) {
	store, _, _, snapshot, pending, victimA, victimB := setupPreemptionTransactionStore(t)
	plan := preemptionReplacementPlan(snapshot, pending, victimA, victimB)
	record, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction() error = %v", err)
	}
	record, _, err = store.ReserveTransaction(record.TransactionID, 1)
	if err != nil {
		t.Fatalf("ReserveTransaction() error = %v", err)
	}
	if _, err := store.MutateResources(store.Revision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.PendingUnits = append(working.PendingUnits, &tgsrlv1.PendingUnit{
			PendingUnitId: "unrelated-pending",
			ExecutionId:   "other-exec",
			StageId:       "other-stage",
			Priority:      9,
		})
		findDevice(working.GetDevices(), "device-2").Allocatable.CpuMillis = 777
		return nil
	}); err != nil {
		t.Fatalf("MutateResources(unrelated updates) error = %v", err)
	}
	record, final, err := store.FinalizeTransaction(TransactionFinalizeRequest{
		TransactionID:      record.TransactionID,
		ExpectedGeneration: 2,
		Succeeded:          false,
		FinalState:         TransactionStateAborted,
		Results: []*tgsrlv1.ActionResult{
			{ActionId: plan.GetActions()[0].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED},
			{ActionId: plan.GetActions()[1].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED},
			{ActionId: plan.GetActions()[2].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED},
		},
	})
	if err != nil {
		t.Fatalf("FinalizeTransaction(abort) error = %v", err)
	}
	if record.State != TransactionStateAborted {
		t.Fatalf("final record = %+v, want aborted", record)
	}
	if len(final.GetAllocations()) != 2 {
		t.Fatalf("final allocations = %+v, want victims restored only", final.GetAllocations())
	}
	for _, allocation := range final.GetAllocations() {
		if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
			t.Fatalf("allocation after abort = %+v, want ACTIVE victim", allocation)
		}
	}
	if got := findDevice(final.GetDevices(), "device-2").GetAllocatable().GetCpuMillis(); got != 777 {
		t.Fatalf("unrelated device update lost after abort: got %d want 777", got)
	}
	foundPending := false
	for _, unit := range final.GetPendingUnits() {
		if unit.GetPendingUnitId() == "unrelated-pending" {
			foundPending = true
			break
		}
	}
	if !foundPending {
		t.Fatalf("pending units after abort = %+v, want unrelated pending preserved", final.GetPendingUnits())
	}
}

func TestFinalizeTransactionPreemptionSameDeviceAbortPreservesConcurrentConsumption(t *testing.T) {
	store, _, _, snapshot, pending, victimA, _ := setupPreemptionTransactionStore(t)
	plan := preemptionReplacementPlan(snapshot, pending, victimA)
	plan.Bindings[0].DeviceIds = []string{"device-1"}
	plan.Actions[len(plan.Actions)-1].Binding.DeviceIds = []string{"device-1"}
	record, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction() error = %v", err)
	}
	record, _, err = store.ReserveTransaction(record.TransactionID, 1)
	if err != nil {
		t.Fatalf("ReserveTransaction() error = %v", err)
	}
	if _, err := store.MutateResources(store.Revision(), func(working *tgsrlv1.ClusterSnapshot) error {
		findDevice(working.GetDevices(), "device-1").Allocatable.CpuMillis -= 100
		return nil
	}); err != nil {
		t.Fatalf("MutateResources(concurrent same-device) error = %v", err)
	}
	_, final, err := store.FinalizeTransaction(TransactionFinalizeRequest{
		TransactionID:      record.TransactionID,
		ExpectedGeneration: 2,
		Succeeded:          false,
		FinalState:         TransactionStateAborted,
		Results: []*tgsrlv1.ActionResult{
			{ActionId: plan.GetActions()[0].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED, RollbackAttempted: true, RollbackStatus: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK},
			{ActionId: plan.GetActions()[1].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED},
		},
	})
	if err != nil {
		t.Fatalf("FinalizeTransaction(abort same-device) error = %v", err)
	}
	if got := findDevice(final.GetDevices(), "device-1").GetAllocatable().GetCpuMillis(); got != 400 {
		t.Fatalf("device-1 allocatable CPU after same-device abort = %d, want 400", got)
	}
}

func TestReserveAndFinalizePreemptionTransactionRace(t *testing.T) {
	store, _, _, snapshot, pending, victimA, victimB := setupPreemptionTransactionStore(t)
	plan := preemptionReplacementPlan(snapshot, pending, victimA, victimB)
	record, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction() error = %v", err)
	}
	var wg sync.WaitGroup
	var reserveErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, reserveErr = store.ReserveTransaction(record.TransactionID, 1)
	}()
	wg.Wait()
	if reserveErr != nil {
		t.Fatalf("ReserveTransaction(race) error = %v", reserveErr)
	}
	record, ok := store.GetTransaction(record.TransactionID)
	if !ok {
		t.Fatal("GetTransaction() missing record")
	}
	wg.Add(2)
	errs := make([]error, 2)
	for index := 0; index < 2; index++ {
		go func(i int) {
			defer wg.Done()
			_, _, errs[i] = store.FinalizeTransaction(TransactionFinalizeRequest{
				TransactionID:      record.TransactionID,
				ExpectedGeneration: 2,
				Succeeded:          true,
				FinalState:         TransactionStateCommitted,
				Results: []*tgsrlv1.ActionResult{
					{ActionId: plan.GetActions()[0].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED},
					{ActionId: plan.GetActions()[1].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED},
					{ActionId: plan.GetActions()[2].GetActionId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED},
				},
			})
		}(index)
	}
	wg.Wait()
	successes := 0
	for _, err := range errs {
		if err == nil {
			successes++
			continue
		}
		if !errors.Is(err, ErrTransactionImmutable) && !errors.Is(err, ErrTransactionConflict) {
			t.Fatalf("FinalizeTransaction(race) unexpected error = %v", err)
		}
	}
	if successes == 0 {
		t.Fatalf("FinalizeTransaction(race) successes = 0, want at least one")
	}
}
