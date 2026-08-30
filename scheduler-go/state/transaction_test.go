package state

import (
	"context"
	"errors"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

func testTransactionalPlan(t *testing.T, store *Store, clock *fakeClock, planID string) *tgsrlv1.PlacementPlan {
	t.Helper()
	intent := testIntent(clock, 1, "txn-"+planID)
	intent.UnitCount = 1
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if _, err := store.MutateResources(snapshot.GetRevision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.Devices = []*tgsrlv1.Device{{
			DeviceId: "device-1",
			Health:   tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096},
			Allocatable: &tgsrlv1.ResourceVector{
				CpuMillis:   1000,
				MemoryBytes: 4096,
			},
		}}
		return nil
	}); err != nil {
		t.Fatalf("MutateResources() error = %v", err)
	}
	snapshot, err = store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	pending := snapshot.GetPendingUnits()[0]
	binding := &tgsrlv1.Binding{
		BindingId:     planID + "-binding",
		PendingUnitId: pending.GetPendingUnitId(),
		DeviceIds:     []string{"device-1"},
		Resources:     cloneResourceVector(intent.GetResourcesPerUnit()),
	}
	return &tgsrlv1.PlacementPlan{
		PlanId:           planID,
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		Bindings:         []*tgsrlv1.Binding{binding},
		Actions: []*tgsrlv1.Action{{
			ActionId: "action-" + planID,
			Binding:  proto.Clone(binding).(*tgsrlv1.Binding),
		}},
	}
}

func TestTransactionLifecycleUsesGenerationCAS(t *testing.T) {
	store, clock := newTestStore(t)
	plan := testTransactionalPlan(t, store, clock, "txn-plan-1")

	record, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction() error = %v", err)
	}
	if record.State != TransactionStateProposed || record.Generation != 1 || record.ProviderGeneration != 1 {
		t.Fatalf("created record = %+v", record)
	}

	record, snapshot, err := store.ReserveTransaction(record.TransactionID, 1)
	if err != nil {
		t.Fatalf("ReserveTransaction() error = %v", err)
	}
	if snapshot == nil || record.State != TransactionStateReserved || record.Generation != 2 || !record.ReservationHeld {
		t.Fatalf("reserved record = %+v snapshot=%+v", record, snapshot)
	}

	record, err = store.AdvanceTransaction(TransactionAdvanceRequest{
		TransactionID:      record.TransactionID,
		ExpectedGeneration: 2,
		NextState:          TransactionStatePrepared,
	})
	if err != nil {
		t.Fatalf("AdvanceTransaction(prepared) error = %v", err)
	}
	if record.State != TransactionStatePrepared || record.Generation != 3 {
		t.Fatalf("prepared record = %+v", record)
	}

	record, err = store.AdvanceTransaction(TransactionAdvanceRequest{
		TransactionID:      record.TransactionID,
		ExpectedGeneration: 3,
		NextState:          TransactionStateApplying,
	})
	if err != nil {
		t.Fatalf("AdvanceTransaction(applying) error = %v", err)
	}
	if record.State != TransactionStateApplying || record.Generation != 4 {
		t.Fatalf("applying record = %+v", record)
	}

	record, snapshot, err = store.FinalizeTransaction(TransactionFinalizeRequest{
		TransactionID:      record.TransactionID,
		ExpectedGeneration: 4,
		Succeeded:          true,
		FinalState:         TransactionStateCommitted,
	})
	if err != nil {
		t.Fatalf("FinalizeTransaction() error = %v", err)
	}
	if snapshot == nil || record.State != TransactionStateCommitted || record.Generation != 5 || !record.Terminal || record.ReservationHeld {
		t.Fatalf("final record = %+v snapshot=%+v", record, snapshot)
	}
}

func TestTransactionRejectsIllegalTransitionAndStaleGeneration(t *testing.T) {
	store, clock := newTestStore(t)
	plan := testTransactionalPlan(t, store, clock, "txn-plan-2")

	record, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction() error = %v", err)
	}
	if _, err := store.AdvanceTransaction(TransactionAdvanceRequest{
		TransactionID:      record.TransactionID,
		ExpectedGeneration: 1,
		NextState:          TransactionStateApplying,
	}); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("AdvanceTransaction(illegal) error = %v, want ErrTransactionConflict", err)
	}
	if _, _, err := store.ReserveTransaction(record.TransactionID, 9); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("ReserveTransaction(stale generation) error = %v, want ErrTransactionConflict", err)
	}
}

func TestTransactionTerminalImmutability(t *testing.T) {
	store, clock := newTestStore(t)
	plan := testTransactionalPlan(t, store, clock, "txn-plan-3")

	record, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction() error = %v", err)
	}
	record, _, err = store.ReserveTransaction(record.TransactionID, 1)
	if err != nil {
		t.Fatalf("ReserveTransaction() error = %v", err)
	}
	record, err = store.AdvanceTransaction(TransactionAdvanceRequest{
		TransactionID:      record.TransactionID,
		ExpectedGeneration: 2,
		NextState:          TransactionStatePrepareFailed,
		FailureClass:       FailureClassProvider,
		FailureReason:      "prepare rejected",
	})
	if err != nil {
		t.Fatalf("AdvanceTransaction(prepare_failed) error = %v", err)
	}
	if _, err := store.AdvanceTransaction(TransactionAdvanceRequest{
		TransactionID:      record.TransactionID,
		ExpectedGeneration: 3,
		NextState:          TransactionStatePrepared,
	}); !errors.Is(err, ErrTransactionImmutable) {
		t.Fatalf("AdvanceTransaction(after terminal) error = %v, want ErrTransactionImmutable", err)
	}
}

func TestReserveTransactionRejectsAffectedAllocationOverlap(t *testing.T) {
	store, clock := newTestStore(t)
	firstPlan := testTransactionalPlan(t, store, clock, "txn-plan-4a")
	secondPlan := proto.Clone(firstPlan).(*tgsrlv1.PlacementPlan)
	secondPlan.PlanId = "txn-plan-4b"

	first, err := store.CreateTransaction(firstPlan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction(first) error = %v", err)
	}
	second, err := store.CreateTransaction(secondPlan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction(second) error = %v", err)
	}
	if _, _, err := store.ReserveTransaction(first.TransactionID, 1); err != nil {
		t.Fatalf("ReserveTransaction(first) error = %v", err)
	}
	if _, _, err := store.ReserveTransaction(second.TransactionID, 1); !errors.Is(err, ErrTransactionLocked) {
		t.Fatalf("ReserveTransaction(second overlap) error = %v, want ErrTransactionLocked", err)
	}
}

func TestTransactionDurableRoundTrip(t *testing.T) {
	store, clock := newTestStore(t)
	plan := testTransactionalPlan(t, store, clock, "txn-plan-5")

	record, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatalf("CreateTransaction() error = %v", err)
	}
	record, _, err = store.ReserveTransaction(record.TransactionID, 1)
	if err != nil {
		t.Fatalf("ReserveTransaction() error = %v", err)
	}
	record, err = store.AdvanceTransaction(TransactionAdvanceRequest{
		TransactionID:      record.TransactionID,
		ExpectedGeneration: 2,
		NextState:          TransactionStatePrepared,
		ProviderReceipt: &Receipt{
			Phase: "prepared",
			Steps: []TransactionStep{{StepIndex: 0, ActionID: "action-" + plan.GetPlanId(), Status: EffectStatusPending}},
		},
	})
	if err != nil {
		t.Fatalf("AdvanceTransaction() error = %v", err)
	}

	durable := store.ExportDurableState()
	restored, err := NewStore(testSnapshotForTransactions())
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if err := restored.Restore(durable); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	got, ok := restored.GetTransaction(record.TransactionID)
	if !ok {
		t.Fatal("GetTransaction() missing restored transaction")
	}
	if got.State != TransactionStatePrepared || got.Generation != 3 || got.ProviderGeneration != 1 || got.ProviderReceipt == nil || got.ProviderReceipt.Phase != "prepared" {
		t.Fatalf("restored transaction = %+v", got)
	}
}

func testSnapshotForTransactions() *tgsrlv1.ClusterSnapshot {
	return &tgsrlv1.ClusterSnapshot{SnapshotId: "restore-seed", Revision: 1}
}
