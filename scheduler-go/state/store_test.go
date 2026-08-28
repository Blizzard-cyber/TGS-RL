package state

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.RLock()
	defer clock.mu.RUnlock()
	return clock.now
}

func (clock *fakeClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

func newTestStore(t *testing.T) (*Store, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)}
	store, err := NewStore(&tgsrlv1.ClusterSnapshot{
		SnapshotId: "seed",
		Revision:   7,
		Annotations: map[string]string{
			"owner": "store",
		},
	}, WithClock(clock))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	return store, clock
}

func testIntent(clock Clock, version uint64, idempotencyKey string) *tgsrlv1.SchedulingIntent {
	submittedAt := clock.Now()
	ttl := 5 * time.Minute
	return &tgsrlv1.SchedulingIntent{
		ExecutionId:    "execution-1",
		StageId:        "stage-1",
		Version:        version,
		ValidUntil:     timestamppb.New(submittedAt.Add(ttl)),
		Ttl:            durationpb.New(ttl),
		IdempotencyKey: idempotencyKey,
		SubmittedAt:    timestamppb.New(submittedAt),
		JobId:          "job-1",
		ResourcesPerUnit: &tgsrlv1.ResourceVector{
			CpuMillis:   500,
			MemoryBytes: 1024,
		},
		UnitCount: 2,
		RequiredCapabilities: &tgsrlv1.CapabilitySet{
			Names: []string{"sandbox"},
		},
		Priority:      12,
		Queue:         "default",
		RolloutMode:   tgsrlv1.RolloutMode_ROLLOUT_MODE_SYNC,
		PhaseKind:     tgsrlv1.PhaseKind_PHASE_KIND_DECODE,
		PolicyVersion: "policy-1",
		ExecutionContract: &tgsrlv1.ExecutionContract{
			ContractId: "contract-1", Version: "1.0.0",
			VersionConstraints: []*tgsrlv1.VersionConstraint{{Component: "protocol", Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_COMPATIBLE, Version: "0.3"}},
		},
	}
}

func TestStoreClonesInitialSnapshotAndReads(t *testing.T) {
	initial := &tgsrlv1.ClusterSnapshot{
		Revision:    3,
		Annotations: map[string]string{"key": "value"},
	}
	store, err := NewStore(initial)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	initial.Revision = 99
	initial.Annotations["key"] = "input-mutated"
	first, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	first.Revision = 100
	first.Annotations["key"] = "read-mutated"

	second, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if second.GetRevision() != 3 || second.GetAnnotations()["key"] != "value" {
		t.Fatalf("committed snapshot was mutated: %+v", second)
	}
}

func TestMutateResourcesCASAndRetainedPointerIsolation(t *testing.T) {
	store, _ := newTestStore(t)
	var retained *tgsrlv1.ClusterSnapshot

	committed, err := store.MutateResources(7, func(snapshot *tgsrlv1.ClusterSnapshot) error {
		retained = snapshot
		snapshot.Devices = append(snapshot.Devices, &tgsrlv1.Device{DeviceId: "device-1"})
		return nil
	})
	if err != nil {
		t.Fatalf("MutateResources() error = %v", err)
	}
	if committed.GetRevision() != 8 {
		t.Fatalf("revision = %d, want 8", committed.GetRevision())
	}
	retained.Devices[0].DeviceId = "corrupt"
	committed.Devices[0].DeviceId = "also-corrupt"

	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if got := snapshot.GetDevices()[0].GetDeviceId(); got != "device-1" {
		t.Fatalf("device ID = %q, want device-1", got)
	}
	if _, err := store.MutateResources(7, func(*tgsrlv1.ClusterSnapshot) error { return nil }); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale mutation error = %v, want ErrRevisionConflict", err)
	}
}

func TestPublishIntentAcceptanceDedupAndPendingMaterialization(t *testing.T) {
	store, clock := newTestStore(t)
	intent := testIntent(clock, 1, "idempotency-1")

	response, err := store.PublishIntent(intent)
	if err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	if response.GetStatus() != tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_ACCEPTED {
		t.Fatalf("status = %s, want accepted", response.GetStatus())
	}
	if store.Revision() != 8 {
		t.Fatalf("revision = %d, want 8", store.Revision())
	}

	// Mutating the caller's message after publish must not affect dedup or reads.
	intent.Priority = 99
	accepted, ok := store.LatestValidIntent("execution-1", "stage-1")
	if !ok || accepted.GetPriority() != 12 {
		t.Fatalf("stored intent = %+v, valid = %v", accepted, ok)
	}
	accepted.Priority = 101
	acceptedAgain, ok := store.LatestValidIntent("execution-1", "stage-1")
	if !ok || acceptedAgain.GetPriority() != 12 {
		t.Fatalf("returned intent was not cloned: %+v", acceptedAgain)
	}

	retry := testIntent(clock, 1, "idempotency-1")
	response, err = store.PublishIntent(retry)
	if err != nil {
		t.Fatalf("duplicate PublishIntent() error = %v", err)
	}
	if response.GetStatus() != tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_DEDUPLICATED || store.Revision() != 8 {
		t.Fatalf("duplicate status = %s, revision = %d", response.GetStatus(), store.Revision())
	}

	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if len(snapshot.GetPendingUnits()) != 2 {
		t.Fatalf("pending units = %d, want 2", len(snapshot.GetPendingUnits()))
	}
	wantIDs := map[string]bool{
		pendingUnitID(retry, 0): false,
		pendingUnitID(retry, 1): false,
	}
	for index, pending := range snapshot.GetPendingUnits() {
		if _, ok := wantIDs[pending.GetPendingUnitId()]; !ok || pending.GetIntentVersion() != 1 || pending.GetRequestedResources().GetCpuMillis() != 500 {
			t.Fatalf("pending unit %d = %+v, want stable ID and intent data", index, pending)
		}
		wantIDs[pending.GetPendingUnitId()] = true
	}
	for pendingID, found := range wantIDs {
		if !found {
			t.Fatalf("pending ID %q was not materialized", pendingID)
		}
	}

	withoutPending, err := store.GetSnapshot(context.Background(), 0, false)
	if err != nil {
		t.Fatalf("GetSnapshot(includePending=false) error = %v", err)
	}
	if withoutPending.PendingUnits != nil {
		t.Fatalf("pending units = %+v, want omitted", withoutPending.PendingUnits)
	}
}

func TestPublishIntentRejectsConflictsAndInvalidLifetime(t *testing.T) {
	store, clock := newTestStore(t)
	if _, err := store.PublishIntent(testIntent(clock, 2, "idempotency-2")); err != nil {
		t.Fatalf("initial PublishIntent() error = %v", err)
	}
	wantRevision := store.Revision()

	tests := []struct {
		name   string
		intent *tgsrlv1.SchedulingIntent
		want   error
	}{
		{name: "stale", intent: testIntent(clock, 1, "stale-key"), want: ErrStaleIntent},
		{name: "same version conflict", intent: func() *tgsrlv1.SchedulingIntent {
			intent := testIntent(clock, 2, "idempotency-2")
			intent.Priority++
			return intent
		}(), want: ErrIdempotencyConflict},
		{name: "same version new key conflict", intent: func() *tgsrlv1.SchedulingIntent {
			intent := testIntent(clock, 2, "different-key")
			intent.Priority++
			return intent
		}(), want: ErrIntentConflict},
		{name: "reused idempotency key", intent: testIntent(clock, 3, "idempotency-2"), want: ErrIdempotencyConflict},
		{name: "mismatched expiry", intent: func() *tgsrlv1.SchedulingIntent {
			intent := testIntent(clock, 3, "idempotency-3")
			intent.ValidUntil = timestamppb.New(intent.ValidUntil.AsTime().Add(time.Second))
			return intent
		}(), want: ErrInvalidIntent},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := store.PublishIntent(test.intent)
			if !errors.Is(err, test.want) {
				t.Fatalf("PublishIntent() error = %v, want %v", err, test.want)
			}
			if response.GetStatus() != tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_REJECTED {
				t.Fatalf("status = %s, want rejected", response.GetStatus())
			}
			if store.Revision() != wantRevision {
				t.Fatalf("revision = %d, want unchanged %d", store.Revision(), wantRevision)
			}
		})
	}

	clock.Advance(6 * time.Minute)
	if _, ok := store.LatestValidIntent("execution-1", "stage-1"); ok {
		t.Fatal("LatestValidIntent() returned expired intent")
	}
	expired := testIntent(ClockFunc(func() time.Time { return clock.Now().Add(-10 * time.Minute) }), 3, "expired")
	if _, err := store.PublishIntent(expired); !errors.Is(err, ErrIntentExpired) {
		t.Fatalf("expired PublishIntent() error = %v, want ErrIntentExpired", err)
	}
}

func TestHigherVersionReplacesPendingUnitsAndPlanValidation(t *testing.T) {
	store, clock := newTestStore(t)
	if _, err := store.PublishIntent(testIntent(clock, 1, "idempotency-1")); err != nil {
		t.Fatalf("PublishIntent(version 1) error = %v", err)
	}
	version2 := testIntent(clock, 2, "idempotency-2")
	version2.UnitCount = 1
	if _, err := store.PublishIntent(version2); err != nil {
		t.Fatalf("PublishIntent(version 2) error = %v", err)
	}

	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if len(snapshot.GetPendingUnits()) != 1 || snapshot.GetPendingUnits()[0].GetIntentVersion() != 2 {
		t.Fatalf("pending units = %+v, want only version 2", snapshot.GetPendingUnits())
	}

	valid := &tgsrlv1.PlacementPlan{
		ExecutionId:      "execution-1",
		StageId:          "stage-1",
		IntentVersion:    2,
		SnapshotRevision: snapshot.GetRevision(),
	}
	if err := store.ValidatePlan(valid); err != nil {
		t.Fatalf("ValidatePlan(valid) error = %v", err)
	}
	staleRevision := proto.Clone(valid).(*tgsrlv1.PlacementPlan)
	staleRevision.SnapshotRevision--
	if err := store.ValidatePlan(staleRevision); !errors.Is(err, ErrPlanRevisionConflict) {
		t.Fatalf("ValidatePlan(stale revision) error = %v", err)
	}
	staleIntent := proto.Clone(valid).(*tgsrlv1.PlacementPlan)
	staleIntent.IntentVersion--
	if err := store.ValidatePlan(staleIntent); !errors.Is(err, ErrPlanIntentConflict) {
		t.Fatalf("ValidatePlan(stale intent) error = %v", err)
	}
	clock.Advance(6 * time.Minute)
	if err := store.ValidatePlan(valid); !errors.Is(err, ErrIntentExpired) {
		t.Fatalf("ValidatePlan(expired intent) error = %v, want ErrIntentExpired", err)
	}
}

func TestRetryOfSupersededVersionIsStale(t *testing.T) {
	store, clock := newTestStore(t)
	version1 := testIntent(clock, 1, "idempotency-1")
	if _, err := store.PublishIntent(version1); err != nil {
		t.Fatalf("PublishIntent(version 1) error = %v", err)
	}
	if _, err := store.PublishIntent(testIntent(clock, 2, "idempotency-2")); err != nil {
		t.Fatalf("PublishIntent(version 2) error = %v", err)
	}
	wantRevision := store.Revision()

	response, err := store.PublishIntent(version1)
	if !errors.Is(err, ErrStaleIntent) {
		t.Fatalf("superseded retry error = %v, want ErrStaleIntent", err)
	}
	if response.GetStatus() != tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_REJECTED || store.Revision() != wantRevision {
		t.Fatalf("superseded retry status = %s, revision = %d", response.GetStatus(), store.Revision())
	}
}

func TestHigherVersionRejectsActiveAllocationSemanticDrift(t *testing.T) {
	store, clock := newTestStore(t)
	version1 := testIntent(clock, 1, "idempotency-1")
	version1.UnitCount = 1
	if _, err := store.PublishIntent(version1); err != nil {
		t.Fatalf("PublishIntent(version 1) error = %v", err)
	}
	snapshot, _ := store.GetSnapshot(context.Background(), 0, true)
	pending := snapshot.GetPendingUnits()[0]
	plan := &tgsrlv1.PlacementPlan{
		PlanId: "plan-v1", ExecutionId: version1.GetExecutionId(), StageId: version1.GetStageId(),
		IntentVersion: 1, SnapshotRevision: snapshot.GetRevision(),
		Bindings: []*tgsrlv1.Binding{{BindingId: "binding-v1", PendingUnitId: pending.GetPendingUnitId(), DeviceIds: []string{"device-1"}, Resources: cloneResourceVector(version1.GetResourcesPerUnit())}},
	}
	// Add enough logical capacity for the reservation without coupling this
	// contract test to scheduler fixtures.
	if _, err := store.MutateResources(snapshot.GetRevision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.Devices = []*tgsrlv1.Device{{DeviceId: "device-1", Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY, Capacity: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096}, Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096}}}
		return nil
	}); err != nil {
		t.Fatalf("MutateResources() error = %v", err)
	}
	current, _ := store.GetSnapshot(context.Background(), 0, true)
	plan.SnapshotRevision = current.GetRevision()
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan() error = %v", err)
	}
	if _, err := store.FinalizePlan(plan, true); err != nil {
		t.Fatalf("FinalizePlan() error = %v", err)
	}

	changed := testIntent(clock, 2, "idempotency-2")
	changed.UnitCount = 1
	changed.ResourcesPerUnit.CpuMillis++
	if _, err := store.PublishIntent(changed); !errors.Is(err, ErrInvalidIntent) {
		t.Fatalf("semantic drift error = %v, want ErrInvalidIntent", err)
	}
	scaleIn := testIntent(clock, 2, "idempotency-scale-in")
	scaleIn.UnitCount = 1
	// Add a second active allocation to make the requested count a scale-in.
	if _, err := store.MutateResources(store.Revision(), func(working *tgsrlv1.ClusterSnapshot) error {
		duplicate := proto.Clone(working.Allocations[0]).(*tgsrlv1.Allocation)
		duplicate.AllocationId = "allocation-v1-duplicate"
		working.Allocations = append(working.Allocations, duplicate)
		return nil
	}); err != nil {
		t.Fatalf("MutateResources(second allocation) error = %v", err)
	}
	if _, err := store.PublishIntent(scaleIn); !errors.Is(err, ErrInvalidIntent) {
		t.Fatalf("scale-in error = %v, want ErrInvalidIntent", err)
	}
}

func TestFinalizePlanResultsRetainsCapacityWhenRollbackIsIncomplete(t *testing.T) {
	store, clock := newTestStore(t)
	intent := testIntent(clock, 1, "idempotency-rollback")
	intent.UnitCount = 1
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	snapshot, _ := store.GetSnapshot(context.Background(), 0, true)
	if _, err := store.MutateResources(snapshot.GetRevision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.Devices = []*tgsrlv1.Device{{DeviceId: "device-1", Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY, Capacity: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096}, Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096}}}
		return nil
	}); err != nil {
		t.Fatalf("MutateResources() error = %v", err)
	}
	snapshot, _ = store.GetSnapshot(context.Background(), 0, true)
	pending := snapshot.GetPendingUnits()[0]
	binding := &tgsrlv1.Binding{BindingId: "rollback-binding", PendingUnitId: pending.GetPendingUnitId(), DeviceIds: []string{"device-1"}, Resources: cloneResourceVector(intent.GetResourcesPerUnit())}
	plan := &tgsrlv1.PlacementPlan{
		PlanId: "rollback-plan", ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(),
		IntentVersion: 1, SnapshotRevision: snapshot.GetRevision(),
		Bindings: []*tgsrlv1.Binding{binding},
		Actions:  []*tgsrlv1.Action{{ActionId: "bind-action", Binding: proto.Clone(binding).(*tgsrlv1.Binding)}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan() error = %v", err)
	}
	final, err := store.FinalizePlanResults(plan, false, []*tgsrlv1.ActionResult{{
		ActionId:          "bind-action",
		Status:            tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED,
		RollbackAttempted: true,
		RollbackStatus:    tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults() error = %v", err)
	}
	if len(final.GetAllocations()) != 1 || final.GetAllocations()[0].GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
		t.Fatalf("allocations = %+v, want conservative active allocation", final.GetAllocations())
	}
	if final.GetDevices()[0].GetAllocatable().GetCpuMillis() != 500 || len(final.GetPendingUnits()) != 0 {
		t.Fatalf("capacity/pending was incorrectly restored: %+v", final)
	}
}

func TestFinalizePlanResultsReconcilesEachBinding(t *testing.T) {
	store, clock := newTestStore(t)
	intent := testIntent(clock, 1, "idempotency-per-binding-rollback")
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	snapshot, _ := store.GetSnapshot(context.Background(), 0, true)
	if _, err := store.MutateResources(snapshot.GetRevision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.Devices = []*tgsrlv1.Device{{
			DeviceId: "device-1", Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096},
			Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096},
		}}
		return nil
	}); err != nil {
		t.Fatalf("MutateResources() error = %v", err)
	}
	snapshot, _ = store.GetSnapshot(context.Background(), 0, true)
	first := &tgsrlv1.Binding{BindingId: "binding-first", PendingUnitId: snapshot.PendingUnits[0].GetPendingUnitId(), DeviceIds: []string{"device-1"}, Resources: cloneResourceVector(intent.GetResourcesPerUnit())}
	second := &tgsrlv1.Binding{BindingId: "binding-second", PendingUnitId: snapshot.PendingUnits[1].GetPendingUnitId(), DeviceIds: []string{"device-1"}, Resources: cloneResourceVector(intent.GetResourcesPerUnit())}
	plan := &tgsrlv1.PlacementPlan{
		PlanId: "per-binding-plan", ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(),
		IntentVersion: intent.GetVersion(), SnapshotRevision: snapshot.GetRevision(),
		Bindings: []*tgsrlv1.Binding{first, second},
		Actions: []*tgsrlv1.Action{
			{ActionId: "first-action", Binding: proto.Clone(first).(*tgsrlv1.Binding)},
			{ActionId: "second-action", Binding: proto.Clone(second).(*tgsrlv1.Binding)},
		},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan() error = %v", err)
	}
	final, err := store.FinalizePlanResults(plan, false, []*tgsrlv1.ActionResult{
		{ActionId: "first-action", Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED, RollbackAttempted: true, RollbackStatus: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK},
		{ActionId: "second-action", Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED, RollbackAttempted: true, RollbackStatus: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED},
	})
	if err != nil {
		t.Fatalf("FinalizePlanResults() error = %v", err)
	}
	if len(final.GetAllocations()) != 1 || final.GetAllocations()[0].GetPendingUnitId() != second.GetPendingUnitId() {
		t.Fatalf("allocations = %+v, want only the incompletely rolled-back binding", final.GetAllocations())
	}
	if len(final.GetPendingUnits()) != 1 || final.GetPendingUnits()[0].GetPendingUnitId() != first.GetPendingUnitId() {
		t.Fatalf("pending units = %+v, want only the successfully rolled-back binding", final.GetPendingUnits())
	}
	if got := final.GetDevices()[0].GetAllocatable().GetCpuMillis(); got != 500 {
		t.Fatalf("allocatable CPU = %d, want 500 retained and 500 released", got)
	}
}

func TestFinalizePlanResultsRetainsReservationWithoutProviderEvidence(t *testing.T) {
	store, clock := newTestStore(t)
	intent := testIntent(clock, 1, "idempotency-missing-results")
	intent.UnitCount = 1
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	snapshot, _ := store.GetSnapshot(context.Background(), 0, true)
	if _, err := store.MutateResources(snapshot.GetRevision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.Devices = []*tgsrlv1.Device{{
			DeviceId: "device-1", Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096},
			Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096},
		}}
		return nil
	}); err != nil {
		t.Fatalf("MutateResources() error = %v", err)
	}
	snapshot, _ = store.GetSnapshot(context.Background(), 0, true)
	pending := snapshot.GetPendingUnits()[0]
	binding := &tgsrlv1.Binding{BindingId: "missing-result-binding", PendingUnitId: pending.GetPendingUnitId(), DeviceIds: []string{"device-1"}, Resources: cloneResourceVector(intent.GetResourcesPerUnit())}
	plan := &tgsrlv1.PlacementPlan{
		PlanId: "missing-result-plan", ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(),
		IntentVersion: intent.GetVersion(), SnapshotRevision: snapshot.GetRevision(),
		Bindings: []*tgsrlv1.Binding{binding},
		Actions:  []*tgsrlv1.Action{{ActionId: "missing-result-action", Binding: proto.Clone(binding).(*tgsrlv1.Binding)}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan() error = %v", err)
	}
	final, err := store.FinalizePlanResults(plan, false, nil)
	if err != nil {
		t.Fatalf("FinalizePlanResults() error = %v", err)
	}
	if len(final.GetAllocations()) != 1 || final.GetAllocations()[0].GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE || len(final.GetPendingUnits()) != 0 {
		t.Fatalf("missing evidence released reserved resources: %+v", final)
	}
}

func TestGetSnapshotWaitsForMinimumRevisionAndContext(t *testing.T) {
	store, _ := newTestStore(t)
	result := make(chan *tgsrlv1.ClusterSnapshot, 1)
	errResult := make(chan error, 1)
	go func() {
		snapshot, err := store.GetSnapshot(context.Background(), 8, true)
		result <- snapshot
		errResult <- err
	}()

	select {
	case <-result:
		t.Fatal("GetSnapshot() returned before minimum revision")
	case <-time.After(10 * time.Millisecond):
	}
	if _, err := store.MutateResources(7, func(*tgsrlv1.ClusterSnapshot) error { return nil }); err != nil {
		t.Fatalf("MutateResources() error = %v", err)
	}
	select {
	case snapshot := <-result:
		if err := <-errResult; err != nil || snapshot.GetRevision() != 8 {
			t.Fatalf("wait result snapshot = %+v, error = %v", snapshot, err)
		}
	case <-time.After(time.Second):
		t.Fatal("GetSnapshot() did not wake after revision commit")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.GetSnapshot(ctx, 9, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetSnapshot(canceled) error = %v, want context.Canceled", err)
	}
}

func TestConcurrentCASHasSingleWinnerPerRevision(t *testing.T) {
	const (
		startRevision = uint64(7)
		writers       = 32
	)
	store, _ := newTestStore(t)

	var waitGroup sync.WaitGroup
	results := make(chan error, writers)
	for writer := 0; writer < writers; writer++ {
		waitGroup.Add(1)
		go func(id int) {
			defer waitGroup.Done()
			_, err := store.MutateResources(startRevision, func(snapshot *tgsrlv1.ClusterSnapshot) error {
				snapshot.Annotations["winner"] = fmt.Sprint(id)
				return nil
			})
			results <- err
		}(writer)
	}
	waitGroup.Wait()
	close(results)

	successes := 0
	conflicts := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrRevisionConflict):
			conflicts++
		default:
			t.Fatalf("unexpected mutation error = %v", err)
		}
	}
	if successes != 1 || conflicts != writers-1 || store.Revision() != startRevision+1 {
		t.Fatalf("successes = %d, conflicts = %d, revision = %d", successes, conflicts, store.Revision())
	}
}

func TestConcurrentReadersAndWriters(t *testing.T) {
	const writes = 64
	store, _ := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var readers sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			var previous uint64
			for {
				snapshot, err := store.GetSnapshot(ctx, 0, true)
				if err != nil {
					return
				}
				if snapshot.GetRevision() < previous {
					t.Errorf("reader observed revision regression: %d after %d", snapshot.GetRevision(), previous)
					return
				}
				previous = snapshot.GetRevision()
				if snapshot.Annotations == nil {
					snapshot.Annotations = make(map[string]string)
				}
				snapshot.Annotations["reader-mutation"] = "private"
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}()
	}

	for write := 0; write < writes; write++ {
		expected := uint64(7 + write)
		if _, err := store.MutateResources(expected, func(snapshot *tgsrlv1.ClusterSnapshot) error {
			snapshot.Annotations["writes"] = fmt.Sprint(write + 1)
			return nil
		}); err != nil {
			t.Fatalf("MutateResources(write %d) error = %v", write, err)
		}
	}
	cancel()
	readers.Wait()

	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if snapshot.GetRevision() != 7+writes || snapshot.GetAnnotations()["writes"] != fmt.Sprint(writes) {
		t.Fatalf("final snapshot revision = %d, annotations = %+v", snapshot.GetRevision(), snapshot.GetAnnotations())
	}
	if _, leaked := snapshot.GetAnnotations()["reader-mutation"]; leaked {
		t.Fatal("mutation of a read clone leaked into committed state")
	}
}

func TestDurableStateRoundTripPreservesIntentAndReservation(t *testing.T) {
	store, clock := newTestStore(t)
	intent := testIntent(clock, 1, "idempotency-1")
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := store.GetSnapshot(context.Background(), 0, true)
	if _, err := store.MutateResources(snapshot.GetRevision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.Devices = []*tgsrlv1.Device{{DeviceId: "device-1", Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY, Capacity: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096}, Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = store.GetSnapshot(context.Background(), 0, true)
	binding := &tgsrlv1.Binding{BindingId: "binding-1", PendingUnitId: snapshot.PendingUnits[0].GetPendingUnitId(), DeviceIds: []string{"device-1"}, Resources: cloneResourceVector(intent.GetResourcesPerUnit())}
	plan := &tgsrlv1.PlacementPlan{PlanId: "plan-1", ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(), IntentVersion: intent.GetVersion(), SnapshotRevision: snapshot.GetRevision(), Bindings: []*tgsrlv1.Binding{binding}}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatal(err)
	}
	durable := store.ExportDurableState()
	restored, err := NewStore(nil, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(durable); err != nil {
		t.Fatal(err)
	}
	got := restored.ExportDurableState()
	if !proto.Equal(got.Snapshot, durable.Snapshot) || len(got.Intents) != 1 || len(got.Reservations) != 1 || !proto.Equal(got.Reservations[0].Plan, plan) {
		t.Fatalf("restored durable state = %+v", got)
	}
}

func TestPropertyAcceptedIntentsAdvanceExactlyOnce(t *testing.T) {
	const cases = 200
	random := rand.New(rand.NewSource(20260827))

	for caseIndex := 0; caseIndex < cases; caseIndex++ {
		store, clock := newTestStore(t)
		version := uint64(random.Intn(1_000) + 1)
		intent := testIntent(clock, version, fmt.Sprintf("key-%d-%d", caseIndex, version))
		intent.UnitCount = uint32(random.Intn(16) + 1)
		intent.Priority = int32(random.Intn(201) - 100)

		response, err := store.PublishIntent(intent)
		if err != nil || response.GetStatus() != tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_ACCEPTED {
			t.Fatalf("case %d: PublishIntent() = (%v, %v)", caseIndex, response, err)
		}
		if store.Revision() != 8 {
			t.Fatalf("case %d: revision = %d, want 8", caseIndex, store.Revision())
		}

		snapshot, err := store.GetSnapshot(context.Background(), 0, true)
		if err != nil {
			t.Fatalf("case %d: GetSnapshot() error = %v", caseIndex, err)
		}
		if len(snapshot.GetPendingUnits()) != int(intent.GetUnitCount()) {
			t.Fatalf("case %d: pending count = %d, want %d", caseIndex, len(snapshot.GetPendingUnits()), intent.GetUnitCount())
		}
		seen := make(map[string]struct{}, len(snapshot.GetPendingUnits()))
		for _, pending := range snapshot.GetPendingUnits() {
			if pending.GetIntentVersion() != version {
				t.Fatalf("case %d: pending version = %d, want %d", caseIndex, pending.GetIntentVersion(), version)
			}
			if _, duplicate := seen[pending.GetPendingUnitId()]; duplicate {
				t.Fatalf("case %d: duplicate pending ID %q", caseIndex, pending.GetPendingUnitId())
			}
			seen[pending.GetPendingUnitId()] = struct{}{}
		}

		response, err = store.PublishIntent(proto.Clone(intent).(*tgsrlv1.SchedulingIntent))
		if err != nil || response.GetStatus() != tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_DEDUPLICATED || store.Revision() != 8 {
			t.Fatalf("case %d: retry = (%v, %v), revision = %d", caseIndex, response, err, store.Revision())
		}
	}
}

func TestBootstrapProviderSnapshotPreservesStoreOwnedAllocationsAndPendingUnits(t *testing.T) {
	store, clock := newTestStore(t)
	intent := testIntent(clock, 1, "projection-1")
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
			DeviceId: "device-1", Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096},
			Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096},
		}}
		return nil
	}); err != nil {
		t.Fatalf("MutateResources() error = %v", err)
	}
	snapshot, _ = store.GetSnapshot(context.Background(), 0, true)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:           "projection-plan",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		Bindings: []*tgsrlv1.Binding{{
			BindingId:     "projection-binding",
			PendingUnitId: snapshot.GetPendingUnits()[0].GetPendingUnitId(),
			DeviceIds:     []string{"device-1"},
			Resources:     cloneResourceVector(intent.GetResourcesPerUnit()),
		}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan() error = %v", err)
	}
	reserved, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot(reserved) error = %v", err)
	}
	projected := &tgsrlv1.ClusterSnapshot{
		Revision:   99,
		SnapshotId: "provider-99",
		Devices: []*tgsrlv1.Device{{
			DeviceId: "device-1", Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 2000, MemoryBytes: 8192},
			Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 2000, MemoryBytes: 8192},
		}},
		Annotations: map[string]string{"provider": "projected"},
	}
	bootstrapped, changed, err := store.BootstrapProviderSnapshot("mock", projected)
	if err != nil {
		t.Fatalf("BootstrapProviderSnapshot() error = %v", err)
	}
	if !changed {
		t.Fatal("BootstrapProviderSnapshot() changed=false, want true")
	}
	if len(bootstrapped.GetAllocations()) != len(reserved.GetAllocations()) {
		t.Fatalf("allocations after bootstrap = %d, want preserved %d", len(bootstrapped.GetAllocations()), len(reserved.GetAllocations()))
	}
	if len(bootstrapped.GetPendingUnits()) != len(reserved.GetPendingUnits()) {
		t.Fatalf("pending units after bootstrap = %d, want preserved %d", len(bootstrapped.GetPendingUnits()), len(reserved.GetPendingUnits()))
	}
	if got := bootstrapped.GetDevices()[0].GetCapacity().GetCpuMillis(); got != 2000 {
		t.Fatalf("bootstrapped device CPU = %d, want provider-projected 2000", got)
	}
}

func TestRestoreResetsProviderProjectionState(t *testing.T) {
	store, clock := newTestStore(t)
	if _, changed, err := store.BootstrapProviderSnapshot("mock", &tgsrlv1.ClusterSnapshot{
		Revision:   10,
		SnapshotId: "provider-10",
		Devices: []*tgsrlv1.Device{{
			DeviceId: "device-1",
			Health:   tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
		}},
	}); err != nil || !changed {
		t.Fatalf("BootstrapProviderSnapshot() = changed:%v err:%v, want changed true nil err", changed, err)
	}
	if changed, err := store.ReplaceProjectedSandboxes([]*tgsrlv1.Sandbox{{
		SandboxId:  "sandbox-1",
		Generation: 3,
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
	}}); err != nil || !changed {
		t.Fatalf("ReplaceProjectedSandboxes() = changed:%v err:%v, want changed true nil err", changed, err)
	}
	durable := store.ExportDurableState()

	restored, err := NewStore(nil, WithClock(clock))
	if err != nil {
		t.Fatalf("NewStore(restored) error = %v", err)
	}
	if err := restored.Restore(durable); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if sandboxes := restored.ListProjectedSandboxes(); len(sandboxes) != 0 {
		t.Fatalf("ListProjectedSandboxes() after Restore = %+v, want cleared projection state", sandboxes)
	}
	if _, changed, err := restored.BootstrapProviderSnapshot("mock", &tgsrlv1.ClusterSnapshot{
		Revision:   10,
		SnapshotId: "provider-10",
		Devices: []*tgsrlv1.Device{{
			DeviceId: "device-1",
			Health:   tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
		}},
	}); err != nil || !changed {
		t.Fatalf("BootstrapProviderSnapshot(after Restore) = changed:%v err:%v, want changed true nil err", changed, err)
	}
}
