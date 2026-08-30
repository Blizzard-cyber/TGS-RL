package planexecutor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scheduler"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeTransactionalProvider struct {
	mu                   sync.Mutex
	prepareErr           error
	commitErr            error
	abortErr             error
	reconcileErr         error
	executeErrAt         map[int]error
	stepReceipts         map[int]*provider.TransactionReceipt
	reconcileReceipt     *provider.TransactionReceipt
	commitReceipt        *provider.TransactionReceipt
	abortReceipt         *provider.TransactionReceipt
	preparedPlan         *tgsrlv1.PlacementPlan
	prepareCalls         int
	commitCalls          int
	abortCalls           int
	reconcileCalls       int
	executeStepCalls     []int
	prepareGenerations   []uint64
	executeGenerations   []uint64
	commitGenerations    []uint64
	abortGenerations     []uint64
	reconcileGenerations []uint64
}

func (f *fakeTransactionalProvider) Capabilities(context.Context) (*tgsrlv1.CapabilitySet, error) {
	return &tgsrlv1.CapabilitySet{}, nil
}
func (f *fakeTransactionalProvider) Snapshot(context.Context) (*tgsrlv1.ClusterSnapshot, error) {
	return &tgsrlv1.ClusterSnapshot{}, nil
}
func (f *fakeTransactionalProvider) ListDevices(context.Context) ([]*tgsrlv1.Device, error) {
	return nil, nil
}
func (f *fakeTransactionalProvider) ListSandboxes(context.Context) ([]provider.Sandbox, error) {
	return nil, nil
}
func (f *fakeTransactionalProvider) GetSandbox(context.Context, string) (provider.Sandbox, error) {
	return provider.Sandbox{}, nil
}
func (f *fakeTransactionalProvider) PreparePlan(_ context.Context, transactionID string, generation uint64, plan *tgsrlv1.PlacementPlan) (*provider.TransactionReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepareCalls++
	f.prepareGenerations = append(f.prepareGenerations, generation)
	if plan != nil {
		f.preparedPlan = proto.Clone(plan).(*tgsrlv1.PlacementPlan)
	}
	if f.prepareErr != nil {
		return nil, f.prepareErr
	}
	effects := make([]provider.TransactionEffect, 0, len(plan.GetActions()))
	for index, action := range plan.GetActions() {
		effects = append(effects, provider.TransactionEffect{StepIndex: index, ActionID: action.GetActionId(), IdempotencyKey: action.GetIdempotencyKey(), Generation: generation, Status: provider.EffectStatusNotApplied})
	}
	return receiptFor(transactionID, generation, provider.TransactionPhasePrepared, effects...), nil
}
func (f *fakeTransactionalProvider) ExecuteStep(_ context.Context, transactionID string, generation uint64, stepIndex int) (*provider.TransactionReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.executeStepCalls = append(f.executeStepCalls, stepIndex)
	f.executeGenerations = append(f.executeGenerations, generation)
	if err := f.executeErrAt[stepIndex]; err != nil {
		return nil, err
	}
	if receipt := f.stepReceipts[stepIndex]; receipt != nil {
		return CloneReceipt(receipt), nil
	}
	return receiptFor(transactionID, generation, provider.TransactionPhaseExecuting, provider.TransactionEffect{StepIndex: stepIndex, ActionID: f.preparedPlan.GetActions()[stepIndex].GetActionId(), IdempotencyKey: f.preparedPlan.GetActions()[stepIndex].GetIdempotencyKey(), Generation: generation, Status: provider.EffectStatusApplied}), nil
}
func (f *fakeTransactionalProvider) CommitPlan(_ context.Context, transactionID string, generation uint64) (*provider.TransactionReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitCalls++
	f.commitGenerations = append(f.commitGenerations, generation)
	if f.commitErr != nil {
		return nil, f.commitErr
	}
	if f.commitReceipt != nil {
		return CloneReceipt(f.commitReceipt), nil
	}
	return receiptFor(transactionID, generation, provider.TransactionPhaseCommitted), nil
}
func (f *fakeTransactionalProvider) AbortPlan(_ context.Context, transactionID string, generation uint64) (*provider.TransactionReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.abortCalls++
	f.abortGenerations = append(f.abortGenerations, generation)
	if f.abortErr != nil {
		return nil, f.abortErr
	}
	if f.abortReceipt != nil {
		return CloneReceipt(f.abortReceipt), nil
	}
	return receiptFor(transactionID, generation, provider.TransactionPhaseAborted), nil
}
func (f *fakeTransactionalProvider) ReconcilePlanTransaction(_ context.Context, transactionID string, generation uint64) (*provider.TransactionReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconcileCalls++
	f.reconcileGenerations = append(f.reconcileGenerations, generation)
	if f.reconcileErr != nil {
		return nil, f.reconcileErr
	}
	if f.reconcileReceipt != nil {
		return CloneReceipt(f.reconcileReceipt), nil
	}
	return receiptFor(transactionID, generation, provider.TransactionPhaseCommitted), nil
}
func (*fakeTransactionalProvider) DescribeCapabilities(context.Context) (provider.TransactionCapabilities, error) {
	return provider.TransactionCapabilities{}, nil
}

func TestExecuteHappyPath(t *testing.T) {
	store, plan := testStoreAndPlan(t, "plan-1", "action-1", "action-2")
	checkpoints := make([]state.TransactionState, 0)
	providerFake := &fakeTransactionalProvider{executeErrAt: map[int]error{}, stepReceipts: map[int]*provider.TransactionReceipt{
		0: receiptFor("plan-1", 1, provider.TransactionPhaseExecuting, provider.TransactionEffect{StepIndex: 0, ActionID: "action-1", Generation: 1, Status: provider.EffectStatusApplied}),
		1: receiptFor("plan-1", 1, provider.TransactionPhaseExecuting, provider.TransactionEffect{StepIndex: 1, ActionID: "action-2", Generation: 1, Status: provider.EffectStatusApplied}),
	}}
	executor := newTestExecutor(t, store, providerFake, func(_ context.Context, record *state.TransactionRecord) error {
		checkpoints = append(checkpoints, record.State)
		return nil
	})
	outcome, err := executor.Execute(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Succeeded() || outcome.Transaction.State != state.TransactionStateCommitted {
		t.Fatalf("outcome = %+v, want committed", outcome)
	}
	if len(checkpoints) < 5 {
		t.Fatalf("checkpoint count = %d, want multiple durable boundaries", len(checkpoints))
	}
	assertEffects(t, outcome.Results(), tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED, tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED)
	assertProviderGenerations(t, providerFake)
}

func TestPrepareFailureFinalizesReservation(t *testing.T) {
	store, plan := testStoreAndPlan(t, "plan-prepare-fail", "action-1")
	providerFake := &fakeTransactionalProvider{prepareErr: errors.New("prepare failed"), executeErrAt: map[int]error{}, stepReceipts: map[int]*provider.TransactionReceipt{}}
	outcome, err := newTestExecutor(t, store, providerFake, nil).Execute(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Transaction.State != state.TransactionStatePrepareFailed || outcome.Transaction.FailureClass != state.FailureClassProvider {
		t.Fatalf("transaction = %+v", outcome.Transaction)
	}
	assertEffects(t, outcome.Results(), tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SKIPPED)
}

func TestStepFailureAbortsAndPreservesCompensationEvidence(t *testing.T) {
	store, plan := testStoreAndPlan(t, "plan-apply-fail", "action-1", "action-2")
	providerFake := &fakeTransactionalProvider{executeErrAt: map[int]error{}, stepReceipts: map[int]*provider.TransactionReceipt{
		0: receiptFor(plan.GetPlanId(), 1, provider.TransactionPhaseExecuting, provider.TransactionEffect{StepIndex: 0, ActionID: "action-1", Status: provider.EffectStatusApplied}),
		1: receiptFor(plan.GetPlanId(), 1, provider.TransactionPhaseExecuting, provider.TransactionEffect{StepIndex: 0, ActionID: "action-1", Status: provider.EffectStatusApplied}, provider.TransactionEffect{StepIndex: 1, ActionID: "action-2", Status: provider.EffectStatusFailed}),
	}, abortReceipt: receiptFor(plan.GetPlanId(), 1, provider.TransactionPhaseAborted,
		provider.TransactionEffect{StepIndex: 0, ActionID: "action-1", Status: provider.EffectStatusCompensated, Compensated: true},
		provider.TransactionEffect{StepIndex: 1, ActionID: "action-2", Status: provider.EffectStatusFailed}),
	}
	outcome, err := newTestExecutor(t, store, providerFake, nil).Execute(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Transaction.State != state.TransactionStateAborted || providerFake.abortCalls != 1 {
		t.Fatalf("outcome=%+v abort_calls=%d", outcome, providerFake.abortCalls)
	}
	results := outcome.Results()
	assertEffects(t, results, tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED, tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED)
	if !results[0].GetRollbackAttempted() || results[0].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK {
		t.Fatalf("rollback result = %+v", results[0])
	}
}

func TestAbortFailureBecomesDegraded(t *testing.T) {
	store, plan := testStoreAndPlan(t, "plan-degraded", "action-1")
	providerFake := &fakeTransactionalProvider{executeErrAt: map[int]error{0: errors.New("ambiguous step")}, stepReceipts: map[int]*provider.TransactionReceipt{}, abortErr: errors.New("abort failed"), reconcileReceipt: receiptFor(plan.GetPlanId(), 1, provider.TransactionPhaseDegraded, provider.TransactionEffect{StepIndex: 0, ActionID: "action-1", Status: provider.EffectStatusDegraded})}
	outcome, err := newTestExecutor(t, store, providerFake, nil).Execute(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Degraded() || outcome.Transaction.FailureClass != state.FailureClassCompensation {
		t.Fatalf("transaction = %+v", outcome.Transaction)
	}
}

func TestUnreconciledExecutionFailureBecomesDegraded(t *testing.T) {
	store, plan := testStoreAndPlan(t, "plan-unreconciled", "action-1")
	providerFake := &fakeTransactionalProvider{
		executeErrAt: map[int]error{0: errors.New("execution outcome unknown")},
		stepReceipts: map[int]*provider.TransactionReceipt{},
		reconcileErr: errors.New("provider ledger unavailable"),
	}
	outcome, err := newTestExecutor(t, store, providerFake, nil).Execute(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Degraded() || !outcome.Terminal {
		t.Fatalf("outcome = %+v, want terminal degraded", outcome)
	}
	if outcome.Transaction.ReservationHeld {
		t.Fatalf("transaction retained reservation after degraded finalization: %+v", outcome.Transaction)
	}
	if outcome.Transaction.FailureClass != state.FailureClassInfrastructure {
		t.Fatalf("failure class = %s, want infrastructure", outcome.Transaction.FailureClass)
	}
}

func TestAmbiguousExecutionUsesProviderReconciliation(t *testing.T) {
	store, plan := testStoreAndPlan(t, "plan-ambiguous", "action-1")
	providerFake := &fakeTransactionalProvider{executeErrAt: map[int]error{0: errors.New("unknown execution")}, stepReceipts: map[int]*provider.TransactionReceipt{}, reconcileReceipt: receiptFor(plan.GetPlanId(), 1, provider.TransactionPhaseCommitted, provider.TransactionEffect{StepIndex: 0, ActionID: "action-1", Status: provider.EffectStatusApplied})}
	outcome, err := newTestExecutor(t, store, providerFake, nil).Execute(context.Background(), plan)
	if err != nil || !outcome.Succeeded() || providerFake.reconcileCalls == 0 {
		t.Fatalf("outcome=%+v error=%v reconcile_calls=%d", outcome, err, providerFake.reconcileCalls)
	}
}

func TestDuplicatePlanReturnsDurableTerminalRecord(t *testing.T) {
	store, plan := testStoreAndPlan(t, "plan-dup", "action-1")
	providerFake := &fakeTransactionalProvider{executeErrAt: map[int]error{}, stepReceipts: map[int]*provider.TransactionReceipt{}}
	executor := newTestExecutor(t, store, providerFake, nil)
	first, err := executor.Execute(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	second, err := executor.Execute(context.Background(), plan)
	if err != nil || !first.Succeeded() || !second.Succeeded() || providerFake.prepareCalls != 1 {
		t.Fatalf("first=%+v second=%+v error=%v prepare_calls=%d", first, second, err, providerFake.prepareCalls)
	}
}

func TestResumeAllUsesStableProviderGeneration(t *testing.T) {
	store, plan := testStoreAndPlan(t, "plan-resume-all", "action-1")
	if _, err := store.CreateTransaction(plan, 1); err != nil {
		t.Fatal(err)
	}
	providerFake := &fakeTransactionalProvider{executeErrAt: map[int]error{}, stepReceipts: map[int]*provider.TransactionReceipt{}}
	outcomes, err := newTestExecutor(t, store, providerFake, nil).ResumeAll(context.Background())
	if err != nil || len(outcomes) != 1 || !outcomes[0].Succeeded() || outcomes[0].Transaction.ProviderGeneration != 1 {
		t.Fatalf("outcomes=%+v error=%v", outcomes, err)
	}
	assertProviderGenerations(t, providerFake)
}

func TestResumeApplyingWithoutProviderLedgerBecomesDegraded(t *testing.T) {
	store, plan := testStoreAndPlan(t, "plan-lost-ledger", "action-1")
	record := advanceToApplying(t, store, plan)
	providerFake := &fakeTransactionalProvider{reconcileErr: provider.ErrNotFound, executeErrAt: map[int]error{}, stepReceipts: map[int]*provider.TransactionReceipt{}}
	outcome, err := newTestExecutor(t, store, providerFake, nil).Resume(context.Background(), record.TransactionID)
	if err != nil || !outcome.Degraded() || len(providerFake.executeStepCalls) != 0 {
		t.Fatalf("outcome=%+v error=%v execute_calls=%v", outcome, err, providerFake.executeStepCalls)
	}
}

func TestResumePreparedReestablishesProviderState(t *testing.T) {
	store, plan := testStoreAndPlan(t, "plan-prepared", "action-1")
	created, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatal(err)
	}
	reserved, _, err := store.ReserveTransaction(created.TransactionID, created.Generation)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := store.AdvanceTransaction(state.TransactionAdvanceRequest{TransactionID: reserved.TransactionID, ExpectedGeneration: reserved.Generation, NextState: state.TransactionStatePrepared})
	if err != nil {
		t.Fatal(err)
	}
	providerFake := &fakeTransactionalProvider{executeErrAt: map[int]error{}, stepReceipts: map[int]*provider.TransactionReceipt{}}
	outcome, err := newTestExecutor(t, store, providerFake, nil).Resume(context.Background(), prepared.TransactionID)
	if err != nil || !outcome.Succeeded() || providerFake.prepareCalls != 1 || len(providerFake.executeStepCalls) != 1 {
		t.Fatalf("outcome=%+v error=%v prepare=%d steps=%v", outcome, err, providerFake.prepareCalls, providerFake.executeStepCalls)
	}
}

func TestResumeApplyingUnknownEffectBecomesDegradedWithoutReplay(t *testing.T) {
	store, plan := testStoreAndPlan(t, "plan-unknown-effect", "action-1")
	record := advanceToApplying(t, store, plan)
	providerFake := &fakeTransactionalProvider{executeErrAt: map[int]error{}, stepReceipts: map[int]*provider.TransactionReceipt{}, reconcileReceipt: receiptFor(plan.GetPlanId(), 1, provider.TransactionPhaseExecuting, provider.TransactionEffect{StepIndex: 0, ActionID: "action-1", Status: provider.EffectStatusUnknown})}
	outcome, err := newTestExecutor(t, store, providerFake, nil).Resume(context.Background(), record.TransactionID)
	if err != nil || !outcome.Degraded() || len(providerFake.executeStepCalls) != 0 {
		t.Fatalf("outcome=%+v error=%v execute_calls=%v", outcome, err, providerFake.executeStepCalls)
	}
}

func TestResumeApplyingRejectsReceiptForDifferentAction(t *testing.T) {
	store, plan := testStoreAndPlan(t, "plan-mismatched-receipt", "action-1")
	record := advanceToApplying(t, store, plan)
	providerFake := &fakeTransactionalProvider{executeErrAt: map[int]error{}, stepReceipts: map[int]*provider.TransactionReceipt{}, reconcileReceipt: receiptFor(plan.GetPlanId(), 1, provider.TransactionPhaseReconciled, provider.TransactionEffect{StepIndex: 0, ActionID: "other-action", IdempotencyKey: "action-1-key", Status: provider.EffectStatusApplied})}
	outcome, err := newTestExecutor(t, store, providerFake, nil).Resume(context.Background(), record.TransactionID)
	if err != nil || !outcome.Degraded() || !strings.Contains(outcome.Transaction.FailureReason, "does not match plan") || len(providerFake.executeStepCalls) != 0 {
		t.Fatalf("outcome=%+v error=%v execute_calls=%v", outcome, err, providerFake.executeStepCalls)
	}
}

func newTestExecutor(t *testing.T, store *state.Store, transactionProvider provider.TransactionExecutor, checkpoint CheckpointFunc) *Executor {
	t.Helper()
	if checkpoint == nil {
		checkpoint = func(context.Context, *state.TransactionRecord) error { return nil }
	}
	executor, err := NewExecutor(store, transactionProvider, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	return executor
}

func testStoreAndPlan(t *testing.T, planID string, actionIDs ...string) (*state.Store, *tgsrlv1.PlacementPlan) {
	t.Helper()
	now := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	resources := &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 1024, AcceleratorUnits: 0.1}
	store, err := state.NewStore(&tgsrlv1.ClusterSnapshot{SnapshotId: "snapshot-1", Revision: 1, ObservedAt: timestamppb.New(now), Devices: []*tgsrlv1.Device{{DeviceId: "device-1", Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY, Capacity: &tgsrlv1.ResourceVector{CpuMillis: 10000, MemoryBytes: 1 << 30, AcceleratorUnits: 10}, Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 10000, MemoryBytes: 1 << 30, AcceleratorUnits: 10}}}}, state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	intent := &tgsrlv1.SchedulingIntent{ExecutionId: "execution-1", StageId: "stage-1", Version: 1, SubmittedAt: timestamppb.New(now), ValidUntil: timestamppb.New(now.Add(time.Hour)), Ttl: durationpb.New(time.Hour), JobId: "job-1", RunId: "run-1", TraceId: "trace-1", UnitCount: uint32(len(actionIDs)), ResourcesPerUnit: resources, PolicyVersion: "policy-1"}
	intent.IdempotencyKey = scheduler.IntentIdempotencyKey(intent.GetExecutionId(), intent.GetStageId(), intent.GetVersion())
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	plan := &tgsrlv1.PlacementPlan{PlanId: planID, ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(), RunId: intent.GetRunId(), TraceId: intent.GetTraceId(), IntentVersion: 1, SnapshotRevision: snapshot.GetRevision()}
	for index, actionID := range actionIDs {
		unit := snapshot.GetPendingUnits()[index]
		binding := &tgsrlv1.Binding{BindingId: fmt.Sprintf("binding-%d", index), PendingUnitId: unit.GetPendingUnitId(), RuntimeUnitId: unit.GetRuntimeUnitId(), SandboxId: fmt.Sprintf("sandbox-%d", index), DeviceIds: []string{"device-1"}, Resources: proto.Clone(resources).(*tgsrlv1.ResourceVector), Generation: 1}
		plan.Bindings = append(plan.Bindings, binding)
		plan.Actions = append(plan.Actions, &tgsrlv1.Action{ActionId: actionID, PlanId: planID, ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND, Binding: binding, TargetId: unit.GetPendingUnitId(), IdempotencyKey: actionID + "-key", ExpectedSnapshotRevision: snapshot.GetRevision(), ExpectedGeneration: 1})
	}
	return store, plan
}

func advanceToApplying(t *testing.T, store *state.Store, plan *tgsrlv1.PlacementPlan) *state.TransactionRecord {
	t.Helper()
	created, err := store.CreateTransaction(plan, 1)
	if err != nil {
		t.Fatal(err)
	}
	reserved, _, err := store.ReserveTransaction(created.TransactionID, created.Generation)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := store.AdvanceTransaction(state.TransactionAdvanceRequest{TransactionID: reserved.TransactionID, ExpectedGeneration: reserved.Generation, NextState: state.TransactionStatePrepared})
	if err != nil {
		t.Fatal(err)
	}
	applying, err := store.AdvanceTransaction(state.TransactionAdvanceRequest{TransactionID: prepared.TransactionID, ExpectedGeneration: prepared.Generation, NextState: state.TransactionStateApplying})
	if err != nil {
		t.Fatal(err)
	}
	return applying
}

func receiptFor(transactionID string, generation uint64, phase provider.TransactionPhase, effects ...provider.TransactionEffect) *provider.TransactionReceipt {
	for index := range effects {
		effects[index].Generation = generation
	}
	return &provider.TransactionReceipt{TransactionID: transactionID, PlanID: transactionID, Generation: generation, Phase: phase, ObservedRevision: 11, Effects: append([]provider.TransactionEffect(nil), effects...), PreparedAt: time.Unix(100, 0).UTC(), UpdatedAt: time.Unix(101, 0).UTC()}
}

func assertEffects(t *testing.T, results []*tgsrlv1.ActionResult, statuses ...tgsrlv1.ActionResultStatus) {
	t.Helper()
	if len(results) != len(statuses) {
		t.Fatalf("result count = %d, want %d", len(results), len(statuses))
	}
	for index, status := range statuses {
		if results[index].GetStatus() != status {
			t.Fatalf("result[%d] status = %s, want %s", index, results[index].GetStatus(), status)
		}
	}
}

func assertProviderGenerations(t *testing.T, providerFake *fakeTransactionalProvider) {
	t.Helper()
	for name, values := range map[string][]uint64{"prepare": providerFake.prepareGenerations, "execute": providerFake.executeGenerations, "commit": providerFake.commitGenerations, "abort": providerFake.abortGenerations, "reconcile": providerFake.reconcileGenerations} {
		for index, value := range values {
			if value != 1 {
				t.Fatalf("%s generation[%d] = %d, want 1", name, index, value)
			}
		}
	}
}
