package state

import (
	"context"
	"errors"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scheduler"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/semantics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func setupActiveAllocationStore(
	t *testing.T,
) (*Store, *fakeClock, *tgsrlv1.SchedulingIntent, *tgsrlv1.ClusterSnapshot, *tgsrlv1.Allocation) {
	t.Helper()
	store, clock := newTestStore(t)
	intent := testIntent(clock, 1, "mutation-intent")
	intent.UnitCount = 1
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	snapshot, _ := store.GetSnapshot(context.Background(), 0, true)
	if _, err := store.MutateResources(snapshot.GetRevision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.Devices = []*tgsrlv1.Device{
			{
				DeviceId: "device-1",
				Health:   tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
				Capacity: &tgsrlv1.ResourceVector{
					CpuMillis:   2000,
					MemoryBytes: 8192,
				},
				Allocatable: &tgsrlv1.ResourceVector{
					CpuMillis:   2000,
					MemoryBytes: 8192,
				},
			},
			{
				DeviceId: "device-2",
				Health:   tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
				Capacity: &tgsrlv1.ResourceVector{
					CpuMillis:   2000,
					MemoryBytes: 8192,
				},
				Allocatable: &tgsrlv1.ResourceVector{
					CpuMillis:   2000,
					MemoryBytes: 8192,
				},
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("MutateResources() error = %v", err)
	}
	snapshot, _ = store.GetSnapshot(context.Background(), 0, true)
	pending := snapshot.GetPendingUnits()[0]
	admission := &tgsrlv1.PlacementPlan{
		PlanId:           "admission-plan",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		Purpose:          tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION,
		Bindings: []*tgsrlv1.Binding{{
			BindingId:     "bind-1",
			PendingUnitId: pending.GetPendingUnitId(),
			DeviceIds:     []string{"device-1"},
			Resources:     cloneResourceVector(intent.GetResourcesPerUnit()),
		}},
	}
	if _, err := store.ReservePlan(admission); err != nil {
		t.Fatalf("ReservePlan(admission) error = %v", err)
	}
	active, err := store.FinalizePlan(admission, true)
	if err != nil {
		t.Fatalf("FinalizePlan(admission) error = %v", err)
	}
	if len(active.GetAllocations()) != 1 {
		t.Fatalf("allocations = %+v, want one active allocation", active.GetAllocations())
	}
	return store, clock, intent, active, proto.Clone(active.GetAllocations()[0]).(*tgsrlv1.Allocation)
}

func TestReservePlanReleaseMutation(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "release-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "release-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE,
			TargetId:   allocation.GetAllocationId(),
		}},
	}
	reserved, err := store.ReservePlan(plan)
	if err != nil {
		t.Fatalf("ReservePlan(release) error = %v", err)
	}
	if reserved.GetAllocations()[0].GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_RELEASING {
		t.Fatalf("allocation state = %s, want RELEASING", reserved.GetAllocations()[0].GetState())
	}
	final, err := store.FinalizePlanResults(plan, true, []*tgsrlv1.ActionResult{{
		ActionId: "release-action",
		Status:   tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults(release) error = %v", err)
	}
	if len(final.GetAllocations()) != 0 {
		t.Fatalf("allocations after successful release = %+v, want removed", final.GetAllocations())
	}
	if got := final.GetDevices()[0].GetAllocatable().GetCpuMillis(); got != 2000 {
		t.Fatalf("allocatable CPU after release = %d, want 2000", got)
	}
}

func TestReservePlanLifecycleMutationRollbackRestoresBeforeImage(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "pause-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "pause-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
			TargetId:   allocation.GetAllocationId(),
		}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan(lifecycle) error = %v", err)
	}
	final, err := store.FinalizePlanResults(plan, false, []*tgsrlv1.ActionResult{{
		ActionId:          "pause-action",
		Status:            tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
		RollbackAttempted: true,
		RollbackStatus:    tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults(lifecycle) error = %v", err)
	}
	if len(final.GetAllocations()) != 1 || !proto.Equal(final.GetAllocations()[0], allocation) {
		t.Fatalf("allocation after lifecycle rollback = %+v, want restored before-image %+v", final.GetAllocations(), allocation)
	}
}

func TestReservePlanResizeMutationGrowAndRollback(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	targetResources := &tgsrlv1.ResourceVector{
		CpuMillis:   800,
		MemoryBytes: 1024,
	}
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "resize-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "resize-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESIZE,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId: "resize-binding",
				Resources: cloneResourceVector(targetResources),
			},
		}},
	}
	reserved, err := store.ReservePlan(plan)
	if err != nil {
		t.Fatalf("ReservePlan(resize) error = %v", err)
	}
	if got := reserved.GetAllocations()[0].GetResources().GetCpuMillis(); got != 800 {
		t.Fatalf("resized allocation CPU = %d, want 800", got)
	}
	if got := reserved.GetDevices()[0].GetAllocatable().GetCpuMillis(); got != 1200 {
		t.Fatalf("allocatable CPU after grow reserve = %d, want 1200", got)
	}
	final, err := store.FinalizePlanResults(plan, false, []*tgsrlv1.ActionResult{{
		ActionId:          "resize-action",
		Status:            tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
		RollbackAttempted: true,
		RollbackStatus:    tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults(resize rollback) error = %v", err)
	}
	if got := final.GetAllocations()[0].GetResources().GetCpuMillis(); got != allocation.GetResources().GetCpuMillis() {
		t.Fatalf("allocation CPU after rollback = %d, want %d", got, allocation.GetResources().GetCpuMillis())
	}
	if got := final.GetDevices()[0].GetAllocatable().GetCpuMillis(); got != 1500 {
		t.Fatalf("allocatable CPU after rollback = %d, want 1500", got)
	}
}

func TestReservePlanResizeMutationMixedDirectionAccountsPerDimension(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	targetResources := &tgsrlv1.ResourceVector{
		CpuMillis:   300,
		MemoryBytes: 2048,
	}
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "mixed-resize-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "mixed-resize-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESIZE,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId: "mixed-resize-binding",
				Resources: cloneResourceVector(targetResources),
			},
		}},
	}
	reserved, err := store.ReservePlan(plan)
	if err != nil {
		t.Fatalf("ReservePlan(mixed resize) error = %v", err)
	}
	if !proto.Equal(reserved.GetAllocations()[0].GetResources(), targetResources) {
		t.Fatalf("allocation resources = %+v, want %+v", reserved.GetAllocations()[0].GetResources(), targetResources)
	}
	allocatable := findDevice(reserved.GetDevices(), "device-1").GetAllocatable()
	if allocatable.GetCpuMillis() != 1700 || allocatable.GetMemoryBytes() != 6144 {
		t.Fatalf("allocatable after mixed resize = %+v, want cpu=1700 memory=6144", allocatable)
	}
	final, err := store.FinalizePlanResults(plan, false, []*tgsrlv1.ActionResult{{
		ActionId:          "mixed-resize-action",
		Status:            tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
		RollbackAttempted: true,
		RollbackStatus:    tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults(mixed resize rollback) error = %v", err)
	}
	allocatable = findDevice(final.GetDevices(), "device-1").GetAllocatable()
	if allocatable.GetCpuMillis() != 1500 || allocatable.GetMemoryBytes() != 7168 {
		t.Fatalf("allocatable after mixed resize rollback = %+v, want cpu=1500 memory=7168", allocatable)
	}
	if !proto.Equal(final.GetAllocations()[0].GetResources(), allocation.GetResources()) {
		t.Fatalf("allocation resources after mixed resize rollback = %+v, want %+v", final.GetAllocations()[0].GetResources(), allocation.GetResources())
	}
}

func TestReservePlanResizeFailedNotAppliedRestoresReservation(t *testing.T) {
	for _, status := range []tgsrlv1.ActionResultStatus{
		tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
		tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SKIPPED,
	} {
		t.Run(status.String(), func(t *testing.T) {
			store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
			plan := &tgsrlv1.PlacementPlan{
				PlanId:                "not-applied-resize-plan-" + status.String(),
				ExecutionId:           intent.GetExecutionId(),
				StageId:               intent.GetStageId(),
				IntentVersion:         intent.GetVersion(),
				SnapshotRevision:      snapshot.GetRevision(),
				Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
				AffectedAllocationIds: []string{allocation.GetAllocationId()},
				Actions: []*tgsrlv1.Action{{
					ActionId:   "not-applied-resize-action",
					ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESIZE,
					TargetId:   allocation.GetAllocationId(),
					Binding: &tgsrlv1.Binding{
						BindingId: "not-applied-resize-binding",
						Resources: &tgsrlv1.ResourceVector{
							CpuMillis:   800,
							MemoryBytes: allocation.GetResources().GetMemoryBytes(),
						},
					},
				}},
			}
			if _, err := store.ReservePlan(plan); err != nil {
				t.Fatalf("ReservePlan(resize) error = %v", err)
			}
			final, err := store.FinalizePlanResults(plan, false, []*tgsrlv1.ActionResult{{
				ActionId: "not-applied-resize-action",
				Status:   status,
			}})
			if err != nil {
				t.Fatalf("FinalizePlanResults(not applied) error = %v", err)
			}
			if len(final.GetAllocations()) != 1 || !proto.Equal(final.GetAllocations()[0], allocation) {
				t.Fatalf("allocation after not-applied result = %+v, want before-image %+v", final.GetAllocations(), allocation)
			}
			if got := findDevice(final.GetDevices(), "device-1").GetAllocatable().GetCpuMillis(); got != 1500 {
				t.Fatalf("allocatable CPU after not-applied result = %d, want 1500", got)
			}
		})
	}
}

func TestReservePlanRebindMutationSuccessReleasesSource(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "rebind-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "rebind-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_REBIND,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId: "rebind-binding",
				DeviceIds: []string{"device-2"},
				Resources: cloneResourceVector(allocation.GetResources()),
			},
		}},
	}
	reserved, err := store.ReservePlan(plan)
	if err != nil {
		t.Fatalf("ReservePlan(rebind) error = %v", err)
	}
	if len(reserved.GetAllocations()) != 2 {
		t.Fatalf("allocations after rebind reserve = %+v, want source + target", reserved.GetAllocations())
	}
	final, err := store.FinalizePlanResults(plan, true, []*tgsrlv1.ActionResult{{
		ActionId: "rebind-action",
		Status:   tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults(rebind) error = %v", err)
	}
	if len(final.GetAllocations()) != 1 {
		t.Fatalf("allocations after rebind finalize = %+v, want only target", final.GetAllocations())
	}
	if got := final.GetAllocations()[0].GetDeviceIds(); len(got) != 1 || got[0] != "device-2" {
		t.Fatalf("target allocation devices = %+v, want [device-2]", got)
	}
	if got := final.GetDevices()[0].GetAllocatable().GetCpuMillis(); got != 2000 {
		t.Fatalf("source device allocatable CPU = %d, want 2000 after source release", got)
	}
}

func TestRebindFinalizePreservesNextGenerationIdentity(t *testing.T) {
	store, clock, intent, _, _ := setupActiveAllocationStore(t)
	capabilities := provider.DefaultMockCapabilities()
	capabilities.Names = append(capabilities.Names, "sandbox")
	if _, err := store.MutateResources(store.Revision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.Allocations[0].Generation = 3
		working.Allocations[0].RuntimeUnitId = "runtime-rebind"
		for _, device := range working.GetDevices() {
			device.Capabilities = cloneCapabilitySet(capabilities)
		}
		return nil
	}); err != nil {
		t.Fatalf("MutateResources(seed production identity) error = %v", err)
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	source := snapshot.GetAllocations()[0]
	sourceBinding := &tgsrlv1.Binding{
		BindingId:     "binding-generation-3",
		PendingUnitId: source.GetPendingUnitId(),
		DeviceIds:     append([]string(nil), source.GetDeviceIds()...),
		Resources:     cloneResourceVector(source.GetResources()),
		SandboxId:     "sandbox-rebind",
		Generation:    source.GetGeneration(),
		RuntimeUnitId: source.GetRuntimeUnitId(),
	}
	observed := &tgsrlv1.Sandbox{
		SandboxId:  sourceBinding.GetSandboxId(),
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		Generation: sourceBinding.GetGeneration(),
		Binding:    proto.Clone(sourceBinding).(*tgsrlv1.Binding),
		SafePoint:  true,
		ObservedAt: timestamppb.New(clock.Now()),
	}
	planner := scheduler.NewCoordinator(scheduler.Config{})
	first := scheduler.PlanningInput{
		Snapshot: snapshot,
		Intent:   intent,
		EvaluationContext: &tgsrlv1.EvaluationContext{
			TickKind:         tgsrlv1.TickKind_TICK_KIND_SLOW,
			EvaluationTime:   timestamppb.New(clock.Now()),
			DecisionSequence: 41,
		},
		Sandboxes: []*tgsrlv1.Sandbox{observed},
		SafePoint: semantics.SafePointResolution{Present: true, Value: true},
		Directives: scheduler.Directives{
			AllowRebind: true,
			ReplacementBindings: map[string]*tgsrlv1.Binding{
				source.GetAllocationId(): {DeviceIds: []string{"device-2"}, Generation: 4},
			},
		},
		DecisionID: "rebind-generation-decision-1",
	}
	proposal, err := planner.Plan(first)
	if err != nil {
		t.Fatalf("Plan(first rebind) error = %v", err)
	}
	plan := proposal.Plan
	if plan == nil || len(plan.GetActions()) != 1 || plan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_REBIND {
		t.Fatalf("first proposal = %+v, want one rebind action", proposal)
	}
	if plan.GetActions()[0].GetExpectedGeneration() != 3 || plan.GetActions()[0].GetBinding().GetGeneration() != 4 {
		t.Fatalf("first rebind generations = expected:%d replacement:%d, want 3/4", plan.GetActions()[0].GetExpectedGeneration(), plan.GetActions()[0].GetBinding().GetGeneration())
	}
	resourceProvider, err := provider.NewMockResourceProvider(
		provider.WithNow(clock.Now),
		provider.WithCapabilities(capabilities),
		provider.WithSandboxes(provider.Sandbox{
			SandboxID: sourceBinding.GetSandboxId(), Generation: 3, State: provider.SandboxStateRunning,
			Binding: proto.Clone(sourceBinding).(*tgsrlv1.Binding), SafePoint: true, UpdatedAt: clock.Now(),
		}),
	)
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan(rebind) error = %v", err)
	}
	results, err := resourceProvider.ExecutePlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("ExecutePlan(rebind) error = %v", err)
	}
	final, err := store.FinalizePlanResults(plan, true, results)
	if err != nil {
		t.Fatalf("FinalizePlanResults(rebind) error = %v", err)
	}
	providerSandbox, err := resourceProvider.GetSandbox(context.Background(), sourceBinding.GetSandboxId())
	if err != nil {
		t.Fatalf("GetSandbox(after rebind) error = %v", err)
	}
	if len(final.GetAllocations()) != 1 {
		t.Fatalf("final allocations = %+v, want replacement only", final.GetAllocations())
	}
	replacement := final.GetAllocations()[0]
	if replacement.GetGeneration() != 4 || providerSandbox.Generation != 4 || providerSandbox.Binding.GetGeneration() != 4 {
		t.Fatalf("post-rebind generations = allocation:%d sandbox:%d binding:%d, want all 4", replacement.GetGeneration(), providerSandbox.Generation, providerSandbox.Binding.GetGeneration())
	}
	nextObserved := &tgsrlv1.Sandbox{
		SandboxId:  providerSandbox.SandboxID,
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND,
		Generation: providerSandbox.Generation,
		Binding:    proto.Clone(providerSandbox.Binding).(*tgsrlv1.Binding),
		SafePoint:  providerSandbox.SafePoint,
		ObservedAt: timestamppb.New(clock.Now()),
	}
	next := first
	next.Snapshot = final
	next.Sandboxes = []*tgsrlv1.Sandbox{nextObserved}
	next.EvaluationContext = &tgsrlv1.EvaluationContext{
		TickKind:         tgsrlv1.TickKind_TICK_KIND_SLOW,
		EvaluationTime:   timestamppb.New(clock.Now()),
		DecisionSequence: 42,
	}
	next.Directives.ReplacementBindings = map[string]*tgsrlv1.Binding{
		replacement.GetAllocationId(): {DeviceIds: []string{"device-1"}, Generation: 5},
	}
	next.DecisionID = "rebind-generation-decision-2"
	nextProposal, err := planner.Plan(next)
	if err != nil {
		t.Fatalf("Plan(next rebind) error = %v", err)
	}
	if nextProposal.Plan == nil || len(nextProposal.Plan.GetActions()) != 1 {
		t.Fatalf("next proposal = %+v, want matched rebind target", nextProposal)
	}
	nextAction := nextProposal.Plan.GetActions()[0]
	if nextAction.GetExpectedGeneration() != 4 || nextAction.GetBinding().GetGeneration() != 5 {
		t.Fatalf("next rebind generations = expected:%d replacement:%d, want 4/5", nextAction.GetExpectedGeneration(), nextAction.GetBinding().GetGeneration())
	}
}

func TestReservePlanRebindRollbackRestoresSourceAndTargetCapacity(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	targetBefore := snapshot.GetDevices()[1].GetAllocatable().GetCpuMillis()
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "rebind-rollback-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "rebind-rollback-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_REBIND,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId: "rebind-rollback-binding",
				DeviceIds: []string{"device-2"},
				Resources: cloneResourceVector(allocation.GetResources()),
			},
		}},
	}
	reserved, err := store.ReservePlan(plan)
	if err != nil {
		t.Fatalf("ReservePlan(rebind) error = %v", err)
	}
	if got := reserved.GetDevices()[1].GetAllocatable().GetCpuMillis(); got >= targetBefore {
		t.Fatalf("target allocatable CPU after reserve = %d, want less than %d", got, targetBefore)
	}
	final, err := store.FinalizePlanResults(plan, false, []*tgsrlv1.ActionResult{{
		ActionId:          "rebind-rollback-action",
		Status:            tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
		RollbackAttempted: true,
		RollbackStatus:    tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults(rebind rollback) error = %v", err)
	}
	if len(final.GetAllocations()) != 1 || !proto.Equal(final.GetAllocations()[0], allocation) {
		t.Fatalf("allocations after rollback = %+v, want source allocation %+v", final.GetAllocations(), allocation)
	}
	if got := final.GetDevices()[1].GetAllocatable().GetCpuMillis(); got != targetBefore {
		t.Fatalf("target allocatable CPU after rollback = %d, want %d", got, targetBefore)
	}
}

func TestReservePlanRebindRollbackPreservesTargetDeviceResourceUpdate(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "rebind-rollback-same-device-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "rebind-rollback-same-device-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_REBIND,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId: "rebind-rollback-same-device-binding",
				DeviceIds: []string{"device-2"},
				Resources: cloneResourceVector(allocation.GetResources()),
			},
		}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan(rebind) error = %v", err)
	}
	if _, err := store.MutateResources(store.Revision(), func(working *tgsrlv1.ClusterSnapshot) error {
		allocatable := findDevice(working.GetDevices(), "device-2").Allocatable
		allocatable.CpuMillis -= 100
		allocatable.MemoryBytes -= 256
		return nil
	}); err != nil {
		t.Fatalf("MutateResources(target device) error = %v", err)
	}
	final, err := store.FinalizePlanResults(plan, false, []*tgsrlv1.ActionResult{{
		ActionId:          "rebind-rollback-same-device-action",
		Status:            tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
		RollbackAttempted: true,
		RollbackStatus:    tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults(rebind rollback) error = %v", err)
	}
	allocatable := findDevice(final.GetDevices(), "device-2").GetAllocatable()
	if allocatable.GetCpuMillis() != 1900 || allocatable.GetMemoryBytes() != 7936 {
		t.Fatalf("target allocatable after rollback = %+v, want cpu=1900 memory=7936", allocatable)
	}
}

func TestReservePlanResizeMutationWithCurrentIntentVersion(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "resize-current-version-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "resize-current-version-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESIZE,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId: "resize-current-version-binding",
				Resources: &tgsrlv1.ResourceVector{
					CpuMillis:   800,
					MemoryBytes: allocation.GetResources().GetMemoryBytes(),
				},
			},
		}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan(current version resize) error = %v", err)
	}
}

func TestReservePlanRebindMutationWithCurrentIntentVersion(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "rebind-current-version-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "rebind-current-version-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_REBIND,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId: "rebind-current-version-binding",
				DeviceIds: []string{"device-2"},
				Resources: cloneResourceVector(allocation.GetResources()),
			},
		}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan(current version rebind) error = %v", err)
	}
}

func TestReservePlanLifecycleMutationFailureMarksRetainedFailed(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "lifecycle-failure-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "sleep-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_SLEEP,
			TargetId:   allocation.GetAllocationId(),
		}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan(lifecycle failure) error = %v", err)
	}
	final, err := store.FinalizePlanResults(plan, false, []*tgsrlv1.ActionResult{{
		ActionId:          "sleep-action",
		Status:            tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED,
		RollbackAttempted: false,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults(lifecycle failure) error = %v", err)
	}
	if len(final.GetAllocations()) != 1 || final.GetAllocations()[0].GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_FAILED {
		t.Fatalf("allocations after lifecycle failure = %+v, want retained FAILED allocation", final.GetAllocations())
	}
}

func TestMutationReservationDurableRestorePreservesBeforeSnapshotAndLocks(t *testing.T) {
	store, clock, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "restore-release-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "restore-release-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE,
			TargetId:   allocation.GetAllocationId(),
		}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan(restore) error = %v", err)
	}
	durable := store.ExportDurableState()
	var mutationRecord *ReservationRecord
	for index := range durable.Reservations {
		record := &durable.Reservations[index]
		if record.Plan != nil && record.Plan.GetPlanId() == plan.GetPlanId() {
			mutationRecord = record
			break
		}
	}
	if mutationRecord == nil || mutationRecord.BeforeSnapshot == nil {
		t.Fatalf("durable reservations = %+v, want before_snapshot persisted", durable.Reservations)
	}
	restored, err := NewStore(nil, WithClock(clock))
	if err != nil {
		t.Fatalf("NewStore(restored) error = %v", err)
	}
	if err := restored.Restore(durable); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	again := restored.ExportDurableState()
	mutationRecord = nil
	for index := range again.Reservations {
		record := &again.Reservations[index]
		if record.Plan != nil && record.Plan.GetPlanId() == plan.GetPlanId() {
			mutationRecord = record
			break
		}
	}
	if mutationRecord == nil || mutationRecord.BeforeSnapshot == nil {
		t.Fatalf("restored reservations = %+v, want before_snapshot retained", again.Reservations)
	}
	competing := proto.Clone(plan).(*tgsrlv1.PlacementPlan)
	competing.PlanId = "competing-plan"
	if _, err := restored.ReservePlan(competing); err == nil {
		t.Fatal("ReservePlan(competing) error = nil, want target lock conflict")
	}
}

func TestReservePlanMutationDoesNotRequireBindingsButLegacyAdmissionStillDoes(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	mutation := &tgsrlv1.PlacementPlan{
		PlanId:                "mutation-no-bindings",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "pause-no-bindings",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
			TargetId:   allocation.GetAllocationId(),
		}},
	}
	if _, err := store.ReservePlan(mutation); err != nil {
		t.Fatalf("ReservePlan(mutation without bindings) error = %v", err)
	}
	admission := &tgsrlv1.PlacementPlan{
		PlanId:           "admission-no-bindings",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: store.Revision(),
		Purpose:          tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION,
	}
	if _, err := store.ReservePlan(admission); err == nil {
		t.Fatal("ReservePlan(admission without bindings) error = nil, want invalid intent")
	}
}

func TestReservePlanRecreateMutationSuccessReplacesOnSameDevice(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "recreate-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "recreate-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_RECREATE,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId:     "recreate-binding",
				DeviceIds:     append([]string(nil), allocation.GetDeviceIds()...),
				Resources:     cloneResourceVector(allocation.GetResources()),
				Generation:    allocation.GetGeneration() + 1,
				RuntimeUnitId: "runtime-unit-recreated",
			},
		}},
	}
	reserved, err := store.ReservePlan(plan)
	if err != nil {
		t.Fatalf("ReservePlan(recreate) error = %v", err)
	}
	if len(reserved.GetAllocations()) != 2 {
		t.Fatalf("allocations after recreate reserve = %+v, want source + replacement", reserved.GetAllocations())
	}
	final, err := store.FinalizePlanResults(plan, true, []*tgsrlv1.ActionResult{{
		ActionId: "recreate-action",
		Status:   tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults(recreate) error = %v", err)
	}
	if len(final.GetAllocations()) != 1 {
		t.Fatalf("allocations after recreate finalize = %+v, want only replacement", final.GetAllocations())
	}
	replacement := final.GetAllocations()[0]
	if replacement.GetDeviceIds()[0] != allocation.GetDeviceIds()[0] {
		t.Fatalf("replacement devices = %+v, want same device %+v", replacement.GetDeviceIds(), allocation.GetDeviceIds())
	}
	if replacement.GetGeneration() != allocation.GetGeneration()+1 || replacement.GetRuntimeUnitId() != "runtime-unit-recreated" {
		t.Fatalf("replacement identity = generation:%d runtime_unit_id:%q", replacement.GetGeneration(), replacement.GetRuntimeUnitId())
	}
	if got := final.GetDevices()[0].GetAllocatable().GetCpuMillis(); got != 1500 {
		t.Fatalf("allocatable CPU after recreate finalize = %d, want unchanged 1500 on same-device replacement", got)
	}
}

func TestReservePlanRecreateMutationRollbackPreservesUnrelatedUpdates(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "recreate-rollback-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "recreate-rollback-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_RECREATE,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId:     "recreate-rollback-binding",
				DeviceIds:     append([]string(nil), allocation.GetDeviceIds()...),
				Resources:     cloneResourceVector(allocation.GetResources()),
				Generation:    allocation.GetGeneration() + 5,
				RuntimeUnitId: "runtime-unit-rollback",
			},
		}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan(recreate rollback) error = %v", err)
	}
	if _, err := store.MutateResources(store.Revision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.PendingUnits = append(working.PendingUnits, &tgsrlv1.PendingUnit{
			PendingUnitId: "unrelated-pending",
			ExecutionId:   "other-execution",
			StageId:       "other-stage",
			Priority:      3,
		})
		device2 := findDevice(working.GetDevices(), "device-2")
		device2.Allocatable.CpuMillis = 1337
		return nil
	}); err != nil {
		t.Fatalf("MutateResources(unrelated updates) error = %v", err)
	}
	final, err := store.FinalizePlanResults(plan, false, []*tgsrlv1.ActionResult{{
		ActionId:          "recreate-rollback-action",
		Status:            tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
		RollbackAttempted: true,
		RollbackStatus:    tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults(recreate rollback) error = %v", err)
	}
	if len(final.GetAllocations()) != 1 || !proto.Equal(final.GetAllocations()[0], allocation) {
		t.Fatalf("allocations after recreate rollback = %+v, want restored source %+v", final.GetAllocations(), allocation)
	}
	if findDevice(final.GetDevices(), "device-2").GetAllocatable().GetCpuMillis() != 1337 {
		t.Fatalf("unrelated device allocatable was overwritten: %+v", final.GetDevices())
	}
	foundPending := false
	for _, unit := range final.GetPendingUnits() {
		if unit.GetPendingUnitId() == "unrelated-pending" {
			foundPending = true
			break
		}
	}
	if !foundPending {
		t.Fatalf("pending units after rollback = %+v, want unrelated pending preserved", final.GetPendingUnits())
	}
}

func TestReservePlanResizeRollbackPreservesUnrelatedUpdates(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "resize-rollback-merge-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "resize-rollback-merge-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESIZE,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId: "resize-rollback-merge-binding",
				Resources: &tgsrlv1.ResourceVector{
					CpuMillis:   800,
					MemoryBytes: allocation.GetResources().GetMemoryBytes(),
				},
			},
		}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan(resize rollback merge) error = %v", err)
	}
	if _, err := store.MutateResources(store.Revision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.PendingUnits = append(working.PendingUnits, &tgsrlv1.PendingUnit{
			PendingUnitId: "resize-unrelated-pending",
			ExecutionId:   "other-execution",
			StageId:       "other-stage",
			Priority:      7,
		})
		device2 := findDevice(working.GetDevices(), "device-2")
		device2.Allocatable.CpuMillis = 1777
		return nil
	}); err != nil {
		t.Fatalf("MutateResources(unrelated after resize reserve) error = %v", err)
	}
	final, err := store.FinalizePlanResults(plan, false, []*tgsrlv1.ActionResult{{
		ActionId:          "resize-rollback-merge-action",
		Status:            tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
		RollbackAttempted: true,
		RollbackStatus:    tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults(resize rollback merge) error = %v", err)
	}
	if got := final.GetAllocations()[0].GetResources().GetCpuMillis(); got != allocation.GetResources().GetCpuMillis() {
		t.Fatalf("allocation CPU after rollback merge = %d, want %d", got, allocation.GetResources().GetCpuMillis())
	}
	if findDevice(final.GetDevices(), "device-2").GetAllocatable().GetCpuMillis() != 1777 {
		t.Fatalf("unrelated device update lost after resize rollback: %+v", final.GetDevices())
	}
	foundPending := false
	for _, unit := range final.GetPendingUnits() {
		if unit.GetPendingUnitId() == "resize-unrelated-pending" {
			foundPending = true
			break
		}
	}
	if !foundPending {
		t.Fatalf("pending units after resize rollback = %+v, want unrelated pending preserved", final.GetPendingUnits())
	}
}

func TestReservePlanResizeRollbackPreservesSameDeviceResourceUpdate(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "resize-rollback-same-device-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "resize-rollback-same-device-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESIZE,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId: "resize-rollback-same-device-binding",
				Resources: &tgsrlv1.ResourceVector{
					CpuMillis:   300,
					MemoryBytes: 2048,
				},
			},
		}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan(resize) error = %v", err)
	}
	// Another authoritative update consumes resources from the same device while
	// provider execution is in flight. Rollback must reverse only this resize's
	// mixed-direction delta, leaving the unrelated consumption intact.
	if _, err := store.MutateResources(store.Revision(), func(working *tgsrlv1.ClusterSnapshot) error {
		allocatable := findDevice(working.GetDevices(), "device-1").Allocatable
		allocatable.CpuMillis -= 100
		allocatable.MemoryBytes -= 256
		return nil
	}); err != nil {
		t.Fatalf("MutateResources(same device) error = %v", err)
	}
	final, err := store.FinalizePlanResults(plan, false, []*tgsrlv1.ActionResult{{
		ActionId:          "resize-rollback-same-device-action",
		Status:            tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
		RollbackAttempted: true,
		RollbackStatus:    tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK,
	}})
	if err != nil {
		t.Fatalf("FinalizePlanResults(resize rollback) error = %v", err)
	}
	if got := findDevice(final.GetDevices(), "device-1").GetAllocatable().GetCpuMillis(); got != 1400 {
		t.Fatalf("same-device allocatable CPU after rollback = %d, want 1400", got)
	}
	if got := findDevice(final.GetDevices(), "device-1").GetAllocatable().GetMemoryBytes(); got != 6912 {
		t.Fatalf("same-device allocatable memory after rollback = %d, want 6912", got)
	}
	if !proto.Equal(final.GetAllocations()[0].GetResources(), allocation.GetResources()) {
		t.Fatalf("allocation resources after rollback = %+v, want %+v", final.GetAllocations()[0].GetResources(), allocation.GetResources())
	}
}

func TestFinalizeMutationRejectsConflictingOutcome(t *testing.T) {
	store, _, intent, snapshot, allocation := setupActiveAllocationStore(t)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:                "conflicting-finalize-plan",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      snapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "conflicting-finalize-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESIZE,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId: "conflicting-finalize-binding",
				Resources: &tgsrlv1.ResourceVector{
					CpuMillis:   800,
					MemoryBytes: allocation.GetResources().GetMemoryBytes(),
				},
			},
		}},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan(resize) error = %v", err)
	}
	notApplied := []*tgsrlv1.ActionResult{{
		ActionId: "conflicting-finalize-action",
		Status:   tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
	}}
	if _, err := store.FinalizePlanResults(plan, false, notApplied); err != nil {
		t.Fatalf("FinalizePlanResults(not applied) error = %v", err)
	}
	if _, err := store.FinalizePlanResults(plan, false, notApplied); err != nil {
		t.Fatalf("FinalizePlanResults(idempotent not applied) error = %v", err)
	}
	possiblyApplied := []*tgsrlv1.ActionResult{{
		ActionId: "conflicting-finalize-action",
		Status:   tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED,
	}}
	if _, err := store.FinalizePlanResults(plan, false, possiblyApplied); !errors.Is(err, ErrPlanAlreadyReserved) {
		t.Fatalf("FinalizePlanResults(conflicting retained outcome) error = %v, want ErrPlanAlreadyReserved", err)
	}
	if _, err := store.FinalizePlanResults(plan, true, possiblyApplied); !errors.Is(err, ErrPlanAlreadyReserved) {
		t.Fatalf("FinalizePlanResults(conflicting success) error = %v, want ErrPlanAlreadyReserved", err)
	}
}
