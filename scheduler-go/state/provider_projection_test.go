package state

import (
	"context"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/internal/protocolmeta"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProviderProjectionUsesStoreClockForDualSandboxTimes(t *testing.T) {
	store, clock := newTestStore(t)
	receivedAt := clock.Now()
	sourceAt := receivedAt.Add(-time.Hour)
	apply := func(id string, revision uint64, state tgsrlv1.RuntimeState, occurredAt time.Time) *tgsrlv1.Sandbox {
		t.Helper()
		sandbox, changed, err := store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
			EventId: id, SandboxId: "dual-time", Generation: 1, ProviderRevision: revision,
			State: state, OccurredAt: timestamppb.New(occurredAt),
		})
		if err != nil || !changed {
			t.Fatalf("ApplyProviderSandboxEvent(%s) = changed:%v err:%v", id, changed, err)
		}
		return sandbox
	}

	first := apply("first", 1, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, sourceAt)
	if !first.GetObservedAt().AsTime().Equal(sourceAt) || !first.GetStateChangedAt().AsTime().Equal(receivedAt) || !first.GetLastConfirmedAt().AsTime().Equal(receivedAt) {
		t.Fatalf("first sandbox times = %+v", first)
	}
	clock.Advance(10 * time.Second)
	confirmed := apply("confirm", 2, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, sourceAt.Add(time.Second))
	if !confirmed.GetStateChangedAt().AsTime().Equal(receivedAt) || !confirmed.GetLastConfirmedAt().AsTime().Equal(clock.Now()) {
		t.Fatalf("same-state confirmation reset transition time: %+v", confirmed)
	}
	clock.Advance(10 * time.Second)
	transitioned := apply("transition", 3, tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, sourceAt.Add(2*time.Second))
	if !transitioned.GetStateChangedAt().AsTime().Equal(clock.Now()) || !transitioned.GetLastConfirmedAt().AsTime().Equal(clock.Now()) {
		t.Fatalf("transition times = %+v", transitioned)
	}
}

func TestProviderProjectionPreservesSafePointWhenMutableReadbackOmitsIt(t *testing.T) {
	store, _ := newTestStore(t)
	observedAt := time.Date(2026, time.August, 29, 9, 0, 0, 0, time.UTC)
	safePoint := true
	if _, changed, err := store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId: "runtime-observation", SandboxId: "sandbox-1", Generation: 4, ProviderRevision: 10,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, SafePoint: &safePoint, OccurredAt: timestamppb.New(observedAt),
	}); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent(runtime) = changed:%v err:%v", changed, err)
	}
	share := 0.75
	projected, changed, err := store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId: "share-readback", SandboxId: "sandbox-1", Generation: 4, ProviderRevision: 11,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, Share: &share, OccurredAt: timestamppb.New(observedAt.Add(time.Second)),
	})
	if err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent(share) = changed:%v err:%v", changed, err)
	}
	if !projected.GetSafePoint() || projected.GetShare() != share {
		t.Fatalf("mutable readback projection = %+v, want preserved safe point and share %v", projected, share)
	}
}

func TestTerminalSandboxObservationReleasesAllocationAndRetiresRun(t *testing.T) {
	store, clock, intent, allocation := terminalAllocationStore(t)
	event := terminalAllocationEvent(clock, intent, allocation, "run-stop")
	protocolmeta.SetRunRetirement(event, true)

	_, changed, err := store.ApplyProviderSandboxEvent(event)
	if err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent() = changed:%v err:%v, want true nil", changed, err)
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.GetAllocations()) != 0 || len(snapshot.GetPendingUnits()) != 0 {
		t.Fatalf("terminal run retained scheduler work: allocations=%+v pending=%+v", snapshot.GetAllocations(), snapshot.GetPendingUnits())
	}
	if got := snapshot.GetDevices()[0].GetAllocatable().GetCpuMillis(); got != snapshot.GetDevices()[0].GetCapacity().GetCpuMillis() {
		t.Fatalf("allocatable CPU after terminal observation = %d, want %d", got, snapshot.GetDevices()[0].GetCapacity().GetCpuMillis())
	}
	if _, ok := store.LatestIntent(intent.GetExecutionId(), intent.GetStageId()); ok {
		t.Fatal("terminal run retained its scheduling intent")
	}
	revision := snapshot.GetRevision()
	if _, changed, err := store.ApplyProviderSandboxEvent(proto.Clone(event).(*tgsrlv1.SandboxEvent)); err != nil || changed || store.Revision() != revision {
		t.Fatalf("terminal replay = changed:%v revision:%d err:%v, want false %d nil", changed, store.Revision(), err, revision)
	}
}

func TestTerminalRunWaitsForEveryAllocationBeforeRetirement(t *testing.T) {
	store, clock, intent, first := terminalAllocationStore(t)
	second := proto.Clone(first).(*tgsrlv1.Allocation)
	second.AllocationId = "allocation-terminal-2"
	second.PendingUnitId = "pending-terminal-2"
	second.RuntimeUnitId = "runtime-terminal-2"
	second.SandboxId = "sandbox-terminal-2"
	second.BindingId = "binding-terminal-2"
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MutateResources(snapshot.GetRevision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.Allocations = append(working.Allocations, second)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	firstEvent := terminalAllocationEvent(clock, intent, first, "run-stop-first")
	protocolmeta.SetRunRetirement(firstEvent, true)
	if _, changed, err := store.ApplyProviderSandboxEvent(firstEvent); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent(first) = changed:%v err:%v, want true nil", changed, err)
	}
	snapshot, err = store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.GetAllocations()) != 2 {
		t.Fatalf("first terminal event released live sibling allocations: %+v", snapshot.GetAllocations())
	}
	if _, ok := store.LatestIntent(intent.GetExecutionId(), intent.GetStageId()); !ok {
		t.Fatal("first terminal event retired the run before all allocations stopped")
	}

	secondEvent := terminalAllocationEvent(clock, intent, second, "run-stop-second")
	protocolmeta.SetRunRetirement(secondEvent, true)
	if _, changed, err := store.ApplyProviderSandboxEvent(secondEvent); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent(second) = changed:%v err:%v, want true nil", changed, err)
	}
	snapshot, err = store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.GetAllocations()) != 0 {
		t.Fatalf("fully terminal run retained allocations: %+v", snapshot.GetAllocations())
	}
	if _, ok := store.LatestIntent(intent.GetExecutionId(), intent.GetStageId()); ok {
		t.Fatal("fully terminal run retained its intent")
	}
}

func TestTerminalAllocationReleaseDoesNotRetireRunWithoutAuthority(t *testing.T) {
	store, clock, intent, allocation := terminalAllocationStore(t)
	event := terminalAllocationEvent(clock, intent, allocation, "scheduler-scale-in")
	protocolmeta.SetRunRetirement(event, false)

	if _, changed, err := store.ApplyProviderSandboxEvent(event); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent() = changed:%v err:%v, want true nil", changed, err)
	}
	if _, ok := store.LatestIntent(intent.GetExecutionId(), intent.GetStageId()); !ok {
		t.Fatal("single allocation release retired the complete run intent")
	}
}

func TestNewSandboxGenerationClearsTerminalRetirementDisposition(t *testing.T) {
	store, clock, intent, allocation := terminalAllocationStore(t)
	event := terminalAllocationEvent(clock, intent, allocation, "scheduler-scale-in")
	protocolmeta.SetRunRetirement(event, false)
	if _, changed, err := store.ApplyProviderSandboxEvent(event); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent(terminal) = changed:%v err:%v, want true nil", changed, err)
	}
	clock.Advance(time.Second)
	projected, changed, err := store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId: "replacement-generation", SandboxId: allocation.GetSandboxId(), RunId: intent.GetRunId(),
		Generation: allocation.GetGeneration() + 1, ProviderRevision: event.GetProviderRevision() + 1,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, OccurredAt: timestamppb.New(clock.Now()),
	})
	if err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent(replacement) = changed:%v err:%v, want true nil", changed, err)
	}
	if protocolmeta.HasRunRetirementDisposition(projected.GetSemanticContext()) {
		t.Fatalf("replacement generation inherited terminal disposition: %+v", projected.GetSemanticContext())
	}
}

func TestReconcileProviderSandboxesRepairsLegacyExpiredTerminalAllocation(t *testing.T) {
	source, clock, intent, allocation := terminalAllocationStore(t)
	clock.Advance(6 * time.Minute)
	durable := source.ExportDurableState()
	durable.ProjectedSandboxes = []*tgsrlv1.Sandbox{{
		SandboxId: allocation.GetSandboxId(), RunId: intent.GetRunId(), Generation: allocation.GetGeneration(),
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED, Binding: &tgsrlv1.Binding{
			BindingId: allocation.GetBindingId(), SandboxId: allocation.GetSandboxId(), Generation: allocation.GetGeneration(),
		}, ObservedAt: timestamppb.New(clock.Now()),
	}}
	store, err := NewStore(nil, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Restore(durable); err != nil {
		t.Fatal(err)
	}
	if !store.ReconcileProviderSandboxes(nil) {
		t.Fatal("ReconcileProviderSandboxes() = false, want legacy terminal repair")
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.GetAllocations()) != 0 {
		t.Fatalf("legacy terminal allocation remains: %+v", snapshot.GetAllocations())
	}
	if _, ok := store.LatestIntent(intent.GetExecutionId(), intent.GetStageId()); ok {
		t.Fatal("legacy terminal run retained its scheduling intent")
	}
	if len(snapshot.GetPendingUnits()) != 0 {
		t.Fatalf("expired legacy terminal allocation was requeued: %+v", snapshot.GetPendingUnits())
	}
}

func TestReconcileProviderSandboxesKeepsUnexpiredMissingSandboxFailClosed(t *testing.T) {
	store, _, intent, allocation := terminalAllocationStore(t)
	if store.ReconcileProviderSandboxes(nil) {
		t.Fatal("ReconcileProviderSandboxes() changed unexpired allocation")
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.GetAllocations()) != 1 || snapshot.GetAllocations()[0].GetAllocationId() != allocation.GetAllocationId() {
		t.Fatalf("unexpired allocation was not preserved: %+v", snapshot.GetAllocations())
	}
	if _, ok := store.LatestIntent(intent.GetExecutionId(), intent.GetStageId()); !ok {
		t.Fatal("unexpired intent was not preserved")
	}
	if len(snapshot.GetPendingUnits()) != 0 {
		t.Fatalf("unexpired allocation was requeued: %+v", snapshot.GetPendingUnits())
	}
}

func TestReconcileProviderSandboxesPreservesDurableSchedulerReleaseDisposition(t *testing.T) {
	source, clock, intent, allocation := terminalAllocationStore(t)
	event := terminalAllocationEvent(clock, intent, allocation, "scheduler-scale-in")
	protocolmeta.SetRunRetirement(event, false)
	if _, changed, err := source.ApplyProviderSandboxEvent(event); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent() = changed:%v err:%v, want true nil", changed, err)
	}
	store, err := NewStore(nil, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Restore(source.ExportDurableState()); err != nil {
		t.Fatal(err)
	}
	live := proto.Clone(source.ExportDurableState().ProjectedSandboxes[0]).(*tgsrlv1.Sandbox)
	live.SemanticContext = nil
	if !store.ReconcileProviderSandboxes([]*tgsrlv1.Sandbox{live}) {
		t.Fatal("ReconcileProviderSandboxes() = false, want terminal allocation repair")
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.GetAllocations()) != 0 {
		t.Fatalf("terminal Scheduler release retained allocation: %+v", snapshot.GetAllocations())
	}
	if len(snapshot.GetPendingUnits()) != 0 {
		t.Fatalf("terminal Scheduler release was requeued: %+v", snapshot.GetPendingUnits())
	}
	if _, ok := store.LatestIntent(intent.GetExecutionId(), intent.GetStageId()); !ok {
		t.Fatal("durable Scheduler release disposition retired the complete run")
	}
}

func TestReconcileProviderSandboxesDoesNotRequeuePartiallyObservedRunRetirement(t *testing.T) {
	source, clock, intent, first := terminalAllocationStore(t)
	second := proto.Clone(first).(*tgsrlv1.Allocation)
	second.AllocationId = "allocation-terminal-2"
	second.PendingUnitId = "pending-terminal-2"
	second.RuntimeUnitId = "runtime-terminal-2"
	second.SandboxId = "sandbox-terminal-2"
	second.BindingId = "binding-terminal-2"
	snapshot, err := source.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.MutateResources(snapshot.GetRevision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.Allocations = append(working.Allocations, second)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	firstEvent := terminalAllocationEvent(clock, intent, first, "run-stop-first")
	protocolmeta.SetRunRetirement(firstEvent, true)
	if _, changed, err := source.ApplyProviderSandboxEvent(firstEvent); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent() = changed:%v err:%v, want true nil", changed, err)
	}
	store, err := NewStore(nil, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Restore(source.ExportDurableState()); err != nil {
		t.Fatal(err)
	}
	liveFirst := proto.Clone(source.ExportDurableState().ProjectedSandboxes[0]).(*tgsrlv1.Sandbox)
	liveFirst.SemanticContext = nil
	liveSecond := &tgsrlv1.Sandbox{
		SandboxId: second.GetSandboxId(), RunId: intent.GetRunId(), Generation: second.GetGeneration(),
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, ObservedAt: timestamppb.New(clock.Now()),
	}
	if store.ReconcileProviderSandboxes([]*tgsrlv1.Sandbox{liveFirst, liveSecond}) {
		t.Fatal("ReconcileProviderSandboxes() changed state before whole-run retirement converged")
	}
	snapshot, err = store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.GetAllocations()) != 2 {
		t.Fatalf("partial retirement released allocations before every sibling stopped: %+v", snapshot.GetAllocations())
	}
	if len(snapshot.GetPendingUnits()) != 0 {
		t.Fatalf("partial whole-run retirement requeued stopped allocation: %+v", snapshot.GetPendingUnits())
	}
	if _, ok := store.LatestIntent(intent.GetExecutionId(), intent.GetStageId()); !ok {
		t.Fatal("partial retirement removed intent before live sibling stopped")
	}
}

func terminalAllocationEvent(clock Clock, intent *tgsrlv1.SchedulingIntent, allocation *tgsrlv1.Allocation, id string) *tgsrlv1.SandboxEvent {
	return &tgsrlv1.SandboxEvent{
		EventId: id, SandboxId: allocation.GetSandboxId(), RunId: intent.GetRunId(),
		Generation: allocation.GetGeneration(), ProviderRevision: 10,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED, Binding: &tgsrlv1.Binding{
			BindingId: allocation.GetBindingId(), SandboxId: allocation.GetSandboxId(), Generation: allocation.GetGeneration(),
		}, OccurredAt: timestamppb.New(clock.Now()),
	}
}

func terminalAllocationStore(t *testing.T) (*Store, *fakeClock, *tgsrlv1.SchedulingIntent, *tgsrlv1.Allocation) {
	t.Helper()
	store, clock := newTestStore(t)
	intent := testIntent(clock, 1, "terminal-intent")
	intent.RunId = "run-terminal"
	intent.TraceId = "trace-terminal"
	intent.UnitCount = 1
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := store.GetSnapshot(context.Background(), 0, true)
	if _, err := store.MutateResources(snapshot.GetRevision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.Devices = []*tgsrlv1.Device{{
			DeviceId: "device-terminal", Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096}, Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4096},
		}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = store.GetSnapshot(context.Background(), 0, true)
	binding := &tgsrlv1.Binding{
		BindingId: "binding-terminal", PendingUnitId: snapshot.GetPendingUnits()[0].GetPendingUnitId(),
		RuntimeUnitId: "runtime-terminal", SandboxId: "sandbox-terminal", Generation: 1,
		DeviceIds: []string{"device-terminal"}, Resources: cloneResourceVector(intent.GetResourcesPerUnit()),
	}
	plan := &tgsrlv1.PlacementPlan{
		PlanId: "plan-terminal", DecisionId: "decision-terminal", ExecutionId: intent.GetExecutionId(),
		StageId: intent.GetStageId(), IntentVersion: intent.GetVersion(), SnapshotRevision: snapshot.GetRevision(),
		RunId: intent.GetRunId(), TraceId: intent.GetTraceId(), ExpiresAt: cloneTimestamp(intent.GetValidUntil()), Bindings: []*tgsrlv1.Binding{binding},
	}
	if _, err := store.ReservePlan(plan); err != nil {
		t.Fatal(err)
	}
	active, err := store.FinalizePlan(plan, true)
	if err != nil {
		t.Fatal(err)
	}
	return store, clock, intent, proto.Clone(active.GetAllocations()[0]).(*tgsrlv1.Allocation)
}

func TestBootstrapProviderSnapshotDeepClonesComponentVersions(t *testing.T) {
	store, _ := newTestStore(t)
	observedAt := time.Date(2026, time.August, 29, 8, 30, 0, 0, time.UTC)
	capabilities := projectedDriverCapabilities(observedAt, 8)
	providerSnapshot := &tgsrlv1.ClusterSnapshot{
		Revision:   8,
		SnapshotId: "provider-8",
		Devices: []*tgsrlv1.Device{{
			DeviceId:     "gpu-0",
			Kind:         tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
			Capabilities: capabilities,
		}},
	}
	wantComponent := proto.Clone(capabilities.GetComponentVersions()[0]).(*tgsrlv1.ComponentVersion)

	projected, changed, err := store.BootstrapProviderSnapshot("nvidia", providerSnapshot)
	if err != nil || !changed {
		t.Fatalf("BootstrapProviderSnapshot() = changed:%v err:%v, want changed true nil error", changed, err)
	}
	assertProjectedComponentVersion(t, projected.GetDevices()[0].GetCapabilities(), wantComponent)

	providerSnapshot.Devices[0].Capabilities.ComponentVersions[0].Version = "mutated-bootstrap-input"
	projected.Devices[0].Capabilities.ComponentVersions[0].Attributes["query_field"] = "mutated-result"
	stored, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	assertProjectedComponentVersion(t, stored.GetDevices()[0].GetCapabilities(), wantComponent)
}

func TestApplyProviderCapabilityEventDeepClonesComponentVersions(t *testing.T) {
	store, _ := newTestStore(t)
	if _, changed, err := store.BootstrapProviderSnapshot("nvidia", &tgsrlv1.ClusterSnapshot{
		Revision:   8,
		SnapshotId: "provider-8",
		Devices: []*tgsrlv1.Device{
			{DeviceId: "gpu-0", Kind: tgsrlv1.DeviceKind_DEVICE_KIND_GPU},
			{DeviceId: "gpu-1", Kind: tgsrlv1.DeviceKind_DEVICE_KIND_GPU},
		},
	}); err != nil || !changed {
		t.Fatalf("BootstrapProviderSnapshot() = changed:%v err:%v, want changed true nil error", changed, err)
	}

	observedAt := time.Date(2026, time.August, 29, 8, 30, 0, 0, time.UTC)
	capabilities := projectedDriverCapabilities(observedAt, 9)
	wantComponent := proto.Clone(capabilities.GetComponentVersions()[0]).(*tgsrlv1.ComponentVersion)
	projected, changed, err := store.ApplyProviderResourceEvent(&tgsrlv1.ResourceEvent{
		EventId:          "capability-9",
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_CAPABILITY_REFRESHED,
		ProviderRevision: 9,
		Provider:         "nvidia",
		Capabilities:     capabilities,
	})
	if err != nil || !changed {
		t.Fatalf("ApplyProviderResourceEvent() = changed:%v err:%v, want changed true nil error", changed, err)
	}
	if len(projected.GetDevices()) != 2 {
		t.Fatalf("projected devices = %d, want 2", len(projected.GetDevices()))
	}
	assertProjectedComponentVersion(t, projected.GetDevices()[0].GetCapabilities(), wantComponent)
	assertProjectedComponentVersion(t, projected.GetDevices()[1].GetCapabilities(), wantComponent)

	capabilities.ComponentVersions[0].Version = "mutated-event-input"
	capabilities.ComponentVersions[0].Attributes["query_field"] = "mutated-event-input"
	projected.Devices[0].Capabilities.ComponentVersions[0].Version = "mutated-result"
	projected.Devices[0].Capabilities.ComponentVersions[0].Attributes["query_field"] = "mutated-result"
	assertProjectedComponentVersion(t, projected.GetDevices()[1].GetCapabilities(), wantComponent)

	stored, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	for index, device := range stored.GetDevices() {
		assertProjectedComponentVersion(t, device.GetCapabilities(), wantComponent)
		if device.GetCapabilities() == capabilities {
			t.Fatalf("stored device %d aliases event capabilities", index)
		}
	}
}

func TestApplyProviderResourceEventRejectsOlderRevisionAndExactReplay(t *testing.T) {
	store, _ := newTestStore(t)
	first := &tgsrlv1.ResourceEvent{
		EventId: "resource-9", EventType: tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED,
		Provider: "mock", ProviderRevision: 9, Device: &tgsrlv1.Device{DeviceId: "device-9"},
	}
	if _, changed, err := store.ApplyProviderResourceEvent(first); err != nil || !changed {
		t.Fatalf("ApplyProviderResourceEvent(first) = changed:%v err:%v, want true nil", changed, err)
	}
	for _, event := range []*tgsrlv1.ResourceEvent{
		first,
		{EventId: "resource-8", EventType: tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED, Provider: "mock", ProviderRevision: 8, Device: &tgsrlv1.Device{DeviceId: "older"}},
	} {
		projected, changed, err := store.ApplyProviderResourceEvent(event)
		if err != nil || changed {
			t.Fatalf("ApplyProviderResourceEvent(%q) = changed:%v err:%v, want false nil", event.GetEventId(), changed, err)
		}
		if len(projected.GetDevices()) != 1 || projected.GetDevices()[0].GetDeviceId() != "device-9" {
			t.Fatalf("projected devices after %q = %+v, want only device-9", event.GetEventId(), projected.GetDevices())
		}
	}
	projected, changed, err := store.ApplyProviderResourceEvent(&tgsrlv1.ResourceEvent{
		EventId: "resource-9-other", EventType: tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED,
		Provider: "mock", ProviderRevision: 9, Device: &tgsrlv1.Device{DeviceId: "same-revision"},
	})
	if err != nil || !changed || len(projected.GetDevices()) != 2 {
		t.Fatalf("ApplyProviderResourceEvent(same revision distinct event) = devices:%+v changed:%v err:%v, want two devices true nil", projected.GetDevices(), changed, err)
	}
}

func TestApplyProviderSandboxEventFencesSameGenerationRevisionAndRetries(t *testing.T) {
	store, _ := newTestStore(t)
	observedAt := time.Date(2026, time.August, 29, 9, 0, 0, 0, time.UTC)
	initial := &tgsrlv1.SandboxEvent{
		EventId:          "sandbox-revision-10",
		IdempotencyKey:   "sandbox-key-10",
		SandboxId:        "sandbox-1",
		Generation:       4,
		ProviderRevision: 10,
		State:            tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		OccurredAt:       timestamppb.New(observedAt),
	}
	if _, changed, err := store.ApplyProviderSandboxEvent(initial); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent(initial) = changed:%v err:%v, want true nil", changed, err)
	}

	tests := []struct {
		name  string
		event *tgsrlv1.SandboxEvent
	}{
		{
			name: "lower revision despite newer timestamp",
			event: &tgsrlv1.SandboxEvent{
				EventId: "sandbox-revision-9", IdempotencyKey: "sandbox-key-9", SandboxId: "sandbox-1", Generation: 4, ProviderRevision: 9,
				State: tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED, OccurredAt: timestamppb.New(observedAt.Add(3 * time.Second)),
			},
		},
		{
			name: "equal revision with older occurrence",
			event: &tgsrlv1.SandboxEvent{
				EventId: "sandbox-revision-10-other", IdempotencyKey: "sandbox-key-10-other", SandboxId: "sandbox-1", Generation: 4, ProviderRevision: 10,
				State: tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED, OccurredAt: timestamppb.New(observedAt.Add(-time.Second)),
			},
		},
		{
			name: "reused event id at higher revision",
			event: &tgsrlv1.SandboxEvent{
				EventId: "sandbox-revision-10", IdempotencyKey: "sandbox-key-11", SandboxId: "sandbox-1", Generation: 4, ProviderRevision: 11,
				State: tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, OccurredAt: timestamppb.New(observedAt.Add(5 * time.Second)),
			},
		},
		{
			name: "same idempotency key with distinct event id is ordered by revision",
			event: &tgsrlv1.SandboxEvent{
				IdempotencyKey: "sandbox-key-10", SandboxId: "sandbox-1", Generation: 4, ProviderRevision: 10,
				State: tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, OccurredAt: timestamppb.New(observedAt),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projected, changed, err := store.ApplyProviderSandboxEvent(test.event)
			if err != nil || changed {
				t.Fatalf("ApplyProviderSandboxEvent() = changed:%v err:%v, want false nil", changed, err)
			}
			if projected.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
				t.Fatalf("projected state = %s, want RUNNING", projected.GetState())
			}
		})
	}

	projected, changed, err := store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId: "sandbox-revision-10-next", IdempotencyKey: "sandbox-key-10", SandboxId: "sandbox-1", Generation: 4, ProviderRevision: 10,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, OccurredAt: timestamppb.New(observedAt.Add(time.Second)),
	})
	if err != nil || !changed || projected.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED {
		t.Fatalf("ApplyProviderSandboxEvent(newer occurrence) = sandbox:%+v changed:%v err:%v, want paused true nil", projected, changed, err)
	}
	projected, changed, err = store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId: "sandbox-revision-11-next", IdempotencyKey: "sandbox-key-11-next", SandboxId: "sandbox-1", Generation: 4, ProviderRevision: 11,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING, OccurredAt: timestamppb.New(observedAt.Add(-time.Second)),
	})
	if err != nil || !changed || projected.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING {
		t.Fatalf("ApplyProviderSandboxEvent(new revision) = sandbox:%+v changed:%v err:%v, want sleeping true nil", projected, changed, err)
	}
	projected, changed, err = store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId: "sandbox-revision-10-next", IdempotencyKey: "sandbox-key-replayed-with-new-id", SandboxId: "sandbox-1", Generation: 4, ProviderRevision: 12,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED, OccurredAt: timestamppb.New(observedAt.Add(3 * time.Second)),
	})
	if err != nil || changed || projected.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING {
		t.Fatalf("ApplyProviderSandboxEvent(non-adjacent replay) = sandbox:%+v changed:%v err:%v, want sleeping false nil", projected, changed, err)
	}

	projected, changed, err = store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId: "sandbox-generation-5", IdempotencyKey: "sandbox-key-5", SandboxId: "sandbox-1", Generation: 5, ProviderRevision: 1,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_REQUESTED, OccurredAt: timestamppb.New(observedAt.Add(-time.Hour)),
	})
	if err != nil || !changed || projected.GetGeneration() != 5 {
		t.Fatalf("ApplyProviderSandboxEvent(new generation) = sandbox:%+v changed:%v err:%v, want generation 5 true nil", projected, changed, err)
	}
}

func TestApplyProviderSandboxEventLegacyCompatibilityFence(t *testing.T) {
	store, _ := newTestStore(t)
	observedAt := time.Date(2026, time.August, 29, 9, 30, 0, 0, time.UTC)
	if _, changed, err := store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId: "modern-running", SandboxId: "sandbox-legacy", Generation: 2, ProviderRevision: 20,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, OccurredAt: timestamppb.New(observedAt),
	}); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent(modern) = changed:%v err:%v", changed, err)
	}

	legacyCases := []struct {
		name       string
		event      *tgsrlv1.SandboxEvent
		wantChange bool
		wantState  tgsrlv1.RuntimeState
	}{
		{
			name: "missing occurred_at",
			event: &tgsrlv1.SandboxEvent{EventId: "legacy-undated", SandboxId: "sandbox-legacy", Generation: 2,
				State: tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED},
			wantState: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		},
		{
			name: "older occurred_at",
			event: &tgsrlv1.SandboxEvent{EventId: "legacy-old", SandboxId: "sandbox-legacy", Generation: 2,
				State: tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED, OccurredAt: timestamppb.New(observedAt.Add(-time.Second))},
			wantState: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		},
		{
			name: "newer timestamp but regressing state",
			event: &tgsrlv1.SandboxEvent{EventId: "legacy-regression", SandboxId: "sandbox-legacy", Generation: 2,
				State: tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND, OccurredAt: timestamppb.New(observedAt.Add(time.Second))},
			wantState: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		},
		{
			name: "newer non-regressing state",
			event: &tgsrlv1.SandboxEvent{EventId: "legacy-paused", SandboxId: "sandbox-legacy", Generation: 2,
				State: tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, OccurredAt: timestamppb.New(observedAt.Add(2 * time.Second))},
			wantChange: true,
			wantState:  tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED,
		},
	}
	for _, test := range legacyCases {
		t.Run(test.name, func(t *testing.T) {
			projected, changed, err := store.ApplyProviderSandboxEvent(test.event)
			if err != nil || changed != test.wantChange {
				t.Fatalf("ApplyProviderSandboxEvent() = changed:%v err:%v, want %v nil", changed, err, test.wantChange)
			}
			if projected.GetState() != test.wantState {
				t.Fatalf("projected state = %s, want %s", projected.GetState(), test.wantState)
			}
		})
	}
}

func TestApplyProviderSandboxEventRejectsReusedIdentityAcrossGenerations(t *testing.T) {
	store, _ := newTestStore(t)
	observedAt := time.Date(2026, time.August, 29, 9, 45, 0, 0, time.UTC)
	if _, changed, err := store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId: "event-shared", IdempotencyKey: "key-1", SandboxId: "sandbox-identity", Generation: 1, ProviderRevision: 10,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, OccurredAt: timestamppb.New(observedAt),
	}); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent(initial) = changed:%v err:%v", changed, err)
	}
	projected, changed, err := store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId: "event-shared", IdempotencyKey: "key-2", SandboxId: "sandbox-identity", Generation: 2, ProviderRevision: 1,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_REQUESTED, OccurredAt: timestamppb.New(observedAt.Add(time.Second)),
	})
	if err != nil || changed || projected.GetGeneration() != 1 {
		t.Fatalf("ApplyProviderSandboxEvent(reused event id) = sandbox:%+v changed:%v err:%v, want generation 1 false nil", projected, changed, err)
	}
	projected, changed, err = store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		IdempotencyKey: "key-1", SandboxId: "sandbox-identity", Generation: 2, ProviderRevision: 1,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_REQUESTED, OccurredAt: timestamppb.New(observedAt.Add(2 * time.Second)),
	})
	if err != nil || changed || projected.GetGeneration() != 1 {
		t.Fatalf("ApplyProviderSandboxEvent(reused idempotency key) = sandbox:%+v changed:%v err:%v, want generation 1 false nil", projected, changed, err)
	}
}

func TestProviderProjectionRestoreFencesReplayAndDefinesBootstrapRefresh(t *testing.T) {
	store, clock := newTestStore(t)
	observedAt := time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC)
	resourceEvent := &tgsrlv1.ResourceEvent{
		EventId: "resource-12", EventType: tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED, Provider: "mock", ProviderRevision: 12,
		Device: &tgsrlv1.Device{DeviceId: "restored-device", Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY},
	}
	if _, changed, err := store.ApplyProviderResourceEvent(resourceEvent); err != nil || !changed {
		t.Fatalf("ApplyProviderResourceEvent() = changed:%v err:%v", changed, err)
	}
	sandboxEvent := &tgsrlv1.SandboxEvent{
		EventId: "sandbox-12", IdempotencyKey: "sandbox-key-12", SandboxId: "sandbox-restart", Generation: 3, ProviderRevision: 12,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, OccurredAt: timestamppb.New(observedAt),
	}
	if _, changed, err := store.ApplyProviderSandboxEvent(sandboxEvent); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent() = changed:%v err:%v", changed, err)
	}

	restored, err := NewStore(nil, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(store.ExportDurableState()); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if _, changed, err := restored.ApplyProviderResourceEvent(resourceEvent); err != nil || changed {
		t.Fatalf("ApplyProviderResourceEvent(replay) = changed:%v err:%v, want false nil", changed, err)
	}
	if _, changed, err := restored.ApplyProviderSandboxEvent(sandboxEvent); err != nil || changed {
		t.Fatalf("ApplyProviderSandboxEvent(replay) = changed:%v err:%v, want false nil", changed, err)
	}

	changed, err := restored.ReplaceProjectedSandboxes([]*tgsrlv1.Sandbox{{
		SandboxId: "sandbox-restart", Generation: 3, State: tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED,
		ObservedAt: timestamppb.New(observedAt),
	}})
	if err != nil || changed {
		t.Fatalf("ReplaceProjectedSandboxes(same timestamp) = changed:%v err:%v, want false nil", changed, err)
	}
	if got := restored.ListProjectedSandboxes()[0].GetState(); got != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("same-timestamp bootstrap state = %s, want RUNNING", got)
	}

	changed, err = restored.ReplaceProjectedSandboxes([]*tgsrlv1.Sandbox{{
		SandboxId: "sandbox-restart", Generation: 3, State: tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED,
		ObservedAt: timestamppb.New(observedAt.Add(time.Second)),
	}})
	if err != nil || !changed {
		t.Fatalf("ReplaceProjectedSandboxes(newer authoritative list) = changed:%v err:%v, want true nil", changed, err)
	}
	if got := restored.ListProjectedSandboxes()[0].GetState(); got != tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED {
		t.Fatalf("newer bootstrap state = %s, want PAUSED", got)
	}
	changed, err = restored.ReplaceProjectedSandboxes(nil)
	if err != nil || !changed {
		t.Fatalf("ReplaceProjectedSandboxes(remove all) = changed:%v err:%v, want true nil", changed, err)
	}
	projected, changed, err := restored.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId: "sandbox-delayed", SandboxId: "sandbox-restart", Generation: 3,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED, OccurredAt: timestamppb.New(observedAt),
	})
	if err != nil || changed || projected != nil {
		t.Fatalf("ApplyProviderSandboxEvent(delayed after removal) = sandbox:%+v changed:%v err:%v, want nil false nil", projected, changed, err)
	}
}

func TestExportDurableStateDeepClonesProviderProjection(t *testing.T) {
	store, _ := newTestStore(t)
	occurredAt := time.Date(2026, time.August, 29, 10, 15, 0, 0, time.UTC)
	share := 0.5
	if _, changed, err := store.ApplyProviderSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId: "sandbox-clone", SandboxId: "sandbox-clone", Generation: 2, ProviderRevision: 8,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, Share: &share, OccurredAt: timestamppb.New(occurredAt),
	}); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent() = changed:%v err:%v", changed, err)
	}

	durable := store.ExportDurableState()
	durable.ProjectedSandboxes[0].State = tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED
	cursor := durable.SandboxCursors["sandbox-clone"]
	cursor.OccurredAt.Seconds++
	durable.SandboxCursors["sandbox-clone"] = cursor

	again := store.ExportDurableState()
	if got := again.ProjectedSandboxes[0].GetState(); got != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("stored projection state = %s, want RUNNING", got)
	}
	if got := again.SandboxCursors["sandbox-clone"].OccurredAt.AsTime(); !got.Equal(occurredAt) {
		t.Fatalf("stored cursor occurred_at = %s, want %s", got, occurredAt)
	}
}

func projectedDriverCapabilities(observedAt time.Time, revision uint64) *tgsrlv1.CapabilitySet {
	return &tgsrlv1.CapabilitySet{
		Source:     "nvidia",
		Revision:   revision,
		MeasuredAt: timestamppb.New(observedAt),
		ComponentVersions: []*tgsrlv1.ComponentVersion{{
			Kind:       tgsrlv1.ComponentKind_COMPONENT_KIND_CUDA_DRIVER,
			Name:       "nvidia-driver",
			Version:    "550.54.14",
			ObservedAt: timestamppb.New(observedAt),
			Source:     "nvidia-smi",
			Revision:   revision,
			Attributes: map[string]string{
				"query_field": "driver_version",
				"scope":       "nvidia-kernel-driver",
			},
		}},
	}
}

func assertProjectedComponentVersion(t *testing.T, capabilities *tgsrlv1.CapabilitySet, want *tgsrlv1.ComponentVersion) {
	t.Helper()
	if capabilities == nil || len(capabilities.GetComponentVersions()) != 1 {
		t.Fatalf("capabilities = %+v, want one component version", capabilities)
	}
	if got := capabilities.GetComponentVersions()[0]; !proto.Equal(got, want) {
		t.Fatalf("component version = %+v, want %+v", got, want)
	}
}
