package provider_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider/nvidia"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var complianceNow = time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)

type nvidiaTestDriver struct {
	devices      []*tgsrlv1.Device
	capabilities *tgsrlv1.CapabilitySet
}

func newNVIDIATestDriver(devices []*tgsrlv1.Device, capabilities *tgsrlv1.CapabilitySet) *nvidiaTestDriver {
	if capabilities == nil {
		capabilities = &tgsrlv1.CapabilitySet{
			Names: []string{nvidia.CapabilityName}, Source: nvidia.ProviderID, Revision: 1,
			SupportedActions: []string{"bind", "release", "pause", "resume", "set_share", "set_priority", "sleep", "offload", "resize", "rebind", "recreate"},
		}
	}
	return &nvidiaTestDriver{devices: cloneDevices(devices), capabilities: proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)}
}

func (*nvidiaTestDriver) ID() string { return "fake" }
func (d *nvidiaTestDriver) Probe(context.Context) (*nvidia.ProbeResult, error) {
	return &nvidia.ProbeResult{Available: true, Devices: cloneDevices(d.devices), Capabilities: proto.Clone(d.capabilities).(*tgsrlv1.CapabilitySet)}, nil
}
func (*nvidiaTestDriver) ExecuteAction(_ context.Context, _ *nvidia.DriverState, _ *tgsrlv1.Action) (*nvidia.ActionExecution, error) {
	return &nvidia.ActionExecution{Detail: "fake action applied"}, nil
}
func (d *nvidiaTestDriver) Reconcile(context.Context, *nvidia.DriverState, *tgsrlv1.PlacementPlan) (*nvidia.ReconcileState, error) {
	return &nvidia.ReconcileState{Healthy: true, Devices: cloneDevices(d.devices), Capabilities: proto.Clone(d.capabilities).(*tgsrlv1.CapabilitySet)}, nil
}

func cloneDevices(devices []*tgsrlv1.Device) []*tgsrlv1.Device {
	result := make([]*tgsrlv1.Device, len(devices))
	for index, device := range devices {
		if device != nil {
			result[index] = proto.Clone(device).(*tgsrlv1.Device)
		}
	}
	return result
}

type completeFactory struct {
	name        string
	newProvider func(t *testing.T) provider.CompleteResourceProvider
	wantHealthy bool
	wantSource  string
}

func TestCompleteProvidersCompliance(t *testing.T) {
	factories := []completeFactory{
		{
			name: "mock",
			newProvider: func(t *testing.T) provider.CompleteResourceProvider {
				t.Helper()
				p, err := provider.NewMockResourceProvider(
					provider.WithNow(func() time.Time { return complianceNow }),
					provider.WithEventRetention(4),
					provider.WithSandboxes(provider.Sandbox{
						SandboxID:  "sandbox-a",
						State:      provider.SandboxStateRunning,
						Generation: 1,
						Binding:    complianceBinding("sandbox-a", 1),
						SafePoint:  true,
						UpdatedAt:  complianceNow,
					}),
				)
				if err != nil {
					t.Fatalf("NewMockResourceProvider() error = %v", err)
				}
				return p
			},
			wantHealthy: true,
			wantSource:  "mock",
		},
		{
			name: "nvidia fake",
			newProvider: func(t *testing.T) provider.CompleteResourceProvider {
				t.Helper()
				p, err := nvidia.New(
					nvidia.WithNow(func() time.Time { return complianceNow }),
					nvidia.WithEventRetention(4),
					nvidia.WithDriver(newNVIDIATestDriver([]*tgsrlv1.Device{{
						DeviceId:    "nvidia-0",
						Kind:        tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
						Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
						Capacity:    &tgsrlv1.ResourceVector{AcceleratorUnits: 1, MemoryBytes: 80 << 30},
						Allocatable: &tgsrlv1.ResourceVector{AcceleratorUnits: 1, MemoryBytes: 80 << 30},
						Capabilities: &tgsrlv1.CapabilitySet{
							Names:            []string{nvidia.CapabilityName},
							Source:           nvidia.ProviderID,
							Revision:         1,
							SupportedActions: []string{"bind", "release", "pause", "resume", "set_share", "set_priority", "sleep", "offload", "resize", "rebind", "recreate"},
						},
						Labels: map[string]string{"provider": nvidia.ProviderID},
					}}, nil)),
				)
				if err != nil {
					t.Fatalf("nvidia.New() error = %v", err)
				}
				return p
			},
			wantHealthy: true,
			wantSource:  nvidia.ProviderID,
		},
		{
			name: "nvidia unavailable",
			newProvider: func(t *testing.T) provider.CompleteResourceProvider {
				t.Helper()
				p, err := nvidia.New(
					nvidia.WithNow(func() time.Time { return complianceNow }),
					nvidia.WithDriver(nvidia.NewUnavailableDriver("no GPU present")),
				)
				if err != nil {
					t.Fatalf("nvidia.New(unavailable) error = %v", err)
				}
				return p
			},
			wantHealthy: false,
			wantSource:  nvidia.ProviderID,
		},
	}

	for _, factory := range factories {
		t.Run(factory.name, func(t *testing.T) {
			p := factory.newProvider(t)
			testHealthAndIdentity(t, p, factory.wantHealthy, factory.wantSource)
			testReplayableWatches(t, p)
			testRecoveryReconcile(t, p)
		})
	}
}

func TestStrictAdmissionPlanCapabilityParity(t *testing.T) {
	factories := []completeFactory{
		{
			name: "mock",
			newProvider: func(t *testing.T) provider.CompleteResourceProvider {
				p, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return complianceNow }))
				if err != nil {
					t.Fatal(err)
				}
				return p
			},
		},
		{
			name: "nvidia fake",
			newProvider: func(t *testing.T) provider.CompleteResourceProvider {
				p, err := nvidia.New(nvidia.WithNow(func() time.Time { return complianceNow }), nvidia.WithDriver(newNVIDIATestDriver([]*tgsrlv1.Device{{
					DeviceId:    "mock-cpu-0",
					Kind:        tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
					Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
					Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024},
					Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024},
				}}, nil)))
				if err != nil {
					t.Fatal(err)
				}
				return p
			},
		},
	}
	for _, factory := range factories {
		t.Run(factory.name, func(t *testing.T) {
			p := factory.newProvider(t)
			snapshot, err := p.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			binding := complianceBinding("admission-sandbox", 1)
			action := &tgsrlv1.Action{
				ActionId:                 "admission-action",
				ActionType:               tgsrlv1.ActionType_ACTION_TYPE_BIND,
				Level:                    tgsrlv1.ActionLevel_ACTION_LEVEL_L1,
				TargetId:                 binding.GetPendingUnitId(),
				Binding:                  binding,
				Rollback:                 &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, TargetId: binding.GetBindingId()},
				Order:                    1,
				PlanId:                   "admission-plan",
				SandboxId:                binding.GetSandboxId(),
				ExpectedSnapshotRevision: snapshot.GetRevision(),
				RequiredCapabilities:     &tgsrlv1.CapabilitySet{SupportedActions: []string{"bind"}},
				Deadline:                 timestamppb.New(complianceNow.Add(time.Minute)),
				IdempotencyKey:           "admission-key",
				TickKind:                 tgsrlv1.TickKind_TICK_KIND_FAST,
				Preconditions:            []tgsrlv1.ActionPrecondition{tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_SNAPSHOT_REVISION_MATCH},
				ExpectedImpacts: []tgsrlv1.ExpectedImpact{
					tgsrlv1.ExpectedImpact_EXPECTED_IMPACT_ALLOCATION_CREATED,
					tgsrlv1.ExpectedImpact_EXPECTED_IMPACT_CAPACITY_RESERVED,
				},
			}
			plan := &tgsrlv1.PlacementPlan{
				PlanId:                 "admission-plan",
				SnapshotRevision:       snapshot.GetRevision(),
				Purpose:                tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION,
				RollbackPolicy:         tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_NOT_REQUIRED,
				CapabilityRequirements: nil,
				Actions:                []*tgsrlv1.Action{action},
			}
			if err := provider.ValidateExecutionCapabilities(context.Background(), p, plan); err != nil {
				t.Fatalf("ValidateExecutionCapabilities() error = %v", err)
			}
			if err := executeTransaction(context.Background(), p, plan); err != nil {
				t.Fatalf("transaction execution error = %v", err)
			}
		})
	}
}

func TestCompleteProvidersCapabilityHandshakeParity(t *testing.T) {
	factories := []struct {
		name           string
		capabilityName string
		newProvider    func(t *testing.T) provider.CompleteResourceProvider
	}{
		{name: "mock", capabilityName: "logical-cpu", newProvider: func(t *testing.T) provider.CompleteResourceProvider {
			p, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return complianceNow }))
			if err != nil {
				t.Fatal(err)
			}
			return p
		}},
		{name: "nvidia fake", capabilityName: nvidia.CapabilityName, newProvider: func(t *testing.T) provider.CompleteResourceProvider {
			p, err := nvidia.New(nvidia.WithNow(func() time.Time { return complianceNow }), nvidia.WithDriver(newNVIDIATestDriver([]*tgsrlv1.Device{{
				DeviceId: "nvidia-0", Kind: tgsrlv1.DeviceKind_DEVICE_KIND_GPU, Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY, Capacity: &tgsrlv1.ResourceVector{AcceleratorUnits: 1}, Allocatable: &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
			}}, nil)))
			if err != nil {
				t.Fatal(err)
			}
			return p
		}},
	}
	for _, factory := range factories {
		t.Run(factory.name, func(t *testing.T) {
			p := factory.newProvider(t)
			plan := &tgsrlv1.PlacementPlan{PlanId: "capability-parity", CapabilityRequirements: []*tgsrlv1.CapabilityRequirement{{
				Kind: tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_PROVIDER_CAPABILITY, Name: factory.capabilityName, MinVersion: "1.0.0", Required: true,
			}}}
			if err := provider.ValidateExecutionCapabilities(context.Background(), p, plan); err != nil {
				t.Fatalf("provider capability handshake failed: %v", err)
			}
			plan.CapabilityRequirements = []*tgsrlv1.CapabilityRequirement{{Kind: tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ATOMIC_REPLACEMENT, Required: false}}
			if err := provider.ValidateExecutionCapabilities(context.Background(), p, plan); err != nil {
				t.Fatalf("optional capability blocked handshake: %v", err)
			}
		})
	}
}

func testHealthAndIdentity(t *testing.T, p provider.CompleteResourceProvider, wantHealthy bool, wantSource string) {
	t.Helper()
	id, err := p.ID(context.Background())
	if err != nil {
		t.Fatalf("ID() error = %v", err)
	}
	if id == "" {
		t.Fatal("ID() = empty, want stable provider id")
	}
	health, err := p.Health(context.Background())
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if health.Healthy != wantHealthy {
		t.Fatalf("Health().Healthy = %v, want %v", health.Healthy, wantHealthy)
	}
	if health.Source != wantSource {
		t.Fatalf("Health().Source = %q, want %q", health.Source, wantSource)
	}
}

func testReplayableWatches(t *testing.T, p provider.CompleteResourceProvider) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resourceWatch, err := p.WatchResources(ctx, 0)
	if err != nil {
		t.Fatalf("WatchResources() error = %v", err)
	}
	sandboxWatch, err := p.WatchSandboxes(ctx, 0)
	if err != nil {
		t.Fatalf("WatchSandboxes() error = %v", err)
	}

	firstResource := recvResource(t, resourceWatch)
	if firstResource.Cursor == 0 {
		t.Fatalf("initial resource cursor = %d, want positive", firstResource.Cursor)
	}
	if _, err := p.ObserveSandbox(context.Background(), &tgsrlv1.SandboxEvent{
		EventId:    "watch-event-1",
		SandboxId:  "sandbox-a",
		Generation: 1,
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
	}); err != nil {
		t.Fatalf("ObserveSandbox() error = %v", err)
	}

	var latestResource provider.WatchedResourceEvent
	var latestSandbox provider.WatchedSandboxEvent
	timeout := time.After(2 * time.Second)
	for latestResource.Cursor == 0 || latestSandbox.Cursor == 0 {
		select {
		case event := <-resourceWatch:
			if event.Cursor > firstResource.Cursor {
				latestResource = event
			}
		case event := <-sandboxWatch:
			if event.Cursor > 0 {
				latestSandbox = event
			}
		case <-timeout:
			t.Fatalf("timed out waiting for post-action events")
		}
	}
	if latestSandbox.Event.GetProviderRevision() == 0 || latestSandbox.Event.GetIdempotencyKey() == "" {
		t.Fatalf("sandbox watch event lacks ordering/idempotency metadata: %+v", latestSandbox.Event)
	}
	if latestSandbox.Event.Share == nil || latestSandbox.Event.Priority == nil || latestSandbox.Event.Offloaded == nil {
		t.Fatalf("sandbox watch event lacks mutable-state presence: %+v", latestSandbox.Event)
	}

	replayCtx, replayCancel := context.WithCancel(context.Background())
	defer replayCancel()
	replayedResourceWatch, err := p.WatchResources(replayCtx, firstResource.Cursor)
	if err != nil {
		t.Fatalf("WatchResources(replay) error = %v", err)
	}
	replayedSandboxWatch, err := p.WatchSandboxes(replayCtx, 0)
	if err != nil {
		t.Fatalf("WatchSandboxes(replay) error = %v", err)
	}
	replayedResource := recvResource(t, replayedResourceWatch)
	replayedSandbox := recvSandbox(t, replayedSandboxWatch)
	if replayedResource.Cursor <= firstResource.Cursor {
		t.Fatalf("replayed resource cursor = %d, want > %d", replayedResource.Cursor, firstResource.Cursor)
	}
	if replayedSandbox.Cursor > latestSandbox.Cursor || replayedSandbox.Cursor == 0 {
		t.Fatalf("replayed sandbox cursor = %d, want positive retained cursor <= %d", replayedSandbox.Cursor, latestSandbox.Cursor)
	}
}

func testRecoveryReconcile(t *testing.T, p provider.CompleteResourceProvider) {
	t.Helper()
	snapshot, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	plan := &tgsrlv1.PlacementPlan{
		PlanId:           "recovery-plan",
		SnapshotRevision: snapshot.GetRevision(),
		Purpose:          tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		RollbackPolicy:   tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_NOT_REQUIRED,
		Actions: []*tgsrlv1.Action{
			complianceAction("share-recovery", "recovery-plan", "share-recovery-key", "sandbox-a"),
		},
	}
	plan.Actions[0].ActionType = tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE
	plan.Actions[0].Level = tgsrlv1.ActionLevel_ACTION_LEVEL_L1
	plan.Actions[0].Share = 0.6
	plan.Actions[0].ExpectedSnapshotRevision = snapshot.GetRevision()
	plan.Actions[0].Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	definition, _ := actionpolicy.DefinitionForAction(plan.Actions[0].GetActionType())
	plan.Actions[0].Order = 1
	plan.Actions[0].Preconditions = actionpolicy.RequiredPreconditions(plan.Actions[0].GetActionType(), false, false)
	plan.Actions[0].ExpectedImpacts = append([]tgsrlv1.ExpectedImpact(nil), definition.ExpectedImpacts...)
	plan.Actions[0].RequiredCapabilities = &tgsrlv1.CapabilitySet{SupportedActions: []string{definition.CapabilityName}}
	if err := executeTransaction(context.Background(), p, plan); err != nil {
		if errors.Is(err, provider.ErrNotFound) || strings.Contains(err.Error(), "UNAVAILABLE") {
			recovered, recoverErr := p.RecoverInFlightPlans(context.Background())
			if recoverErr != nil {
				t.Fatalf("RecoverInFlightPlans() error = %v", recoverErr)
			}
			if len(recovered) != 0 {
				t.Fatalf("RecoverInFlightPlans() = %d records, want 0 when no runnable sandbox", len(recovered))
			}
			return
		}
		t.Fatalf("ExecutePlan() error = %v", err)
	}
	record, err := p.ReconcilePlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("ReconcilePlan() error = %v", err)
	}
	if record.Status != provider.PlanStatusSucceeded {
		t.Fatalf("ReconcilePlan().Status = %q, want succeeded", record.Status)
	}
	recovered, err := p.RecoverInFlightPlans(context.Background())
	if err != nil {
		t.Fatalf("RecoverInFlightPlans() error = %v", err)
	}
	if len(recovered) != 0 {
		t.Fatalf("RecoverInFlightPlans() = %d, want 0 after succeeded plan", len(recovered))
	}
}

func executeTransaction(ctx context.Context, p provider.CompleteResourceProvider, plan *tgsrlv1.PlacementPlan) error {
	const generation = uint64(1)
	if _, err := p.PreparePlan(ctx, plan.GetPlanId(), generation, plan); err != nil {
		return err
	}
	for index := range plan.GetActions() {
		if _, err := p.ExecuteStep(ctx, plan.GetPlanId(), generation, index); err != nil {
			_, _ = p.AbortPlan(context.WithoutCancel(ctx), plan.GetPlanId(), generation)
			return err
		}
	}
	_, err := p.CommitPlan(ctx, plan.GetPlanId(), generation)
	return err
}

func recvResource(t *testing.T, ch <-chan provider.WatchedResourceEvent) provider.WatchedResourceEvent {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for resource event")
		return provider.WatchedResourceEvent{}
	}
}

func recvSandbox(t *testing.T, ch <-chan provider.WatchedSandboxEvent) provider.WatchedSandboxEvent {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sandbox event")
		return provider.WatchedSandboxEvent{}
	}
}

func complianceBinding(sandboxID string, generation uint64) *tgsrlv1.Binding {
	return &tgsrlv1.Binding{
		BindingId:     "binding-a",
		PendingUnitId: "unit-a",
		DeviceIds:     []string{"mock-cpu-0"},
		Resources:     &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
		SandboxId:     sandboxID,
		Generation:    generation,
	}
}

func complianceAction(actionID, planID, key, sandboxID string) *tgsrlv1.Action {
	return &tgsrlv1.Action{
		ActionId:                 actionID,
		PlanId:                   planID,
		TargetId:                 sandboxID,
		SandboxId:                sandboxID,
		Binding:                  complianceBinding(sandboxID, 1),
		ExpectedGeneration:       1,
		ExpectedSnapshotRevision: 1,
		Deadline:                 timestamppb.New(complianceNow.Add(time.Minute)),
		IdempotencyKey:           key,
		TickKind:                 tgsrlv1.TickKind_TICK_KIND_SLOW,
	}
}
