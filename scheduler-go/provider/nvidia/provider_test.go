package nvidia

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCommandBuilderBuildsArgvOnly(t *testing.T) {
	command, err := NewCommandBuilder("nvidia-smi").Arg("--query-gpu=index", "--format=csv,noheader").Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	want := []string{"nvidia-smi", "--query-gpu=index", "--format=csv,noheader"}
	if !reflect.DeepEqual(command.Argv, want) {
		t.Fatalf("command argv = %v, want %v", command.Argv, want)
	}
}

func TestUnavailableDriverReportsNoGPU(t *testing.T) {
	driver := NewUnavailableDriver("no GPU present")
	probe, err := driver.Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if probe.Available {
		t.Fatal("Probe().Available = true, want false")
	}
	if probe.Reason != "no GPU present" {
		t.Fatalf("Probe().Reason = %q, want no GPU present", probe.Reason)
	}
}

func TestProviderHealthReflectsDriverAvailability(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(NewUnavailableDriver("no GPU present")))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	health, err := p.Health(context.Background())
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if health.Healthy {
		t.Fatal("Health().Healthy = true, want false")
	}
	if health.Reason != "no GPU present" {
		t.Fatalf("Health().Reason = %q, want no GPU present", health.Reason)
	}
}

func TestProviderRejectsUnsupportedDriverAction(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(NewFakeDriver([]*tgsrlv1.Device{{
		DeviceId:    "nvidia-0",
		Kind:        tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
		Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
		Capacity:    &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
		Allocatable: &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
		Capabilities: &tgsrlv1.CapabilitySet{
			Names:            []string{CapabilityName},
			Source:           ProviderID,
			Revision:         1,
			SupportedActions: []string{"bind"},
		},
	}}, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	action := &tgsrlv1.Action{
		ActionId:                 "unknown",
		ActionType:               tgsrlv1.ActionType_ACTION_TYPE_UNKNOWN,
		PlanId:                   "plan",
		ExpectedSnapshotRevision: 1,
		Deadline:                 timestamppb.New(now.Add(time.Minute)),
		IdempotencyKey:           "key",
	}
	_, err = p.ExecuteAction(context.Background(), action)
	if !errors.Is(err, provider.ErrUnsupported) {
		t.Fatalf("ExecuteAction() error = %v, want ErrUnsupported", err)
	}
}

func TestProviderTickPolicyCompatibility(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		action  *tgsrlv1.Action
		wantErr error
	}{
		{
			name: "legacy unknown L1 allowed",
			action: &tgsrlv1.Action{
				ActionId:                 "bind",
				ActionType:               tgsrlv1.ActionType_ACTION_TYPE_BIND,
				Level:                    tgsrlv1.ActionLevel_ACTION_LEVEL_L1,
				PlanId:                   "plan",
				SandboxId:                "sandbox-a",
				TargetId:                 "sandbox-a",
				Binding:                  &tgsrlv1.Binding{BindingId: "binding-a", SandboxId: "sandbox-a", DeviceIds: []string{"nvidia-0"}, Resources: &tgsrlv1.ResourceVector{AcceleratorUnits: 1}},
				ExpectedSnapshotRevision: 1,
				ExpectedGeneration:       1,
				Deadline:                 timestamppb.New(now.Add(time.Minute)),
				IdempotencyKey:           "key-bind",
				TickKind:                 tgsrlv1.TickKind_TICK_KIND_UNKNOWN,
			},
		},
		{
			name: "unknown tick rejects L2",
			action: &tgsrlv1.Action{
				ActionId:                 "pause",
				ActionType:               tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
				Level:                    tgsrlv1.ActionLevel_ACTION_LEVEL_L2,
				PlanId:                   "plan",
				SandboxId:                "sandbox-a",
				TargetId:                 "sandbox-a",
				ExpectedSnapshotRevision: 1,
				ExpectedGeneration:       1,
				Deadline:                 timestamppb.New(now.Add(time.Minute)),
				IdempotencyKey:           "key-pause",
				TickKind:                 tgsrlv1.TickKind_TICK_KIND_UNKNOWN,
			},
			wantErr: provider.ErrUnsupported,
		},
		{
			name: "fast rejects L2",
			action: &tgsrlv1.Action{
				ActionId:                 "pause-fast",
				ActionType:               tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
				Level:                    tgsrlv1.ActionLevel_ACTION_LEVEL_L2,
				PlanId:                   "plan",
				SandboxId:                "sandbox-a",
				TargetId:                 "sandbox-a",
				ExpectedSnapshotRevision: 1,
				ExpectedGeneration:       1,
				Deadline:                 timestamppb.New(now.Add(time.Minute)),
				IdempotencyKey:           "key-pause-fast",
				TickKind:                 tgsrlv1.TickKind_TICK_KIND_FAST,
			},
			wantErr: provider.ErrUnsupported,
		},
		{
			name: "medium allows L3",
			action: &tgsrlv1.Action{
				ActionId:                 "sleep-medium",
				ActionType:               tgsrlv1.ActionType_ACTION_TYPE_SLEEP,
				Level:                    tgsrlv1.ActionLevel_ACTION_LEVEL_L3,
				PlanId:                   "plan",
				SandboxId:                "sandbox-a",
				TargetId:                 "sandbox-a",
				ExpectedSnapshotRevision: 1,
				ExpectedGeneration:       1,
				Deadline:                 timestamppb.New(now.Add(time.Minute)),
				IdempotencyKey:           "key-sleep-medium",
				TickKind:                 tgsrlv1.TickKind_TICK_KIND_MEDIUM,
			},
			wantErr: provider.ErrNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p, err := New(WithNow(func() time.Time { return now }), WithDriver(NewFakeDriver([]*tgsrlv1.Device{{
				DeviceId:    "nvidia-0",
				Kind:        tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
				Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
				Capacity:    &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
				Allocatable: &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
				Capabilities: &tgsrlv1.CapabilitySet{
					Names:            []string{CapabilityName},
					Source:           ProviderID,
					Revision:         1,
					SupportedActions: []string{"bind", "pause", "sleep"},
				},
			}}, nil)))
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			_, err = p.ExecuteAction(context.Background(), test.action)
			if test.wantErr == nil {
				if err != nil {
					t.Fatalf("ExecuteAction() error = %v", err)
				}
				return
			}
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ExecuteAction() error = %v, want errors.Is(%v)", err, test.wantErr)
			}
		})
	}
}

func TestProviderRejectsMissingRequiredCapabilitySurface(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(NewFakeDriver([]*tgsrlv1.Device{{
		DeviceId:    "nvidia-0",
		Kind:        tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
		Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
		Capacity:    &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
		Allocatable: &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
		Capabilities: &tgsrlv1.CapabilitySet{
			Names:            []string{CapabilityName},
			Source:           ProviderID,
			Revision:         1,
			SupportedActions: []string{"bind"},
		},
	}}, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	action := &tgsrlv1.Action{
		ActionId:                 "bind",
		ActionType:               tgsrlv1.ActionType_ACTION_TYPE_BIND,
		Level:                    tgsrlv1.ActionLevel_ACTION_LEVEL_L1,
		PlanId:                   "plan",
		SandboxId:                "sandbox-a",
		Binding:                  &tgsrlv1.Binding{BindingId: "binding-a", SandboxId: "sandbox-a", DeviceIds: []string{"nvidia-0"}},
		ExpectedSnapshotRevision: 1,
		Deadline:                 timestamppb.New(now.Add(time.Minute)),
		IdempotencyKey:           "key",
		TickKind:                 tgsrlv1.TickKind_TICK_KIND_FAST,
		RequiredCapabilities:     &tgsrlv1.CapabilitySet{Names: []string{"missing"}, SupportedActions: []string{"bind"}},
	}
	if _, err := p.ExecuteAction(context.Background(), action); !errors.Is(err, provider.ErrUnsupported) {
		t.Fatalf("ExecuteAction() error = %v, want ErrUnsupported", err)
	}
}

func TestProviderRejectsFencesBeforeDriverDispatch(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	makeProvider := func(t *testing.T) (*Provider, *recordingDriver) {
		t.Helper()
		driver := &recordingDriver{Driver: NewFakeDriver([]*tgsrlv1.Device{{
			DeviceId:    "nvidia-0",
			Kind:        tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
			Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:    &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
			Allocatable: &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
		}}, nil)}
		p, err := New(WithNow(func() time.Time { return now }), WithDriver(driver))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		return p, driver
	}
	baseAction := func() *tgsrlv1.Action {
		return &tgsrlv1.Action{
			ActionId:                 "pause",
			ActionType:               tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
			Level:                    tgsrlv1.ActionLevel_ACTION_LEVEL_L2,
			PlanId:                   "plan",
			SandboxId:                "sandbox-a",
			TargetId:                 "sandbox-a",
			ExpectedSnapshotRevision: 2,
			ExpectedGeneration:       1,
			Deadline:                 timestamppb.New(now.Add(time.Minute)),
			IdempotencyKey:           "key",
			TickKind:                 tgsrlv1.TickKind_TICK_KIND_MEDIUM,
		}
	}
	tests := []struct {
		name   string
		mutate func(*tgsrlv1.Action)
	}{
		{name: "stale snapshot", mutate: func(action *tgsrlv1.Action) { action.ExpectedSnapshotRevision = 1 }},
		{name: "stale generation", mutate: func(action *tgsrlv1.Action) { action.ExpectedGeneration = 9 }},
		{name: "safe point unavailable", mutate: func(action *tgsrlv1.Action) { action.RequiresSafePoint = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p, driver := makeProvider(t)
			safePoint := false
			if err := p.ApplySandboxEvent(context.Background(), provider.SandboxEvent{SandboxID: "sandbox-a", Generation: 1, State: provider.SandboxStateRunning, SafePoint: &safePoint}); err != nil {
				t.Fatalf("ApplySandboxEvent() error = %v", err)
			}
			action := baseAction()
			test.mutate(action)
			if _, err := p.ExecuteAction(context.Background(), action); !errors.Is(err, provider.ErrFailedPrecondition) {
				t.Fatalf("ExecuteAction() error = %v, want ErrFailedPrecondition", err)
			}
			if driver.executeCalls != 0 {
				t.Fatalf("driver ExecuteAction calls = %d, want 0", driver.executeCalls)
			}
		})
	}
}

func TestProviderCompensatesSingleActionProjectionFailure(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	driver := &recordingDriver{Driver: NewFakeDriver([]*tgsrlv1.Device{{
		DeviceId:    "nvidia-0",
		Kind:        tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
		Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
		Capacity:    &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
		Allocatable: &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
	}}, nil)}
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	safePoint := true
	if err := p.ApplySandboxEvent(context.Background(), provider.SandboxEvent{SandboxID: "sandbox-a", Generation: 1, State: provider.SandboxStatePaused, SafePoint: &safePoint}); err != nil {
		t.Fatal(err)
	}
	action := &tgsrlv1.Action{
		ActionId:                 "pause",
		ActionType:               tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
		Level:                    tgsrlv1.ActionLevel_ACTION_LEVEL_L2,
		PlanId:                   "plan",
		SandboxId:                "sandbox-a",
		TargetId:                 "sandbox-a",
		ExpectedSnapshotRevision: 2,
		ExpectedGeneration:       1,
		Deadline:                 timestamppb.New(now.Add(time.Minute)),
		IdempotencyKey:           "key",
		TickKind:                 tgsrlv1.TickKind_TICK_KIND_MEDIUM,
		Rollback:                 &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"},
	}
	result, err := p.ExecuteAction(context.Background(), action)
	if !errors.Is(err, provider.ErrFailedPrecondition) {
		t.Fatalf("ExecuteAction() error = %v, want ErrFailedPrecondition", err)
	}
	if driver.executeCalls != 2 {
		t.Fatalf("driver calls = %d, want forward plus compensation", driver.executeCalls)
	}
	if !result.GetRollbackAttempted() || result.GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK {
		t.Fatalf("rollback result = %v", result)
	}
}

func TestProviderExecutePlanUsesStoreRevisionAndCompensatesInReverse(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	driver := &scriptedDriver{Driver: NewFakeDriver(testDevices(), nil), failActionID: "pause"}
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	safePoint := true
	if err := p.ApplySandboxEvent(context.Background(), provider.SandboxEvent{SandboxID: "sandbox-a", Generation: 1, State: provider.SandboxStateRunning, SafePoint: &safePoint}); err != nil {
		t.Fatal(err)
	}
	first := nvidiaAction(now, "share", "plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, 99)
	first.Share = 0.6
	first.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	second := nvidiaAction(now, "priority", "plan", "priority-key", tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, 99)
	second.Priority = 7
	second.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, TargetId: "sandbox-a"}
	third := nvidiaAction(now, "pause", "plan", "pause-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, 99)
	third.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}
	plan := nvidiaPlan("plan", 99, first, second, third)

	results, err := p.ExecutePlan(context.Background(), plan)
	if !errors.Is(err, provider.ErrPartialFailure) {
		t.Fatalf("ExecutePlan() error = %v, want ErrPartialFailure", err)
	}
	if len(results) != 3 {
		t.Fatalf("results = %d, want 3", len(results))
	}
	for _, index := range []int{0, 1} {
		if !results[index].GetRollbackAttempted() || results[index].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK {
			t.Fatalf("result[%d] rollback = %v", index, results[index])
		}
	}
	wantCalls := []string{"share", "priority", "pause", "priority-rollback", "share-rollback"}
	if !reflect.DeepEqual(driver.actionIDs(), wantCalls) {
		t.Fatalf("driver calls = %v, want %v", driver.actionIDs(), wantCalls)
	}
	sandbox, lookupErr := p.GetSandbox(context.Background(), "sandbox-a")
	if lookupErr != nil {
		t.Fatal(lookupErr)
	}
	if sandbox.Share != 0 || sandbox.Priority != 0 || sandbox.State != provider.SandboxStateRunning {
		t.Fatalf("sandbox after compensation = %+v", sandbox)
	}
	record, reconcileErr := p.ReconcilePlan(context.Background(), plan)
	if reconcileErr != nil {
		t.Fatal(reconcileErr)
	}
	if record.Status != provider.PlanStatusFailed || !errors.Is(err, provider.ErrPartialFailure) {
		t.Fatalf("plan record = %+v", record)
	}
}

func TestProviderExecutePlanMarksRemainingSkippedAndRollbackFailure(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	driver := &scriptedDriver{Driver: NewFakeDriver(testDevices(), nil), failActionID: "priority", failRollbackID: "share-rollback"}
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	safePoint := true
	if err := p.ApplySandboxEvent(context.Background(), provider.SandboxEvent{SandboxID: "sandbox-a", Generation: 1, State: provider.SandboxStateRunning, SafePoint: &safePoint}); err != nil {
		t.Fatal(err)
	}
	share := nvidiaAction(now, "share", "plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, 77)
	share.Share = 0.7
	share.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	priority := nvidiaAction(now, "priority", "plan", "priority-key", tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, 77)
	priority.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, TargetId: "sandbox-a"}
	pause := nvidiaAction(now, "pause", "plan", "pause-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, 77)
	pause.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}

	results, err := p.ExecutePlan(context.Background(), nvidiaPlan("plan", 77, share, priority, pause))
	if !errors.Is(err, provider.ErrPartialFailure) {
		t.Fatalf("ExecutePlan() error = %v, want ErrPartialFailure", err)
	}
	if len(results) != 3 || results[2].GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SKIPPED {
		t.Fatalf("results = %v, want trailing skipped", results)
	}
	if !results[0].GetRollbackAttempted() || results[0].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED {
		t.Fatalf("rollback failure result = %v", results[0])
	}
	sandbox, _ := p.GetSandbox(context.Background(), "sandbox-a")
	if sandbox.Share != 0.7 {
		t.Fatalf("failed compensation erased forward effect: share = %v", sandbox.Share)
	}
}

func TestProviderExecutePlanRejectsAtomicReplacementBeforeDriver(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	driver := &recordingDriver{Driver: NewFakeDriver(testDevices(), nil)}
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	action := nvidiaAction(now, "bind", "atomic", "bind-key", tgsrlv1.ActionType_ACTION_TYPE_BIND, 42)
	action.ExpectedGeneration = 0
	action.Binding = &tgsrlv1.Binding{BindingId: "binding-a", PendingUnitId: "unit-a", SandboxId: "sandbox-a", DeviceIds: []string{"nvidia-0"}}
	action.TargetId = "unit-a"
	action.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, TargetId: "binding-a"}
	plan := nvidiaPlan("atomic", 42, action)
	plan.CapabilityRequirements = actionpolicy.StableCapabilityRequirements(&tgsrlv1.CapabilityRequirement{Kind: tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ATOMIC_REPLACEMENT, Required: true})

	if _, err := p.ExecutePlan(context.Background(), plan); !errors.Is(err, provider.ErrUnsupported) {
		t.Fatalf("ExecutePlan() error = %v, want ErrUnsupported", err)
	}
	if driver.executeCalls != 0 {
		t.Fatalf("driver calls = %d, want 0", driver.executeCalls)
	}
}

func TestProviderFenceFailureCanRetryWithSameIdempotencyKey(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	driver := &recordingDriver{Driver: NewFakeDriver(testDevices(), nil)}
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	safePoint := false
	if err := p.ApplySandboxEvent(context.Background(), provider.SandboxEvent{SandboxID: "sandbox-a", Generation: 1, State: provider.SandboxStateRunning, SafePoint: &safePoint}); err != nil {
		t.Fatal(err)
	}
	action := nvidiaAction(now, "pause", "plan", "same-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, 2)
	action.RequiresSafePoint = true
	if _, err := p.ExecuteAction(context.Background(), action); !errors.Is(err, provider.ErrFailedPrecondition) {
		t.Fatalf("first ExecuteAction() error = %v", err)
	}
	safePoint = true
	if err := p.ApplySandboxEvent(context.Background(), provider.SandboxEvent{SandboxID: "sandbox-a", Generation: 1, State: provider.SandboxStateRunning, SafePoint: &safePoint}); err != nil {
		t.Fatal(err)
	}
	action.ExpectedSnapshotRevision = 3
	if _, err := p.ExecuteAction(context.Background(), action); err != nil {
		t.Fatalf("retry ExecuteAction() error = %v", err)
	}
	if driver.executeCalls != 1 {
		t.Fatalf("driver calls = %d, want 1", driver.executeCalls)
	}
}

func TestProviderConcurrentPlanRetryReturnsInFlightError(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	started := make(chan struct{})
	release := make(chan struct{})
	driver := &blockingDriver{Driver: NewFakeDriver(testDevices(), nil), started: started, release: release}
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	action := nvidiaAction(now, "bind", "plan", "bind-key", tgsrlv1.ActionType_ACTION_TYPE_BIND, 55)
	action.ExpectedGeneration = 0
	action.Binding = &tgsrlv1.Binding{BindingId: "binding-a", PendingUnitId: "unit-a", SandboxId: "sandbox-a", DeviceIds: []string{"nvidia-0"}}
	action.TargetId = "unit-a"
	action.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, TargetId: "binding-a"}
	plan := nvidiaPlan("plan", 55, action)
	done := make(chan error, 1)
	go func() {
		_, executeErr := p.ExecutePlan(context.Background(), plan)
		done <- executeErr
	}()
	<-started
	if results, err := p.ExecutePlan(context.Background(), plan); !errors.Is(err, provider.ErrFailedPrecondition) || len(results) != 0 {
		t.Fatalf("concurrent retry = (%v, %v), want empty retryable error", results, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first ExecutePlan() error = %v", err)
	}
}

func TestProviderConcurrentActionUsesOneDriverCall(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	started := make(chan struct{})
	release := make(chan struct{})
	driver := &blockingDriver{Driver: NewFakeDriver(testDevices(), nil), started: started, release: release}
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	action := nvidiaAction(now, "bind", "plan", "same-key", tgsrlv1.ActionType_ACTION_TYPE_BIND, 1)
	action.ExpectedGeneration = 0
	action.TargetId = "unit-a"
	action.Binding = &tgsrlv1.Binding{BindingId: "binding-a", PendingUnitId: "unit-a", SandboxId: "sandbox-a", DeviceIds: []string{"nvidia-0"}}

	type outcome struct {
		result *tgsrlv1.ActionResult
		err    error
	}
	firstDone := make(chan outcome, 1)
	duplicateDone := make(chan outcome, 1)
	go func() {
		result, executeErr := p.ExecuteAction(context.Background(), action)
		firstDone <- outcome{result: result, err: executeErr}
	}()
	<-started
	go func() {
		result, executeErr := p.ExecuteAction(context.Background(), action)
		duplicateDone <- outcome{result: result, err: executeErr}
	}()

	conflict := proto.Clone(action).(*tgsrlv1.Action)
	conflict.TargetId = "other-unit"
	if _, err := p.ExecuteAction(context.Background(), conflict); !errors.Is(err, provider.ErrIdempotencyConflict) {
		t.Fatalf("conflicting ExecuteAction() error = %v, want ErrIdempotencyConflict", err)
	}
	if calls := driver.executeCalls(); calls != 1 {
		t.Fatalf("driver calls before release = %d, want 1", calls)
	}
	select {
	case duplicate := <-duplicateDone:
		t.Fatalf("duplicate returned before owner completed: %+v", duplicate)
	default:
	}
	close(release)
	first := <-firstDone
	duplicate := <-duplicateDone
	if first.err != nil || duplicate.err != nil {
		t.Fatalf("action errors = (%v, %v)", first.err, duplicate.err)
	}
	if !proto.Equal(first.result, duplicate.result) {
		t.Fatalf("duplicate result = %v, want %v", duplicate.result, first.result)
	}
	if calls := driver.executeCalls(); calls != 1 {
		t.Fatalf("driver calls = %d, want 1", calls)
	}
}

func TestProviderStandaloneActionPlanCrossRaceRestoresProjection(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	started := make(chan struct{})
	release := make(chan struct{})
	driver := &crossRaceDriver{Driver: NewFakeDriver(testDevices(), nil), started: started, release: release, failActionID: "pause"}
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	safePoint := true
	if err := p.ApplySandboxEvent(context.Background(), provider.SandboxEvent{SandboxID: "sandbox-a", Generation: 1, State: provider.SandboxStateRunning, SafePoint: &safePoint}); err != nil {
		t.Fatal(err)
	}
	share := nvidiaAction(now, "share", "cross-race-plan", "shared-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, 2)
	share.Share = 0.8
	share.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	pause := nvidiaAction(now, "pause", "cross-race-plan", "pause-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, 2)
	pause.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}
	plan := nvidiaPlan("cross-race-plan", 2, share, pause)

	standaloneDone := make(chan error, 1)
	go func() {
		_, executeErr := p.ExecuteAction(context.Background(), share)
		standaloneDone <- executeErr
	}()
	<-started

	type planOutcome struct {
		results []*tgsrlv1.ActionResult
		err     error
	}
	planDone := make(chan planOutcome, 1)
	go func() {
		results, executeErr := p.ExecutePlan(context.Background(), plan)
		planDone <- planOutcome{results: results, err: executeErr}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		p.mu.Lock()
		waiters := p.actions[share.GetIdempotencyKey()].waiters
		p.mu.Unlock()
		if waiters == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("plan action did not wait for standalone owner")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := <-standaloneDone; err != nil {
		t.Fatalf("standalone ExecuteAction() error = %v", err)
	}
	outcome := <-planDone
	if !errors.Is(outcome.err, provider.ErrPartialFailure) {
		t.Fatalf("ExecutePlan() error = %v, want ErrPartialFailure", outcome.err)
	}
	if len(outcome.results) != 2 || outcome.results[0].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK {
		t.Fatalf("plan results = %v, want first action rolled back", outcome.results)
	}
	if calls := driver.actionIDs(); !reflect.DeepEqual(calls, []string{"share", "pause", "share-rollback"}) {
		t.Fatalf("driver calls = %v", calls)
	}
	sandbox, err := p.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatal(err)
	}
	if sandbox.Share != 0 || sandbox.State != provider.SandboxStateRunning {
		t.Fatalf("sandbox after cross-race compensation = %+v, want original projection", sandbox)
	}
}

func testDevices() []*tgsrlv1.Device {
	return []*tgsrlv1.Device{{DeviceId: "nvidia-0", Kind: tgsrlv1.DeviceKind_DEVICE_KIND_GPU, Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY, Capacity: &tgsrlv1.ResourceVector{AcceleratorUnits: 1}, Allocatable: &tgsrlv1.ResourceVector{AcceleratorUnits: 1}}}
}

func nvidiaAction(now time.Time, actionID, planID, key string, actionType tgsrlv1.ActionType, revision uint64) *tgsrlv1.Action {
	definition, _ := actionpolicy.DefinitionForAction(actionType)
	return &tgsrlv1.Action{
		ActionId: actionID, ActionType: actionType, Level: definition.Level, TargetId: "sandbox-a", SandboxId: "sandbox-a", PlanId: planID, ExpectedGeneration: 1, ExpectedSnapshotRevision: revision, Deadline: timestamppb.New(now.Add(time.Minute)), IdempotencyKey: key, TickKind: tgsrlv1.TickKind_TICK_KIND_SLOW,
		RequiredCapabilities: &tgsrlv1.CapabilitySet{SupportedActions: []string{definition.CapabilityName}},
		Preconditions:        actionpolicy.RequiredPreconditions(actionType, false, false), ExpectedImpacts: append([]tgsrlv1.ExpectedImpact(nil), definition.ExpectedImpacts...),
	}
}

func nvidiaPlan(planID string, revision uint64, actions ...*tgsrlv1.Action) *tgsrlv1.PlacementPlan {
	for index, action := range actions {
		action.Order = uint32(index + 1)
	}
	rollbackPolicy := tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_NOT_REQUIRED
	var capabilities []*tgsrlv1.CapabilityRequirement
	if len(actions) > 1 {
		rollbackPolicy = tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION
		capabilities = actionpolicy.StableCapabilityRequirements(
			actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION),
			actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK),
		)
	}
	return &tgsrlv1.PlacementPlan{PlanId: planID, SnapshotRevision: revision, Purpose: tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION, RollbackPolicy: rollbackPolicy, CapabilityRequirements: capabilities, Actions: actions}
}

type recordingDriver struct {
	Driver
	executeCalls int
}

func (d *recordingDriver) ExecuteAction(ctx context.Context, state *DriverState, action *tgsrlv1.Action) (*ActionExecution, error) {
	d.executeCalls++
	return d.Driver.ExecuteAction(ctx, state, action)
}

type scriptedDriver struct {
	Driver
	mu             sync.Mutex
	calls          []string
	failActionID   string
	failRollbackID string
}

func (d *scriptedDriver) ExecuteAction(ctx context.Context, state *DriverState, action *tgsrlv1.Action) (*ActionExecution, error) {
	d.mu.Lock()
	d.calls = append(d.calls, action.GetActionId())
	d.mu.Unlock()
	if action.GetActionId() == d.failActionID || action.GetActionId() == d.failRollbackID {
		return nil, errors.New("scripted driver failure")
	}
	return d.Driver.ExecuteAction(ctx, state, action)
}

func (d *scriptedDriver) actionIDs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.calls...)
}

type blockingDriver struct {
	Driver
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	calls   int
}

func (d *blockingDriver) ExecuteAction(ctx context.Context, state *DriverState, action *tgsrlv1.Action) (*ActionExecution, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	d.once.Do(func() { close(d.started) })
	select {
	case <-d.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return d.Driver.ExecuteAction(ctx, state, action)
}

func (d *blockingDriver) executeCalls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

type crossRaceDriver struct {
	Driver
	started      chan struct{}
	release      chan struct{}
	failActionID string
	once         sync.Once
	mu           sync.Mutex
	calls        []string
}

func (d *crossRaceDriver) ExecuteAction(ctx context.Context, state *DriverState, action *tgsrlv1.Action) (*ActionExecution, error) {
	d.mu.Lock()
	d.calls = append(d.calls, action.GetActionId())
	d.mu.Unlock()
	if action.GetActionId() == "share" {
		d.once.Do(func() { close(d.started) })
		select {
		case <-d.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if action.GetActionId() == d.failActionID {
		return nil, errors.New("scripted driver failure")
	}
	return d.Driver.ExecuteAction(ctx, state, action)
}

func (d *crossRaceDriver) actionIDs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.calls...)
}
