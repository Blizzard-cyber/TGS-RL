package provider

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ ResourceProvider = (*MockResourceProvider)(nil)

var fixtureNow = time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)

func TestMockProviderReturnsMockLogicalDeviceClones(t *testing.T) {
	binding := testBinding("sandbox-a", 1)
	semanticContext := testSemanticContext()
	seed := Sandbox{
		SandboxID:       "sandbox-a",
		State:           SandboxStateRunning,
		Generation:      1,
		Binding:         binding,
		SemanticContext: semanticContext,
		Share:           0.5,
		SafePoint:       true,
	}
	provider := newTestProvider(t, WithSandboxes(seed))
	binding.DeviceIds[0] = "caller-mutated"
	semanticContext.Attributes["source"] = "caller-mutated"

	capabilities, err := provider.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities() error = %v", err)
	}
	if capabilities.GetSource() != "mock" {
		t.Fatalf("Capabilities().Source = %q, want mock", capabilities.GetSource())
	}
	capabilities.Source = "caller-mutated"
	capabilities.SupportedActions[0] = "caller-mutated"

	devices, err := provider.ListDevices(context.Background())
	if err != nil {
		t.Fatalf("ListDevices() error = %v", err)
	}
	if len(devices) == 0 || devices[0].GetKind() != tgsrlv1.DeviceKind_DEVICE_KIND_CPU {
		t.Fatalf("ListDevices() = %v, want logical CPU device", devices)
	}
	devices[0].Labels["provider"] = "caller-mutated"
	devices[0].Capacity.CpuMillis = 1

	sandbox, err := provider.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if got := sandbox.Binding.GetDeviceIds()[0]; got != "mock-cpu-0" {
		t.Fatalf("GetSandbox().Binding.DeviceIds[0] = %q, want mock-cpu-0", got)
	}
	sandbox.Binding.DeviceIds[0] = "return-mutated"
	sandbox.SemanticContext.Attributes["source"] = "return-mutated"

	capabilitiesAgain, _ := provider.Capabilities(context.Background())
	devicesAgain, _ := provider.ListDevices(context.Background())
	sandboxAgain, _ := provider.GetSandbox(context.Background(), "sandbox-a")
	if capabilitiesAgain.GetSource() != "mock" || capabilitiesAgain.GetSupportedActions()[0] == "caller-mutated" {
		t.Fatalf("capability return aliases provider state: %v", capabilitiesAgain)
	}
	if devicesAgain[0].GetLabels()["provider"] != "mock" || devicesAgain[0].GetCapacity().GetCpuMillis() == 1 {
		t.Fatalf("device return aliases provider state: %v", devicesAgain[0])
	}
	if got := sandboxAgain.Binding.GetDeviceIds()[0]; got != "mock-cpu-0" {
		t.Fatalf("sandbox return aliases provider state: device = %q", got)
	}
	if got := sandboxAgain.SemanticContext.GetAttributes()["source"]; got != "runtime" {
		t.Fatalf("sandbox semantic context aliases caller state: source = %q", got)
	}

	snapshot, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.GetRevision() != 1 || snapshot.GetAnnotations()["provider"] != "mock" {
		t.Fatalf("Snapshot() = %v, want revision 1 and mock annotation", snapshot)
	}
}

func TestSandboxObservationTimestampsAndSemanticContext(t *testing.T) {
	current := fixtureNow
	p, err := NewMockResourceProvider(WithNow(func() time.Time { return current }))
	if err != nil {
		t.Fatal(err)
	}
	semanticContext := testSemanticContext()
	observedStateChangedAt := current.Add(-time.Minute)
	if _, err := p.ObserveSandbox(context.Background(), &tgsrlv1.SandboxEvent{
		EventId: "created", SandboxId: "sandbox-a", Generation: 1, State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, SemanticContext: semanticContext, OccurredAt: timestamppb.New(observedStateChangedAt),
	}); err != nil {
		t.Fatal(err)
	}
	created, err := p.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatal(err)
	}
	if !created.UpdatedAt.Equal(current) || !created.StateChangedAt.Equal(observedStateChangedAt) {
		t.Fatalf("created timestamps = updated %v state changed %v, want %v/%v", created.UpdatedAt, created.StateChangedAt, current, observedStateChangedAt)
	}
	semanticContext.Attributes["source"] = "caller-mutated"
	created.SemanticContext.Attributes["source"] = "return-mutated"

	stateChangedAt := created.StateChangedAt
	current = current.Add(time.Minute)
	if _, err := p.ObserveSandbox(context.Background(), &tgsrlv1.SandboxEvent{
		EventId: "confirmed", SandboxId: "sandbox-a", Generation: 1, State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
	}); err != nil {
		t.Fatal(err)
	}
	confirmed, _ := p.GetSandbox(context.Background(), "sandbox-a")
	if !confirmed.UpdatedAt.Equal(current) || !confirmed.StateChangedAt.Equal(stateChangedAt) {
		t.Fatalf("same-state confirmation timestamps = updated %v state changed %v", confirmed.UpdatedAt, confirmed.StateChangedAt)
	}
	if got := confirmed.SemanticContext.GetAttributes()["source"]; got != "runtime" {
		t.Fatalf("stored semantic context was aliased: source = %q", got)
	}

	watchContext, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()
	watch, err := p.WatchSandboxes(watchContext, 0)
	if err != nil {
		t.Fatal(err)
	}
	var semanticEvent *tgsrlv1.SandboxEvent
	for semanticEvent == nil {
		select {
		case watched := <-watch:
			if watched.Event.GetSemanticContext() != nil {
				semanticEvent = watched.Event
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for semantic sandbox event")
		}
	}
	if got := semanticEvent.GetSemanticContext().GetAttributes()["source"]; got != "runtime" {
		t.Fatalf("watch semantic context source = %q, want runtime", got)
	}
	if fields := semanticEvent.GetSemanticContext().GetTypedFields(); len(fields) != 1 || fields[0].GetKey() != "sample.policy_lag" || fields[0].GetValue().GetUint64Value() != 2 {
		t.Fatalf("watch semantic facts = %+v", fields)
	}
	semanticEvent.SemanticContext.Attributes["source"] = "watch-mutated"
	replayContext, cancelReplay := context.WithCancel(context.Background())
	defer cancelReplay()
	replay, err := p.WatchSandboxes(replayContext, 0)
	if err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case watched := <-replay:
			if watched.Event.GetSemanticContext() != nil {
				if got := watched.Event.GetSemanticContext().GetAttributes()["source"]; got != "runtime" {
					t.Fatalf("watch event aliases retained log: source = %q", got)
				}
				return
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for replayed semantic sandbox event")
		}
	}
}

func TestSandboxActionsUpdateStateChangedAtOnlyForLifecycleChanges(t *testing.T) {
	current := fixtureNow
	initial := testSandbox(SandboxStateRunning)
	initial.StateChangedAt = current
	initial.UpdatedAt = current
	p, err := NewMockResourceProvider(WithNow(func() time.Time { return current }), WithSandboxes(initial))
	if err != nil {
		t.Fatal(err)
	}

	current = current.Add(time.Minute)
	share := testAction("share-time", "share-time-plan", "share-time-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	share.Share = 0.75
	share.Deadline = timestamppb.New(current.Add(time.Minute))
	if _, err := p.ExecuteAction(context.Background(), share); err != nil {
		t.Fatal(err)
	}
	afterShare, _ := p.GetSandbox(context.Background(), "sandbox-a")
	if !afterShare.UpdatedAt.Equal(current) || !afterShare.StateChangedAt.Equal(initial.StateChangedAt) {
		t.Fatalf("set_share timestamps = updated %v state changed %v", afterShare.UpdatedAt, afterShare.StateChangedAt)
	}

	current = current.Add(time.Minute)
	pause := testAction("pause-time", "pause-time-plan", "pause-time-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 2)
	pause.Deadline = timestamppb.New(current.Add(time.Minute))
	if _, err := p.ExecuteAction(context.Background(), pause); err != nil {
		t.Fatal(err)
	}
	afterPause, _ := p.GetSandbox(context.Background(), "sandbox-a")
	if !afterPause.StateChangedAt.Equal(current) {
		t.Fatalf("pause StateChangedAt = %v, want %v", afterPause.StateChangedAt, current)
	}

	current = current.Add(time.Minute)
	rebind := testAction("rebind-time", "rebind-time-plan", "rebind-time-key", tgsrlv1.ActionType_ACTION_TYPE_REBIND, tgsrlv1.ActionLevel_ACTION_LEVEL_L4, 3)
	rebind.Deadline = timestamppb.New(current.Add(time.Minute))
	if _, err := p.ExecuteAction(context.Background(), rebind); err != nil {
		t.Fatal(err)
	}
	afterRebind, _ := p.GetSandbox(context.Background(), "sandbox-a")
	if afterRebind.Generation != 2 || !afterRebind.StateChangedAt.Equal(current) {
		t.Fatalf("rebind sandbox = generation %d state changed %v", afterRebind.Generation, afterRebind.StateChangedAt)
	}
}

func TestExistingBindRollbackRestoresObservationMetadata(t *testing.T) {
	initial := testSandbox(SandboxStateRequested)
	initial.Share = 0.25
	initial.Priority = 3
	initial.StateChangedAt = fixtureNow.Add(-time.Hour)
	p := newTestProvider(t, WithSandboxes(initial), WithFaults(FaultOptions{PartialFailureAt: 2}))
	bind := testAction("bind-rollback", "bind-rollback-plan", "bind-rollback-key", tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	bind.Share = 0.75
	bind.Priority = 9
	bind.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, TargetId: bind.GetBinding().GetBindingId()}
	fail := testAction("fail-after-bind", "bind-rollback-plan", "fail-after-bind-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	fail.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}

	if _, err := executeTestTransaction(context.Background(), p, strictReconciliationPlan("bind-rollback-plan", 1, bind, fail)); !errors.Is(err, ErrPartialFailure) {
		t.Fatalf("transaction error = %v, want ErrPartialFailure", err)
	}
	after, err := p.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatal(err)
	}
	if after.State != initial.State || after.Share != initial.Share || after.Priority != initial.Priority || !proto.Equal(after.Binding, initial.Binding) || !after.StateChangedAt.Equal(initial.StateChangedAt) {
		t.Fatalf("sandbox after existing bind rollback = %+v, want %+v", after, initial)
	}
}

func TestExecuteActionVendorNeutralSemantics(t *testing.T) {
	tests := []struct {
		name           string
		initialState   SandboxState
		actionType     tgsrlv1.ActionType
		level          tgsrlv1.ActionLevel
		configure      func(*tgsrlv1.Action)
		wantState      SandboxState
		wantGeneration uint64
		wantShare      float64
		wantPriority   int32
		wantOffloaded  bool
		withoutSandbox bool
	}{
		{name: "bind", actionType: tgsrlv1.ActionType_ACTION_TYPE_BIND, level: tgsrlv1.ActionLevel_ACTION_LEVEL_L1, configure: func(action *tgsrlv1.Action) { action.Share = 0.25; action.Priority = 7 }, wantState: SandboxStateBound, wantGeneration: 1, wantShare: 0.25, wantPriority: 7, withoutSandbox: true},
		{name: "release", initialState: SandboxStateBound, actionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, level: tgsrlv1.ActionLevel_ACTION_LEVEL_L1, wantState: SandboxStateTerminated, wantGeneration: 1},
		{name: "set share", initialState: SandboxStateRunning, actionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, level: tgsrlv1.ActionLevel_ACTION_LEVEL_L1, configure: func(action *tgsrlv1.Action) { action.Share = 0.75 }, wantState: SandboxStateRunning, wantGeneration: 1, wantShare: 0.75},
		{name: "set priority", initialState: SandboxStateRunning, actionType: tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, level: tgsrlv1.ActionLevel_ACTION_LEVEL_L1, configure: func(action *tgsrlv1.Action) { action.Priority = 23 }, wantState: SandboxStateRunning, wantGeneration: 1, wantPriority: 23},
		{name: "resize", initialState: SandboxStateRunning, actionType: tgsrlv1.ActionType_ACTION_TYPE_RESIZE, level: tgsrlv1.ActionLevel_ACTION_LEVEL_L1, configure: func(action *tgsrlv1.Action) { action.Binding.Resources.CpuMillis = 2000 }, wantState: SandboxStateRunning, wantGeneration: 1},
		{name: "pause", initialState: SandboxStateRunning, actionType: tgsrlv1.ActionType_ACTION_TYPE_PAUSE, level: tgsrlv1.ActionLevel_ACTION_LEVEL_L2, wantState: SandboxStatePaused, wantGeneration: 1},
		{name: "resume paused", initialState: SandboxStatePaused, actionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, level: tgsrlv1.ActionLevel_ACTION_LEVEL_L2, wantState: SandboxStateRunning, wantGeneration: 1},
		{name: "sleep", initialState: SandboxStateRunning, actionType: tgsrlv1.ActionType_ACTION_TYPE_SLEEP, level: tgsrlv1.ActionLevel_ACTION_LEVEL_L3, wantState: SandboxStateSleeping, wantGeneration: 1},
		{name: "offload", initialState: SandboxStatePaused, actionType: tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD, level: tgsrlv1.ActionLevel_ACTION_LEVEL_L3, wantState: SandboxStateSleeping, wantGeneration: 1, wantOffloaded: true},
		{name: "rebind", initialState: SandboxStateRunning, actionType: tgsrlv1.ActionType_ACTION_TYPE_REBIND, level: tgsrlv1.ActionLevel_ACTION_LEVEL_L4, wantState: SandboxStateBound, wantGeneration: 2},
		{name: "recreate", initialState: SandboxStateRunning, actionType: tgsrlv1.ActionType_ACTION_TYPE_RECREATE, level: tgsrlv1.ActionLevel_ACTION_LEVEL_L4, wantState: SandboxStateBound, wantGeneration: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := []MockOption{WithNow(func() time.Time { return fixtureNow })}
			if !test.withoutSandbox {
				options = append(options, WithSandboxes(testSandbox(test.initialState)))
			}
			provider := newTestProvider(t, options...)
			action := testAction("action-1", "plan-1", "key-1", test.actionType, test.level, 1)
			if test.configure != nil {
				test.configure(action)
			}

			result, err := provider.ExecuteAction(context.Background(), action)
			if err != nil {
				t.Fatalf("ExecuteAction() error = %v", err)
			}
			if result.GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED {
				t.Fatalf("ExecuteAction().Status = %s, want succeeded", result.GetStatus())
			}
			sandbox, err := provider.GetSandbox(context.Background(), "sandbox-a")
			if err != nil {
				t.Fatalf("GetSandbox() error = %v", err)
			}
			if sandbox.State != test.wantState || sandbox.Generation != test.wantGeneration {
				t.Fatalf("sandbox state/generation = %s/%d, want %s/%d", sandbox.State, sandbox.Generation, test.wantState, test.wantGeneration)
			}
			if sandbox.Share != test.wantShare || sandbox.Priority != test.wantPriority || sandbox.Offloaded != test.wantOffloaded {
				t.Fatalf("sandbox scalar state = share %v priority %d offloaded %v", sandbox.Share, sandbox.Priority, sandbox.Offloaded)
			}
			if test.actionType == tgsrlv1.ActionType_ACTION_TYPE_RESIZE && sandbox.Binding.GetResources().GetCpuMillis() != 2000 {
				t.Fatalf("resized CPU = %d, want 2000", sandbox.Binding.GetResources().GetCpuMillis())
			}
		})
	}
}

func TestSandboxWatchPublishesMutableStateAndOrderingMetadata(t *testing.T) {
	provider := newTestProvider(t)
	watch, err := provider.WatchSandboxes(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	action := testAction("bind-observed", "plan-observed", "bind-observed-key", tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	action.Share = 0.375
	action.Priority = 13
	if _, err := provider.ExecuteAction(context.Background(), action); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case watched := <-watch:
			event := watched.Event
			if event.GetSandboxId() != action.GetSandboxId() || event.GetDetail() != "action applied" {
				continue
			}
			if event.Share == nil || event.GetShare() != action.GetShare() || event.Priority == nil || event.GetPriority() != action.GetPriority() || event.Offloaded == nil || event.GetOffloaded() {
				t.Fatalf("sandbox event mutable state = %+v", event)
			}
			if event.GetProviderRevision() == 0 || event.GetIdempotencyKey() == "" {
				t.Fatalf("sandbox event ordering metadata = %+v", event)
			}
			if event.GetPlanId() != action.GetPlanId() || event.GetActionId() != action.GetActionId() || event.GetIdempotencyKey() != action.GetIdempotencyKey() {
				t.Fatalf("sandbox event action correlation = %+v, want plan/action/key %q/%q/%q", event, action.GetPlanId(), action.GetActionId(), action.GetIdempotencyKey())
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for bind sandbox event")
		}
	}
}

func TestExecuteActionIsIdempotentAndClonesResults(t *testing.T) {
	provider := newTestProvider(t, WithSandboxes(testSandbox(SandboxStateRunning)))
	action := testAction("share", "plan", "same-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	action.Share = 0.7

	first, err := provider.ExecuteAction(context.Background(), action)
	if err != nil {
		t.Fatalf("first ExecuteAction() error = %v", err)
	}
	first.ErrorCode = "caller-mutated"
	second, err := provider.ExecuteAction(context.Background(), proto.Clone(action).(*tgsrlv1.Action))
	if err != nil {
		t.Fatalf("duplicate ExecuteAction() error = %v", err)
	}
	if second.GetErrorCode() != "" || second.GetObservedRevision() != 2 {
		t.Fatalf("duplicate result = %v, want pristine result at revision 2", second)
	}
	snapshot, _ := provider.Snapshot(context.Background())
	if snapshot.GetRevision() != 2 {
		t.Fatalf("revision after duplicate = %d, want 2", snapshot.GetRevision())
	}

	conflict := proto.Clone(action).(*tgsrlv1.Action)
	conflict.Share = 0.9
	result, err := provider.ExecuteAction(context.Background(), conflict)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting ExecuteAction() error = %v, want ErrIdempotencyConflict", err)
	}
	if result.GetErrorCode() != ErrorCodeIdempotencyConflict {
		t.Fatalf("conflicting result code = %q, want %q", result.GetErrorCode(), ErrorCodeIdempotencyConflict)
	}
}

func TestExecuteActionRejectsUnsupportedCapability(t *testing.T) {
	capabilities := DefaultMockCapabilities()
	capabilities.SupportedActions = removeString(capabilities.GetSupportedActions(), "pause")
	provider := newTestProvider(t, WithCapabilities(capabilities), WithSandboxes(testSandbox(SandboxStateRunning)))
	action := testAction("pause", "plan", "pause-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)

	result, err := provider.ExecuteAction(context.Background(), action)
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("ExecuteAction() error = %v, want ErrUnsupported", err)
	}
	if result.GetErrorCode() != ErrorCodeUnsupported {
		t.Fatalf("result error code = %q, want %q", result.GetErrorCode(), ErrorCodeUnsupported)
	}
	snapshot, _ := provider.Snapshot(context.Background())
	if snapshot.GetRevision() != 1 {
		t.Fatalf("revision after unsupported action = %d, want 1", snapshot.GetRevision())
	}
}

func TestExecuteActionTickPolicyCompatibility(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*tgsrlv1.Action)
		wantErr  error
		wantCode string
	}{
		{
			name: "legacy unknown tick allows L1",
			mutate: func(action *tgsrlv1.Action) {
				action.TickKind = tgsrlv1.TickKind_TICK_KIND_UNKNOWN
			},
		},
		{
			name: "unknown tick rejects L2",
			mutate: func(action *tgsrlv1.Action) {
				action.ActionType = tgsrlv1.ActionType_ACTION_TYPE_PAUSE
				action.Level = tgsrlv1.ActionLevel_ACTION_LEVEL_L2
				action.TickKind = tgsrlv1.TickKind_TICK_KIND_UNKNOWN
			},
			wantErr:  ErrUnsupported,
			wantCode: ErrorCodeUnsupported,
		},
		{
			name: "fast rejects L2",
			mutate: func(action *tgsrlv1.Action) {
				action.ActionType = tgsrlv1.ActionType_ACTION_TYPE_PAUSE
				action.Level = tgsrlv1.ActionLevel_ACTION_LEVEL_L2
				action.TickKind = tgsrlv1.TickKind_TICK_KIND_FAST
			},
			wantErr:  ErrUnsupported,
			wantCode: ErrorCodeUnsupported,
		},
		{
			name: "medium allows L3",
			mutate: func(action *tgsrlv1.Action) {
				action.ActionType = tgsrlv1.ActionType_ACTION_TYPE_SLEEP
				action.Level = tgsrlv1.ActionLevel_ACTION_LEVEL_L3
				action.TickKind = tgsrlv1.TickKind_TICK_KIND_MEDIUM
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := newTestProvider(t, WithSandboxes(testSandbox(SandboxStateRunning)))
			action := testAction("action", "plan", "key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
			action.Share = 0.5
			test.mutate(action)

			result, err := provider.ExecuteAction(context.Background(), action)
			if test.wantErr == nil {
				if err != nil {
					t.Fatalf("ExecuteAction() error = %v", err)
				}
				if result.GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED {
					t.Fatalf("ExecuteAction() status = %s, want succeeded", result.GetStatus())
				}
				return
			}
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ExecuteAction() error = %v, want errors.Is(%v)", err, test.wantErr)
			}
			if result.GetErrorCode() != test.wantCode {
				t.Fatalf("result error code = %q, want %q", result.GetErrorCode(), test.wantCode)
			}
		})
	}
}

func TestExecuteActionRejectsInvalidPreconditionsWithoutMutation(t *testing.T) {
	tests := []struct {
		name     string
		sandbox  Sandbox
		mutate   func(*tgsrlv1.Action)
		wantErr  error
		wantCode string
	}{
		{
			name:    "stale revision",
			sandbox: testSandbox(SandboxStateRunning),
			mutate: func(action *tgsrlv1.Action) {
				action.ExpectedSnapshotRevision = 99
			},
			wantErr:  ErrFailedPrecondition,
			wantCode: ErrorCodeRevisionConflict,
		},
		{
			name:    "stale generation",
			sandbox: testSandbox(SandboxStateRunning),
			mutate: func(action *tgsrlv1.Action) {
				action.ExpectedGeneration = 99
			},
			wantErr:  ErrFailedPrecondition,
			wantCode: ErrorCodeGenerationConflict,
		},
		{
			name:    "safe point missing",
			sandbox: func() Sandbox { sandbox := testSandbox(SandboxStateRunning); sandbox.SafePoint = false; return sandbox }(),
			mutate: func(action *tgsrlv1.Action) {
				action.RequiresSafePoint = true
			},
			wantErr:  ErrFailedPrecondition,
			wantCode: ErrorCodeFailedPrecondition,
		},
		{
			name:     "invalid transition",
			sandbox:  testSandbox(SandboxStatePaused),
			mutate:   func(*tgsrlv1.Action) {},
			wantErr:  ErrFailedPrecondition,
			wantCode: ErrorCodeFailedPrecondition,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := newTestProvider(t, WithSandboxes(test.sandbox))
			action := testAction("pause", "plan", "key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
			test.mutate(action)
			result, err := provider.ExecuteAction(context.Background(), action)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ExecuteAction() error = %v, want errors.Is(%v)", err, test.wantErr)
			}
			if result.GetErrorCode() != test.wantCode {
				t.Fatalf("result error code = %q, want %q", result.GetErrorCode(), test.wantCode)
			}
			snapshot, _ := provider.Snapshot(context.Background())
			if snapshot.GetRevision() != 1 {
				t.Fatalf("revision after rejection = %d, want 1", snapshot.GetRevision())
			}
		})
	}
}

func TestRequiredCapabilitiesMatchConservatively(t *testing.T) {
	available := DefaultMockCapabilities()
	available.Names = append(available.Names, "generation-fencing")
	available.Attributes = map[string]string{"isolation": "sandbox"}
	available.Algorithms = []string{"ppo"}
	available.RolloutModes = []string{"sync"}
	available.Revision = 2
	available.Limits = map[string]float64{"partitions": 2}

	tests := []struct {
		name     string
		required *tgsrlv1.CapabilitySet
	}{
		{name: "source", required: &tgsrlv1.CapabilitySet{Source: "nvidia"}},
		{name: "revision", required: &tgsrlv1.CapabilitySet{Revision: 3}},
		{name: "algorithm", required: &tgsrlv1.CapabilitySet{Algorithms: []string{"grpo"}}},
		{name: "rollout mode", required: &tgsrlv1.CapabilitySet{RolloutModes: []string{"fully_async"}}},
		{name: "exact capability name", required: &tgsrlv1.CapabilitySet{Names: []string{"generation_fencing"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := newTestProvider(t, WithCapabilities(available), WithSandboxes(testSandbox(SandboxStateRunning)))
			action := testAction("share", "plan", "key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
			action.Share = 0.5
			action.RequiredCapabilities = test.required
			result, err := provider.ExecuteAction(context.Background(), action)
			if !errors.Is(err, ErrUnsupported) {
				t.Fatalf("ExecuteAction() error = %v, want ErrUnsupported", err)
			}
			if result.GetErrorCode() != ErrorCodeUnsupported {
				t.Fatalf("result error code = %q, want %q", result.GetErrorCode(), ErrorCodeUnsupported)
			}
		})
	}
}

func TestBindValidatesLogicalDevice(t *testing.T) {
	tests := []struct {
		name    string
		binding *tgsrlv1.Binding
	}{
		{name: "missing binding"},
		{name: "unknown device", binding: &tgsrlv1.Binding{SandboxId: "sandbox-a", Generation: 1, DeviceIds: []string{"missing-device"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := newTestProvider(t)
			action := testAction("bind", "plan", "key", tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
			action.Binding = test.binding
			result, err := provider.ExecuteAction(context.Background(), action)
			if err == nil || (!errors.Is(err, ErrInvalidArgument) && !errors.Is(err, ErrFailedPrecondition)) {
				t.Fatalf("ExecuteAction() error = %v, want invalid argument or failed precondition", err)
			}
			if result.GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED {
				t.Fatalf("result status = %s, want failed", result.GetStatus())
			}
			if _, lookupErr := provider.GetSandbox(context.Background(), "sandbox-a"); !errors.Is(lookupErr, ErrNotFound) {
				t.Fatalf("GetSandbox() error = %v, want ErrNotFound", lookupErr)
			}
		})
	}
}

func TestExecuteActionDeadlineAndFailedRetry(t *testing.T) {
	provider := newTestProvider(t,
		WithSandboxes(testSandbox(SandboxStateRunning)),
		WithFaults(FaultOptions{DelayByActionType: map[tgsrlv1.ActionType]time.Duration{
			tgsrlv1.ActionType_ACTION_TYPE_PAUSE: 2 * time.Second,
		}}),
	)
	action := testAction("pause", "plan", "timeout-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	action.Deadline = timestamppb.New(fixtureNow.Add(time.Second))

	first, err := provider.ExecuteAction(context.Background(), action)
	if !errors.Is(err, ErrDeadlineExceeded) {
		t.Fatalf("first ExecuteAction() error = %v, want ErrDeadlineExceeded", err)
	}
	if first.GetErrorCode() != ErrorCodeDeadlineExceeded {
		t.Fatalf("first result code = %q, want %q", first.GetErrorCode(), ErrorCodeDeadlineExceeded)
	}
	second, err := provider.ExecuteAction(context.Background(), proto.Clone(action).(*tgsrlv1.Action))
	if !errors.Is(err, ErrDeadlineExceeded) {
		t.Fatalf("duplicate failed ExecuteAction() error = %v, want recorded ErrDeadlineExceeded", err)
	}
	if !proto.Equal(first, second) {
		t.Fatalf("duplicate failed result differs: first=%v second=%v", first, second)
	}
	sandbox, _ := provider.GetSandbox(context.Background(), "sandbox-a")
	if sandbox.State != SandboxStateRunning {
		t.Fatalf("sandbox state after timeout = %s, want running", sandbox.State)
	}
}

func TestTransactionPartialFailureRollsBackInReverseAndIsIdempotent(t *testing.T) {
	initial := testSandbox(SandboxStateRunning)
	initial.Share = 0.25
	initial.Priority = 3
	provider := newTestProvider(t,
		WithSandboxes(initial),
		WithFaults(FaultOptions{PartialFailureAt: 3}),
	)
	first := testAction("share", "plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	first.Order = 1
	first.Share = 0.8
	first.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	second := testAction("priority", "plan", "priority-key", tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	second.Order = 2
	second.Priority = 99
	second.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, TargetId: "sandbox-a"}
	third := testAction("pause", "plan", "pause-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	third.Order = 3
	third.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}
	plan := strictReconciliationPlan("plan", 1, first, second, third)

	results, err := executeTestTransaction(context.Background(), provider, plan)
	if !errors.Is(err, ErrPartialFailure) {
		t.Fatalf("transaction error = %v, want ErrPartialFailure", err)
	}
	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}
	if results[0].GetActionId() != "share" || !results[0].GetRollbackAttempted() || results[0].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK {
		t.Fatalf("first result = %v, want rolled back share", results[0])
	}
	if results[1].GetActionId() != "priority" || !results[1].GetRollbackAttempted() || results[1].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK {
		t.Fatalf("second result = %v, want rolled back priority", results[1])
	}
	if results[2].GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED || results[2].GetErrorCode() != ErrorCodeInjectedFailure {
		t.Fatalf("third result = %v, want injected failure", results[2])
	}
	sandbox, _ := provider.GetSandbox(context.Background(), "sandbox-a")
	if sandbox.Share != initial.Share || sandbox.Priority != initial.Priority || sandbox.State != initial.State {
		t.Fatalf("sandbox after rollback = %+v, want original %+v", sandbox, initial)
	}
	results[0].ErrorCode = "caller-mutated"
	snapshot, _ := provider.Snapshot(context.Background())
	revision := snapshot.GetRevision()
	retry, retryErr := provider.PreparePlan(context.Background(), plan.GetPlanId(), 1, proto.Clone(plan).(*tgsrlv1.PlacementPlan))
	if retryErr != nil {
		t.Fatalf("retry PreparePlan() error = %v", retryErr)
	}
	if retry.Phase != TransactionPhaseAborted || len(retry.Effects) != 3 || retry.Effects[0].Status != EffectStatusCompensated {
		t.Fatalf("retry receipt = %+v, want detached aborted receipt", retry)
	}
	snapshot, _ = provider.Snapshot(context.Background())
	if snapshot.GetRevision() != revision {
		t.Fatalf("revision after plan retry = %d, want %d", snapshot.GetRevision(), revision)
	}
}

func TestTransactionRejectsPlanIDReuseWithDifferentPayload(t *testing.T) {
	provider := newTestProvider(t, WithSandboxes(testSandbox(SandboxStateRunning)))
	action := testAction("share", "plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	action.Share = 0.5
	action.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	plan := strictReconciliationPlan("plan", 1, action)
	if _, err := provider.PreparePlan(context.Background(), plan.GetPlanId(), 1, plan); err != nil {
		t.Fatalf("first PreparePlan() error = %v", err)
	}

	conflict := proto.Clone(plan).(*tgsrlv1.PlacementPlan)
	conflict.Actions[0].ActionId = "different-action"
	conflict.Actions[0].IdempotencyKey = "different-key"
	if _, err := provider.PreparePlan(context.Background(), conflict.GetPlanId(), 1, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting PreparePlan() error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestTransactionRequiresExplicitRollback(t *testing.T) {
	provider := newTestProvider(t, WithSandboxes(testSandbox(SandboxStateRunning)))
	action := testAction("share", "plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	action.Share = 0.5
	plan := strictReconciliationPlan("plan", 1, action)

	if _, err := provider.PreparePlan(context.Background(), plan.GetPlanId(), 1, plan); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("PreparePlan() error = %v, want ErrInvalidArgument for missing rollback", err)
	}
	snapshot, _ := provider.Snapshot(context.Background())
	if snapshot.GetRevision() != 1 {
		t.Fatalf("revision after invalid plan = %d, want 1", snapshot.GetRevision())
	}
}

func TestTransactionRejectsTickPolicyMismatch(t *testing.T) {
	provider := newTestProvider(t, WithSandboxes(testSandbox(SandboxStateRunning)))
	first := testAction("share", "plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	first.TickKind = tgsrlv1.TickKind_TICK_KIND_MEDIUM
	first.Share = 0.5
	first.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	second := testAction("sleep", "plan", "sleep-key", tgsrlv1.ActionType_ACTION_TYPE_SLEEP, tgsrlv1.ActionLevel_ACTION_LEVEL_L3, 1)
	second.TickKind = tgsrlv1.TickKind_TICK_KIND_FAST
	second.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}

	plan := &tgsrlv1.PlacementPlan{
		PlanId:           "plan",
		SnapshotRevision: 1,
		Actions:          []*tgsrlv1.Action{first, second},
	}
	if _, err := provider.PreparePlan(context.Background(), plan.GetPlanId(), 1, plan); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("PreparePlan() error = %v, want ErrUnsupported", err)
	}
}

func TestTransactionPreparesAtomicReplacementWithoutMutation(t *testing.T) {
	initial := testSandbox(SandboxStateRunning)
	provider := newTestProvider(t, WithSandboxes(initial))
	release := testAction("release", "preempt", "release-key", tgsrlv1.ActionType_ACTION_TYPE_RELEASE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	release.TargetId = "allocation-1"
	release.SandboxId = "sandbox-a"
	release.RequiresSafePoint = true
	release.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND, TargetId: "sandbox-a", RestoreBinding: testBinding("sandbox-a", 1)}
	bind := testAction("replacement", "preempt", "bind-key", tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	bind.Binding.RuntimeUnitId = "runtime-replacement"
	bind.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, TargetId: bind.GetBinding().GetBindingId()}
	plan := strictReconciliationPlan("preempt", 1, release, bind)
	plan.Purpose = tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION
	plan.AffectedAllocationIds = []string{"allocation-1"}
	plan.CapabilityRequirements = actionpolicy.StableCapabilityRequirements(
		append(plan.CapabilityRequirements,
			actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_TRANSACTIONAL_PLAN_EXECUTION),
			actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ATOMIC_REPLACEMENT),
		)...,
	)
	release.Preconditions = actionpolicy.RequiredPreconditions(release.GetActionType(), true, true)

	receipt, err := provider.PreparePlan(context.Background(), plan.GetPlanId(), 1, plan)
	if err != nil || receipt.Phase != TransactionPhasePrepared {
		t.Fatalf("PreparePlan(preemption) = (%+v, %v), want prepared", receipt, err)
	}
	after, err := provider.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if after.State != initial.State || after.Generation != initial.Generation || !proto.Equal(after.Binding, initial.Binding) {
		t.Fatalf("preemption prepare mutated sandbox: got %+v want %+v", after, initial)
	}
	snapshot, _ := provider.Snapshot(context.Background())
	if snapshot.GetRevision() != 1 {
		t.Fatalf("preemption prepare advanced revision to %d", snapshot.GetRevision())
	}
}

func TestTransactionCapabilitiesMatchExecutableContract(t *testing.T) {
	provider := newTestProvider(t)
	capabilities, err := provider.DescribeCapabilities(context.Background())
	if err != nil {
		t.Fatalf("DescribeCapabilities() error = %v", err)
	}
	if !capabilities.OrderedStepExecution || !capabilities.CompensatingAbort || !capabilities.StepIdempotency || !capabilities.GenerationFence || !capabilities.PartialEffectReporting || !capabilities.AtomicReplacement {
		t.Fatalf("DescribeCapabilities() = %+v, want complete transaction contract", capabilities)
	}
}

func TestValidateExecutionCapabilitiesHonorsRequiredProviderCapability(t *testing.T) {
	provider := newTestProvider(t)
	plan := &tgsrlv1.PlacementPlan{PlanId: "capability-plan"}
	optional := &tgsrlv1.CapabilityRequirement{
		Kind:       tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_PROVIDER_CAPABILITY,
		Name:       "missing",
		MinVersion: "not-semver",
		Required:   false,
	}
	plan.CapabilityRequirements = []*tgsrlv1.CapabilityRequirement{optional}
	if err := ValidateExecutionCapabilities(context.Background(), provider, plan); err != nil {
		t.Fatalf("optional capability blocked handshake: %v", err)
	}

	plan.CapabilityRequirements = []*tgsrlv1.CapabilityRequirement{{
		Kind:       tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_PROVIDER_CAPABILITY,
		Name:       " Logical-CPU ",
		MinVersion: "1.0.0",
		Required:   true,
	}}
	if err := ValidateExecutionCapabilities(context.Background(), provider, plan); err != nil {
		t.Fatalf("normalized provider capability handshake failed: %v", err)
	}

	plan.CapabilityRequirements[0].MinVersion = "1.0.1"
	if err := ValidateExecutionCapabilities(context.Background(), provider, plan); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("newer provider capability version error = %v, want ErrUnsupported", err)
	}
	plan.CapabilityRequirements[0].Name = "logical-gpu"
	plan.CapabilityRequirements[0].MinVersion = ""
	if err := ValidateExecutionCapabilities(context.Background(), provider, plan); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("missing provider capability error = %v, want ErrUnsupported", err)
	}
}

func TestVersionAtLeastUsesStrictSemanticVersioning(t *testing.T) {
	tests := []struct {
		name      string
		available string
		required  string
		want      bool
		wantErr   bool
	}{
		{name: "greater", available: "1.2.0", required: "1.1.9", want: true},
		{name: "equal", available: "1.2.0", required: "1.2.0", want: true},
		{name: "lower", available: "1.1.9", required: "1.2.0"},
		{name: "prerelease lower than release", available: "1.2.0-rc.1", required: "1.2.0"},
		{name: "prerelease ordering", available: "1.2.0-rc.2", required: "1.2.0-rc.1", want: true},
		{name: "malformed available", available: "1.2", required: "1.1.9", wantErr: true},
		{name: "malformed required", available: "1.2.0", required: "v1.1.9", wantErr: true},
		{name: "leading zero", available: "01.2.0", required: "1.1.9", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := versionAtLeast(test.available, test.required)
			if (err != nil) != test.wantErr {
				t.Fatalf("versionAtLeast(%q, %q) error = %v, wantErr %v", test.available, test.required, err, test.wantErr)
			}
			if got != test.want {
				t.Fatalf("versionAtLeast(%q, %q) = %v, want %v", test.available, test.required, got, test.want)
			}
		})
	}
}

func TestTransactionRejectsMissingProviderCapabilityBeforeMutationOrRecord(t *testing.T) {
	initial := testSandbox(SandboxStateRunning)
	provider := newTestProvider(t, WithSandboxes(initial))
	action := testAction("share", "unsupported-plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	action.Share = 0.5
	action.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	plan := strictReconciliationPlan("unsupported-plan", 1, action)
	plan.CapabilityRequirements = actionpolicy.StableCapabilityRequirements(&tgsrlv1.CapabilityRequirement{
		Kind:     tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_PROVIDER_CAPABILITY,
		Name:     "unavailable-provider-feature",
		Required: true,
	})

	if _, err := provider.PreparePlan(context.Background(), plan.GetPlanId(), 1, plan); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("PreparePlan() error = %v, want ErrUnsupported", err)
	}
	sandbox, err := provider.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatal(err)
	}
	if sandbox.Share != initial.Share || sandbox.State != initial.State {
		t.Fatalf("rejected plan mutated sandbox: %+v", sandbox)
	}
	snapshot, _ := provider.Snapshot(context.Background())
	if snapshot.GetRevision() != 1 {
		t.Fatalf("rejected plan advanced revision to %d", snapshot.GetRevision())
	}
	if _, err := provider.ReconcilePlan(context.Background(), plan); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected plan record error = %v, want ErrNotFound", err)
	}
}

func TestTransactionUsesActionOrder(t *testing.T) {
	provider := newTestProvider(t, WithSandboxes(testSandbox(SandboxStateRunning)))
	pause := testAction("pause", "plan", "pause-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	pause.Order = 1
	pause.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}
	resume := testAction("resume", "plan", "resume-key", tgsrlv1.ActionType_ACTION_TYPE_RESUME, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	resume.Order = 2
	resume.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_PAUSE, TargetId: "sandbox-a"}
	plan := strictReconciliationPlan("plan", 1, pause, resume)

	results, err := executeTestTransaction(context.Background(), provider, plan)
	if err != nil {
		t.Fatalf("transaction error = %v", err)
	}
	if len(results) != 2 || results[0].GetActionId() != "pause" || results[1].GetActionId() != "resume" {
		t.Fatalf("transaction result order = %v, want pause then resume", results)
	}
	sandbox, _ := provider.GetSandbox(context.Background(), "sandbox-a")
	if sandbox.State != SandboxStateRunning {
		t.Fatalf("sandbox state = %s, want running", sandbox.State)
	}
}

func TestTransactionMarksRemainingActionsSkipped(t *testing.T) {
	provider := newTestProvider(t,
		WithSandboxes(testSandbox(SandboxStateRunning)),
		WithFaults(FaultOptions{PartialFailureAt: 2}),
	)
	share := testAction("share", "plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	share.Share = 0.5
	share.Order = 1
	share.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	pause := testAction("pause", "plan", "pause-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	pause.Order = 2
	pause.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}
	priority := testAction("priority", "plan", "priority-key", tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	priority.Priority = 42
	priority.Order = 3
	priority.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, TargetId: "sandbox-a"}
	plan := strictReconciliationPlan("plan", 1, share, pause, priority)

	results, err := executeTestTransaction(context.Background(), provider, plan)
	if !errors.Is(err, ErrPartialFailure) {
		t.Fatalf("transaction error = %v, want ErrPartialFailure", err)
	}
	if len(results) != 3 || results[2].GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SKIPPED {
		t.Fatalf("results = %v, want third action skipped", results)
	}
}

func TestExecuteActionContextCancellation(t *testing.T) {
	provider := newTestProvider(t,
		WithSandboxes(testSandbox(SandboxStateRunning)),
		WithFaults(FaultOptions{Delay: time.Second}),
	)
	action := testAction("pause", "plan", "key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := provider.ExecuteAction(ctx, action)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrDeadlineExceeded) {
		t.Fatalf("ExecuteAction() error = %v, want context.Canceled and ErrDeadlineExceeded", err)
	}
	if result.GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED {
		t.Fatalf("result status = %s, want failed", result.GetStatus())
	}
	sandbox, _ := provider.GetSandbox(context.Background(), "sandbox-a")
	if sandbox.State != SandboxStateRunning {
		t.Fatalf("sandbox state after canceled action = %s, want running", sandbox.State)
	}
}

func TestConfiguredRollbackFailureIsExplicit(t *testing.T) {
	initial := testSandbox(SandboxStateRunning)
	initial.Share = 0.25
	provider := newTestProvider(t,
		WithSandboxes(initial),
		WithFaults(FaultOptions{
			PartialFailureAt:      2,
			FailRollbackActionIDs: map[string]InjectedFailure{"share": {Message: "rollback unavailable"}},
		}),
	)
	share := testAction("share", "plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	share.Share = 0.8
	share.Order = 1
	share.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	pause := testAction("pause", "plan", "pause-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	pause.Order = 2
	pause.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}
	plan := strictReconciliationPlan("plan", 1, share, pause)

	results, err := executeTestTransaction(context.Background(), provider, plan)
	if !errors.Is(err, ErrPartialFailure) {
		t.Fatalf("ExecutePlan() error = %v, want ErrPartialFailure", err)
	}
	if !results[0].GetRollbackAttempted() || results[0].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED {
		t.Fatalf("first result = %v, want explicit failed rollback", results[0])
	}
	sandbox, _ := provider.GetSandbox(context.Background(), "sandbox-a")
	if sandbox.Share != 0.8 {
		t.Fatalf("share after failed rollback = %v, want committed 0.8", sandbox.Share)
	}
}

func TestRollbackFailureIsNotUndoneByEarlierRollbackOnSameSandbox(t *testing.T) {
	initial := testSandbox(SandboxStateRunning)
	initial.Share = 0.25
	initial.Priority = 3
	provider := newTestProvider(t,
		WithSandboxes(initial),
		WithFaults(FaultOptions{
			PartialFailureAt:      3,
			FailRollbackActionIDs: map[string]InjectedFailure{"priority": {Message: "rollback unavailable"}},
		}),
	)
	share := testAction("share", "plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	share.Order = 1
	share.Share = 0.8
	share.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	priority := testAction("priority", "plan", "priority-key", tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	priority.Order = 2
	priority.Priority = 99
	priority.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, TargetId: "sandbox-a"}
	pause := testAction("pause", "plan", "pause-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	pause.Order = 3
	pause.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}

	results, err := executeTestTransaction(context.Background(), provider, strictReconciliationPlan("plan", 1, share, priority, pause))
	if !errors.Is(err, ErrPartialFailure) {
		t.Fatalf("ExecutePlan() error = %v, want ErrPartialFailure", err)
	}
	if results[0].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK || results[1].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED {
		t.Fatalf("rollback results = %v, want share rolled back and priority rollback failed", results)
	}
	sandbox, lookupErr := provider.GetSandbox(context.Background(), "sandbox-a")
	if lookupErr != nil {
		t.Fatalf("GetSandbox() error = %v", lookupErr)
	}
	if sandbox.Share != initial.Share || sandbox.Priority != 99 {
		t.Fatalf("sandbox after rollback = share %v priority %d, want %v/99", sandbox.Share, sandbox.Priority, initial.Share)
	}
}

func TestOverlappingRollbackFailurePreservesLatestSideEffect(t *testing.T) {
	initial := testSandbox(SandboxStateRunning)
	initial.Share = 0.25
	provider := newTestProvider(t,
		WithSandboxes(initial),
		WithFaults(FaultOptions{
			PartialFailureAt:      3,
			FailRollbackActionIDs: map[string]InjectedFailure{"share-latest": {Message: "rollback unavailable"}},
		}),
	)
	first := testAction("share-first", "plan", "share-first-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	first.Order = 1
	first.Share = 0.5
	first.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	latest := testAction("share-latest", "plan", "share-latest-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	latest.Order = 2
	latest.Share = 0.8
	latest.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	pause := testAction("pause", "plan", "pause-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	pause.Order = 3
	pause.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}

	results, err := executeTestTransaction(context.Background(), provider, strictReconciliationPlan("plan", 1, first, latest, pause))
	if !errors.Is(err, ErrPartialFailure) {
		t.Fatalf("ExecutePlan() error = %v, want ErrPartialFailure", err)
	}
	if results[0].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK || results[1].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED {
		t.Fatalf("rollback results = %v, want first rolled back and latest rollback failed", results)
	}
	sandbox, lookupErr := provider.GetSandbox(context.Background(), "sandbox-a")
	if lookupErr != nil {
		t.Fatalf("GetSandbox() error = %v", lookupErr)
	}
	if sandbox.Share != latest.Share {
		t.Fatalf("share after overlapping rollback = %v, want latest side effect %v", sandbox.Share, latest.Share)
	}
}

func TestRollbackDoesNotOverwriteAnotherSandbox(t *testing.T) {
	firstSandbox := testSandbox(SandboxStateRunning)
	firstSandbox.Share = 0.25
	secondSandbox := testSandbox(SandboxStateRunning)
	secondSandbox.SandboxID = "sandbox-b"
	secondSandbox.Binding = testBinding("sandbox-b", 1)
	secondSandbox.Binding.BindingId = "binding-b"
	secondSandbox.Priority = 3
	provider := newTestProvider(t,
		WithSandboxes(firstSandbox, secondSandbox),
		WithFaults(FaultOptions{
			PartialFailureAt:      3,
			FailRollbackActionIDs: map[string]InjectedFailure{"priority-b": {Message: "rollback unavailable"}},
		}),
	)
	share := testAction("share-a", "plan", "share-a-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	share.Order = 1
	share.Share = 0.8
	share.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	priority := testAction("priority-b", "plan", "priority-b-key", tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	priority.Order = 2
	priority.TargetId = "sandbox-b"
	priority.SandboxId = "sandbox-b"
	priority.Binding = testBinding("sandbox-b", 1)
	priority.Priority = 99
	priority.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, TargetId: "sandbox-b"}
	pause := testAction("pause", "plan", "pause-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	pause.Order = 3
	pause.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}

	results, err := executeTestTransaction(context.Background(), provider, strictReconciliationPlan("plan", 1, share, priority, pause))
	if !errors.Is(err, ErrPartialFailure) {
		t.Fatalf("ExecutePlan() error = %v, want ErrPartialFailure", err)
	}
	if results[0].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK || results[1].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED {
		t.Fatalf("rollback results = %v, want sandbox-a rolled back and sandbox-b rollback failed", results)
	}
	firstAfter, firstErr := provider.GetSandbox(context.Background(), "sandbox-a")
	if firstErr != nil {
		t.Fatalf("GetSandbox(sandbox-a) error = %v", firstErr)
	}
	secondAfter, secondErr := provider.GetSandbox(context.Background(), "sandbox-b")
	if secondErr != nil {
		t.Fatalf("GetSandbox(sandbox-b) error = %v", secondErr)
	}
	if firstAfter.Share != firstSandbox.Share || secondAfter.Priority != 99 {
		t.Fatalf("sandboxes after rollback = a.share %v b.priority %d, want %v/99", firstAfter.Share, secondAfter.Priority, firstSandbox.Share)
	}
}

func TestTransactionRejectsFalseRollbackDeclarations(t *testing.T) {
	tests := []struct {
		name     string
		rollback *tgsrlv1.Rollback
	}{
		{name: "wrong action type", rollback: &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY, TargetId: "sandbox-a"}},
		{name: "wrong target", rollback: &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-b"}},
		{name: "unexpected restore binding", rollback: &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a", RestoreBinding: testBinding("sandbox-a", 1)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			initial := testSandbox(SandboxStateRunning)
			initial.Share = 0.25
			provider := newTestProvider(t, WithSandboxes(initial))
			action := testAction("share", "plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
			action.Share = 0.8
			action.Rollback = test.rollback

			if _, err := executeTestTransaction(context.Background(), provider, strictReconciliationPlan("plan", 1, action)); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("ExecutePlan() error = %v, want ErrInvalidArgument", err)
			}
			sandbox, lookupErr := provider.GetSandbox(context.Background(), "sandbox-a")
			if lookupErr != nil {
				t.Fatalf("GetSandbox() error = %v", lookupErr)
			}
			if sandbox.Share != initial.Share {
				t.Fatalf("share after rejected plan = %v, want %v", sandbox.Share, initial.Share)
			}
		})
	}
}

func TestTransactionRejectsMismatchedRestoreBindingAndRollsBackPriorMutation(t *testing.T) {
	initial := testSandbox(SandboxStateRunning)
	initial.Share = 0.25
	provider := newTestProvider(t, WithSandboxes(initial))
	share := testAction("share", "plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	share.Order = 1
	share.Share = 0.8
	share.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	resize := testAction("resize", "plan", "resize-key", tgsrlv1.ActionType_ACTION_TYPE_RESIZE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	resize.Order = 2
	resize.Binding.Resources.CpuMillis = 2000
	mismatched := testBinding("sandbox-a", 1)
	mismatched.Resources.CpuMillis = 500
	resize.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESIZE, TargetId: "sandbox-a", RestoreBinding: mismatched}

	results, err := executeTestTransaction(context.Background(), provider, strictReconciliationPlan("plan", 1, share, resize))
	if !errors.Is(err, ErrPartialFailure) {
		t.Fatalf("ExecutePlan() error = %v, want ErrPartialFailure", err)
	}
	if results[0].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK || results[1].GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED {
		t.Fatalf("results = %v, want first action rolled back and invalid resize failed", results)
	}
	sandbox, lookupErr := provider.GetSandbox(context.Background(), "sandbox-a")
	if lookupErr != nil {
		t.Fatalf("GetSandbox() error = %v", lookupErr)
	}
	if sandbox.Share != initial.Share || !proto.Equal(sandbox.Binding, initial.Binding) {
		t.Fatalf("sandbox after invalid rollback compensation = %+v, want original state", sandbox)
	}
}

func TestTransactionBindRollbackUsesBindingTarget(t *testing.T) {
	provider := newTestProvider(t, WithFaults(FaultOptions{PartialFailureAt: 2}))
	bind := testAction("bind", "plan", "bind-key", tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	bind.Order = 1
	bind.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, TargetId: bind.GetBinding().GetBindingId()}
	share := testAction("share", "plan", "share-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	share.Order = 2
	share.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}

	results, err := executeTestTransaction(context.Background(), provider, strictReconciliationPlan("plan", 1, bind, share))
	if !errors.Is(err, ErrPartialFailure) {
		t.Fatalf("ExecutePlan() error = %v, want ErrPartialFailure", err)
	}
	if results[0].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK {
		t.Fatalf("bind result = %v, want rolled back", results[0])
	}
	if _, lookupErr := provider.GetSandbox(context.Background(), "sandbox-a"); !errors.Is(lookupErr, ErrNotFound) {
		t.Fatalf("GetSandbox() error = %v, want ErrNotFound after bind rollback", lookupErr)
	}
}

func TestL4GenerationFencesLateEvent(t *testing.T) {
	provider := newTestProvider(t, WithSandboxes(testSandbox(SandboxStateRunning)))
	action := testAction("recreate", "plan", "recreate-key", tgsrlv1.ActionType_ACTION_TYPE_RECREATE, tgsrlv1.ActionLevel_ACTION_LEVEL_L4, 1)
	if _, err := provider.ExecuteAction(context.Background(), action); err != nil {
		t.Fatalf("ExecuteAction() error = %v", err)
	}
	before, _ := provider.Snapshot(context.Background())
	_, err := provider.ObserveSandbox(context.Background(), &tgsrlv1.SandboxEvent{
		EventId:    "late-event",
		SandboxId:  "sandbox-a",
		Generation: 1,
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
	})
	if !errors.Is(err, ErrGenerationFenced) {
		t.Fatalf("ObserveSandbox() error = %v, want ErrGenerationFenced", err)
	}
	after, _ := provider.Snapshot(context.Background())
	if after.GetRevision() != before.GetRevision() {
		t.Fatalf("fenced event advanced revision from %d to %d", before.GetRevision(), after.GetRevision())
	}
	sandbox, _ := provider.GetSandbox(context.Background(), "sandbox-a")
	if sandbox.Generation != 2 || sandbox.State != SandboxStateBound {
		t.Fatalf("sandbox after late event = generation %d state %s, want 2/bound", sandbox.Generation, sandbox.State)
	}
}

func TestObserveSandboxClonesBinding(t *testing.T) {
	provider := newTestProvider(t)
	safePoint := true
	binding := testBinding("sandbox-a", 1)
	_, err := provider.ObserveSandbox(context.Background(), &tgsrlv1.SandboxEvent{
		EventId:    "cloned-binding-event",
		SandboxId:  "sandbox-a",
		Generation: 1,
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		Binding:    binding,
		SafePoint:  &safePoint,
	})
	if err != nil {
		t.Fatalf("ObserveSandbox() error = %v", err)
	}
	binding.DeviceIds[0] = "caller-mutated"
	sandbox, err := provider.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if sandbox.Binding.GetDeviceIds()[0] != "mock-cpu-0" || !sandbox.SafePoint {
		t.Fatalf("sandbox event was not cloned or applied: %+v", sandbox)
	}
}

func TestObserveSandboxPreservesUnknownSafePointAndDeduplicatesEvent(t *testing.T) {
	provider := newTestProvider(t, WithSandboxes(testSandbox(SandboxStateRunning)))
	event := &tgsrlv1.SandboxEvent{
		EventId: "observation-without-safe-point", SandboxId: "sandbox-a", Generation: 1,
		State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, OccurredAt: timestamppb.New(fixtureNow),
	}
	before, _ := provider.Snapshot(context.Background())
	if _, err := provider.ObserveSandbox(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	observed, err := provider.GetSandbox(context.Background(), "sandbox-a")
	if err != nil || !observed.SafePoint {
		t.Fatalf("missing safe-point field overwrote known value: sandbox=%+v err=%v", observed, err)
	}
	afterFirst, _ := provider.Snapshot(context.Background())
	if afterFirst.GetRevision() != before.GetRevision()+1 {
		t.Fatalf("first observation revision = %d, want %d", afterFirst.GetRevision(), before.GetRevision()+1)
	}
	if _, err := provider.ObserveSandbox(context.Background(), proto.Clone(event).(*tgsrlv1.SandboxEvent)); err != nil {
		t.Fatalf("idempotent observation error = %v", err)
	}
	afterReplay, _ := provider.Snapshot(context.Background())
	if afterReplay.GetRevision() != afterFirst.GetRevision() {
		t.Fatalf("idempotent observation advanced revision to %d", afterReplay.GetRevision())
	}
}

func TestFaultConfigurationIsCloned(t *testing.T) {
	failures := map[string]InjectedFailure{"pause": {Code: "UNAVAILABLE", Message: "mock outage"}}
	provider := newTestProvider(t, WithSandboxes(testSandbox(SandboxStateRunning)), WithFaults(FaultOptions{FailActionIDs: failures}))
	delete(failures, "pause")
	action := testAction("pause", "plan", "key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	result, err := provider.ExecuteAction(context.Background(), action)
	if err == nil {
		t.Fatal("ExecuteAction() error = nil, want configured failure")
	}
	if result.GetErrorCode() != "UNAVAILABLE" {
		t.Fatalf("result code = %q, want UNAVAILABLE", result.GetErrorCode())
	}
}

func TestConcurrentDuplicateActionHasOneSideEffect(t *testing.T) {
	provider := newTestProvider(t, WithSandboxes(testSandbox(SandboxStateRunning)))
	action := testAction("share", "plan", "same-key", tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1)
	action.Share = 0.6

	const workers = 32
	var waitGroup sync.WaitGroup
	errorsFound := make(chan error, workers)
	for range workers {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			result, err := provider.ExecuteAction(context.Background(), action)
			if err != nil {
				errorsFound <- err
				return
			}
			if result.GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED {
				errorsFound <- fmt.Errorf("status = %s", result.GetStatus())
			}
		}()
	}
	waitGroup.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent ExecuteAction() error = %v", err)
	}
	snapshot, _ := provider.Snapshot(context.Background())
	if snapshot.GetRevision() != 2 {
		t.Fatalf("revision after duplicate storm = %d, want 2", snapshot.GetRevision())
	}
}

func TestMockTransactionalProviderAbortCompensatesInReverseOrder(t *testing.T) {
	provider := newTestProvider(t, WithSandboxes(testSandbox(SandboxStateRunning)))
	txp, ok := any(provider).(TransactionalResourceProvider)
	if !ok {
		t.Fatal("mock provider does not implement TransactionalResourceProvider")
	}

	pause := testAction("pause-tx", "tx-mock", "pause-tx-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	pause.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}
	resume := testAction("resume-tx", "tx-mock", "resume-tx-key", tgsrlv1.ActionType_ACTION_TYPE_RESUME, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	resume.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_PAUSE, TargetId: "sandbox-a"}
	plan := strictReconciliationPlan("tx-mock", 1, pause, resume)

	prepared, err := txp.PreparePlan(context.Background(), "tx-mock", 3, plan)
	if err != nil {
		t.Fatalf("PreparePlan() error = %v", err)
	}
	if prepared.Generation != 3 || prepared.Phase != TransactionPhasePrepared {
		t.Fatalf("prepared receipt = %+v", prepared)
	}

	first, err := txp.ExecuteStep(context.Background(), "tx-mock", 3, 0)
	if err != nil {
		t.Fatalf("ExecuteStep(0) error = %v", err)
	}
	if first.Effects[0].Status != EffectStatusApplied || first.Effects[1].Status != EffectStatusNotApplied {
		t.Fatalf("first receipt = %+v", first)
	}
	second, err := txp.ExecuteStep(context.Background(), "tx-mock", 3, 1)
	if err != nil {
		t.Fatalf("ExecuteStep(1) error = %v", err)
	}
	if second.Effects[1].Status != EffectStatusApplied {
		t.Fatalf("second receipt = %+v", second)
	}

	aborted, err := txp.AbortPlan(context.Background(), "tx-mock", 3)
	if err != nil {
		t.Fatalf("AbortPlan() error = %v", err)
	}
	if aborted.Phase != TransactionPhaseAborted {
		t.Fatalf("abort phase = %s, want aborted", aborted.Phase)
	}
	if aborted.Effects[0].Status != EffectStatusCompensated || aborted.Effects[1].Status != EffectStatusCompensated {
		t.Fatalf("abort receipt effects = %+v", aborted.Effects)
	}
	sandbox, err := provider.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if sandbox.State != SandboxStateRunning {
		t.Fatalf("sandbox state after abort = %s, want running", sandbox.State)
	}
}

func TestMockTransactionalProviderGenerationFence(t *testing.T) {
	provider := newTestProvider(t, WithSandboxes(testSandbox(SandboxStateRunning)))
	txp := any(provider).(TransactionalResourceProvider)
	action := testAction("pause-fence", "tx-fence", "pause-fence-key", tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)
	action.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}
	plan := strictReconciliationPlan("tx-fence", 1, action)

	if _, err := txp.PreparePlan(context.Background(), "tx-fence", 5, plan); err != nil {
		t.Fatalf("PreparePlan() error = %v", err)
	}
	if _, err := txp.ExecuteStep(context.Background(), "tx-fence", 4, 0); !errors.Is(err, ErrGenerationFenced) {
		t.Fatalf("ExecuteStep() error = %v, want ErrGenerationFenced", err)
	}
}

func executeTestTransaction(ctx context.Context, p TransactionalResourceProvider, plan *tgsrlv1.PlacementPlan) ([]*tgsrlv1.ActionResult, error) {
	const generation = uint64(1)
	_, err := p.PreparePlan(ctx, plan.GetPlanId(), generation, plan)
	if err != nil {
		return nil, err
	}
	var receipt *TransactionReceipt
	for index := range plan.GetActions() {
		receipt, err = p.ExecuteStep(ctx, plan.GetPlanId(), generation, index)
		if err != nil {
			aborted, abortErr := p.AbortPlan(context.WithoutCancel(ctx), plan.GetPlanId(), generation)
			if aborted != nil {
				receipt = aborted
			}
			return transactionReceiptResults(receipt), errors.Join(ErrPartialFailure, err, abortErr)
		}
	}
	receipt, err = p.CommitPlan(ctx, plan.GetPlanId(), generation)
	return transactionReceiptResults(receipt), err
}

func newTestProvider(t *testing.T, options ...MockOption) *MockResourceProvider {
	t.Helper()
	options = append(options, WithNow(func() time.Time { return fixtureNow }))
	provider, err := NewMockResourceProvider(options...)
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	return provider
}

func testSandbox(state SandboxState) Sandbox {
	return Sandbox{
		SandboxID:  "sandbox-a",
		State:      state,
		Generation: 1,
		Binding:    testBinding("sandbox-a", 1),
		SafePoint:  true,
	}
}

func testSemanticContext() *tgsrlv1.SemanticEnvelope {
	return &tgsrlv1.SemanticEnvelope{
		EnvelopeId: "semantic-1",
		Attributes: map[string]string{"source": "runtime"},
		TypedFields: []*tgsrlv1.SemanticField{{
			Key:   "sample.policy_lag",
			Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_Uint64Value{Uint64Value: 2}},
		}},
	}
}

func testBinding(sandboxID string, generation uint64) *tgsrlv1.Binding {
	return &tgsrlv1.Binding{
		BindingId:     "binding-a",
		PendingUnitId: "unit-a",
		DeviceIds:     []string{"mock-cpu-0"},
		Resources:     &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024},
		SandboxId:     sandboxID,
		Generation:    generation,
	}
}

func testAction(actionID, planID, idempotencyKey string, actionType tgsrlv1.ActionType, level tgsrlv1.ActionLevel, revision uint64) *tgsrlv1.Action {
	return &tgsrlv1.Action{
		ActionId:                 actionID,
		ActionType:               actionType,
		Level:                    level,
		TargetId:                 "sandbox-a",
		SandboxId:                "sandbox-a",
		Binding:                  testBinding("sandbox-a", 1),
		PlanId:                   planID,
		ExpectedGeneration:       1,
		ExpectedSnapshotRevision: revision,
		Deadline:                 timestamppb.New(fixtureNow.Add(time.Minute)),
		IdempotencyKey:           idempotencyKey,
		TickKind:                 tgsrlv1.TickKind_TICK_KIND_SLOW,
	}
}

func strictReconciliationPlan(planID string, revision uint64, actions ...*tgsrlv1.Action) *tgsrlv1.PlacementPlan {
	for index, action := range actions {
		if action == nil {
			continue
		}
		definition, ok := actionpolicy.DefinitionForAction(action.GetActionType())
		if !ok {
			continue
		}
		action.Order = uint32(index + 1)
		action.Preconditions = actionpolicy.RequiredPreconditions(action.GetActionType(), action.GetRequiresSafePoint(), false)
		action.ExpectedImpacts = append([]tgsrlv1.ExpectedImpact(nil), definition.ExpectedImpacts...)
		if action.RequiredCapabilities == nil {
			action.RequiredCapabilities = &tgsrlv1.CapabilitySet{}
		}
		if !containsString(action.RequiredCapabilities.GetSupportedActions(), definition.CapabilityName) {
			action.RequiredCapabilities.SupportedActions = append(action.RequiredCapabilities.SupportedActions, definition.CapabilityName)
		}
	}
	rollbackPolicy := tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_NOT_REQUIRED
	if len(actions) > 1 {
		rollbackPolicy = tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION
	}
	return &tgsrlv1.PlacementPlan{
		PlanId:           planID,
		SnapshotRevision: revision,
		Purpose:          tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		RollbackPolicy:   rollbackPolicy,
		CapabilityRequirements: func() []*tgsrlv1.CapabilityRequirement {
			values := []*tgsrlv1.CapabilityRequirement{}
			if len(actions) > 1 {
				values = append(values,
					actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION),
					actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK),
				)
			}
			return actionpolicy.StableCapabilityRequirements(values...)
		}(),
		Actions: actions,
	}
}

func removeString(values []string, unwanted string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != unwanted {
			result = append(result, value)
		}
	}
	return result
}
