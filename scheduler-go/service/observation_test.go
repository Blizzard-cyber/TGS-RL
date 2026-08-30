package service

import (
	"context"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scheduler"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestObserveSandboxFeedsProviderWatchProjection(t *testing.T) {
	now := time.Date(2026, 8, 30, 8, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := scheduler.New(scheduler.Config{Fallback: scheduler.FallbackNoOp, Clock: scheduler.ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	resourceProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{Store: store, Scheduler: evaluator, Provider: resourceProvider, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	_, err = implementation.ObserveSandbox(context.Background(), &tgsrlv1.ObserveSandboxRequest{Event: &tgsrlv1.SandboxEvent{
		EventId: "operator-observation-1", SandboxId: "sandbox-1", Generation: 1, State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
	}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, sandbox := range store.ListProjectedSandboxes() {
			if sandbox.GetSandboxId() == "sandbox-1" && sandbox.GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("scheduler projection did not receive provider observation: %+v", store.ListProjectedSandboxes())
}

func TestObserveSandboxRejectsConflictingEventIdentity(t *testing.T) {
	now := time.Date(2026, 8, 30, 8, 0, 0, 0, time.UTC)
	store, _ := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	evaluator, _ := scheduler.New(scheduler.Config{Fallback: scheduler.FallbackNoOp, Clock: scheduler.ClockFunc(func() time.Time { return now })})
	resourceProvider, _ := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	implementation, err := New(Config{Store: store, Scheduler: evaluator, Provider: resourceProvider, Clock: ClockFunc(func() time.Time { return now }), DeferStart: true})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	request := &tgsrlv1.ObserveSandboxRequest{Event: &tgsrlv1.SandboxEvent{EventId: "same-event", SandboxId: "sandbox-1", Generation: 1, State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING}}
	if _, err := implementation.ObserveSandbox(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request.Event.State = tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED
	if _, err := implementation.ObserveSandbox(context.Background(), request); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting event code = %s, want AlreadyExists", status.Code(err))
	}
}
