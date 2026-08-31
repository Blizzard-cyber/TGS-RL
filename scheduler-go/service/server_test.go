package service

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/eventloop"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/persistence"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/policy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/preemption"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/protection"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scheduler"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testBufferSize = 1 << 20

type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time { return f.now }

func TestSchedulerServiceBufconn(t *testing.T) {
	now := time.Date(2026, 8, 27, 9, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 7 }),
	})
	if err != nil {
		t.Fatalf("scheduler.New() error = %v", err)
	}
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	implementation, err := New(Config{Store: store, Scheduler: evaluator, Provider: mockProvider, DecisionRetention: 8, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	client, cleanup := startTestServer(t, implementation)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.PublishIntent(ctx, &tgsrlv1.PublishIntentRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("PublishIntent(nil) code = %s, want InvalidArgument", status.Code(err))
	}
	stream, err := client.WatchDecisions(ctx, &tgsrlv1.WatchDecisionsRequest{JobIds: []string{"job-1"}})
	if err != nil {
		t.Fatalf("WatchDecisions() error = %v", err)
	}
	intent := serviceIntent(now)
	published, err := client.PublishIntent(ctx, &tgsrlv1.PublishIntentRequest{Intent: intent})
	if err != nil || published.GetStatus() != tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_ACCEPTED {
		t.Fatalf("PublishIntent() response=%v error=%v", published, err)
	}
	update, err := stream.Recv()
	if err != nil {
		t.Fatalf("WatchDecisions().Recv() error = %v", err)
	}
	decision := update.GetDecision()
	if decision.GetFallback() || decision.GetSelectedPlan() == nil || len(decision.GetSelectedPlan().GetActions()) != 1 {
		t.Fatalf("published intent decision = %+v, want one non-fallback action", decision)
	}
	if len(decision.GetActionResults()) != 1 || decision.GetActionResults()[0].GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED {
		t.Fatalf("published intent action results = %+v, want one success", decision.GetActionResults())
	}

	snapshotResponse, err := client.GetSnapshot(ctx, &tgsrlv1.GetSnapshotRequest{MinimumRevision: 6, IncludePendingUnits: true})
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	snapshot := snapshotResponse.GetSnapshot()
	if snapshot.GetRevision() < 8 || len(snapshot.GetPendingUnits()) != 0 || len(snapshot.GetAllocations()) != 1 || snapshot.GetAllocations()[0].GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
		t.Fatalf("snapshot = %+v, want converged active allocation after authority seed and reconcile", snapshot)
	}
	providerSnapshot, err := mockProvider.Snapshot(ctx)
	if err != nil {
		t.Fatalf("provider Snapshot() error = %v", err)
	}
	providerRevision := providerSnapshot.GetRevision()
	deduplicated, err := client.PublishIntent(ctx, &tgsrlv1.PublishIntentRequest{Intent: intent})
	if err != nil || deduplicated.GetStatus() != tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_DEDUPLICATED {
		t.Fatalf("duplicate PublishIntent() response=%v error=%v", deduplicated, err)
	}
	providerSnapshot, err = mockProvider.Snapshot(ctx)
	if err != nil || providerSnapshot.GetRevision() != providerRevision {
		t.Fatalf("duplicate publish changed provider revision: snapshot=%v error=%v", providerSnapshot, err)
	}

	scheduled, err := client.Schedule(ctx, &tgsrlv1.ScheduleRequest{Intent: intent, Snapshot: snapshot})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}
	preview := scheduled.GetDecision()
	if preview.GetFallback() || preview.GetSelectedPlan() == nil || len(preview.GetSelectedPlan().GetActions()) != 0 {
		t.Fatalf("preview decision = %+v, want converged no-action plan", preview)
	}
	providerSnapshot, err = mockProvider.Snapshot(ctx)
	if err != nil || providerSnapshot.GetRevision() != providerRevision {
		t.Fatalf("preview Schedule changed provider revision: snapshot=%v error=%v", providerSnapshot, err)
	}
	if _, err := client.GetDecision(ctx, &tgsrlv1.GetDecisionRequest{DecisionId: preview.GetDecisionId()}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetDecision(preview) code = %s, want NotFound because preview decisions must not be retained", status.Code(err))
	}

	gotDecision, err := client.GetDecision(ctx, &tgsrlv1.GetDecisionRequest{DecisionId: decision.GetDecisionId()})
	if err != nil {
		t.Fatalf("GetDecision() error = %v", err)
	}
	if gotDecision.GetDecision().GetDecisionId() != decision.GetDecisionId() || gotDecision.GetCursor() == "" {
		t.Fatalf("GetDecision() = %+v, want retained decision and cursor", gotDecision)
	}

	listed, err := client.ListDecisions(ctx, &tgsrlv1.ListDecisionsRequest{JobId: "job-1", Limit: 10})
	if err != nil {
		t.Fatalf("ListDecisions() error = %v", err)
	}
	if len(listed.GetDecisions()) != 1 || listed.GetCursor() == "" {
		t.Fatalf("ListDecisions() = %+v, want exactly one retained committed decision and cursor", listed)
	}
	for _, item := range listed.GetDecisions() {
		if item.GetRunId() != "" {
			t.Fatalf("ListDecisions() returned unexpected run_id in base service test: %+v", item)
		}
	}
}

func TestDecisionUnaryQueriesPaginationFilteringAndErrors(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 21 }),
	})
	if err != nil {
		t.Fatalf("scheduler.New() error = %v", err)
	}
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	implementation, err := New(Config{Store: store, Scheduler: evaluator, Provider: mockProvider, DecisionRetention: 16, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	client, cleanup := startTestServer(t, implementation)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	intents := []*tgsrlv1.SchedulingIntent{
		serviceIntent(now),
		func() *tgsrlv1.SchedulingIntent {
			intent := serviceIntent(now.Add(time.Second))
			intent.ExecutionId = "execution-2"
			intent.StageId = "stage-2"
			intent.Version = 1
			intent.IdempotencyKey = scheduler.IntentIdempotencyKey(intent.GetExecutionId(), intent.GetStageId(), intent.GetVersion())
			intent.JobId = "job-2"
			intent.RunId = "run-2"
			intent.ExecutionContract.PhaseGraph.Phases[0].PhaseId = "stage-2"
			intent.ExecutionContract.PhaseGraph.EntryPhaseIds = []string{"stage-2"}
			intent.ExecutionContract.ContractId, _ = scheduler.CanonicalContractID(intent.ExecutionContract)
			intent.PhaseKind = tgsrlv1.PhaseKind_PHASE_KIND_DECODE
			return intent
		}(),
		func() *tgsrlv1.SchedulingIntent {
			intent := serviceIntent(now.Add(2 * time.Second))
			intent.ExecutionId = "execution-3"
			intent.StageId = "stage-3"
			intent.Version = 1
			intent.IdempotencyKey = scheduler.IntentIdempotencyKey(intent.GetExecutionId(), intent.GetStageId(), intent.GetVersion())
			intent.JobId = "job-1"
			intent.RunId = "run-3"
			intent.ExecutionContract.PhaseGraph.Phases[0].PhaseId = "stage-3"
			intent.ExecutionContract.PhaseGraph.EntryPhaseIds = []string{"stage-3"}
			intent.ExecutionContract.ContractId, _ = scheduler.CanonicalContractID(intent.ExecutionContract)
			intent.PhaseKind = tgsrlv1.PhaseKind_PHASE_KIND_DECODE
			return intent
		}(),
	}

	var retained []*tgsrlv1.DecisionRecord
	for _, intent := range intents {
		if _, err := client.PublishIntent(ctx, &tgsrlv1.PublishIntentRequest{Intent: intent}); err != nil {
			t.Fatalf("PublishIntent(%s) error = %v", intent.GetExecutionId(), err)
		}
		var page *tgsrlv1.ListDecisionsResponse
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			page, err = client.ListDecisions(ctx, &tgsrlv1.ListDecisionsRequest{JobId: intent.GetJobId(), Limit: 10})
			if err != nil {
				t.Fatalf("ListDecisions(job=%s) error = %v", intent.GetJobId(), err)
			}
			found := false
			for _, item := range page.GetDecisions() {
				if item.GetExecutionId() == intent.GetExecutionId() && item.GetStageId() == intent.GetStageId() {
					retained = append(retained, item)
					found = true
					break
				}
			}
			if found {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if len(retained) < 3 {
		t.Fatalf("retained decisions = %d, want >= 3", len(retained))
	}

	firstPage, err := client.ListDecisions(ctx, &tgsrlv1.ListDecisionsRequest{JobId: "job-1", Limit: 1})
	if err != nil {
		t.Fatalf("ListDecisions(firstPage) error = %v", err)
	}
	if len(firstPage.GetDecisions()) != 1 || firstPage.GetNextPageToken() == "" || firstPage.GetCursor() == "" {
		t.Fatalf("first page = %+v, want one decision, next_page_token, and cursor", firstPage)
	}
	secondPage, err := client.ListDecisions(ctx, &tgsrlv1.ListDecisionsRequest{JobId: "job-1", Limit: 1, PageToken: firstPage.GetNextPageToken()})
	if err != nil {
		t.Fatalf("ListDecisions(secondPage) error = %v", err)
	}
	if len(secondPage.GetDecisions()) == 0 {
		t.Fatalf("second page = %+v, want more decisions for job-1", secondPage)
	}
	if secondPage.GetDecisions()[0].GetDecisionId() == firstPage.GetDecisions()[0].GetDecisionId() {
		t.Fatalf("pagination repeated the first decision: first=%q second=%q", firstPage.GetDecisions()[0].GetDecisionId(), secondPage.GetDecisions()[0].GetDecisionId())
	}

	runFiltered, err := client.ListDecisions(ctx, &tgsrlv1.ListDecisionsRequest{RunId: "run-2", Limit: 10})
	if err != nil {
		t.Fatalf("ListDecisions(run filter) error = %v", err)
	}
	if len(runFiltered.GetDecisions()) == 0 {
		t.Fatal("run filtered decisions are empty, want retained run-2 decisions")
	}
	for _, decision := range runFiltered.GetDecisions() {
		if decision.GetRunId() != "run-2" {
			t.Fatalf("run filtered decision = %+v, want run-2", decision)
		}
	}

	got, err := client.GetDecision(ctx, &tgsrlv1.GetDecisionRequest{DecisionId: retained[0].GetDecisionId()})
	if err != nil {
		t.Fatalf("GetDecision(retained) error = %v", err)
	}
	if got.GetDecision().GetDecisionId() != retained[0].GetDecisionId() || got.GetCursor() == "" {
		t.Fatalf("GetDecision(retained) = %+v, want matching decision and cursor", got)
	}

	if _, err := client.GetDecision(ctx, &tgsrlv1.GetDecisionRequest{DecisionId: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetDecision(missing) code = %s, want NotFound", status.Code(err))
	}
	if _, err := client.ListDecisions(ctx, &tgsrlv1.ListDecisionsRequest{JobId: "job-2", PageToken: firstPage.GetNextPageToken()}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListDecisions(mismatched token filter) code = %s, want InvalidArgument", status.Code(err))
	}
	if _, err := client.ListDecisions(ctx, &tgsrlv1.ListDecisionsRequest{PageToken: "not-base64"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListDecisions(invalid token) code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestServiceBootstrapsProviderProjectionAndTickRuntime(t *testing.T) {
	now := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 11 }),
	})
	if err != nil {
		t.Fatalf("scheduler.New() error = %v", err)
	}
	mockProvider, err := provider.NewMockResourceProvider(
		provider.WithNow(func() time.Time { return now }),
		provider.WithSandboxes(provider.Sandbox{
			SandboxID:  "sandbox-a",
			State:      provider.SandboxStateRunning,
			Generation: 2,
			Binding: &tgsrlv1.Binding{
				BindingId:     "binding-a",
				PendingUnitId: "unit-a",
				DeviceIds:     []string{"mock-cpu-0"},
				SandboxId:     "sandbox-a",
				Generation:    2,
			},
			SafePoint: true,
			UpdatedAt: now,
		}),
	)
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	recorder := observability.NewInMemoryRecorder()
	runtime := eventloop.NewRuntime(recorder, eventloop.RuntimeConfig{
		FastInterval:   10 * time.Millisecond,
		MediumInterval: 20 * time.Millisecond,
		SlowInterval:   30 * time.Millisecond,
	})
	implementation, err := New(Config{
		Store:       store,
		Scheduler:   evaluator,
		Provider:    mockProvider,
		Recorder:    recorder,
		TickRuntime: runtime,
		Clock:       ClockFunc(func() time.Time { return now }),
		DeferStart:  true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer implementation.Close()
	if implementation.tickRuntime != runtime {
		t.Fatalf("service did not retain injected tick runtime: got=%v want=%v", implementation.tickRuntime, runtime)
	}
	if runtimeStarted(runtime) {
		t.Fatal("injected runtime started before Start()")
	}
	if sandboxes := store.ListProjectedSandboxes(); len(sandboxes) != 0 {
		t.Fatalf("pre-start projected sandboxes = %d, want 0", len(sandboxes))
	}
	if err := implementation.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if !runtimeStarted(runtime) {
		t.Fatal("injected runtime was not started")
	}
	projected := store.ListProjectedSandboxes()
	if len(projected) != 1 || projected[0].GetGeneration() != 2 || projected[0].GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		sandbox := (*tgsrlv1.Sandbox)(nil)
		if len(projected) > 0 {
			sandbox = projected[0]
		}
		t.Fatalf("bootstrapped sandbox = %+v, want generation 2 running", sandbox)
	}
	safePoint := false
	if _, err := mockProvider.ObserveSandbox(context.Background(), &tgsrlv1.SandboxEvent{
		EventId:    "sandbox-update",
		SandboxId:  "sandbox-a",
		Generation: 3,
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED,
		SafePoint:  &safePoint,
	}); err != nil {
		t.Fatalf("ObserveSandbox() error = %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		projected = store.ListProjectedSandboxes()
		if len(projected) == 1 && projected[0].GetGeneration() == 3 && projected[0].GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("provider sandbox watch did not update Store projection: %+v", store.ListProjectedSandboxes())
}

type failingEvaluator struct {
	err error
}

type unhealthyProvider struct {
	provider.CompleteResourceProvider
}

func (p unhealthyProvider) ID(context.Context) (string, error) { return "unhealthy", nil }
func (p unhealthyProvider) Health(context.Context) (*provider.HealthStatus, error) {
	return &provider.HealthStatus{ProviderID: "unhealthy", Healthy: false, Reason: "maintenance"}, nil
}
func (p unhealthyProvider) WatchResources(context.Context, uint64) (<-chan provider.WatchedResourceEvent, error) {
	return make(chan provider.WatchedResourceEvent), nil
}
func (p unhealthyProvider) WatchSandboxes(context.Context, uint64) (<-chan provider.WatchedSandboxEvent, error) {
	return make(chan provider.WatchedSandboxEvent), nil
}
func (p unhealthyProvider) ReconcilePlan(context.Context, *tgsrlv1.PlacementPlan) (*provider.PlanRecord, error) {
	return nil, provider.ErrNotFound
}
func (p unhealthyProvider) RecoverInFlightPlans(context.Context) ([]*provider.PlanRecord, error) {
	return nil, nil
}

type cursorExpiringWatchProvider struct {
	provider.CompleteResourceProvider
	resourceCursor  uint64
	sandboxCursor   uint64
	resourceWatched bool
	sandboxWatched  bool
}

func (p *cursorExpiringWatchProvider) WatchResources(_ context.Context, cursor uint64) (<-chan provider.WatchedResourceEvent, error) {
	p.resourceCursor = cursor
	p.resourceWatched = true
	if cursor != 0 {
		return nil, fmt.Errorf("%w: cursor %d predates retained cursor %d", provider.ErrCursorExpired, cursor, 1)
	}
	ch := make(chan provider.WatchedResourceEvent)
	return ch, nil
}

func (p *cursorExpiringWatchProvider) WatchSandboxes(_ context.Context, cursor uint64) (<-chan provider.WatchedSandboxEvent, error) {
	p.sandboxCursor = cursor
	p.sandboxWatched = true
	ch := make(chan provider.WatchedSandboxEvent)
	return ch, nil
}

type watchFailingProvider struct {
	provider.CompleteResourceProvider
	err error
}

func (p watchFailingProvider) WatchResources(context.Context, uint64) (<-chan provider.WatchedResourceEvent, error) {
	return nil, p.err
}

func (p watchFailingProvider) WatchSandboxes(context.Context, uint64) (<-chan provider.WatchedSandboxEvent, error) {
	return nil, p.err
}

type reconnectingWatchProvider struct {
	provider.CompleteResourceProvider

	mu                  sync.Mutex
	resourceWatchCalls  int
	sandboxWatchCalls   int
	resourceReconnectCh chan struct{}
	allowResourceWatch  chan struct{}
}

func newReconnectingWatchProvider(base provider.CompleteResourceProvider) *reconnectingWatchProvider {
	return &reconnectingWatchProvider{
		CompleteResourceProvider: base,
		resourceReconnectCh:      make(chan struct{}, 8),
		allowResourceWatch:       make(chan struct{}),
	}
}

func (p *reconnectingWatchProvider) WatchResources(context.Context, uint64) (<-chan provider.WatchedResourceEvent, error) {
	p.mu.Lock()
	p.resourceWatchCalls++
	ch := make(chan provider.WatchedResourceEvent, 1)
	if p.resourceWatchCalls == 1 {
		p.mu.Unlock()
		close(ch)
	} else {
		select {
		case p.resourceReconnectCh <- struct{}{}:
		default:
		}
		allow := p.allowResourceWatch
		p.mu.Unlock()
		<-allow
	}
	return ch, nil
}

func (p *reconnectingWatchProvider) WatchSandboxes(context.Context, uint64) (<-chan provider.WatchedSandboxEvent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sandboxWatchCalls++
	ch := make(chan provider.WatchedSandboxEvent, 1)
	return ch, nil
}

type panicEvaluator struct {
	panicOnExecutionID string
	delegate           Evaluator
}

func (p panicEvaluator) Evaluate(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	if intent != nil && intent.GetExecutionId() == p.panicOnExecutionID {
		panic("boom")
	}
	return p.delegate.Evaluate(snapshot, intent)
}

func (f failingEvaluator) Evaluate(*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	return nil, nil, f.err
}

func TestServiceExecutesLegacyBindOnlyPlan(t *testing.T) {
	now := time.Date(2026, 8, 28, 6, 45, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{Store: store, Scheduler: legacyBindEvaluator{}, Provider: mockProvider, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	intent := serviceIntent(now)
	response, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent})
	if err != nil || response.GetStatus() != tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_ACCEPTED {
		t.Fatalf("PublishIntent() = (%v, %v)", response, err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		decision := entries[len(entries)-1].decision
		if decision.GetFallback() {
			t.Fatalf("legacy bind decision fell back: %s", decision.GetFallbackReason())
		}
		if len(decision.GetActionResults()) != 1 || decision.GetActionResults()[0].GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED {
			t.Fatalf("legacy bind results = %v", decision.GetActionResults())
		}
		return
	}
	t.Fatal("legacy bind plan did not produce a decision")
}

func TestNewStartsProviderWatchesFromZeroCursor(t *testing.T) {
	now := time.Date(2026, 8, 28, 5, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 61 }),
	})
	if err != nil {
		t.Fatalf("scheduler.New() error = %v", err)
	}
	base, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	watched := &cursorExpiringWatchProvider{CompleteResourceProvider: base}
	implementation, err := New(Config{
		Store:     store,
		Scheduler: evaluator,
		Provider:  watched,
		Clock:     ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer implementation.Close()
	if !watched.resourceWatched || !watched.sandboxWatched {
		t.Fatalf("watch startup flags = resource:%v sandbox:%v, want both true", watched.resourceWatched, watched.sandboxWatched)
	}
	if watched.resourceCursor != 0 {
		t.Fatalf("WatchResources cursor = %d, want 0 so provider watch cursor never reuses cluster snapshot revision", watched.resourceCursor)
	}
	if watched.sandboxCursor != 0 {
		t.Fatalf("WatchSandboxes cursor = %d, want 0", watched.sandboxCursor)
	}
}

func TestNewReturnsProviderWatchStartupError(t *testing.T) {
	now := time.Date(2026, 8, 28, 5, 30, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 62 }),
	})
	if err != nil {
		t.Fatalf("scheduler.New() error = %v", err)
	}
	base, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	watchErr := fmt.Errorf("%w: forced startup failure", provider.ErrCursorExpired)
	if _, err := New(Config{
		Store:     store,
		Scheduler: evaluator,
		Provider:  watchFailingProvider{CompleteResourceProvider: base, err: watchErr},
		Clock:     ClockFunc(func() time.Time { return now }),
	}); err == nil || !strings.Contains(err.Error(), "start resource watch") || !errors.Is(err, provider.ErrCursorExpired) {
		t.Fatalf("New() error = %v, want wrapped start resource watch + ErrCursorExpired", err)
	}
}

func TestProcessAcceptedEvaluatorFailureDoesNotPanicAndAppendsFallback(t *testing.T) {
	now := time.Date(2026, 8, 27, 11, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	implementation, err := New(Config{
		Store:     store,
		Scheduler: failingEvaluator{err: errors.New("boom")},
		Provider:  mockProvider,
		Clock:     ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer implementation.Close()

	intent := serviceIntent(time.Now().UTC())
	response, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent})
	if err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	if response.GetStatus() != tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_ACCEPTED {
		t.Fatalf("PublishIntent() status = %s, want ACCEPTED", response.GetStatus())
	}

	deadline := time.Now().Add(time.Second)
	var decision *tgsrlv1.DecisionRecord
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) > 0 {
			decision = entries[len(entries)-1].decision
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if decision == nil {
		t.Fatal("no fallback decision was retained after evaluator failure")
	}
	if !decision.GetFallback() || decision.GetExecutionId() != intent.GetExecutionId() || decision.GetStageId() != intent.GetStageId() {
		t.Fatalf("decision = %+v, want fallback decision for the failing intent", decision)
	}
	if got := decision.GetFallbackReason(); len(got) < len("EVALUATION_FAILED") || got[:len("EVALUATION_FAILED")] != "EVALUATION_FAILED" {
		t.Fatalf("fallback_reason = %q, want EVALUATION_FAILED prefix", got)
	}
}

func TestPublishIntentPassesTriggerContextIntoContextualEvaluator(t *testing.T) {
	now := time.Date(2026, 8, 28, 7, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	intent := serviceIntent(now)
	sampleCount := uint64(9)
	observation := &tgsrlv1.ContractObservation{
		EventId:     "trigger-observation",
		Source:      "runtime",
		SampleCount: &sampleCount,
	}
	plan := &tgsrlv1.PlacementPlan{
		PlanId:           "context-plan",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: 5,
		DecisionId:       "context-decision",
		Actions:          []*tgsrlv1.Action{},
	}
	decision := &tgsrlv1.DecisionRecord{
		DecisionId:       "context-decision",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: 5,
		SelectedPlan:     proto.Clone(plan).(*tgsrlv1.PlacementPlan),
	}
	spy := &contextualEvaluatorSpy{plan: plan, decision: decision}
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{Store: store, Scheduler: spy, Provider: mockProvider, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("store.PublishIntent() error = %v", err)
	}

	trigger := &eventloop.Trigger{
		ExecutionID:                  intent.GetExecutionId(),
		StageID:                      intent.GetStageId(),
		TickKind:                     tgsrlv1.TickKind_TICK_KIND_SLOW,
		Cause:                        "startup_recovery",
		ObservedRevision:             23,
		ContractObservation:          observation,
		CompatibilityDefaultsApplied: true,
	}
	implementation.enqueue(context.Background(), intent, trigger)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got := spy.LastContext()
		if got == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if got.GetTickKind() != tgsrlv1.TickKind_TICK_KIND_SLOW {
			t.Fatalf("tick_kind = %s, want SLOW", got.GetTickKind())
		}
		if got.GetCause() != "startup_recovery" {
			t.Fatalf("cause = %q, want startup_recovery", got.GetCause())
		}
		if got.GetObservedRevision() != 23 {
			t.Fatalf("observed_revision = %d, want 23", got.GetObservedRevision())
		}
		if !got.GetCompatibilityDefaultsApplied() {
			t.Fatal("compatibility_defaults_applied = false, want true")
		}
		if got.GetContractObservation() == nil || got.GetContractObservation().GetEventId() != "trigger-observation" {
			t.Fatalf("contract_observation = %+v, want trigger observation", got.GetContractObservation())
		}
		return
	}
	t.Fatal("contextual evaluator did not receive an evaluation context")
}

func TestScheduleForwardsExplicitEvaluationContext(t *testing.T) {
	now := time.Date(2026, 8, 28, 7, 5, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	intent := serviceIntent(now)
	plan := &tgsrlv1.PlacementPlan{PlanId: "preview-plan"}
	decision := &tgsrlv1.DecisionRecord{DecisionId: "preview-decision"}
	spy := &contextualEvaluatorSpy{plan: plan, decision: decision}
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{
		Store: store, Scheduler: spy, Provider: mockProvider,
		Clock: ClockFunc(func() time.Time { return now }), DeferStart: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	explicit := &tgsrlv1.EvaluationContext{
		TickKind:         tgsrlv1.TickKind_TICK_KIND_SLOW,
		EvaluationTime:   timestamppb.New(now.Add(3 * time.Second)),
		DecisionSequence: 77,
		Cause:            "replay",
		ObservedRevision: 19,
	}
	explicit.ContractObservation = &tgsrlv1.ContractObservation{EventId: "replay-observation"}

	if _, err := implementation.Schedule(context.Background(), &tgsrlv1.ScheduleRequest{
		Intent: intent, Snapshot: serviceSnapshot(now), EvaluationContext: explicit,
	}); err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}
	got := spy.LastContext()
	if !proto.Equal(got, explicit) {
		t.Fatalf("evaluation context = %+v, want exact request context %+v", got, explicit)
	}
	explicit.Cause = "caller-mutation"
	if spy.LastContext().GetCause() != "replay" {
		t.Fatal("Schedule aliased caller evaluation context")
	}
}

type failingRepository struct{ err error }

func (r failingRepository) SaveCheckpoint(persistence.SchedulerState) error { return r.err }

type contextualEvaluatorSpy struct {
	mu       sync.Mutex
	plan     *tgsrlv1.PlacementPlan
	decision *tgsrlv1.DecisionRecord
	contexts []*tgsrlv1.EvaluationContext
}

type guardingEvaluator struct {
	guard               *protection.Guard
	staleSnapshotOffset uint64
}

type legacyBindEvaluator struct{}

func (legacyBindEvaluator) Evaluate(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	binding := &tgsrlv1.Binding{
		BindingId:     "legacy-binding",
		PendingUnitId: snapshot.GetPendingUnits()[0].GetPendingUnitId(),
		DeviceIds:     []string{snapshot.GetDevices()[0].GetDeviceId()},
		Resources:     proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector),
		SandboxId:     "legacy-sandbox",
		Generation:    1,
	}
	plan := &tgsrlv1.PlacementPlan{
		PlanId:           "legacy-plan",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		Bindings:         []*tgsrlv1.Binding{binding},
		Actions: []*tgsrlv1.Action{{
			ActionId:                 "legacy-action",
			ActionType:               tgsrlv1.ActionType_ACTION_TYPE_BIND,
			Level:                    tgsrlv1.ActionLevel_ACTION_LEVEL_L1,
			TargetId:                 binding.GetPendingUnitId(),
			Binding:                  proto.Clone(binding).(*tgsrlv1.Binding),
			Rollback:                 &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, TargetId: binding.GetBindingId()},
			Order:                    1,
			PlanId:                   "legacy-plan",
			SandboxId:                binding.GetSandboxId(),
			ExpectedGeneration:       1,
			ExpectedSnapshotRevision: snapshot.GetRevision(),
			Deadline:                 proto.Clone(intent.GetValidUntil()).(*timestamppb.Timestamp),
			IdempotencyKey:           "legacy-action-key",
		}},
	}
	decision := &tgsrlv1.DecisionRecord{
		DecisionId:       "legacy-decision",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		SelectedPlan:     proto.Clone(plan).(*tgsrlv1.PlacementPlan),
	}
	return plan, decision, nil
}

func (e guardingEvaluator) Evaluate(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	plan, decision, err := legacyBindEvaluator{}.Evaluate(snapshot, intent)
	if err != nil {
		return nil, nil, err
	}
	plan = proto.Clone(plan).(*tgsrlv1.PlacementPlan)
	decision = proto.Clone(decision).(*tgsrlv1.DecisionRecord)
	plan.SnapshotRevision += e.staleSnapshotOffset
	decision.SnapshotRevision = plan.GetSnapshotRevision()
	decision.SelectedPlan = proto.Clone(plan).(*tgsrlv1.PlacementPlan)
	return plan, decision, nil
}

func (e guardingEvaluator) Guard() *protection.Guard { return e.guard }

func (s *contextualEvaluatorSpy) Evaluate(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	return s.EvaluateWithContext(snapshot, intent, nil)
}

func (s *contextualEvaluatorSpy) EvaluateWithContext(_ *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent, context *tgsrlv1.EvaluationContext) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if context != nil {
		s.contexts = append(s.contexts, proto.Clone(context).(*tgsrlv1.EvaluationContext))
	} else {
		s.contexts = append(s.contexts, nil)
	}
	return proto.Clone(s.plan).(*tgsrlv1.PlacementPlan), proto.Clone(s.decision).(*tgsrlv1.DecisionRecord), nil
}

func (s *contextualEvaluatorSpy) LastContext() *tgsrlv1.EvaluationContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.contexts) == 0 || s.contexts[len(s.contexts)-1] == nil {
		return nil
	}
	return proto.Clone(s.contexts[len(s.contexts)-1]).(*tgsrlv1.EvaluationContext)
}

type failAfterRepository struct {
	mu        sync.Mutex
	saves     []persistence.SchedulerState
	failAfter int
	err       error
}

type failFinalDecisionRepository struct {
	mu    sync.Mutex
	saves []persistence.SchedulerState
	err   error
}

func (r *failFinalDecisionRepository) SaveCheckpoint(checkpoint persistence.SchedulerState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, decision := range checkpoint.Decisions {
		if decision.GetSequence() > 0 {
			return r.err
		}
	}
	r.saves = append(r.saves, checkpoint)
	return nil
}

func (r *failFinalDecisionRepository) SaveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.saves)
}

func (r *failFinalDecisionRepository) Last() persistence.SchedulerState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.saves) == 0 {
		return persistence.SchedulerState{}
	}
	return r.saves[len(r.saves)-1]
}

func (r *failAfterRepository) SaveCheckpoint(checkpoint persistence.SchedulerState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.saves) >= r.failAfter {
		return r.err
	}
	r.saves = append(r.saves, checkpoint)
	return nil
}

func (r *failAfterRepository) SaveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.saves)
}

func (r *failAfterRepository) Last() persistence.SchedulerState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.saves) == 0 {
		return persistence.SchedulerState{}
	}
	return r.saves[len(r.saves)-1]
}

type recordingRepository struct {
	mu    sync.Mutex
	saves []persistence.SchedulerState
}

func (r *recordingRepository) SaveCheckpoint(state persistence.SchedulerState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saves = append(r.saves, state)
	return nil
}

func (r *recordingRepository) SaveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.saves)
}

func (r *recordingRepository) Last() persistence.SchedulerState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.saves) == 0 {
		return persistence.SchedulerState{}
	}
	return r.saves[len(r.saves)-1]
}

type countingWatchProvider struct {
	provider.CompleteResourceProvider
	resourceCh chan provider.WatchedResourceEvent
	sandboxCh  chan provider.WatchedSandboxEvent

	mu             sync.Mutex
	executedSteps  int
	latestSnapshot *tgsrlv1.ClusterSnapshot
}

func newCountingWatchProvider(base provider.CompleteResourceProvider) *countingWatchProvider {
	wrapper := &countingWatchProvider{
		CompleteResourceProvider: base,
		resourceCh:               make(chan provider.WatchedResourceEvent, 16),
		sandboxCh:                make(chan provider.WatchedSandboxEvent, 16),
	}
	if snapshot, err := base.Snapshot(context.Background()); err == nil && snapshot != nil {
		wrapper.latestSnapshot = proto.Clone(snapshot).(*tgsrlv1.ClusterSnapshot)
	}
	return wrapper
}

func (p *countingWatchProvider) Snapshot(ctx context.Context) (*tgsrlv1.ClusterSnapshot, error) {
	p.mu.Lock()
	if p.latestSnapshot != nil {
		snapshot := proto.Clone(p.latestSnapshot).(*tgsrlv1.ClusterSnapshot)
		p.mu.Unlock()
		return snapshot, nil
	}
	p.mu.Unlock()
	return p.CompleteResourceProvider.Snapshot(ctx)
}

func (p *countingWatchProvider) ListDevices(ctx context.Context) ([]*tgsrlv1.Device, error) {
	p.mu.Lock()
	if p.latestSnapshot != nil {
		devices := make([]*tgsrlv1.Device, 0, len(p.latestSnapshot.GetDevices()))
		for _, device := range p.latestSnapshot.GetDevices() {
			devices = append(devices, proto.Clone(device).(*tgsrlv1.Device))
		}
		p.mu.Unlock()
		return devices, nil
	}
	p.mu.Unlock()
	return p.CompleteResourceProvider.ListDevices(ctx)
}

func (p *countingWatchProvider) ExecuteStep(ctx context.Context, transactionID string, generation uint64, stepIndex int) (*provider.TransactionReceipt, error) {
	p.mu.Lock()
	p.executedSteps++
	p.mu.Unlock()
	return p.CompleteResourceProvider.ExecuteStep(ctx, transactionID, generation, stepIndex)
}

func (p *countingWatchProvider) Health(ctx context.Context) (*provider.HealthStatus, error) {
	p.mu.Lock()
	if p.latestSnapshot != nil {
		healthy := true
		reason := ""
		for _, device := range p.latestSnapshot.GetDevices() {
			if device.GetHealth() != tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY {
				healthy = false
				reason = "device health is degraded"
				break
			}
		}
		p.mu.Unlock()
		return &provider.HealthStatus{
			ProviderID: "counting-watch",
			Source:     "test",
			Healthy:    healthy,
			Reason:     reason,
			CheckedAt:  time.Now().UTC(),
		}, nil
	}
	p.mu.Unlock()
	return p.CompleteResourceProvider.Health(ctx)
}

func (p *countingWatchProvider) WatchResources(context.Context, uint64) (<-chan provider.WatchedResourceEvent, error) {
	return p.resourceCh, nil
}

func (p *countingWatchProvider) WatchSandboxes(context.Context, uint64) (<-chan provider.WatchedSandboxEvent, error) {
	return p.sandboxCh, nil
}

func (p *countingWatchProvider) ExecuteStepCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.executedSteps
}

func (p *countingWatchProvider) SendResourceEvent(event *tgsrlv1.ResourceEvent) {
	p.mu.Lock()
	p.applyResourceEventLocked(event)
	p.mu.Unlock()
	p.resourceCh <- provider.WatchedResourceEvent{
		Cursor: event.GetProviderRevision(),
		Event:  proto.Clone(event).(*tgsrlv1.ResourceEvent),
	}
}

func (p *countingWatchProvider) SendSandboxEvent(event *tgsrlv1.SandboxEvent) {
	p.sandboxCh <- provider.WatchedSandboxEvent{
		Cursor: event.GetGeneration(),
		Event:  proto.Clone(event).(*tgsrlv1.SandboxEvent),
	}
}

func (p *countingWatchProvider) applyResourceEventLocked(event *tgsrlv1.ResourceEvent) {
	if event == nil {
		return
	}
	switch event.GetEventType() {
	case tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED:
		if event.GetSnapshot() != nil {
			p.latestSnapshot = proto.Clone(event.GetSnapshot()).(*tgsrlv1.ClusterSnapshot)
		}
	case tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED:
		if event.GetDevice() == nil {
			return
		}
		if p.latestSnapshot == nil {
			p.latestSnapshot = &tgsrlv1.ClusterSnapshot{}
		}
		for index, existing := range p.latestSnapshot.GetDevices() {
			if existing.GetDeviceId() == event.GetDevice().GetDeviceId() {
				p.latestSnapshot.Devices[index] = proto.Clone(event.GetDevice()).(*tgsrlv1.Device)
				return
			}
		}
		p.latestSnapshot.Devices = append(p.latestSnapshot.Devices, proto.Clone(event.GetDevice()).(*tgsrlv1.Device))
	}
}

func TestPersistenceFailureFailsClosed(t *testing.T) {
	now := time.Now().UTC()
	store, err := state.NewStore(serviceSnapshot(now))
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := scheduler.New(scheduler.Config{})
	if err != nil {
		t.Fatal(err)
	}
	mockProvider, err := provider.NewMockResourceProvider()
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{Store: store, Scheduler: evaluator, Provider: mockProvider, Repository: failingRepository{err: errors.New("disk full")}, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	intent := serviceIntent(now)
	if _, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent}); status.Code(err) != codes.Internal {
		t.Fatalf("first PublishIntent() code = %s, want Internal", status.Code(err))
	}
	intent2 := serviceIntent(now)
	intent2.Version = 2
	intent2.IdempotencyKey = scheduler.IntentIdempotencyKey(intent2.GetExecutionId(), intent2.GetStageId(), 2)
	if _, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent2}); status.Code(err) != codes.Unavailable {
		t.Fatalf("second PublishIntent() code = %s, want Unavailable", status.Code(err))
	}
}

func TestPublishIntentCheckpointFailureLeavesStateInvisibleAndWatchersQuiet(t *testing.T) {
	now := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := scheduler.New(scheduler.Config{Clock: scheduler.ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{
		Store:      store,
		Scheduler:  evaluator,
		Provider:   mockProvider,
		Repository: failingRepository{err: errors.New("disk full")},
		Clock:      ClockFunc(func() time.Time { return now }),
		DeferStart: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()

	revisionBefore := store.Revision()
	waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	watcherDone := make(chan error, 1)
	go func() {
		_, err := store.GetSnapshot(waitCtx, revisionBefore+1, true)
		watcherDone <- err
	}()

	intent := serviceIntent(now)
	response, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent})
	if status.Code(err) != codes.Internal {
		t.Fatalf("PublishIntent() code = %s, want Internal", status.Code(err))
	}
	if response == nil || response.GetStatus() != tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_ACCEPTED {
		t.Fatalf("PublishIntent() response = %+v, want accepted response despite failed checkpoint", response)
	}
	if _, ok := store.LatestIntent(intent.GetExecutionId(), intent.GetStageId()); ok {
		t.Fatal("failed checkpoint exposed latest intent")
	}
	if got := store.Revision(); got != revisionBefore {
		t.Fatalf("store revision = %d, want unchanged %d after failed checkpoint", got, revisionBefore)
	}
	err = <-watcherDone
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("snapshot watcher error = %v, want deadline exceeded without revision signal", err)
	}
	if implementation.persistenceFailure() == nil {
		t.Fatal("checkpoint failure did not close persistence gate")
	}
}

func TestStartDelaysProviderBacklogUntilAfterRecovery(t *testing.T) {
	now := time.Date(2026, 8, 28, 9, 30, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 11 }),
	})
	if err != nil {
		t.Fatal(err)
	}
	baseProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	watched := newCountingWatchProvider(baseProvider)
	implementation, err := New(Config{
		Store:      store,
		Scheduler:  evaluator,
		Provider:   watched,
		Recorder:   observability.NewInMemoryRecorder(),
		Clock:      ClockFunc(func() time.Time { return now }),
		DeferStart: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()

	intent := serviceIntent(now)
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("store.PublishIntent() error = %v", err)
	}
	watched.SendResourceEvent(&tgsrlv1.ResourceEvent{
		EventId:          "backlog-ready",
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED,
		Provider:         "mock",
		ProviderRevision: 10,
		ObservedAt:       timestamppb.New(now),
		Device: &tgsrlv1.Device{
			DeviceId:    "mock-cpu-0",
			Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1 << 30},
			Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1 << 30},
		},
	})
	time.Sleep(150 * time.Millisecond)
	if watched.ExecuteStepCount() != 0 {
		t.Fatalf("ExecuteStep count before Start = %d, want 0", watched.ExecuteStepCount())
	}
	entries, _, _ := implementation.decisionsAfter(0)
	if len(entries) != 0 {
		t.Fatalf("decisions before Start = %d, want 0", len(entries))
	}
	if err := implementation.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ = implementation.decisionsAfter(0)
		if len(entries) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("provider state was not reconciled after Start(): decisions=%d", len(entries))
}

func TestAppendDecisionCheckpointFailureKeepsDecisionAndSequenceInvisible(t *testing.T) {
	now := time.Date(2026, 8, 28, 8, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := scheduler.New(scheduler.Config{Clock: scheduler.ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	repository := &failAfterRepository{failAfter: 1, err: errors.New("injected checkpoint failure")}
	implementation, err := New(Config{Store: store, Scheduler: evaluator, Provider: mockProvider, Repository: repository, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	first := &tgsrlv1.DecisionRecord{DecisionId: "decision-committed", JobId: "job-1", DecidedAt: timestamppb.New(now)}
	if err := implementation.appendDecision(first.GetJobId(), first); err != nil {
		t.Fatalf("appendDecision(first) error = %v", err)
	}
	_, changed, _ := implementation.decisionsAfter(1)
	decision := &tgsrlv1.DecisionRecord{DecisionId: "decision-checkpoint-fails", JobId: "job-1", DecidedAt: timestamppb.New(now)}
	if err := implementation.appendDecision(decision.GetJobId(), decision); err == nil {
		t.Fatal("appendDecision() error = nil, want injected checkpoint failure")
	}

	entries, nextChanged, oldest := implementation.decisionsAfter(0)
	if len(entries) != 1 || entries[0].decision.GetDecisionId() != first.GetDecisionId() || oldest != 1 || implementation.sequence != 1 {
		t.Fatalf("visible state after failed append: decisions=%+v oldest=%d sequence=%d, want only sequence 1", entries, oldest, implementation.sequence)
	}
	if nextChanged != changed {
		t.Fatal("failed append replaced watcher notification channel")
	}
	select {
	case <-changed:
		t.Fatal("failed append notified decision watchers")
	default:
	}
	if _, found := implementation.decisionByID(decision.GetDecisionId()); found {
		t.Fatal("failed append exposed decision by ID")
	}
	if repository.SaveCount() != 1 || repository.Last().Cursor != 1 || len(repository.Last().Decisions) != 1 {
		t.Fatalf("durable state after failed append: saves=%d checkpoint=%+v, want only sequence 1", repository.SaveCount(), repository.Last())
	}
	if implementation.persistenceFailure() == nil {
		t.Fatal("checkpoint failure did not close persistence gate")
	}
	if err := implementation.appendDecision("job-1", &tgsrlv1.DecisionRecord{DecisionId: "decision-after-failure"}); err == nil {
		t.Fatal("appendDecision() after failure = nil, want fail-closed error")
	}
	if implementation.sequence != 1 {
		t.Fatalf("sequence after fail-closed retry = %d, want 1", implementation.sequence)
	}
}

func TestDecisionCheckpointFailureRecoversCompletePendingAudit(t *testing.T) {
	now := time.Date(2026, 8, 28, 8, 30, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Clock:          scheduler.ClockFunc(func() time.Time { return now }),
		CodeRevision:   "audit-code",
		ConfigRevision: "audit-config",
	})
	if err != nil {
		t.Fatal(err)
	}
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	// All transaction phase checkpoints succeed; only publication of the
	// sequence-bearing final DecisionRecord is injected to fail.
	repository := &failFinalDecisionRepository{err: errors.New("injected final checkpoint failure")}
	implementation, err := New(Config{Store: store, Scheduler: evaluator, Provider: mockProvider, Repository: repository, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	_, changed, _ := implementation.decisionsAfter(0)
	intent := serviceIntent(now)
	intent.RunId = "run-checkpoint-recovery"
	intent.TraceId = "trace-checkpoint-recovery"
	if _, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent}); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for implementation.persistenceFailure() == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if implementation.persistenceFailure() == nil {
		t.Fatal("final decision checkpoint failure was not observed")
	}
	entries, nextChanged, oldest := implementation.decisionsAfter(0)
	if len(entries) != 0 || oldest != 0 || implementation.sequence != 0 {
		t.Fatalf("visible state after failed final checkpoint: decisions=%d oldest=%d sequence=%d", len(entries), oldest, implementation.sequence)
	}
	if nextChanged != changed {
		t.Fatal("failed final checkpoint replaced watcher notification channel")
	}
	select {
	case <-changed:
		t.Fatal("failed final checkpoint notified decision watchers")
	default:
	}
	durable := repository.Last()
	if repository.SaveCount() < 3 || durable.Cursor != 0 || len(durable.Decisions) != 1 || durable.Decisions[0].GetSequence() != 0 {
		t.Fatalf("durable pending audit = %+v saves=%d, want one sequence-zero audit after transaction checkpoints", durable.Decisions, repository.SaveCount())
	}
	pending := durable.Decisions[0]
	if pending.GetSelectedPlan() == nil || len(pending.GetCandidates()) == 0 || pending.GetCodeRevision() != "audit-code" || pending.GetConfigRevision() != "audit-config" {
		t.Fatalf("pending audit lost complete decision fields: %+v", pending)
	}
	if len(durable.Reservations) != 1 || !durable.Reservations[0].Finalized || !durable.Reservations[0].Succeeded {
		t.Fatalf("durable reservation = %+v, want committed transaction state before audit publication", durable.Reservations)
	}
	if len(durable.Transactions) != 1 || durable.Transactions[0].State != state.TransactionStateCommitted {
		t.Fatalf("durable transactions = %+v, want committed transaction awaiting final audit", durable.Transactions)
	}

	restartedStore, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	if err := persistence.RestoreStore(restartedStore, &durable); err != nil {
		t.Fatalf("RestoreStore() error = %v", err)
	}
	recoveredRepository := &recordingRepository{}
	restarted, err := New(Config{Store: restartedStore, Scheduler: evaluator, Provider: mockProvider, Repository: recoveredRepository, DeferStart: true, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.ResumeRecoveredState(context.Background(), &durable); err != nil {
		t.Fatalf("ResumeRecoveredState() error = %v", err)
	}
	recoveredEntries, _, _ := restarted.decisionsAfter(0)
	if len(recoveredEntries) != 1 {
		t.Fatalf("recovered decisions = %d, want 1", len(recoveredEntries))
	}
	recoveredDecision := recoveredEntries[0].decision
	if recoveredDecision.GetDecisionId() != pending.GetDecisionId() || recoveredDecision.GetSequence() != 1 || recoveredDecision.GetCodeRevision() != pending.GetCodeRevision() || recoveredDecision.GetConfigRevision() != pending.GetConfigRevision() || len(recoveredDecision.GetCandidates()) != len(pending.GetCandidates()) || len(recoveredDecision.GetActionResults()) != 1 {
		t.Fatalf("recovered decision = %+v, want complete pending audit with provider result", recoveredDecision)
	}
	last := recoveredRepository.Last()
	if last.Cursor != 1 || len(last.Decisions) != 1 || last.Decisions[0].GetSequence() != 1 {
		t.Fatalf("recovered checkpoint = %+v, want committed decision cursor 1", last)
	}
}

func TestRevisionConflictRetryDoesNotConsumeProtectionState(t *testing.T) {
	now := time.Date(2026, 8, 29, 13, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return clock.now })))
	if err != nil {
		t.Fatal(err)
	}
	guard := protection.NewGuard(protection.Config{
		Enabled:             true,
		Cooldown:            time.Minute,
		MaxActionsPerWindow: 1,
		Window:              10 * time.Minute,
		BreakerThreshold:    2,
		BreakerResetAfter:   5 * time.Minute,
	}, clock)
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return clock.now }))
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{
		Store:     store,
		Scheduler: guardingEvaluator{guard: guard, staleSnapshotOffset: 1},
		Provider:  mockProvider,
		Clock:     ClockFunc(func() time.Time { return clock.now }),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	intent := serviceIntent(now)
	if _, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent}); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) > 0 {
			decision := entries[len(entries)-1].decision
			if !decision.GetFallback() || !strings.Contains(decision.GetFallbackReason(), "REVISION_CONFLICT_RETRY_EXHAUSTED") {
				t.Fatalf("decision = %+v, want revision-conflict retry exhaustion fallback", decision)
			}
			if check := guard.CheckN(intent.GetExecutionId()+"/"+intent.GetStageId(), 0, 1); !check.Allowed {
				t.Fatalf("guard after revision conflicts = %+v, want unconsumed state", check)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("revision-conflict fallback was not emitted")
}

func TestSuccessfulReservationConsumesProtectionStateAndPersistsIt(t *testing.T) {
	now := time.Date(2026, 8, 29, 13, 30, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return clock.now })))
	if err != nil {
		t.Fatal(err)
	}
	guard := protection.NewGuard(protection.Config{
		Enabled:             true,
		Cooldown:            time.Minute,
		MaxActionsPerWindow: 1,
		Window:              10 * time.Minute,
		BreakerThreshold:    2,
		BreakerResetAfter:   5 * time.Minute,
	}, clock)
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return clock.now }))
	if err != nil {
		t.Fatal(err)
	}
	repository := &recordingRepository{}
	implementation, err := New(Config{
		Store:      store,
		Scheduler:  guardingEvaluator{guard: guard},
		Provider:   mockProvider,
		Repository: repository,
		Clock:      ClockFunc(func() time.Time { return clock.now }),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	intent := serviceIntent(now)
	if _, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent}); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) > 0 {
			decision := entries[len(entries)-1].decision
			if decision.GetFallback() {
				t.Fatalf("decision = %+v, want successful committed decision", decision)
			}
			if check := guard.CheckN(intent.GetExecutionId()+"/"+intent.GetStageId(), 0, 1); check.Allowed || check.Reason != "COOLDOWN" {
				t.Fatalf("guard after successful reservation = %+v, want consumed cooldown", check)
			}
			last := repository.Last()
			entry, ok := last.Protection.Entries[intent.GetExecutionId()+"/"+intent.GetStageId()]
			if !ok || entry.LastAction.IsZero() || entry.WindowCount != 1 {
				t.Fatalf("checkpoint protection state = %+v", last.Protection)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("successful decision was not emitted")
}

func TestResumeRecoveredStateRestoresProtectionCooldown(t *testing.T) {
	now := time.Date(2026, 8, 29, 14, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	checkpoint := &persistence.SchedulerState{
		Snapshot: serviceSnapshot(now),
		Intents:  map[string]*tgsrlv1.SchedulingIntent{},
		Protection: protection.State{
			Entries: map[string]protection.StateEntry{
				"execution-1/stage-1": {
					LastAction:  now,
					LastScore:   1,
					WindowStart: now,
					WindowCount: 1,
				},
			},
		},
	}
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return clock.now })))
	if err != nil {
		t.Fatal(err)
	}
	guard := protection.NewGuard(protection.Config{
		Enabled:             true,
		Cooldown:            time.Minute,
		MaxActionsPerWindow: 1,
		Window:              10 * time.Minute,
		BreakerThreshold:    2,
		BreakerResetAfter:   5 * time.Minute,
	}, clock)
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return clock.now }))
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{
		Store:      store,
		Scheduler:  guardingEvaluator{guard: guard},
		Provider:   mockProvider,
		Repository: &recordingRepository{},
		DeferStart: true,
		Clock:      ClockFunc(func() time.Time { return clock.now }),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	if err := implementation.ResumeRecoveredState(context.Background(), checkpoint); err != nil {
		t.Fatalf("ResumeRecoveredState() error = %v", err)
	}
	if decision := guard.CheckN("execution-1/stage-1", 1, 1); decision.Allowed || decision.Reason != "COOLDOWN" {
		t.Fatalf("restored guard decision = %+v, want restored cooldown", decision)
	}
}

func TestResumeRecoveredStateWithoutDecisionAuditFailsWithRetryPath(t *testing.T) {
	now := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	sourceStore, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	intent := serviceIntent(now)
	if _, err := sourceStore.PublishIntent(intent); err != nil {
		t.Fatal(err)
	}
	snapshot, err := sourceStore.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	binding := &tgsrlv1.Binding{
		BindingId:     "binding-without-audit",
		PendingUnitId: snapshot.GetPendingUnits()[0].GetPendingUnitId(),
		DeviceIds:     []string{"mock-cpu-0"},
		Resources:     proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector),
	}
	plan := &tgsrlv1.PlacementPlan{
		PlanId:           "plan-without-audit",
		DecisionId:       "decision-without-audit",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		Bindings:         []*tgsrlv1.Binding{binding},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "bind-without-audit",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND,
			Binding:    proto.Clone(binding).(*tgsrlv1.Binding),
		}},
	}
	if _, err := sourceStore.ReservePlan(plan); err != nil {
		t.Fatal(err)
	}
	recovered, err := persistence.Capture(sourceStore, nil, 0, protection.State{})
	if err != nil {
		t.Fatal(err)
	}
	restartedStore, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	if err := persistence.RestoreStore(restartedStore, &recovered); err != nil {
		t.Fatal(err)
	}
	mockProvider, err := provider.NewMockResourceProvider(
		provider.WithNow(func() time.Time { return now }),
		provider.WithRecoveredPlans(&provider.PlanRecord{Plan: plan, Status: provider.PlanStatusSucceeded}),
	)
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := scheduler.New(scheduler.Config{Clock: scheduler.ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{Store: restartedStore, Scheduler: evaluator, Provider: mockProvider, Repository: &recordingRepository{}, DeferStart: true, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	err = implementation.ResumeRecoveredState(context.Background(), &recovered)
	if err == nil || !strings.Contains(err.Error(), "missing its pending decision audit") || !strings.Contains(err.Error(), "retry startup") {
		t.Fatalf("ResumeRecoveredState() error = %v, want explicit audit retry path", err)
	}
	durable := restartedStore.ExportDurableState()
	if len(durable.Reservations) != 1 || durable.Reservations[0].Finalized {
		t.Fatalf("reservation after failed recovery = %+v, want unfinalized retryable state", durable.Reservations)
	}
	if entries, _, _ := implementation.decisionsAfter(0); len(entries) != 0 || implementation.sequence != 0 {
		t.Fatalf("visible decision state after failed recovery: entries=%d sequence=%d", len(entries), implementation.sequence)
	}
}

func TestProviderUnavailableEmitsNoOpFallbackAndKeepsIntent(t *testing.T) {
	now := time.Now().UTC()
	store, err := state.NewStore(serviceSnapshot(now))
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := scheduler.New(scheduler.Config{})
	if err != nil {
		t.Fatal(err)
	}
	base, err := provider.NewMockResourceProvider()
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{Store: store, Scheduler: evaluator, Provider: unhealthyProvider{CompleteResourceProvider: base}, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	intent := serviceIntent(now)
	if _, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) > 0 {
			decision := entries[len(entries)-1].decision
			if !decision.GetFallback() || !strings.HasPrefix(decision.GetFallbackReason(), "PROVIDER_UNAVAILABLE") || len(decision.GetActionResults()) != 0 {
				t.Fatalf("decision = %+v, want provider-unavailable no-op", decision)
			}
			if _, ok := store.LatestValidIntent(intent.GetExecutionId(), intent.GetStageId()); !ok {
				t.Fatal("last valid intent was discarded")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("provider-unavailable fallback was not emitted")
}

func TestProviderWatchChannelCloseFailsClosedAndReconnects(t *testing.T) {
	now := time.Date(2026, 8, 28, 6, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 71 }),
	})
	if err != nil {
		t.Fatal(err)
	}
	base, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	watched := newReconnectingWatchProvider(base)
	recorder := observability.NewInMemoryRecorder()
	implementation, err := New(Config{Store: store, Scheduler: evaluator, Provider: watched, Recorder: recorder, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer implementation.Close()

	select {
	case <-watched.resourceReconnectCh:
	case <-time.After(time.Second):
		t.Fatal("resource watch did not reconnect after initial channel close")
	}
	if got := recorder.Counter("provider_watch_resource_reconnects"); got == 0 {
		t.Fatalf("provider_watch_resource_reconnects = %d, want > 0", got)
	}

	intent := serviceIntent(now)
	if _, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent}); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) > 0 {
			decision := entries[len(entries)-1].decision
			if !decision.GetFallback() || !strings.HasPrefix(decision.GetFallbackReason(), "PROVIDER_UNAVAILABLE") {
				t.Fatalf("decision = %+v, want fail-closed provider-unavailable fallback while watch is unhealthy", decision)
			}
			close(watched.allowResourceWatch)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(watched.allowResourceWatch)
	t.Fatal("provider-watch fail-closed fallback was not emitted")
}

func TestProviderWatchApplyErrorFailsClosed(t *testing.T) {
	now := time.Date(2026, 8, 28, 6, 15, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 72 }),
	})
	if err != nil {
		t.Fatal(err)
	}
	base, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	mockProvider := newCountingWatchProvider(base)
	recorder := observability.NewInMemoryRecorder()
	implementation, err := New(Config{Store: store, Scheduler: evaluator, Provider: mockProvider, Recorder: recorder, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer implementation.Close()

	mockProvider.SendSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId:   "bad-sandbox-event",
		SandboxId: "",
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := recorder.Counter("provider_watch_event_apply_failures"); got > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := recorder.Counter("provider_watch_event_apply_failures"); got == 0 {
		t.Fatal("provider_watch_event_apply_failures did not increment")
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := implementation.providerWatchFailure(); err != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := implementation.providerWatchFailure(); err == nil {
		t.Fatal("provider watch remained healthy after apply error")
	}

	intent := serviceIntent(now)
	intent.ExecutionId = "execution-apply-fail"
	intent.IdempotencyKey = scheduler.IntentIdempotencyKey(intent.GetExecutionId(), intent.GetStageId(), intent.GetVersion())
	if _, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent}); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) > 0 {
			decision := entries[len(entries)-1].decision
			if !decision.GetFallback() || !strings.HasPrefix(decision.GetFallbackReason(), "PROVIDER_UNAVAILABLE") {
				t.Fatalf("decision = %+v, want fail-closed provider-unavailable fallback after apply error", decision)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("apply-error fail-closed fallback was not emitted")
}

func TestProviderWatchCheckpointsProjectionBeforeNoChangeSuppression(t *testing.T) {
	now := time.Date(2026, 8, 28, 6, 20, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := scheduler.New(scheduler.Config{Fallback: scheduler.FallbackNoOp, Clock: scheduler.ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	intent := serviceIntent(now)
	intent.Labels = map[string]string{}
	intent.ResourcesPerUnit.AcceleratorUnits = 0
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("store.PublishIntent() error = %v", err)
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	binding := &tgsrlv1.Binding{
		BindingId:     "binding-durable-provider",
		PendingUnitId: snapshot.GetPendingUnits()[0].GetPendingUnitId(),
		RuntimeUnitId: "runtime-durable-provider",
		DeviceIds:     []string{"mock-cpu-0"},
		Resources:     proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector),
		SandboxId:     "sandbox-durable-provider",
		Generation:    1,
	}
	admission := &tgsrlv1.PlacementPlan{
		PlanId:           "plan-durable-provider",
		DecisionId:       "decision-durable-provider",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		Bindings:         []*tgsrlv1.Binding{binding},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "bind-durable-provider",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND,
			Binding:    proto.Clone(binding).(*tgsrlv1.Binding),
		}},
	}
	if _, err := store.ReservePlan(admission); err != nil {
		t.Fatalf("ReservePlan() error = %v", err)
	}
	if _, err := store.FinalizePlan(admission, true); err != nil {
		t.Fatalf("FinalizePlan() error = %v", err)
	}
	base, err := provider.NewMockResourceProvider(
		provider.WithNow(func() time.Time { return now }),
		provider.WithSandboxes(provider.Sandbox{
			SandboxID:  binding.GetSandboxId(),
			State:      provider.SandboxStateRunning,
			Generation: binding.GetGeneration(),
			Binding:    proto.Clone(binding).(*tgsrlv1.Binding),
			SafePoint:  true,
			UpdatedAt:  now,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	watched := newCountingWatchProvider(base)
	repository := &recordingRepository{}
	implementation, err := New(Config{Store: store, Scheduler: evaluator, Provider: watched, Repository: repository, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()

	implementation.tickRuntime.Enqueue(&eventloop.Trigger{
		ExecutionID: intent.GetExecutionId(),
		StageID:     intent.GetStageId(),
		TickKind:    tgsrlv1.TickKind_TICK_KIND_MEDIUM,
		Cause:       "manual_tick",
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) == 1 && isNoChangeDecision(entries[0].decision) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	entries, _, _ := implementation.decisionsAfter(0)
	if len(entries) != 1 || !isNoChangeDecision(entries[0].decision) {
		t.Fatalf("initial decisions = %+v, want one converged no-change audit", entries)
	}
	before := repository.SaveCount()
	eventSnapshot := serviceSnapshot(now.Add(time.Second))
	eventSnapshot.Allocations = snapshot.GetAllocations()
	eventSnapshot.PendingUnits = snapshot.GetPendingUnits()
	eventSnapshot.Annotations["durable-provider-change"] = "true"
	watched.SendResourceEvent(&tgsrlv1.ResourceEvent{
		EventId: "durable-provider-event", EventType: tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED,
		Provider: "mock", ProviderRevision: 2, ObservedAt: timestamppb.New(now.Add(time.Second)), Snapshot: eventSnapshot,
	})
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) && repository.SaveCount() <= before {
		time.Sleep(10 * time.Millisecond)
	}
	if repository.SaveCount() <= before {
		t.Fatal("provider state change was not checkpointed")
	}
	if got := repository.Last().Snapshot.GetAnnotations()["durable-provider-change"]; got != "true" {
		t.Fatalf("checkpointed provider state = %q, want true", got)
	}
	entries, _, _ = implementation.decisionsAfter(0)
	if len(entries) != 1 {
		t.Fatalf("decisions after suppressed provider event = %d, want 1", len(entries))
	}
}

func TestWorkerPanicReleasesKeyAndFailsClosed(t *testing.T) {
	now := time.Date(2026, 8, 28, 6, 30, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	baseEvaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 73 }),
	})
	if err != nil {
		t.Fatal(err)
	}
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	recorder := observability.NewInMemoryRecorder()
	implementation, err := New(Config{
		Store:     store,
		Scheduler: panicEvaluator{panicOnExecutionID: "panic-execution", delegate: baseEvaluator},
		Provider:  mockProvider,
		Recorder:  recorder,
		Clock:     ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer implementation.Close()

	panicIntent := serviceIntent(now)
	panicIntent.ExecutionId = "panic-execution"
	panicIntent.IdempotencyKey = scheduler.IntentIdempotencyKey(panicIntent.GetExecutionId(), panicIntent.GetStageId(), panicIntent.GetVersion())
	if _, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: panicIntent}); err != nil {
		t.Fatalf("PublishIntent(panic) error = %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := recorder.Counter("intent_worker_panics"); got > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := recorder.Counter("intent_worker_panics"); got == 0 {
		t.Fatal("intent_worker_panics did not increment")
	}

	okIntent := serviceIntent(now)
	okIntent.ExecutionId = "panic-execution"
	okIntent.Version = 2
	okIntent.IdempotencyKey = scheduler.IntentIdempotencyKey(okIntent.GetExecutionId(), okIntent.GetStageId(), okIntent.GetVersion())
	if _, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: okIntent}); err != nil {
		t.Fatalf("PublishIntent(recovery) error = %v", err)
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) > 0 {
			last := entries[len(entries)-1].decision
			if last.GetExecutionId() == okIntent.GetExecutionId() && last.GetIntentVersion() == okIntent.GetVersion() {
				if !last.GetFallback() || !strings.HasPrefix(last.GetFallbackReason(), "PROVIDER_UNAVAILABLE") {
					t.Fatalf("last decision = %+v, want fail-closed provider-unavailable fallback after worker panic", last)
				}
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("later intent did not fail closed after worker panic")
}

func TestRuntimeTriggersAuthorityReconcileWithoutPhantomDecisionOrDuplicateBind(t *testing.T) {
	now := time.Date(2026, 8, 28, 2, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 31 }),
	})
	if err != nil {
		t.Fatalf("scheduler.New() error = %v", err)
	}
	base, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	mockProvider := newCountingWatchProvider(base)
	implementation, err := New(Config{
		Store:     store,
		Scheduler: evaluator,
		Provider:  mockProvider,
		Clock:     ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer implementation.Close()

	intent := serviceIntent(now)
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("store.PublishIntent() error = %v", err)
	}
	implementation.tickRuntime.Enqueue(&eventloop.Trigger{
		ExecutionID: intent.GetExecutionId(),
		StageID:     intent.GetStageId(),
		Cause:       "test-runtime",
	})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) == 1 && mockProvider.ExecuteStepCount() == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	entries, _, _ := implementation.decisionsAfter(0)
	if len(entries) != 1 {
		t.Fatalf("retained decisions = %d, want exactly 1 committed decision from authority reconcile", len(entries))
	}
	if mockProvider.ExecuteStepCount() != 1 {
		t.Fatalf("ExecuteStep count = %d, want exactly 1 after runtime triggers", mockProvider.ExecuteStepCount())
	}
	if _, ok := store.LatestIntent(intent.GetExecutionId(), intent.GetStageId()); !ok {
		t.Fatal("Store lost accepted intent")
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if len(snapshot.GetPendingUnits()) != 0 || len(snapshot.GetAllocations()) != 1 || snapshot.GetAllocations()[0].GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
		t.Fatalf("snapshot = %+v, want one active allocation and no pending units", snapshot)
	}
}

func TestResourceWatchTriggersAuthorityReconcileWithoutDuplicateBind(t *testing.T) {
	now := time.Date(2026, 8, 28, 3, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 41 }),
	})
	if err != nil {
		t.Fatalf("scheduler.New() error = %v", err)
	}
	base, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	mockProvider := newCountingWatchProvider(base)
	implementation, err := New(Config{
		Store:     store,
		Scheduler: evaluator,
		Provider:  mockProvider,
		Clock:     ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer implementation.Close()

	intent := serviceIntent(now)
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("store.PublishIntent() error = %v", err)
	}
	mockProvider.SendResourceEvent(&tgsrlv1.ResourceEvent{
		EventId:          "resource-trigger-1",
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED,
		Provider:         "mock",
		ProviderRevision: 2,
		ObservedAt:       timestamppb.New(now.Add(time.Second)),
		Snapshot:         serviceSnapshot(now.Add(time.Second)),
	})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) == 1 && mockProvider.ExecuteStepCount() == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, ok := store.LatestIntent(intent.GetExecutionId(), intent.GetStageId()); !ok {
		t.Fatal("Store lost watched intent")
	}
	firstEntries, _, _ := implementation.decisionsAfter(0)
	if len(firstEntries) != 1 || mockProvider.ExecuteStepCount() != 1 {
		t.Fatalf("after first resource trigger: decisions=%d executedSteps=%d, want 1/1", len(firstEntries), mockProvider.ExecuteStepCount())
	}

	mockProvider.SendResourceEvent(&tgsrlv1.ResourceEvent{
		EventId:          "resource-trigger-2",
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED,
		Provider:         "mock",
		ProviderRevision: 3,
		ObservedAt:       timestamppb.New(now.Add(2 * time.Second)),
		Snapshot:         serviceSnapshot(now.Add(2 * time.Second)),
	})
	time.Sleep(200 * time.Millisecond)

	secondEntries, _, _ := implementation.decisionsAfter(0)
	if len(secondEntries) != 1 {
		t.Fatalf("resource retrigger retained %d decisions, want still 1 because satisfied intent must not emit extra committed decisions", len(secondEntries))
	}
	if mockProvider.ExecuteStepCount() != 1 {
		t.Fatalf("ExecuteStep count = %d, want still 1 because satisfied intent must not bind twice", mockProvider.ExecuteStepCount())
	}
}

func TestResourceWatchProjectsIntoStoreBeforeAuthorityEvaluate(t *testing.T) {
	now := time.Date(2026, 8, 28, 4, 0, 0, 0, time.UTC)
	initial := serviceSnapshot(now)
	initial.Devices[0].Health = tgsrlv1.DeviceHealth_DEVICE_HEALTH_UNAVAILABLE
	initial.Devices[0].Allocatable = &tgsrlv1.ResourceVector{}
	store, err := state.NewStore(initial, state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 51 }),
	})
	if err != nil {
		t.Fatalf("scheduler.New() error = %v", err)
	}
	unavailableDevice := proto.Clone(serviceSnapshot(now).GetDevices()[0]).(*tgsrlv1.Device)
	unavailableDevice.Health = tgsrlv1.DeviceHealth_DEVICE_HEALTH_UNAVAILABLE
	unavailableDevice.Allocatable = &tgsrlv1.ResourceVector{}
	base, err := provider.NewMockResourceProvider(
		provider.WithNow(func() time.Time { return now }),
		provider.WithDevices(unavailableDevice),
	)
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	mockProvider := newCountingWatchProvider(base)
	implementation, err := New(Config{
		Store:     store,
		Scheduler: evaluator,
		Provider:  mockProvider,
		Clock:     ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer implementation.Close()

	intent := serviceIntent(now)
	if _, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent}); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) == 1 {
			if !entries[0].decision.GetFallback() {
				t.Fatalf("initial decision = %+v, want fallback while Store still shows unavailable device", entries[0].decision)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if entries, _, _ := implementation.decisionsAfter(0); len(entries) != 1 || !entries[0].decision.GetFallback() {
		t.Fatalf("initial decisions = %+v, want one fallback while Store still shows unavailable device", entries)
	}
	if mockProvider.ExecuteStepCount() != 0 {
		t.Fatalf("initial ExecuteStep count = %d, want 0 before resource projection", mockProvider.ExecuteStepCount())
	}

	mockProvider.SendResourceEvent(&tgsrlv1.ResourceEvent{
		EventId:          "resource-store-projection",
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED,
		Provider:         "mock",
		ProviderRevision: 2,
		ObservedAt:       timestamppb.New(now.Add(time.Second)),
		Snapshot:         serviceSnapshot(now.Add(time.Second)),
	})

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) >= 2 && mockProvider.ExecuteStepCount() == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	entries, _, _ := implementation.decisionsAfter(0)
	if len(entries) < 2 {
		t.Fatalf("decisions after resource projection = %d, want fallback then committed bind", len(entries))
	}
	last := entries[len(entries)-1].decision
	if last.GetFallback() || len(last.GetSelectedPlan().GetActions()) != 1 {
		t.Fatalf("last decision = %+v, want committed bind after Store projection", last)
	}
	if mockProvider.ExecuteStepCount() != 1 {
		t.Fatalf("ExecuteStep count = %d, want 1 after resource projection", mockProvider.ExecuteStepCount())
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if snapshot.GetDevices()[0].GetHealth() != tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY {
		t.Fatalf("store snapshot device health = %s, want READY from projected provider snapshot", snapshot.GetDevices()[0].GetHealth())
	}
	if got := snapshot.GetDevices()[0].GetAllocatable().GetCpuMillis(); got == 0 {
		t.Fatalf("store snapshot allocatable cpu = %d, want provider capacity projected before Evaluate", got)
	}
}

func TestServiceStampsTickKindAcrossDecisionAndCandidatePlans(t *testing.T) {
	now := time.Date(2026, 8, 28, 7, 15, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	intent := serviceIntent(now)
	binding := &tgsrlv1.Binding{
		BindingId:     "binding-stamped",
		PendingUnitId: "pending-stamped",
		DeviceIds:     []string{"mock-cpu-0"},
		Resources:     proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector),
		SandboxId:     "sandbox-stamped",
		Generation:    1,
	}
	makeAction := func(actionID, planID string) *tgsrlv1.Action {
		return &tgsrlv1.Action{
			ActionId:                 actionID,
			ActionType:               tgsrlv1.ActionType_ACTION_TYPE_BIND,
			Level:                    tgsrlv1.ActionLevel_ACTION_LEVEL_L1,
			PlanId:                   planID,
			TargetId:                 binding.GetPendingUnitId(),
			Binding:                  proto.Clone(binding).(*tgsrlv1.Binding),
			ExpectedGeneration:       1,
			ExpectedSnapshotRevision: 5,
			Deadline:                 timestamppb.New(now.Add(time.Minute)),
		}
	}
	selectedPlan := &tgsrlv1.PlacementPlan{
		PlanId:           "selected-plan",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: 5,
		DecisionId:       "stamped-decision",
		Actions:          []*tgsrlv1.Action{makeAction("selected-action", "selected-plan")},
	}
	candidatePlan := &tgsrlv1.PlacementPlan{
		PlanId:           "candidate-plan",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: 5,
		DecisionId:       "stamped-decision",
		Actions:          []*tgsrlv1.Action{makeAction("candidate-action", "candidate-plan")},
	}
	spy := &contextualEvaluatorSpy{
		plan: &tgsrlv1.PlacementPlan{
			PlanId:           "authoritative-noop",
			ExecutionId:      intent.GetExecutionId(),
			StageId:          intent.GetStageId(),
			IntentVersion:    intent.GetVersion(),
			SnapshotRevision: 5,
			DecisionId:       "stamped-decision",
			Actions:          []*tgsrlv1.Action{},
		},
		decision: &tgsrlv1.DecisionRecord{
			DecisionId:       "stamped-decision",
			ExecutionId:      intent.GetExecutionId(),
			StageId:          intent.GetStageId(),
			IntentVersion:    intent.GetVersion(),
			SnapshotRevision: 5,
			SelectedPlan:     proto.Clone(selectedPlan).(*tgsrlv1.PlacementPlan),
			Candidates: []*tgsrlv1.PlacementCandidate{{
				CandidateId: "candidate-1",
				Plan:        proto.Clone(candidatePlan).(*tgsrlv1.PlacementPlan),
			}},
		},
	}
	mockProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{Store: store, Scheduler: spy, Provider: mockProvider, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()

	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("store.PublishIntent() error = %v", err)
	}
	implementation.tickRuntime.Enqueue(&eventloop.Trigger{
		ExecutionID: intent.GetExecutionId(),
		StageID:     intent.GetStageId(),
		Cause:       "test-stamp",
	})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		decision := entries[len(entries)-1].decision
		if decision.GetTickKind() != tgsrlv1.TickKind_TICK_KIND_FAST {
			t.Fatalf("decision tick_kind = %s, want FAST", decision.GetTickKind())
		}
		if got := decision.GetSelectedPlan().GetActions()[0].GetTickKind(); got != tgsrlv1.TickKind_TICK_KIND_FAST {
			t.Fatalf("selected plan action tick_kind = %s, want FAST", got)
		}
		if got := decision.GetCandidates()[0].GetPlan().GetActions()[0].GetTickKind(); got != tgsrlv1.TickKind_TICK_KIND_FAST {
			t.Fatalf("candidate plan action tick_kind = %s, want FAST", got)
		}
		return
	}
	t.Fatal("stamped decision was not retained")
}

func TestAdaptiveProductionRetainsFirstConvergedAuditAndSuppressesDuplicateProviderEvent(t *testing.T) {
	now := time.Date(2026, 8, 29, 1, 0, 0, 0, time.UTC)
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	intent := serviceIntent(now)
	intent.Labels = map[string]string{}
	intent.ResourcesPerUnit.AcceleratorUnits = 0
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("store.PublishIntent() error = %v", err)
	}
	snapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	binding := &tgsrlv1.Binding{
		BindingId:     "binding-converged",
		PendingUnitId: snapshot.GetPendingUnits()[0].GetPendingUnitId(),
		RuntimeUnitId: "runtime-converged",
		DeviceIds:     []string{"mock-cpu-0"},
		Resources:     proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector),
		SandboxId:     "sandbox-a",
		Generation:    1,
	}
	admission := &tgsrlv1.PlacementPlan{
		PlanId:           "plan-converged-admission",
		DecisionId:       "decision-converged-admission",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		Bindings:         []*tgsrlv1.Binding{binding},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "bind-converged",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND,
			Binding:    proto.Clone(binding).(*tgsrlv1.Binding),
		}},
	}
	if _, err := store.ReservePlan(admission); err != nil {
		t.Fatalf("ReservePlan() error = %v", err)
	}
	if _, err := store.FinalizePlan(admission, true); err != nil {
		t.Fatalf("FinalizePlan() error = %v", err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 91 }),
	})
	if err != nil {
		t.Fatal(err)
	}
	base, err := provider.NewMockResourceProvider(
		provider.WithNow(func() time.Time { return now }),
		provider.WithSandboxes(provider.Sandbox{
			SandboxID:  binding.GetSandboxId(),
			State:      provider.SandboxStateRunning,
			Generation: binding.GetGeneration(),
			Binding:    proto.Clone(binding).(*tgsrlv1.Binding),
			Share:      intent.GetResourcesPerUnit().GetAcceleratorUnits(),
			SafePoint:  true,
			UpdatedAt:  now,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	mockProvider := newCountingWatchProvider(base)
	implementation, err := New(Config{
		Store:     store,
		Scheduler: evaluator,
		Provider:  mockProvider,
		Clock:     ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()

	implementation.tickRuntime.Enqueue(&eventloop.Trigger{
		ExecutionID: intent.GetExecutionId(),
		StageID:     intent.GetStageId(),
		TickKind:    tgsrlv1.TickKind_TICK_KIND_MEDIUM,
		Cause:       "manual_tick",
	})

	deadline := time.Now().Add(2 * time.Second)
	var first *tgsrlv1.DecisionRecord
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		if len(entries) == 0 {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		first = entries[len(entries)-1].decision
		break
	}
	if first == nil {
		t.Fatal("first adaptive converged decision was not retained")
	}
	if first.GetFallback() || len(first.GetSelectedPlan().GetActions()) != 0 {
		t.Fatalf("first converged decision = %+v, want non-fallback no-action", first)
	}
	if first.GetSelectedPlan().GetPurpose() != tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION {
		t.Fatalf("first converged purpose = %s, want RECONCILIATION", first.GetSelectedPlan().GetPurpose())
	}
	if len(first.GetPlannerEvidence()) == 0 || first.GetPlannerEvidence()[0].GetReason() != "CONVERGED_NO_TRIGGER" {
		t.Fatalf("first converged evidence = %+v, want CONVERGED_NO_TRIGGER", first.GetPlannerEvidence())
	}
	if mockProvider.ExecuteStepCount() != 0 {
		t.Fatalf("ExecuteStep count after converged audit = %d, want 0", mockProvider.ExecuteStepCount())
	}

	mockProvider.SendResourceEvent(&tgsrlv1.ResourceEvent{
		EventId:          "provider-event-1",
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED,
		Provider:         "mock",
		ProviderRevision: 2,
		ObservedAt:       timestamppb.New(now.Add(time.Second)),
		Snapshot:         serviceSnapshot(now.Add(time.Second)),
	})
	time.Sleep(300 * time.Millisecond)

	entries, _, _ := implementation.decisionsAfter(0)
	if len(entries) != 1 {
		t.Fatalf("retained decisions after duplicate provider_event = %d, want still 1", len(entries))
	}
	if mockProvider.ExecuteStepCount() != 0 {
		t.Fatalf("ExecuteStep count after duplicate provider_event = %d, want still 0", mockProvider.ExecuteStepCount())
	}
}

func TestAdaptiveProductionMutationReachesProviderAndStore(t *testing.T) {
	now := time.Date(2026, 8, 29, 1, 30, 0, 0, time.UTC)
	capabilities := provider.DefaultMockCapabilities()
	capabilities.SupportedActions = []string{"bind", "pause"}
	initial := serviceSnapshot(now)
	initial.Devices[0].Capabilities = proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)
	store, err := state.NewStore(initial, state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	intent := serviceIntent(now)
	intent.Labels = map[string]string{
		"tgsrl.io/adaptive.target-state": "paused",
		"runtime_unit_id":                "runtime-a",
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 92 }),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishIntent(intent); err != nil {
		t.Fatalf("store.PublishIntent() error = %v", err)
	}
	admissionSnapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot(admission) error = %v", err)
	}
	admissionPlan, _, err := evaluator.Evaluate(admissionSnapshot, intent)
	if err != nil {
		t.Fatalf("Evaluate(admission) error = %v", err)
	}
	if len(admissionPlan.GetBindings()) != 1 || len(admissionPlan.GetActions()) != 1 || admissionPlan.GetActions()[0].GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND {
		t.Fatalf("admission plan = %+v, want one binding action", admissionPlan)
	}
	if _, err := store.ReservePlan(admissionPlan); err != nil {
		t.Fatalf("ReservePlan(admission) error = %v", err)
	}
	activeSnapshot, err := store.FinalizePlan(admissionPlan, true)
	if err != nil {
		t.Fatalf("FinalizePlan(admission) error = %v", err)
	}
	binding := proto.Clone(admissionPlan.GetBindings()[0]).(*tgsrlv1.Binding)
	if len(activeSnapshot.GetAllocations()) != 1 || activeSnapshot.GetAllocations()[0].GetGeneration() != binding.GetGeneration() || activeSnapshot.GetAllocations()[0].GetRuntimeUnitId() != binding.GetRuntimeUnitId() {
		t.Fatalf("active allocation = %+v, want admission binding identity %+v", activeSnapshot.GetAllocations(), binding)
	}
	base, err := provider.NewMockResourceProvider(
		provider.WithNow(func() time.Time { return now }),
		provider.WithCapabilities(capabilities),
		provider.WithSandboxes(provider.Sandbox{
			SandboxID:  binding.GetSandboxId(),
			State:      provider.SandboxStateRunning,
			Generation: binding.GetGeneration(),
			Binding:    binding,
			SafePoint:  true,
			SemanticContext: &tgsrlv1.SemanticEnvelope{TypedFields: []*tgsrlv1.SemanticField{
				{Key: "planner.checkpoint_capable", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_BoolValue{BoolValue: true}}},
				{Key: "planner.buffer_below_watermark", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_BoolValue{BoolValue: true}}},
				{Key: "planner.minimum_residency_nanos", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_Int64Value{Int64Value: 0}}},
				{Key: "planner.action_in_flight", Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_BoolValue{BoolValue: false}}},
			}},
			StateChangedAt: now,
			UpdatedAt:      now,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{
		Store:     store,
		Scheduler: evaluator,
		Provider:  base,
		Clock:     ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()

	implementation.tickRuntime.Enqueue(&eventloop.Trigger{
		ExecutionID: intent.GetExecutionId(),
		StageID:     intent.GetStageId(),
		TickKind:    tgsrlv1.TickKind_TICK_KIND_MEDIUM,
		Cause:       "manual_mutation",
	})

	deadline := time.Now().Add(2 * time.Second)
	var mutationDecision *tgsrlv1.DecisionRecord
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		for _, entry := range entries {
			actions := entry.decision.GetSelectedPlan().GetActions()
			if len(actions) == 1 && actions[0].GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_PAUSE {
				mutationDecision = entry.decision
				break
			}
		}
		if mutationDecision == nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if mutationDecision.GetFallback() || len(mutationDecision.GetActionResults()) != 1 || mutationDecision.GetActionResults()[0].GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED {
			t.Fatalf("adaptive mutation decision = %+v, want one successful non-fallback pause", mutationDecision)
		}
		record, err := base.ReconcilePlan(context.Background(), mutationDecision.GetSelectedPlan())
		if err != nil || record.Status != provider.PlanStatusSucceeded {
			t.Fatalf("ReconcilePlan(mutation) = %+v, error = %v; want succeeded provider record", record, err)
		}
		providerSandbox, err := base.GetSandbox(context.Background(), binding.GetSandboxId())
		if err != nil || providerSandbox.State != provider.SandboxStatePaused {
			t.Fatalf("provider sandbox = %+v, error = %v; want paused", providerSandbox, err)
		}
		sandboxes, err := store.ListSandboxes(context.Background())
		if err != nil {
			t.Fatalf("ListSandboxes() error = %v", err)
		}
		if len(sandboxes) == 0 || sandboxes[0].GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if !proto.Equal(sandboxes[0].GetBinding(), binding) {
			t.Fatalf("projected sandbox binding = %+v, want admission binding %+v", sandboxes[0].GetBinding(), binding)
		}
		return
	}
	t.Fatalf("adaptive pause did not converge in provider and Store; decision=%+v sandboxes=%+v", mutationDecision, store.ListProjectedSandboxes())
}

func TestPreemptionTransactionReplacesVictimWithoutExposingCapacity(t *testing.T) {
	now := time.Date(2026, 8, 29, 2, 0, 0, 0, time.UTC)
	resources := &tgsrlv1.ResourceVector{CpuMillis: 500, MemoryBytes: 1 << 30, AcceleratorUnits: .25}
	capabilities := provider.DefaultMockCapabilities()
	capabilities.SupportedActions = []string{"bind", "release"}
	priority := int32(1)
	victimBinding := &tgsrlv1.Binding{BindingId: "binding-victim", PendingUnitId: "pending-victim", RuntimeUnitId: "runtime-victim", SandboxId: "sandbox-victim", DeviceIds: []string{"mock-cpu-0"}, Resources: proto.Clone(resources).(*tgsrlv1.ResourceVector), Generation: 1}
	initial := &tgsrlv1.ClusterSnapshot{
		SnapshotId: "snapshot-preemption", Revision: 5, ObservedAt: timestamppb.New(now),
		Annotations: map[string]string{scheduler.SafePointAnnotation: "true"},
		Devices:     []*tgsrlv1.Device{{DeviceId: "mock-cpu-0", Kind: tgsrlv1.DeviceKind_DEVICE_KIND_CPU, Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY, Capacity: proto.Clone(resources).(*tgsrlv1.ResourceVector), Allocatable: &tgsrlv1.ResourceVector{}, Capabilities: proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)}},
		Allocations: []*tgsrlv1.Allocation{{AllocationId: "allocation-victim", ExecutionId: "old-execution", StageId: "old-stage", IntentVersion: 1, JobId: "old-job", PendingUnitId: victimBinding.GetPendingUnitId(), DeviceIds: append([]string(nil), victimBinding.GetDeviceIds()...), Resources: proto.Clone(resources).(*tgsrlv1.ResourceVector), State: tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE, RuntimeUnitId: victimBinding.GetRuntimeUnitId(), SandboxId: victimBinding.GetSandboxId(), BindingId: victimBinding.GetBindingId(), Generation: 1, Priority: &priority}},
	}
	store, err := state.NewStore(initial, state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatal(err)
	}
	intent := serviceIntent(now)
	intent.Priority = 100
	intent.ResourcesPerUnit = proto.Clone(resources).(*tgsrlv1.ResourceVector)
	intent.RequiredCapabilities = proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)
	intent.Labels = map[string]string{"runtime_unit_id": "runtime-replacement"}
	evaluator, err := scheduler.New(scheduler.Config{Fallback: scheduler.FallbackNoOp, Clock: scheduler.ClockFunc(func() time.Time { return now }), Policy: policy.Bundle{ID: "preempt", Version: "1", Strategy: policy.StrategyScoreFirst, TopK: 1, AllowPreemption: true, PreemptionPolicy: "low_priority_first", RequireSafePoint: true}, Preemption: preemption.LowPriorityFirst{}})
	if err != nil {
		t.Fatal(err)
	}
	resourceProvider, err := provider.NewMockResourceProvider(provider.WithNow(func() time.Time { return now }), provider.WithDevices(initial.GetDevices()...), provider.WithCapabilities(capabilities), provider.WithSandboxes(provider.Sandbox{SandboxID: victimBinding.GetSandboxId(), State: provider.SandboxStateRunning, Generation: 1, Binding: victimBinding, SafePoint: true, UpdatedAt: now}))
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := New(Config{Store: store, Scheduler: evaluator, Provider: resourceProvider, Clock: ClockFunc(func() time.Time { return now })})
	if err != nil {
		t.Fatal(err)
	}
	defer implementation.Close()
	if _, err := implementation.PublishIntent(context.Background(), &tgsrlv1.PublishIntentRequest{Intent: intent}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, _, _ := implementation.decisionsAfter(0)
		for _, entry := range entries {
			decision := entry.decision
			if decision.GetSelectedPlan().GetPurpose() != tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION {
				continue
			}
			if decision.GetFallback() || len(decision.GetActionResults()) != 2 {
				t.Fatalf("preemption decision = %+v", decision)
			}
			snapshot, snapshotErr := store.GetSnapshot(context.Background(), 0, true)
			if snapshotErr != nil {
				t.Fatal(snapshotErr)
			}
			if len(snapshot.GetAllocations()) != 1 || snapshot.GetAllocations()[0].GetPendingUnitId() == victimBinding.GetPendingUnitId() || snapshot.GetAllocations()[0].GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
				t.Fatalf("allocations after preemption = %+v, want one active replacement", snapshot.GetAllocations())
			}
			if got := snapshot.GetDevices()[0].GetAllocatable().GetCpuMillis(); got != 0 {
				t.Fatalf("allocatable cpu = %d, want 0 while replacement owns released capacity", got)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("preemption did not converge; decisions=%+v", implementation.decisions)
}

func TestResumeRecoveredStateReconcilesReservationsAndReplaysIntent(t *testing.T) {
	now := time.Date(2026, 8, 28, 1, 0, 0, 0, time.UTC)
	sourceStore, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore(source) error = %v", err)
	}
	intent := serviceIntent(now)
	intent.RunId = "run-recovered"
	if _, err := sourceStore.PublishIntent(intent); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	snapshot, err := sourceStore.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	pending := snapshot.GetPendingUnits()
	if len(pending) != 1 {
		t.Fatalf("pending units = %d, want 1", len(pending))
	}
	binding := &tgsrlv1.Binding{
		BindingId:     "binding-recovered",
		PendingUnitId: pending[0].GetPendingUnitId(),
		DeviceIds:     []string{"mock-cpu-0"},
		Resources:     proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector),
	}
	plan := &tgsrlv1.PlacementPlan{
		PlanId:           "plan-recovered",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		Bindings:         []*tgsrlv1.Binding{binding},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "bind-recovered",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND,
			Binding:    proto.Clone(binding).(*tgsrlv1.Binding),
		}},
	}
	if _, err := sourceStore.ReservePlan(plan); err != nil {
		t.Fatalf("ReservePlan() error = %v", err)
	}
	recoveredDecision := &tgsrlv1.DecisionRecord{
		DecisionId:       "decision-recovered",
		Sequence:         4,
		JobId:            intent.GetJobId(),
		RunId:            intent.GetRunId(),
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		SelectedPlan:     proto.Clone(plan).(*tgsrlv1.PlacementPlan),
		DecidedAt:        timestamppb.New(now),
	}
	recovered, err := persistence.Capture(sourceStore, []*tgsrlv1.DecisionRecord{recoveredDecision}, recoveredDecision.GetSequence(), protection.State{})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore(restored) error = %v", err)
	}
	if err := persistence.RestoreStore(store, &recovered); err != nil {
		t.Fatalf("RestoreStore() error = %v", err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 17 }),
	})
	if err != nil {
		t.Fatalf("scheduler.New() error = %v", err)
	}
	mockProvider, err := provider.NewMockResourceProvider(
		provider.WithNow(func() time.Time { return now }),
		provider.WithRecoveredPlans(&provider.PlanRecord{
			Plan:   proto.Clone(plan).(*tgsrlv1.PlacementPlan),
			Status: provider.PlanStatusSucceeded,
			Results: []*tgsrlv1.ActionResult{{
				ActionId: "bind-recovered",
				Status:   tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED,
			}},
		}),
	)
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	repository := &recordingRepository{}
	implementation, err := New(Config{
		Store:             store,
		Scheduler:         evaluator,
		Provider:          mockProvider,
		Repository:        repository,
		DecisionRetention: 8,
		Clock:             ClockFunc(func() time.Time { return now }),
		DeferStart:        true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer implementation.Close()

	if err := implementation.ResumeRecoveredState(context.Background(), &recovered); err != nil {
		t.Fatalf("ResumeRecoveredState() error = %v", err)
	}
	if err := implementation.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got, ok := implementation.decisionByID(recoveredDecision.GetDecisionId()); !ok || got.GetDecisionId() != recoveredDecision.GetDecisionId() {
		t.Fatalf("restored decision missing after resume: got=%+v ok=%v", got, ok)
	}
	time.Sleep(200 * time.Millisecond)
	entries, _, _ := implementation.decisionsAfter(0)
	if len(entries) != 1 {
		t.Fatalf("retained decisions = %d, want only the restored committed decision after satisfied replay", len(entries))
	}
	if _, ok := store.LatestIntent(intent.GetExecutionId(), intent.GetStageId()); !ok {
		t.Fatal("Store lost recovered intent")
	}
	finalSnapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot(final) error = %v", err)
	}
	if len(finalSnapshot.GetPendingUnits()) != 0 || len(finalSnapshot.GetAllocations()) != 1 || finalSnapshot.GetAllocations()[0].GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
		t.Fatalf("final snapshot = %+v, want finalized active allocation with no pending units", finalSnapshot)
	}
	durable := store.ExportDurableState()
	if len(durable.Reservations) != 1 || !durable.Reservations[0].Finalized || !durable.Reservations[0].Succeeded {
		t.Fatalf("durable reservations = %+v, want one finalized successful reservation", durable.Reservations)
	}
	if repository.SaveCount() == 0 {
		t.Fatal("ResumeRecoveredState() did not checkpoint reconciled state")
	}
	lastCheckpoint := repository.Last()
	if len(lastCheckpoint.Reservations) != 1 || !lastCheckpoint.Reservations[0].Finalized || !lastCheckpoint.Reservations[0].Succeeded {
		t.Fatalf("last checkpoint reservations = %+v, want finalized successful reservation", lastCheckpoint.Reservations)
	}
	if lastCheckpoint.Cursor != recoveredDecision.GetSequence() {
		t.Fatalf("last checkpoint cursor = %d, want restored cursor %d because satisfied replay must not emit new decisions", lastCheckpoint.Cursor, recoveredDecision.GetSequence())
	}
}

func TestResumeRecoveredStateMutationRollbackPreservesUnrelatedUpdates(t *testing.T) {
	now := time.Date(2026, 8, 28, 2, 0, 0, 0, time.UTC)
	sourceStore, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore(source) error = %v", err)
	}
	intent := serviceIntent(now)
	intent.UnitCount = 1
	if _, err := sourceStore.PublishIntent(intent); err != nil {
		t.Fatalf("PublishIntent() error = %v", err)
	}
	snapshot, err := sourceStore.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	binding := &tgsrlv1.Binding{
		BindingId:     "resume-bind",
		PendingUnitId: snapshot.GetPendingUnits()[0].GetPendingUnitId(),
		DeviceIds:     []string{"mock-cpu-0"},
		Resources:     proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector),
	}
	admission := &tgsrlv1.PlacementPlan{
		PlanId:           "resume-admission",
		DecisionId:       "resume-admission-decision",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		Bindings:         []*tgsrlv1.Binding{binding},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "resume-bind-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND,
			Binding:    proto.Clone(binding).(*tgsrlv1.Binding),
		}},
	}
	if _, err := sourceStore.ReservePlan(admission); err != nil {
		t.Fatalf("ReservePlan(admission) error = %v", err)
	}
	activeSnapshot, err := sourceStore.FinalizePlan(admission, true)
	if err != nil {
		t.Fatalf("FinalizePlan(admission) error = %v", err)
	}
	allocation := proto.Clone(activeSnapshot.GetAllocations()[0]).(*tgsrlv1.Allocation)
	mutationPlan := &tgsrlv1.PlacementPlan{
		PlanId:                "resume-recreate",
		DecisionId:            "resume-recreate-decision",
		ExecutionId:           intent.GetExecutionId(),
		StageId:               intent.GetStageId(),
		IntentVersion:         intent.GetVersion(),
		SnapshotRevision:      activeSnapshot.GetRevision(),
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		AffectedAllocationIds: []string{allocation.GetAllocationId()},
		Actions: []*tgsrlv1.Action{{
			ActionId:   "resume-recreate-action",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_RECREATE,
			TargetId:   allocation.GetAllocationId(),
			Binding: &tgsrlv1.Binding{
				BindingId:     "resume-recreate-binding",
				DeviceIds:     append([]string(nil), allocation.GetDeviceIds()...),
				Resources:     proto.Clone(allocation.GetResources()).(*tgsrlv1.ResourceVector),
				Generation:    allocation.GetGeneration() + 1,
				RuntimeUnitId: "runtime-recreated",
			},
		}},
	}
	if _, err := sourceStore.ReservePlan(mutationPlan); err != nil {
		t.Fatalf("ReservePlan(mutation) error = %v", err)
	}
	recoveredDecision := &tgsrlv1.DecisionRecord{
		DecisionId:       mutationPlan.GetDecisionId(),
		Sequence:         9,
		JobId:            intent.GetJobId(),
		RunId:            intent.GetRunId(),
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: activeSnapshot.GetRevision(),
		SelectedPlan:     proto.Clone(mutationPlan).(*tgsrlv1.PlacementPlan),
		DecidedAt:        timestamppb.New(now),
	}
	recovered, err := persistence.Capture(sourceStore, []*tgsrlv1.DecisionRecord{recoveredDecision}, recoveredDecision.GetSequence(), protection.State{})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	var recoveredMutation *persistence.ReservationRecord
	for index := range recovered.Reservations {
		record := &recovered.Reservations[index]
		if record.Plan != nil && record.Plan.GetPlanId() == mutationPlan.GetPlanId() {
			recoveredMutation = record
			break
		}
	}
	if recoveredMutation == nil || recoveredMutation.BeforeSnapshot == nil {
		t.Fatalf("captured reservations = %+v, want mutation before_snapshot persisted", recovered.Reservations)
	}
	if got := recoveredMutation.DeviceAllocatable["mock-cpu-0"]; got == nil || got.GetCpuMillis() == 0 {
		t.Fatalf("captured mutation device allocatable = %+v, want persisted rollback delta for mock-cpu-0", recoveredMutation.DeviceAllocatable)
	}
	store, err := state.NewStore(serviceSnapshot(now), state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewStore(restored) error = %v", err)
	}
	if err := persistence.RestoreStore(store, &recovered); err != nil {
		t.Fatalf("RestoreStore() error = %v", err)
	}
	restoredDurable := store.ExportDurableState()
	var restoredMutation *state.ReservationRecord
	for index := range restoredDurable.Reservations {
		record := &restoredDurable.Reservations[index]
		if record.Plan != nil && record.Plan.GetPlanId() == mutationPlan.GetPlanId() {
			restoredMutation = record
			break
		}
	}
	if restoredMutation == nil || restoredMutation.BeforeSnapshot == nil {
		t.Fatalf("restored durable reservations = %+v, want mutation before_snapshot retained", restoredDurable.Reservations)
	}
	if got := restoredMutation.DeviceAllocatable["mock-cpu-0"]; got == nil || got.GetCpuMillis() == 0 {
		t.Fatalf("restored mutation device allocatable = %+v, want rollback delta retained", restoredMutation.DeviceAllocatable)
	}
	if _, err := store.MutateResources(store.Revision(), func(working *tgsrlv1.ClusterSnapshot) error {
		working.PendingUnits = append(working.PendingUnits, &tgsrlv1.PendingUnit{
			PendingUnitId: "service-unrelated-pending",
			ExecutionId:   "other-execution",
			StageId:       "other-stage",
			Priority:      5,
		})
		if len(working.Devices) > 1 {
			working.Devices[1].Allocatable.CpuMillis = 1234
		}
		return nil
	}); err != nil {
		t.Fatalf("MutateResources(unrelated post-restore updates) error = %v", err)
	}
	evaluator, err := scheduler.New(scheduler.Config{
		Fallback: scheduler.FallbackNoOp,
		Clock:    scheduler.ClockFunc(func() time.Time { return now }),
		Sequence: scheduler.SequenceFunc(func() uint64 { return 19 }),
	})
	if err != nil {
		t.Fatalf("scheduler.New() error = %v", err)
	}
	mockProvider, err := provider.NewMockResourceProvider(
		provider.WithNow(func() time.Time { return now }),
		provider.WithRecoveredPlans(&provider.PlanRecord{
			Plan:   proto.Clone(mutationPlan).(*tgsrlv1.PlacementPlan),
			Status: provider.PlanStatusFailed,
			Results: []*tgsrlv1.ActionResult{{
				ActionId:          "resume-recreate-action",
				Status:            tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED,
				RollbackAttempted: true,
				RollbackStatus:    tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK,
			}},
		}),
	)
	if err != nil {
		t.Fatalf("NewMockResourceProvider() error = %v", err)
	}
	repository := &recordingRepository{}
	implementation, err := New(Config{
		Store:             store,
		Scheduler:         evaluator,
		Provider:          mockProvider,
		Repository:        repository,
		DecisionRetention: 8,
		Clock:             ClockFunc(func() time.Time { return now }),
		DeferStart:        true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer implementation.Close()
	if err := implementation.ResumeRecoveredState(context.Background(), &recovered); err != nil {
		t.Fatalf("ResumeRecoveredState() error = %v", err)
	}
	finalSnapshot, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot(final) error = %v", err)
	}
	if len(finalSnapshot.GetAllocations()) != 1 || !proto.Equal(finalSnapshot.GetAllocations()[0], allocation) {
		t.Fatalf("final allocations after recovered rollback = %+v, want restored source allocation %+v", finalSnapshot.GetAllocations(), allocation)
	}
	foundPending := false
	for _, unit := range finalSnapshot.GetPendingUnits() {
		if unit.GetPendingUnitId() == "service-unrelated-pending" {
			foundPending = true
			break
		}
	}
	if !foundPending {
		t.Fatalf("final pending units = %+v, want unrelated pending preserved", finalSnapshot.GetPendingUnits())
	}
	if len(finalSnapshot.GetDevices()) > 1 && finalSnapshot.GetDevices()[1].GetAllocatable().GetCpuMillis() != 1234 {
		t.Fatalf("final devices = %+v, want unrelated device allocatable preserved", finalSnapshot.GetDevices())
	}
}

func runtimeStarted(runtime *eventloop.Runtime) bool {
	value := reflect.ValueOf(runtime)
	if !value.IsValid() || value.IsNil() {
		return false
	}
	cancel := value.Elem().FieldByName("cancel")
	return cancel.IsValid() && !cancel.IsNil()
}

func startTestServer(t *testing.T, implementation *Server) (tgsrlv1.SchedulerServiceClient, func()) {
	t.Helper()
	listener := bufconn.Listen(testBufferSize)
	server := grpc.NewServer()
	tgsrlv1.RegisterSchedulerServiceServer(server, implementation)
	tgsrlv1.RegisterSchedulerObservationServiceServer(server, implementation)
	serveErrors := make(chan error, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				serveErrors <- status.Errorf(codes.Internal, "test server panic: %v", recovered)
			}
		}()
		serveErrors <- server.Serve(listener)
	}()
	connection, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		server.Stop()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	cleanup := func() {
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
		select {
		case err := <-serveErrors:
			if err != nil && err != grpc.ErrServerStopped {
				t.Errorf("server.Serve() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("server did not stop")
		}
	}
	return tgsrlv1.NewSchedulerServiceClient(connection), cleanup
}

func serviceSnapshot(now time.Time) *tgsrlv1.ClusterSnapshot {
	resources := &tgsrlv1.ResourceVector{CpuMillis: 4000, MemoryBytes: 8 << 30, AcceleratorUnits: 1}
	return &tgsrlv1.ClusterSnapshot{
		SnapshotId:  "snapshot-5",
		Revision:    5,
		ObservedAt:  timestamppb.New(now),
		Annotations: map[string]string{scheduler.SafePointAnnotation: "true"},
		Devices: []*tgsrlv1.Device{{
			DeviceId:    "mock-cpu-0",
			Kind:        tgsrlv1.DeviceKind_DEVICE_KIND_CPU,
			Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:    resources,
			Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 4000, MemoryBytes: 8 << 30, AcceleratorUnits: 1},
			Capabilities: &tgsrlv1.CapabilitySet{
				Names:            []string{"logical-cpu"},
				Source:           "mock",
				Revision:         1,
				MeasuredAt:       timestamppb.New(now),
				SupportedActions: []string{"bind"},
			},
		}},
	}
}

func serviceIntent(now time.Time) *tgsrlv1.SchedulingIntent {
	intent := &tgsrlv1.SchedulingIntent{
		ExecutionId:      "execution-1",
		StageId:          "stage-1",
		Version:          1,
		SubmittedAt:      timestamppb.New(now),
		Ttl:              durationpb.New(time.Minute),
		ValidUntil:       timestamppb.New(now.Add(time.Minute)),
		IdempotencyKey:   scheduler.IntentIdempotencyKey("execution-1", "stage-1", 1),
		JobId:            "job-1",
		ResourcesPerUnit: &tgsrlv1.ResourceVector{CpuMillis: 500, MemoryBytes: 1 << 30, AcceleratorUnits: .25},
		UnitCount:        1,
		RequiredCapabilities: &tgsrlv1.CapabilitySet{
			Names:            []string{"logical-cpu"},
			Source:           "mock",
			Revision:         1,
			SupportedActions: []string{"bind"},
		},
		ExecutionContract: &tgsrlv1.ExecutionContract{
			Version:            "1.0.0",
			PhaseGraph:         &tgsrlv1.PhaseGraph{Phases: []*tgsrlv1.Phase{{PhaseId: "stage-1", DisplayName: "Stage", Kind: tgsrlv1.PhaseKind_PHASE_KIND_DECODE, Parallelism: 1, MaxAttempts: 1}}, EntryPhaseIds: []string{"stage-1"}},
			ValidityRules:      []*tgsrlv1.ValidityRule{{RuleId: "valid", Description: "valid", Expression: "true", FailureMode: tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_REJECT}},
			VersionConstraints: []*tgsrlv1.VersionConstraint{{Component: "protocol", Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_COMPATIBLE, Version: "0.3"}},
			CommitPolicy:       &tgsrlv1.CommitPolicy{Mode: tgsrlv1.CommitMode_COMMIT_MODE_ALL_OR_NOTHING, MinimumSuccessfulUnits: 1, CommitTimeout: durationpb.New(time.Minute)},
			BackpressurePolicy: &tgsrlv1.BackpressurePolicy{Mode: tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_BLOCK_PRODUCER, MaximumBufferLevel: 1, StallTimeout: durationpb.New(time.Minute)},
			SafePointPolicy:    &tgsrlv1.SafePointPolicy{Enabled: true, Trigger: tgsrlv1.SafePointTrigger_SAFE_POINT_TRIGGER_EXPLICIT, MaximumWait: durationpb.New(time.Minute)},
			Capabilities:       &tgsrlv1.Capabilities{DeterministicReplay: true, TransactionalCommits: true},
		},
		PolicyVersion: "policy-1",
		Queue:         "default",
		RolloutMode:   tgsrlv1.RolloutMode_ROLLOUT_MODE_PARTIALLY_ASYNC,
		PhaseKind:     tgsrlv1.PhaseKind_PHASE_KIND_DECODE,
	}
	intent.ExecutionContract.ContractId, _ = scheduler.CanonicalContractID(intent.ExecutionContract)
	return intent
}
