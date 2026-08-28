package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/admission"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/cursor"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/statuswatch"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestWorkerConsumesSecondDecisionWhileFirstObservationBlocks(t *testing.T) {
	dir := t.TempDir()
	cursorRepo := cursor.NewFileRepository(filepath.Join(dir, "cursor.json"))
	ledger := NewFileDeliveryRepository(filepath.Join(dir, "delivery.json"))
	observer := newControlledObserver()
	publisher := &fakePublisher{}
	first := decisionForSequence(9)
	second := decisionForSequence(10)

	w, err := New(Config{
		Source: &gatedDecisionSource{
			decisions:           []*tgsrlv1.DecisionRecord{first, second},
			firstWatcherStarted: observer.watcherStarted(),
		},
		JobRuns:       flexibleJobRunClient{},
		Manifests:     flexibleManifestClient{},
		Reconciler:    deterministicReconciler{},
		Observer:      observer,
		Publisher:     publisher,
		Cursors:       cursorRepo,
		Deliveries:    ledger,
		Registrations: ledger,
		QueuePolicy:   queuePolicy(),
		Namespace:     "test-ns",
	})
	if err != nil {
		t.Fatal(err)
	}
	done, cancel := startObservationManager(t, w.observations)
	defer func() {
		if done != nil {
			stopObservationManager(t, cancel, done)
		}
	}()

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	runDone := make(chan error, 1)
	go func() { runDone <- w.RunOnce(runCtx) }()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("RunOnce() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("decision stream was blocked by the first observation watcher")
	}
	select {
	case <-observer.watcherStarted():
	case <-time.After(2 * time.Second):
		t.Fatal("second observation watcher did not enter Recv")
	}
	saved, err := cursorRepo.Load()
	if err != nil {
		t.Fatal(err)
	}
	if saved.Sequence != 10 || saved.DecisionID != second.GetDecisionId() {
		t.Fatalf("cursor = %+v, want second decision", saved)
	}
	registrations, err := ledger.ListRegistrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(registrations) != 2 {
		t.Fatalf("registrations = %d, want 2", len(registrations))
	}
	if got := observer.watchCount(); got != 2 {
		t.Fatalf("watch count = %d, want 2 independent watchers", got)
	}
}

func TestObservationManagerPublishesInBackgroundAndReconnects(t *testing.T) {
	ledger := NewFileDeliveryRepository(filepath.Join(t.TempDir(), "delivery.json"))
	observer := &reconnectingObserver{
		streams: []statuswatch.Stream{
			&scriptedErrorStream{err: errors.New("temporary watch loss")},
			&scriptedObservationStream{snapshots: []*statuswatch.Snapshot{{
				ObservedGeneration:      3,
				WorkloadAdmitted:        true,
				ResourceClaimsAllocated: true,
			}}},
		},
	}
	publisher := &fakePublisher{}
	manager, err := NewObservationManager(observer, publisher, ledger)
	if err != nil {
		t.Fatal(err)
	}
	manager.retryBase = time.Millisecond
	manager.retryMax = 2 * time.Millisecond
	done, cancel := startObservationManager(t, manager)

	if err := manager.Register(context.Background(), observationRegistration(decisionForSequence(9))); err != nil {
		t.Fatal(err)
	}
	publisher.waitForCount(t, 2, 2*time.Second)
	if got := observer.watchCount(); got < 2 {
		t.Fatalf("watch count = %d, want reconnect", got)
	}
	stopObservationManager(t, cancel, done)
	if active := manager.Active(); active != 0 {
		t.Fatalf("active watchers after cancel = %d", active)
	}
}

func TestObservationManagerRecoversDurableAndMaterializedRegistrations(t *testing.T) {
	ledger := NewFileDeliveryRepository(filepath.Join(t.TempDir(), "delivery.json"))
	durable := observationRegistration(decisionForSequence(9))
	if err := ledger.SaveRegistration(durable); err != nil {
		t.Fatal(err)
	}
	recovered := bundleForDecision(decisionForSequence(10))
	observer := newControlledObserver()
	manager, err := NewObservationManager(observer, &fakePublisher{}, ledger)
	if err != nil {
		t.Fatal(err)
	}
	manager.SetBundleSource(staticBundleSource{bundles: []*api.Bundle{durable.Bundle, recovered}})
	done, cancel := startObservationManager(t, manager)

	deadline := time.Now().Add(2 * time.Second)
	for observer.watchCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := observer.watchCount(); got != 2 {
		t.Fatalf("watch count = %d, want durable plus reconstructed watcher", got)
	}
	registrations, err := ledger.ListRegistrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(registrations) != 2 {
		t.Fatalf("registrations = %d, want 2", len(registrations))
	}
	var reconstructed ObservationRegistration
	for _, registration := range registrations {
		if registration.BundleKey == recovered.Key {
			reconstructed = registration
		}
	}
	if reconstructed.Decision == nil || reconstructed.Decision.GetDecisionId() != "decision-10" {
		t.Fatalf("reconstructed decision = %+v", reconstructed.Decision)
	}
	bindings := reconstructed.Decision.GetSelectedPlan().GetBindings()
	if len(bindings) != 2 || bindings[0].GetBindingId() != "binding-10-1" {
		t.Fatalf("reconstructed bindings = %+v", bindings)
	}
	stopObservationManager(t, cancel, done)
	if active := manager.Active(); active != 0 {
		t.Fatalf("active watchers after cancel = %d", active)
	}
}

func TestObservationManagerPublishesLifecycleControlOnlyAfterReadback(t *testing.T) {
	fakeBackend := backend.NewFake()
	decision := decisionForSequence(9)
	bundle := bundleForDecision(decision)
	bundle.Workload.TypeMeta = api.TypeMeta{APIVersion: "kueue.x-k8s.io/v1beta1", Kind: "Workload"}
	bundle.Workload.ObjectMeta = api.ObjectMeta{Name: "workload-observed", Namespace: bundle.Namespace, Generation: int64(bundle.Generation)}
	bundle.Job.TypeMeta = api.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}
	bundle.Job.ObjectMeta = api.ObjectMeta{Name: "job-observed", Namespace: bundle.Namespace, Generation: int64(bundle.Generation)}
	bundle.Job.Spec.Parallelism = 2
	bundle.RuntimeClass = &api.RuntimeClass{
		TypeMeta:   api.TypeMeta{APIVersion: "node.k8s.io/v1", Kind: "RuntimeClass"},
		ObjectMeta: api.ObjectMeta{Name: "runtime-observed", Generation: int64(bundle.Generation)},
	}
	bundle.Fingerprint = "observation-lifecycle-fingerprint"
	if _, err := fakeBackend.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}

	ledger := NewFileDeliveryRepository(filepath.Join(t.TempDir(), "delivery.json"))
	publisher := &fakePublisher{}
	manager, err := NewObservationManager(statuswatch.NewFake(fakeBackend), publisher, ledger)
	if err != nil {
		t.Fatal(err)
	}
	manager.retryBase = time.Millisecond
	manager.retryMax = 2 * time.Millisecond
	done, cancel := startObservationManager(t, manager)
	registerCurrentBundle(t, manager, fakeBackend, bundle.Key, decision)
	publisher.waitForStateCount(t, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, 2, 2*time.Second)

	control := func(action tgsrlv1.JobCommandType, key string) uint64 {
		t.Helper()
		targets := make([]backend.ControlTarget, 0, len(bundle.RuntimeTargets))
		for _, target := range bundle.RuntimeTargets {
			targets = append(targets, backend.ControlTarget{
				RuntimeUnitID:      target.RuntimeUnitID,
				SandboxID:          target.SandboxID,
				ExpectedGeneration: target.Generation,
			})
		}
		before := publisher.count()
		result, err := fakeBackend.Control(context.Background(), backend.ControlRequest{
			Action: action, JobID: bundle.SourceJobID, RunID: bundle.SourceRunID, RequestID: "request-" + key, IdempotencyKey: key, Targets: targets,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Accepted {
			t.Fatalf("control %s was not accepted", action)
		}
		if got := publisher.count(); got != before {
			t.Fatalf("control acceptance published %d events directly", got-before)
		}
		return result.BackendRevision
	}
	pauseRevision := control(tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, "pause")
	registerCurrentBundle(t, manager, fakeBackend, bundle.Key, decision)
	publisher.waitForStateCount(t, tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, 2, 2*time.Second)
	assertCausalityForState(t, publisher.snapshot(), tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, "pause", pauseRevision)
	resumeRevision := control(tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME, "resume")
	registerCurrentBundle(t, manager, fakeBackend, bundle.Key, decision)
	publisher.waitForStateCount(t, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, 4, 2*time.Second)
	assertCausalityForState(t, publisher.snapshot(), tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, "resume", resumeRevision)
	stopRevision := control(tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP, "stop")
	registerCurrentBundle(t, manager, fakeBackend, bundle.Key, decision)
	publisher.waitForStateCount(t, tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED, 2, 2*time.Second)
	assertCausalityForState(t, publisher.snapshot(), tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED, "stop", stopRevision)

	stopObservationManager(t, cancel, done)
	for _, state := range []tgsrlv1.RuntimeState{
		tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED,
		tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED,
	} {
		if countState(publisher.snapshot(), state) < 2 {
			t.Fatalf("missing manager-published %s observations", state)
		}
	}
}

func assertCausalityForState(t *testing.T, events []*tgsrlv1.SandboxEvent, state tgsrlv1.RuntimeState, key string, revision uint64) {
	t.Helper()
	matched := 0
	for _, event := range events {
		if event.GetState() != state || event.GetIdempotencyKey() != key {
			continue
		}
		matched++
		if event.GetProviderRevision() != revision {
			t.Fatalf("state %s revision=%d want=%d", state, event.GetProviderRevision(), revision)
		}
		if event.GetDecisionId() == "" || event.GetPlanId() == "" {
			t.Fatalf("state %s missing scheduling causality: %+v", state, event)
		}
	}
	if matched != 2 {
		t.Fatalf("state %s causality events=%d want=2", state, matched)
	}
}

func countState(events []*tgsrlv1.SandboxEvent, state tgsrlv1.RuntimeState) int {
	count := 0
	for _, event := range events {
		if event.GetState() == state {
			count++
		}
	}
	return count
}

func registerCurrentBundle(t *testing.T, manager *ObservationManager, source BundleSource, bundleKey string, decision *tgsrlv1.DecisionRecord) {
	t.Helper()
	bundles, err := source.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, bundle := range bundles {
		if bundle.Key != bundleKey {
			continue
		}
		if err := manager.Register(context.Background(), ObservationRegistration{
			BundleKey: bundle.Key, Bundle: bundle, Decision: decision, JobRun: testJobRun(),
		}); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatalf("bundle %q not found", bundleKey)
}

type deterministicReconciler struct{}

func (deterministicReconciler) Reconcile(_ context.Context, input compiler.CompileInput, _ admission.QueuePolicy) (*ReconcileResult, error) {
	decision := &tgsrlv1.DecisionRecord{
		DecisionId:   input.PlacementPlan.GetDecisionId(),
		RunId:        input.PlacementPlan.GetRunId(),
		TraceId:      input.PlacementPlan.GetTraceId(),
		Generation:   input.Generation,
		SelectedPlan: input.PlacementPlan,
	}
	return &ReconcileResult{Bundle: bundleForDecision(decision), Applied: true}, nil
}

type flexibleJobRunClient struct{}

func (flexibleJobRunClient) GetJobRun(_ context.Context, in *tgsrlv1.GetJobRunRequest, _ ...grpc.CallOption) (*tgsrlv1.GetJobRunResponse, error) {
	return &tgsrlv1.GetJobRunResponse{Run: &tgsrlv1.JobRun{RunId: in.GetRunId(), JobId: in.GetJobId(), TraceId: "trace-1"}}, nil
}

type flexibleManifestClient struct{}

func (flexibleManifestClient) GetRuntimeManifest(_ context.Context, in *tgsrlv1.GetRuntimeManifestRequest, _ ...grpc.CallOption) (*tgsrlv1.GetRuntimeManifestResponse, error) {
	return &tgsrlv1.GetRuntimeManifestResponse{Manifest: &tgsrlv1.RuntimeManifest{RunId: in.GetRunId(), JobId: "job-1"}}, nil
}

type controlledObserver struct {
	mu          sync.Mutex
	watches     int
	recvStarted chan struct{}
}

func newControlledObserver() *controlledObserver {
	return &controlledObserver{recvStarted: make(chan struct{}, 2)}
}

func (o *controlledObserver) Watch(ctx context.Context, _ statuswatch.Request) (statuswatch.Stream, error) {
	o.mu.Lock()
	o.watches++
	o.mu.Unlock()
	return &contextObservationStream{ctx: ctx, started: o.recvStarted}, nil
}

func (o *controlledObserver) watcherStarted() <-chan struct{} {
	return o.recvStarted
}

func (o *controlledObserver) watchCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.watches
}

type contextObservationStream struct {
	ctx     context.Context
	started chan<- struct{}
}

func (s *contextObservationStream) Recv() (*statuswatch.Snapshot, error) {
	select {
	case s.started <- struct{}{}:
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

type gatedDecisionSource struct {
	decisions           []*tgsrlv1.DecisionRecord
	firstWatcherStarted <-chan struct{}
}

func (s *gatedDecisionSource) Watch(ctx context.Context, after cursor.Cursor) (DecisionStream, error) {
	start := 0
	for index, decision := range s.decisions {
		if decision.GetSequence() <= after.Sequence {
			start = index + 1
		}
	}
	return &gatedDecisionStream{
		ctx:                 ctx,
		decisions:           s.decisions[start:],
		firstWatcherStarted: s.firstWatcherStarted,
	}, nil
}

type gatedDecisionStream struct {
	ctx                 context.Context
	decisions           []*tgsrlv1.DecisionRecord
	firstWatcherStarted <-chan struct{}
	index               int
}

func (s *gatedDecisionStream) Recv() (*tgsrlv1.DecisionRecord, error) {
	if s.index >= len(s.decisions) {
		return nil, io.EOF
	}
	if s.index == 1 {
		select {
		case <-s.firstWatcherStarted:
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		}
	}
	decision := s.decisions[s.index]
	s.index++
	return decision, nil
}

type scriptedErrorStream struct {
	err error
}

func (s *scriptedErrorStream) Recv() (*statuswatch.Snapshot, error) {
	return nil, s.err
}

type reconnectingObserver struct {
	mu      sync.Mutex
	streams []statuswatch.Stream
	watches int
}

func (o *reconnectingObserver) Watch(_ context.Context, _ statuswatch.Request) (statuswatch.Stream, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	index := o.watches
	o.watches++
	if index >= len(o.streams) {
		return &scriptedErrorStream{err: io.EOF}, nil
	}
	return o.streams[index], nil
}

func (o *reconnectingObserver) watchCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.watches
}

type staticBundleSource struct {
	bundles []*api.Bundle
}

func (s staticBundleSource) List(context.Context) ([]*api.Bundle, error) {
	return s.bundles, nil
}

func decisionForSequence(sequence uint64) *tgsrlv1.DecisionRecord {
	decision := proto.Clone(successfulDecision()).(*tgsrlv1.DecisionRecord)
	decision.DecisionId = fmt.Sprintf("decision-%d", sequence)
	decision.Sequence = sequence
	decision.Cursor = fmt.Sprintf("cursor-%d", sequence)
	decision.SelectedPlan.PlanId = fmt.Sprintf("plan-%d", sequence)
	decision.SelectedPlan.DecisionId = decision.DecisionId
	for index, binding := range decision.SelectedPlan.Bindings {
		binding.BindingId = fmt.Sprintf("binding-%d-%d", sequence, index+1)
		binding.RuntimeUnitId = fmt.Sprintf("unit-%d-%d", sequence, index+1)
		binding.PendingUnitId = binding.RuntimeUnitId
		binding.SandboxId = fmt.Sprintf("sandbox-%d-%d", sequence, index+1)
	}
	return decision
}

func bundleForDecision(decision *tgsrlv1.DecisionRecord) *api.Bundle {
	bundle := &api.Bundle{
		Key:           "test-ns/bundle-" + decision.GetDecisionId(),
		Namespace:     "test-ns",
		Generation:    decisionGeneration(decision),
		SourceRunID:   decision.GetRunId(),
		SourceJobID:   "job-1",
		SourceTraceID: decision.GetTraceId(),
	}
	for _, binding := range decision.GetSelectedPlan().GetBindings() {
		bundle.RuntimeTargets = append(bundle.RuntimeTargets, api.RuntimeTarget{
			RuntimeUnitID: binding.GetRuntimeUnitId(),
			SandboxID:     binding.GetSandboxId(),
			BindingID:     binding.GetBindingId(),
			DecisionID:    decision.GetDecisionId(),
			PlanID:        decision.GetSelectedPlan().GetPlanId(),
			ActionID:      "action-" + binding.GetBindingId(),
			DeviceIDs:     append([]string(nil), binding.GetDeviceIds()...),
			CPUMillis:     binding.GetResources().GetCpuMillis(),
			MemoryBytes:   binding.GetResources().GetMemoryBytes(),
			Accelerators:  binding.GetResources().GetAcceleratorUnits(),
			Generation:    binding.GetGeneration(),
		})
	}
	return bundle
}

func observationRegistration(decision *tgsrlv1.DecisionRecord) ObservationRegistration {
	return ObservationRegistration{
		BundleKey:         bundleForDecision(decision).Key,
		Bundle:            bundleForDecision(decision),
		Decision:          decision,
		JobRun:            &tgsrlv1.JobRun{RunId: decision.GetRunId(), JobId: "job-1", TraceId: decision.GetTraceId()},
		PublishedEventIDs: map[string]bool{},
	}
}
