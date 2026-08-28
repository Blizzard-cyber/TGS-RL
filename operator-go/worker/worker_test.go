package worker

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/admission"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/controller"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/cursor"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/statuswatch"
	"google.golang.org/grpc"
)

func TestWorkerPublishesEventsAndPersistsCursor(t *testing.T) {
	dir := t.TempDir()
	repo := cursor.NewFileRepository(filepath.Join(dir, "cursor.json"))
	publisher := &fakePublisher{}
	fakeBackend := backend.NewFake()
	w, err := New(Config{
		Source:      &sliceDecisionSource{decisions: []*tgsrlv1.DecisionRecord{successfulDecision()}},
		JobRuns:     fakeJobRunClient{run: testJobRun()},
		Manifests:   fakeManifestClient{manifest: testManifest()},
		Reconciler:  NewControllerReconciler(controller.New(fakeBackend)),
		Observer:    statuswatch.NewFake(fakeBackend),
		Publisher:   publisher,
		Cursors:     repo,
		Deliveries:  NewFileDeliveryRepository(filepath.Join(dir, "delivery.json")),
		QueuePolicy: queuePolicy(),
		Namespace:   "test-ns",
		GPUProfiles: []string{"nvidia-device-plugin"},
	})
	if err != nil {
		t.Fatalf("new worker failed: %v", err)
	}
	done, cancel := startObservationManager(t, w.observations)
	if err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("run once failed: %v", err)
	}
	publisher.waitForCount(t, 4, 2*time.Second)
	stopObservationManager(t, cancel, done)
	saved, err := repo.Load()
	if err != nil {
		t.Fatalf("load cursor failed: %v", err)
	}
	if saved.DecisionID != "decision-1" || saved.Sequence != 9 {
		t.Fatalf("unexpected saved cursor: %+v", saved)
	}
}

func TestWorkerRestartIsIdempotentFromCursor(t *testing.T) {
	dir := t.TempDir()
	cursorPath := filepath.Join(dir, "cursor.json")
	repo := cursor.NewFileRepository(cursorPath)
	if err := repo.Save(cursor.Cursor{DecisionID: "decision-1", Sequence: 9, Cursor: "cursor-1"}); err != nil {
		t.Fatalf("seed cursor failed: %v", err)
	}
	publisher := &fakePublisher{}
	source := &sliceDecisionSource{
		afterSequenceSeen: make([]uint64, 0, 1),
		decisions:         []*tgsrlv1.DecisionRecord{successfulDecision()},
	}
	fakeBackend := backend.NewFake()
	w, err := New(Config{
		Source:      source,
		JobRuns:     fakeJobRunClient{run: testJobRun()},
		Manifests:   fakeManifestClient{manifest: testManifest()},
		Reconciler:  NewControllerReconciler(controller.New(fakeBackend)),
		Observer:    statuswatch.NewFake(fakeBackend),
		Publisher:   publisher,
		Cursors:     repo,
		Deliveries:  NewFileDeliveryRepository(filepath.Join(dir, "delivery.json")),
		QueuePolicy: queuePolicy(),
		Namespace:   "test-ns",
		GPUProfiles: []string{"nvidia-device-plugin"},
	})
	if err != nil {
		t.Fatalf("new worker failed: %v", err)
	}
	if err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("run once failed: %v", err)
	}
	if got := publisher.count(); got != 0 {
		t.Fatalf("expected no replay after restart cursor catch-up, got %d", got)
	}
	if len(source.afterSequenceSeen) != 1 || source.afterSequenceSeen[0] != 9 {
		t.Fatalf("worker did not resume from saved cursor: %+v", source.afterSequenceSeen)
	}
	payload, err := os.ReadFile(cursorPath)
	if err != nil {
		t.Fatalf("read cursor file failed: %v", err)
	}
	if len(payload) == 0 {
		t.Fatalf("cursor file should not be empty")
	}
	saved, err := repo.Load()
	if err != nil {
		t.Fatalf("load cursor failed: %v", err)
	}
	if saved.Sequence != 9 || saved.DecisionID != "decision-1" {
		t.Fatalf("unexpected saved cursor after restart: %+v", saved)
	}
}

func TestWorkerDoesNotPublishFromApplyWithoutObservedState(t *testing.T) {
	publisher := &fakePublisher{}
	w, err := New(Config{
		Source:      &sliceDecisionSource{decisions: []*tgsrlv1.DecisionRecord{successfulDecision()}},
		JobRuns:     fakeJobRunClient{run: testJobRun()},
		Manifests:   fakeManifestClient{manifest: testManifest()},
		Reconciler:  NewControllerReconciler(controller.New(backend.NewFake())),
		Observer:    &scriptedObserver{},
		Publisher:   publisher,
		Cursors:     cursor.NewFileRepository(filepath.Join(t.TempDir(), "cursor.json")),
		Deliveries:  NewFileDeliveryRepository(filepath.Join(t.TempDir(), "delivery.json")),
		QueuePolicy: queuePolicy(),
		Namespace:   "test-ns",
		GPUProfiles: []string{"nvidia-device-plugin"},
	})
	if err != nil {
		t.Fatalf("new worker failed: %v", err)
	}
	done, cancel := startObservationManager(t, w.observations)
	if err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("run once failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	stopObservationManager(t, cancel, done)
	if got := publisher.count(); got != 0 {
		t.Fatalf("Apply emitted %d runtime events without backend observations", got)
	}
}

func TestWorkerPublishesOnlyObservedTransitions(t *testing.T) {
	publisher := &fakePublisher{}
	w, err := New(Config{
		Source:     &sliceDecisionSource{decisions: []*tgsrlv1.DecisionRecord{successfulDecision()}},
		JobRuns:    fakeJobRunClient{run: testJobRun()},
		Manifests:  fakeManifestClient{manifest: testManifest()},
		Reconciler: NewControllerReconciler(controller.New(backend.NewFake())),
		Observer: &scriptedObserver{snapshots: []*statuswatch.Snapshot{
			{ObservedGeneration: 2, WorkloadAdmitted: true, ResourceClaimsAllocated: true}, // stale
			{ObservedGeneration: 3, WorkloadAdmitted: true},                                // claim pending
			{ObservedGeneration: 3, WorkloadAdmitted: true, ResourceClaimsAllocated: true},
			{ObservedGeneration: 3, WorkloadAdmitted: true, ResourceClaimsAllocated: true}, // duplicate
			{ObservedGeneration: 3, WorkloadAdmitted: true, ResourceClaimsAllocated: true, JobActive: 2},
			{ObservedGeneration: 3, JobSucceeded: 2},
		}},
		Publisher:   publisher,
		Cursors:     cursor.NewFileRepository(filepath.Join(t.TempDir(), "cursor.json")),
		Deliveries:  NewFileDeliveryRepository(filepath.Join(t.TempDir(), "delivery.json")),
		QueuePolicy: queuePolicy(),
		Namespace:   "test-ns",
		GPUProfiles: []string{"nvidia-device-plugin"},
	})
	if err != nil {
		t.Fatalf("new worker failed: %v", err)
	}
	done, cancel := startObservationManager(t, w.observations)
	if err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("run once failed: %v", err)
	}
	want := []tgsrlv1.RuntimeState{
		tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND, tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND,
		tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED, tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED,
	}
	publisher.waitForCount(t, len(want), 2*time.Second)
	stopObservationManager(t, cancel, done)
	events := publisher.snapshot()
	if len(events) != len(want) {
		t.Fatalf("got %d events, want %d", len(events), len(want))
	}
	for i, state := range want {
		if events[i].GetState() != state {
			t.Fatalf("event[%d] state = %s, want %s", i, events[i].GetState(), state)
		}
	}
}

func TestWorkerResumesPartialPublishFromDeliveryLedger(t *testing.T) {
	dir := t.TempDir()
	cursorRepo := cursor.NewFileRepository(filepath.Join(dir, "cursor.json"))
	deliveryRepo := NewFileDeliveryRepository(filepath.Join(dir, "delivery.json"))
	publisher := &fakePublisher{}
	record := DeliveryRecord{
		DecisionID: "decision-1",
		Sequence:   9,
		Cursor:     "cursor-1",
		Phase:      "publishing",
		PublishedEventIDs: map[string]bool{
			"decision-1:SANDBOX_EVENT_TYPE_BOUND:3:binding-1": true,
		},
	}
	if err := deliveryRepo.Save(record); err != nil {
		t.Fatalf("seed delivery ledger failed: %v", err)
	}

	fakeBackend := backend.NewFake()
	w, err := New(Config{
		Source:      &sliceDecisionSource{decisions: []*tgsrlv1.DecisionRecord{successfulDecision()}},
		JobRuns:     fakeJobRunClient{run: testJobRun()},
		Manifests:   fakeManifestClient{manifest: testManifest()},
		Reconciler:  NewControllerReconciler(controller.New(fakeBackend)),
		Observer:    statuswatch.NewFake(fakeBackend),
		Publisher:   publisher,
		Cursors:     cursorRepo,
		Deliveries:  deliveryRepo,
		QueuePolicy: queuePolicy(),
		Namespace:   "test-ns",
		GPUProfiles: []string{"nvidia-device-plugin"},
	})
	if err != nil {
		t.Fatalf("new worker failed: %v", err)
	}
	done, cancel := startObservationManager(t, w.observations)
	if err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("run once failed: %v", err)
	}
	publisher.waitForCount(t, 3, 2*time.Second)
	stopObservationManager(t, cancel, done)
	events := publisher.snapshot()
	if got := len(events); got != 3 {
		t.Fatalf("expected 3 newly published events after resume, got %d", got)
	}
	if events[0].GetEventId() != "decision-1:SANDBOX_EVENT_TYPE_BOUND:3:binding-2" {
		t.Fatalf("first resumed event = %q", events[0].GetEventId())
	}
	saved, err := cursorRepo.Load()
	if err != nil {
		t.Fatalf("load cursor failed: %v", err)
	}
	if saved.Sequence != 9 {
		t.Fatalf("unexpected cursor after resume: %+v", saved)
	}
	ledger, err := deliveryRepo.Load()
	if err != nil {
		t.Fatalf("load delivery ledger failed: %v", err)
	}
	if ledger.Sequence != 0 || ledger.DecisionID != "" {
		t.Fatalf("delivery ledger should be cleared, got %+v", ledger)
	}
}

func TestWorkerRunReconnectsAfterEOFAndResumesFromCursor(t *testing.T) {
	dir := t.TempDir()
	repo := cursor.NewFileRepository(filepath.Join(dir, "cursor.json"))
	publisher := &fakePublisher{}
	source := &reconnectingDecisionSource{
		streams: [][]*tgsrlv1.DecisionRecord{
			{successfulDecision()},
			nil,
		},
	}
	fakeBackend := backend.NewFake()
	w, err := New(Config{
		Source:      source,
		JobRuns:     fakeJobRunClient{run: testJobRun()},
		Manifests:   fakeManifestClient{manifest: testManifest()},
		Reconciler:  NewControllerReconciler(controller.New(fakeBackend)),
		Observer:    statuswatch.NewFake(fakeBackend),
		Publisher:   publisher,
		Cursors:     repo,
		Deliveries:  NewFileDeliveryRepository(filepath.Join(dir, "delivery.json")),
		QueuePolicy: queuePolicy(),
		Namespace:   "test-ns",
		GPUProfiles: []string{"nvidia-device-plugin"},
	})
	if err != nil {
		t.Fatalf("new worker failed: %v", err)
	}
	w.retryBase = time.Millisecond
	w.retryMax = 2 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx)
	}()

	source.waitForWatchCount(t, 2, 2*time.Second)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not stop after cancel")
	}
	if got := publisher.count(); got != 4 {
		t.Fatalf("expected 4 events from first stream only, got %d", got)
	}
	if len(source.afterSeen) < 2 || source.afterSeen[0] != 0 || source.afterSeen[1] != 9 {
		t.Fatalf("unexpected cursor resume history: %+v", source.afterSeen)
	}
}

func TestWorkerRunRetriesTemporaryWatchErrorAndStopsOnCancel(t *testing.T) {
	repo := cursor.NewFileRepository(filepath.Join(t.TempDir(), "cursor.json"))
	publisher := &fakePublisher{}
	source := &failingThenBlockingSource{
		err:       errors.New("temporary watch failure"),
		ready:     make(chan struct{}),
		release:   make(chan struct{}),
		watchSeen: make(chan struct{}, 4),
	}
	fakeBackend := backend.NewFake()
	w, err := New(Config{
		Source:      source,
		JobRuns:     fakeJobRunClient{run: testJobRun()},
		Manifests:   fakeManifestClient{manifest: testManifest()},
		Reconciler:  NewControllerReconciler(controller.New(fakeBackend)),
		Observer:    statuswatch.NewFake(fakeBackend),
		Publisher:   publisher,
		Cursors:     repo,
		Deliveries:  NewFileDeliveryRepository(filepath.Join(t.TempDir(), "delivery.json")),
		QueuePolicy: queuePolicy(),
		Namespace:   "test-ns",
		GPUProfiles: []string{"nvidia-device-plugin"},
	})
	if err != nil {
		t.Fatalf("new worker failed: %v", err)
	}
	w.retryBase = time.Millisecond
	w.retryMax = 2 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx)
	}()

	source.waitForAttempts(t, 2, 2*time.Second)
	cancel()
	close(source.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not stop after cancel")
	}
}

func TestWorkerAdvancesCursorForSkippedDecision(t *testing.T) {
	dir := t.TempDir()
	repo := cursor.NewFileRepository(filepath.Join(dir, "cursor.json"))
	decision := successfulDecision()
	decision.Fallback = true
	w, err := New(Config{
		Source:      &sliceDecisionSource{decisions: []*tgsrlv1.DecisionRecord{decision}},
		JobRuns:     fakeJobRunClient{run: testJobRun()},
		Manifests:   fakeManifestClient{manifest: testManifest()},
		Reconciler:  NewControllerReconciler(controller.New(backend.NewFake())),
		Observer:    &scriptedObserver{},
		Publisher:   &fakePublisher{},
		Cursors:     repo,
		Deliveries:  NewFileDeliveryRepository(filepath.Join(dir, "delivery.json")),
		QueuePolicy: queuePolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	saved, err := repo.Load()
	if err != nil {
		t.Fatal(err)
	}
	if saved.Sequence != decision.GetSequence() || saved.DecisionID != decision.GetDecisionId() {
		t.Fatalf("cursor = %+v, want skipped decision checkpoint", saved)
	}
}

func TestWorkerDoesNotOverwriteUnfinishedDelivery(t *testing.T) {
	dir := t.TempDir()
	ledger := NewFileDeliveryRepository(filepath.Join(dir, "delivery.json"))
	unfinished := DeliveryRecord{DecisionID: "decision-old", Sequence: 8, Phase: "reconciling"}
	if err := ledger.Save(unfinished); err != nil {
		t.Fatal(err)
	}
	w, err := New(Config{
		Source:      &sliceDecisionSource{decisions: []*tgsrlv1.DecisionRecord{successfulDecision()}},
		JobRuns:     fakeJobRunClient{run: testJobRun()},
		Manifests:   fakeManifestClient{manifest: testManifest()},
		Reconciler:  NewControllerReconciler(controller.New(backend.NewFake())),
		Observer:    &scriptedObserver{},
		Publisher:   &fakePublisher{},
		Cursors:     cursor.NewFileRepository(filepath.Join(dir, "cursor.json")),
		Deliveries:  ledger,
		QueuePolicy: queuePolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.RunOnce(context.Background()); err == nil {
		t.Fatal("RunOnce() should reject a different decision while a delivery is unfinished")
	}
	got, err := ledger.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.DecisionID != unfinished.DecisionID || got.Sequence != unfinished.Sequence {
		t.Fatalf("unfinished delivery was overwritten: %+v", got)
	}
}

func TestWorkerRegistersIdempotentReconcileBeforeAdvancingCursor(t *testing.T) {
	dir := t.TempDir()
	ledger := NewFileDeliveryRepository(filepath.Join(dir, "delivery.json"))
	repo := cursor.NewFileRepository(filepath.Join(dir, "cursor.json"))
	decision := successfulDecision()
	w, err := New(Config{
		Source:      &sliceDecisionSource{decisions: []*tgsrlv1.DecisionRecord{decision}},
		JobRuns:     fakeJobRunClient{run: testJobRun()},
		Manifests:   fakeManifestClient{manifest: testManifest()},
		Reconciler:  staticReconciler{result: &ReconcileResult{Bundle: bundleForDecision(decision), Idempotent: true}},
		Observer:    &scriptedObserver{},
		Publisher:   &fakePublisher{},
		Cursors:     repo,
		Deliveries:  ledger,
		QueuePolicy: queuePolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	registrations, err := ledger.ListRegistrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(registrations) != 1 || registrations[0].Decision.GetDecisionId() != decision.GetDecisionId() {
		t.Fatalf("registrations = %+v", registrations)
	}
	saved, err := repo.Load()
	if err != nil {
		t.Fatal(err)
	}
	if saved.Sequence != decision.GetSequence() {
		t.Fatalf("cursor = %+v", saved)
	}
}

type staticReconciler struct {
	result *ReconcileResult
	err    error
}

func (r staticReconciler) Reconcile(context.Context, compiler.CompileInput, admission.QueuePolicy) (*ReconcileResult, error) {
	return r.result, r.err
}

type sliceDecisionSource struct {
	afterSequenceSeen []uint64
	decisions         []*tgsrlv1.DecisionRecord
}

func (s *sliceDecisionSource) Watch(_ context.Context, after cursor.Cursor) (DecisionStream, error) {
	s.afterSequenceSeen = append(s.afterSequenceSeen, after.Sequence)
	start := 0
	for i, decision := range s.decisions {
		if decision.GetSequence() <= after.Sequence {
			start = i + 1
		}
	}
	return &sliceDecisionStream{decisions: s.decisions[start:]}, nil
}

type reconnectingDecisionSource struct {
	mu        sync.Mutex
	streams   [][]*tgsrlv1.DecisionRecord
	watchCall int
	afterSeen []uint64
}

func (s *reconnectingDecisionSource) Watch(_ context.Context, after cursor.Cursor) (DecisionStream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.afterSeen = append(s.afterSeen, after.Sequence)
	index := s.watchCall
	s.watchCall++
	if index >= len(s.streams) {
		return &sliceDecisionStream{}, nil
	}
	return &sliceDecisionStream{decisions: s.streams[index]}, nil
}

func (s *reconnectingDecisionSource) waitForWatchCount(t *testing.T, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		s.mu.Lock()
		got := s.watchCall
		s.mu.Unlock()
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("watch count = %d, want >= %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

type failingThenBlockingSource struct {
	mu        sync.Mutex
	attempts  int
	err       error
	ready     chan struct{}
	release   chan struct{}
	watchSeen chan struct{}
}

func (s *failingThenBlockingSource) Watch(ctx context.Context, _ cursor.Cursor) (DecisionStream, error) {
	s.mu.Lock()
	s.attempts++
	attempt := s.attempts
	s.mu.Unlock()
	select {
	case s.watchSeen <- struct{}{}:
	default:
	}
	if attempt == 1 {
		return nil, s.err
	}
	select {
	case <-s.ready:
	default:
		close(s.ready)
	}
	return &blockingDecisionStream{ctx: ctx, release: s.release}, nil
}

func (s *failingThenBlockingSource) waitForAttempts(t *testing.T, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		s.mu.Lock()
		got := s.attempts
		s.mu.Unlock()
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("attempts = %d, want >= %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

type blockingDecisionStream struct {
	ctx     context.Context
	release <-chan struct{}
}

func (s *blockingDecisionStream) Recv() (*tgsrlv1.DecisionRecord, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case <-s.release:
		return nil, io.EOF
	}
}

type sliceDecisionStream struct {
	decisions []*tgsrlv1.DecisionRecord
	index     int
}

type scriptedObserver struct {
	snapshots []*statuswatch.Snapshot
}

func (o *scriptedObserver) Watch(_ context.Context, _ statuswatch.Request) (statuswatch.Stream, error) {
	return &scriptedObservationStream{snapshots: o.snapshots}, nil
}

type scriptedObservationStream struct {
	snapshots []*statuswatch.Snapshot
	index     int
}

func (s *scriptedObservationStream) Recv() (*statuswatch.Snapshot, error) {
	if s.index >= len(s.snapshots) {
		return nil, io.EOF
	}
	snapshot := s.snapshots[s.index]
	s.index++
	return snapshot, nil
}

func (s *sliceDecisionStream) Recv() (*tgsrlv1.DecisionRecord, error) {
	if s.index >= len(s.decisions) {
		return nil, io.EOF
	}
	decision := s.decisions[s.index]
	s.index++
	return decision, nil
}

type fakeJobRunClient struct {
	run *tgsrlv1.JobRun
}

func (c fakeJobRunClient) GetJobRun(_ context.Context, in *tgsrlv1.GetJobRunRequest, _ ...grpc.CallOption) (*tgsrlv1.GetJobRunResponse, error) {
	if in.GetJobId() != "job-1" || in.GetRunId() != "run-1" {
		return nil, io.ErrUnexpectedEOF
	}
	return &tgsrlv1.GetJobRunResponse{Run: c.run}, nil
}

type fakeManifestClient struct {
	manifest *tgsrlv1.RuntimeManifest
}

func (c fakeManifestClient) GetRuntimeManifest(_ context.Context, in *tgsrlv1.GetRuntimeManifestRequest, _ ...grpc.CallOption) (*tgsrlv1.GetRuntimeManifestResponse, error) {
	if in.GetRunId() != "run-1" {
		return nil, io.ErrUnexpectedEOF
	}
	return &tgsrlv1.GetRuntimeManifestResponse{Manifest: c.manifest}, nil
}

type fakePublisher struct {
	mu     sync.Mutex
	events []*tgsrlv1.SandboxEvent
}

func (p *fakePublisher) Publish(_ context.Context, event *tgsrlv1.SandboxEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func (p *fakePublisher) snapshot() []*tgsrlv1.SandboxEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*tgsrlv1.SandboxEvent(nil), p.events...)
}

func (p *fakePublisher) waitForCount(t *testing.T, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for p.count() < want {
		if time.Now().After(deadline) {
			t.Fatalf("published event count = %d, want >= %d", p.count(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func (p *fakePublisher) waitForStateCount(t *testing.T, state tgsrlv1.RuntimeState, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		count := 0
		for _, event := range p.snapshot() {
			if event.GetState() == state {
				count++
			}
		}
		if count >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("published %s count = %d, want >= %d", state, count, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func startObservationManager(t *testing.T, manager *ObservationManager) (<-chan error, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		manager.mu.Lock()
		running := manager.running
		manager.mu.Unlock()
		if running {
			return done, cancel
		}
		select {
		case err := <-done:
			t.Fatalf("observation manager stopped during startup: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("observation manager did not start")
		}
		time.Sleep(time.Millisecond)
	}
}

func stopObservationManager(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("observation manager stopped with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("observation manager did not stop after cancel")
	}
}

func queuePolicy() admission.QueuePolicy {
	return admission.QueuePolicy{
		Name:            "train",
		QuotaGroup:      "team-a",
		Capacity:        map[string]string{"cpu": "4", "memory": "8192", "nvidia.com/gpu": "2"},
		AllowPreemption: true,
	}
}

func successfulDecision() *tgsrlv1.DecisionRecord {
	return &tgsrlv1.DecisionRecord{
		DecisionId: "decision-1",
		Sequence:   9,
		RunId:      "run-1",
		TraceId:    "trace-1",
		Generation: 3,
		Cursor:     "cursor-1",
		SelectedPlan: &tgsrlv1.PlacementPlan{
			PlanId:           "plan-1",
			ExecutionId:      "exec-1",
			StageId:          "stage-1",
			IntentVersion:    3,
			SnapshotRevision: 11,
			DecisionId:       "decision-1",
			RunId:            "run-1",
			TraceId:          "trace-1",
			Generation:       3,
			Bindings: []*tgsrlv1.Binding{
				{
					BindingId:     "binding-1",
					PendingUnitId: "unit-1",
					SandboxId:     "sandbox-1",
					Generation:    3,
					Resources: &tgsrlv1.ResourceVector{
						CpuMillis:        2000,
						MemoryBytes:      4096,
						AcceleratorUnits: 1,
					},
				},
				{
					BindingId:     "binding-2",
					PendingUnitId: "unit-2",
					SandboxId:     "sandbox-2",
					Generation:    3,
					Resources: &tgsrlv1.ResourceVector{
						CpuMillis:        2000,
						MemoryBytes:      4096,
						AcceleratorUnits: 1,
					},
				},
			},
		},
		Candidates: []*tgsrlv1.PlacementCandidate{
			{CandidateId: "job:job-1"},
		},
		ActionResults: []*tgsrlv1.ActionResult{
			{ActionId: "a1", Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED, ObservedGeneration: 3},
		},
	}
}

func testJobRun() *tgsrlv1.JobRun {
	return &tgsrlv1.JobRun{
		RunId:    "run-1",
		JobId:    "job-1",
		TraceId:  "trace-1",
		State:    tgsrlv1.JobState_JOB_STATE_RUNNING,
		RunState: tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING,
		Labels: map[string]string{
			"queue":            "train",
			"quota_group":      "team-a",
			"desired_units":    "2",
			"allow_preemption": "true",
		},
		Runtime: &tgsrlv1.FrameworkRuntimeSpec{
			Command:     []string{"python", "train.py"},
			Args:        []string{"--steps", "10"},
			Environment: map[string]string{"alpha.beta/value": "1"},
		},
	}
}

func testManifest() *tgsrlv1.RuntimeManifest {
	return &tgsrlv1.RuntimeManifest{
		ManifestId:       "manifest-1",
		RunId:            "run-1",
		JobId:            "job-1",
		TraceId:          "trace-1",
		ExecutionBackend: "kubernetes",
		ImageDigests:     []string{"repo/image@sha256:abc"},
		Annotations:      map[string]string{"team": "rl"},
	}
}
