// Package service implements the gRPC scheduling control-plane surface.
package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/cache"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/eventloop"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/protection"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scheduler"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Evaluator is the generated-DTO scheduling boundary used by the service.
type Evaluator interface {
	Evaluate(*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error)
}

// ContextualEvaluator is the optional extended scheduling boundary that accepts
// EvaluationContext while preserving compatibility with existing fakes.
type ContextualEvaluator interface {
	EvaluateWithContext(*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent, *tgsrlv1.EvaluationContext) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error)
}

// AdaptiveEvaluator is an optional service-local seam for adaptive evaluation.
// Root note: this is a temporary compatibility interface owned by service
// until scheduler core lands its final EvaluateAdaptive API shape.
type AdaptiveEvaluator interface {
	EvaluateAdaptive(*scheduler.AdaptiveEvaluationInput) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error)
}

// Clock provides deterministic wall-clock capture for service-side evaluation
// context construction.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

// Now returns the current time.
func (f ClockFunc) Now() time.Time { return f() }

// Config defines the dependencies and bounded decision retention for a Server.
type Config struct {
	Store             *state.Store
	Scheduler         Evaluator
	Provider          provider.ResourceProvider
	DecisionRetention int
	EventLoopRuntime  *eventloop.Runtime
	EventLoop         *eventloop.EventLoop
	Recorder          observability.Recorder
	Repository        DurableRepository
	Clock             Clock
	// DeferStart lets a composition root restore durable state before provider
	// watches and scheduling workers become active. The zero value preserves
	// construct-and-start behavior for embedded callers.
	DeferStart bool
}

// Server serves the generated SchedulerService contract.
type Server struct {
	tgsrlv1.UnimplementedSchedulerServiceServer

	store          *state.Store
	scheduler      Evaluator
	guard          *protection.Guard
	provider       provider.ResourceProvider
	retention      int
	recorder       observability.Recorder
	repository     DurableRepository
	persistenceErr error
	clock          Clock
	eventLoop      *eventloop.EventLoop
	runtime        *eventloop.Runtime
	lifecycleCtx   context.Context
	lifecycleStop  context.CancelFunc
	providerWatch  providerWatchState
	projectionLive projectionReadyState

	mu sync.Mutex
	// planMu serializes Store snapshot/evaluate/reserve/finalize transactions.
	// Provider I/O runs outside this mutex, so independent streams still execute
	// concurrently while optimistic revision conflicts cannot starve a valid
	// intent merely because other streams are committing state.
	planMu    sync.Mutex
	decisions []decisionEntry
	changed   chan struct{}
	workers   map[workKey]*workState
	closeOnce sync.Once
	startOnce sync.Once
	started   bool
	startErr  error
	sequence  uint64
}

type decisionEntry struct {
	jobID    string
	decision *tgsrlv1.DecisionRecord
}

type decisionPageToken struct {
	JobID         string `json:"job_id,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	AfterSequence uint64 `json:"after_sequence,omitempty"`
}

type workKey struct {
	executionID string
	stageID     string
}

type workState struct {
	running bool
	pending *reconcileWork
	gate    sync.Mutex
}

const maxRevisionRetries = 4

type reconcileWork struct {
	intent   *tgsrlv1.SchedulingIntent
	triggers []*eventloop.Trigger
}

type providerWatchState struct {
	mu                 sync.RWMutex
	resourceHealthy    bool
	sandboxHealthy     bool
	resourcePoisoned   bool
	sandboxPoisoned    bool
	resourceReason     string
	sandboxReason      string
	resourceReconnects int
	sandboxReconnects  int
}

type projectionReadyState struct {
	mu              sync.RWMutex
	resourceLive    bool
	sandboxLive     bool
	resourceAttempt bool
	sandboxAttempt  bool
}

// New constructs a scheduling service.
func New(config Config) (*Server, error) {
	if config.Store == nil {
		return nil, errors.New("service: store is required")
	}
	if config.Scheduler == nil {
		return nil, errors.New("service: scheduler is required")
	}
	if config.Provider == nil {
		return nil, errors.New("service: provider is required")
	}
	if config.DecisionRetention <= 0 {
		config.DecisionRetention = 1024
	}
	if config.Recorder == nil {
		config.Recorder = observability.NopRecorder{}
	}
	if config.Clock == nil {
		config.Clock = ClockFunc(func() time.Time { return time.Now().UTC() })
	}
	lifecycleCtx, lifecycleStop := context.WithCancel(context.Background())
	server := &Server{
		store:         config.Store,
		scheduler:     config.Scheduler,
		guard:         schedulerGuard(config.Scheduler),
		provider:      config.Provider,
		retention:     config.DecisionRetention,
		recorder:      config.Recorder,
		repository:    config.Repository,
		clock:         config.Clock,
		lifecycleCtx:  lifecycleCtx,
		lifecycleStop: lifecycleStop,
		changed:       make(chan struct{}),
		workers:       make(map[workKey]*workState),
	}
	if config.EventLoop != nil {
		server.eventLoop = config.EventLoop
	} else {
		initial, err := config.Store.GetSnapshot(server.backgroundContext(), 0, true)
		if err != nil {
			server.lifecycleStop()
			return nil, fmt.Errorf("service: initialize event loop from store: %w", err)
		}
		cacheState := cache.NewState(initial, nil)
		server.eventLoop = eventloop.New(cacheState, nil, config.Recorder)
	}
	if config.EventLoopRuntime != nil {
		server.runtime = config.EventLoopRuntime
	} else if server.eventLoop != nil {
		server.runtime = eventloop.NewRuntime(server.eventLoop, eventloop.RuntimeConfig{})
	}
	if server.runtime != nil {
		server.runtime.SetTrigger(server.triggerReconcile)
	}
	if !config.DeferStart {
		if err := server.Start(); err != nil {
			return nil, err
		}
	}
	return server, nil
}

// Start seeds event-loop state and begins provider/runtime background work.
// Repeated calls are safe.
func (s *Server) Start() error {
	if s == nil {
		return nil
	}
	s.startOnce.Do(func() {
		if s.eventLoop != nil {
			s.seedEventLoop(s.backgroundContext())
			if err := s.startProviderWatches(); err != nil {
				s.startErr = err
				return
			}
		}
		if s.runtime != nil {
			s.runtime.Start(s.backgroundContext())
		}
		s.mu.Lock()
		s.started = true
		s.mu.Unlock()
		for _, intent := range s.store.ExportDurableState().Intents {
			s.enqueueThroughRuntime(s.backgroundContext(), intent, s.runtimeTriggerForIntent(intent, "startup_replay", tgsrlv1.TickKind_TICK_KIND_SLOW))
		}
	})
	if s.startErr != nil && s.lifecycleStop != nil {
		s.lifecycleStop()
	}
	return s.startErr
}

func schedulerGuard(evaluator Evaluator) *protection.Guard {
	if provider, ok := evaluator.(interface{ Guard() *protection.Guard }); ok {
		return provider.Guard()
	}
	return nil
}

// PublishIntent validates and stores the latest scheduling intent.
func (s *Server) PublishIntent(ctx context.Context, request *tgsrlv1.PublishIntentRequest) (*tgsrlv1.PublishIntentResponse, error) {
	if request == nil || request.Intent == nil {
		return nil, status.Error(codes.InvalidArgument, "intent is required")
	}
	if err := scheduler.ValidateIntent(request.Intent); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	key := workKey{executionID: request.Intent.GetExecutionId(), stageID: request.Intent.GetStageId()}
	work := s.workState(key)
	work.gate.Lock()
	defer work.gate.Unlock()
	s.planMu.Lock()
	if s.persistenceFailure() != nil {
		s.planMu.Unlock()
		return nil, status.Error(codes.Unavailable, "scheduler persistence is unavailable")
	}
	prepared, response, err := s.store.PreparePublishIntent(request.Intent)
	if err == nil && response.GetStatus() == tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_ACCEPTED {
		s.mu.Lock()
		err = s.checkpointStateLocked(s.decisions, s.sequence, prepared)
		s.mu.Unlock()
		if err == nil {
			prepared.Commit()
		}
	}
	s.planMu.Unlock()
	if err == nil {
		if response.GetStatus() == tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_ACCEPTED {
			s.enqueueThroughRuntime(s.workerContext(ctx), request.Intent, s.runtimeTriggerForIntent(request.Intent, "publish_intent", tgsrlv1.TickKind_TICK_KIND_FAST))
		}
		return response, nil
	}
	if errors.Is(err, state.ErrInvalidIntent) {
		return response, status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, state.ErrIntentExpired) || errors.Is(err, state.ErrStaleIntent) || errors.Is(err, state.ErrIntentConflict) || errors.Is(err, state.ErrIdempotencyConflict) {
		return response, status.Error(codes.FailedPrecondition, err.Error())
	}
	return response, status.Error(codes.Internal, "intent publication failed")
}

// Close stops the bounded intent worker. It is safe to call more than once.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		if s.lifecycleStop != nil {
			s.lifecycleStop()
		}
		if s.runtime != nil {
			s.runtime.Stop()
		}
	})
}

// GetSnapshot returns an immutable copy at or after the requested revision.
func (s *Server) GetSnapshot(ctx context.Context, request *tgsrlv1.GetSnapshotRequest) (*tgsrlv1.GetSnapshotResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	snapshot, err := s.store.GetSnapshot(ctx, request.MinimumRevision, request.IncludePendingUnits)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &tgsrlv1.GetSnapshotResponse{Snapshot: snapshot}, nil
}

// Schedule performs a pure scheduling preview against caller-supplied inputs.
func (s *Server) Schedule(_ context.Context, request *tgsrlv1.ScheduleRequest) (*tgsrlv1.ScheduleResponse, error) {
	if request == nil || request.Intent == nil || request.Snapshot == nil {
		return nil, status.Error(codes.InvalidArgument, "intent and snapshot are required")
	}
	var evaluationContext *tgsrlv1.EvaluationContext
	if request.GetEvaluationContext() != nil {
		evaluationContext = proto.Clone(request.GetEvaluationContext()).(*tgsrlv1.EvaluationContext)
	} else {
		evaluationContext = buildEvaluationContext(s.now(), request.Intent, nil)
		evaluationContext.CompatibilityDefaultsApplied = true
	}
	// Schedule is a pure caller-supplied preview. It must not mix that snapshot
	// with the service's live provider projection or retained decision history.
	// Adaptive runtime mutation is evaluated only by the authoritative reconcile
	// path, where all three inputs share one Store revision boundary.
	var (
		decision *tgsrlv1.DecisionRecord
		err      error
	)
	if evaluator, ok := s.scheduler.(ContextualEvaluator); ok {
		_, decision, err = evaluator.EvaluateWithContext(request.Snapshot, request.Intent, evaluationContext)
	} else {
		_, decision, err = s.scheduler.Evaluate(request.Snapshot, request.Intent)
	}
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	fillDecisionMetadataFromIntent(decision, request.Intent)
	applyDecisionEvaluationContext(decision, evaluationContext)
	return &tgsrlv1.ScheduleResponse{Decision: cloneDecision(decision)}, nil
}

// ListDecisions returns retained decisions filtered by job_id and/or run_id
// using a stable page token bound to the same filter surface.
func (s *Server) ListDecisions(_ context.Context, request *tgsrlv1.ListDecisionsRequest) (*tgsrlv1.ListDecisionsResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	token, err := decodeDecisionPageToken(request.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if token.JobID != "" && token.JobID != request.GetJobId() {
		return nil, status.Error(codes.InvalidArgument, "page_token job_id does not match request.job_id")
	}
	if token.RunID != "" && token.RunID != request.GetRunId() {
		return nil, status.Error(codes.InvalidArgument, "page_token run_id does not match request.run_id")
	}
	limit := int(request.GetLimit())
	if limit <= 0 {
		limit = 50
	}
	if limit > s.retention {
		limit = s.retention
	}

	filtered, oldest := s.filteredDecisions(token.AfterSequence, request.GetJobId(), request.GetRunId())
	if token.AfterSequence > 0 && oldest > 0 && token.AfterSequence+1 < oldest {
		return nil, status.Errorf(codes.OutOfRange, "decision cursor %d predates retained sequence %d", token.AfterSequence, oldest)
	}

	response := &tgsrlv1.ListDecisionsResponse{}
	for index, entry := range filtered {
		if index >= limit {
			next, encodeErr := encodeDecisionPageToken(decisionPageToken{
				JobID:         request.GetJobId(),
				RunID:         request.GetRunId(),
				AfterSequence: filtered[index-1].decision.GetSequence(),
			})
			if encodeErr != nil {
				return nil, status.Error(codes.Internal, "failed to encode next page token")
			}
			response.NextPageToken = next
			break
		}
		response.Decisions = append(response.Decisions, cloneDecision(entry.decision))
	}
	if len(response.Decisions) > 0 {
		response.Cursor = decisionCursor(response.Decisions[len(response.Decisions)-1])
	}
	return response, nil
}

// GetDecision returns one retained decision by stable decision_id.
func (s *Server) GetDecision(_ context.Context, request *tgsrlv1.GetDecisionRequest) (*tgsrlv1.GetDecisionResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if strings.TrimSpace(request.GetDecisionId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "decision_id is required")
	}
	decision, found := s.decisionByID(request.GetDecisionId())
	if !found {
		return nil, status.Errorf(codes.NotFound, "decision %q was not retained", request.GetDecisionId())
	}
	return &tgsrlv1.GetDecisionResponse{
		Decision: decision,
		Cursor:   decisionCursor(decision),
	}, nil
}
