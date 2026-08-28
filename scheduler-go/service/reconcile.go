package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scheduler"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) workState(key workKey) *workState {
	s.mu.Lock()
	defer s.mu.Unlock()
	work := s.workers[key]
	if work == nil {
		work = &workState{}
		s.workers[key] = work
	}
	return work
}

func (s *Server) enqueue(ctx context.Context, intent *tgsrlv1.SchedulingIntent) {
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started {
		return
	}
	key := workKey{executionID: intent.GetExecutionId(), stageID: intent.GetStageId()}
	if s.eventLoop != nil {
		s.eventLoop.PublishIntent(intent)
	}
	s.mu.Lock()
	work := s.workers[key]
	if work == nil {
		work = &workState{}
		s.workers[key] = work
	}
	work.accepted = append(work.accepted, proto.Clone(intent).(*tgsrlv1.SchedulingIntent))
	s.recordQueueDepthLocked(intent, len(work.accepted))
	if work.running {
		s.mu.Unlock()
		return
	}
	work.running = true
	s.mu.Unlock()
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.mu.Lock()
				work := s.workers[key]
				if work != nil {
					work.running = false
				}
				s.mu.Unlock()
				s.setProviderWatchUnhealthy("resource", fmt.Sprintf("intent worker panic: %v", recovered))
				s.setProviderWatchUnhealthy("sandbox", fmt.Sprintf("intent worker panic: %v", recovered))
				s.recorder.IncCounter("intent_worker_panics", 1)
				slog.Error("intent worker panicked", "panic", recovered)
			}
		}()
		s.processKey(ctx, key)
	}()
}

func (s *Server) triggerReconcile(ctx context.Context, executionID, stageID string) {
	intent, ok := s.store.LatestValidIntent(executionID, stageID)
	if !ok {
		return
	}
	s.enqueue(s.workerContext(ctx), intent)
}

func (s *Server) processKey(ctx context.Context, key workKey) {
	for {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			if work := s.workers[key]; work != nil {
				work.running = false
			}
			s.mu.Unlock()
			return
		default:
		}
		s.mu.Lock()
		work := s.workers[key]
		if work == nil || len(work.accepted) == 0 {
			if work != nil {
				work.running = false
			}
			s.mu.Unlock()
			return
		}
		accepted := work.accepted
		work.accepted = nil
		s.recordQueueDepthLocked(accepted[len(accepted)-1], 0)
		s.mu.Unlock()

		sort.SliceStable(accepted, func(i, j int) bool { return accepted[i].GetVersion() < accepted[j].GetVersion() })
		accepted = dedupeAcceptedIntents(accepted)
		for _, superseded := range accepted[:len(accepted)-1] {
			if err := s.appendTerminalFallback(superseded, "INTENT_SUPERSEDED", "a newer accepted intent replaced this version"); err != nil {
				return
			}
		}
		s.processAccepted(ctx, accepted[len(accepted)-1])
	}
}

func dedupeAcceptedIntents(intents []*tgsrlv1.SchedulingIntent) []*tgsrlv1.SchedulingIntent {
	if len(intents) < 2 {
		return intents
	}
	deduped := intents[:1]
	for _, intent := range intents[1:] {
		last := deduped[len(deduped)-1]
		if last.GetVersion() == intent.GetVersion() && proto.Equal(last, intent) {
			continue
		}
		deduped = append(deduped, intent)
	}
	return deduped
}

func (s *Server) processAccepted(ctx context.Context, intent *tgsrlv1.SchedulingIntent) {
	key := workKey{executionID: intent.GetExecutionId(), stageID: intent.GetStageId()}
	work := s.workState(key)
	for attempt := 0; attempt < maxRevisionRetries; attempt++ {
		retry := false
		completed := func() bool {
			if err := ctx.Err(); err != nil {
				return true
			}
			work.gate.Lock()
			s.planMu.Lock()
			defer func() {
				s.planMu.Unlock()
				work.gate.Unlock()
			}()

			latest, exists := s.store.LatestIntent(intent.GetExecutionId(), intent.GetStageId())
			if !exists {
				if err := s.appendTerminalFallback(intent, "INTENT_NOT_FOUND", "accepted intent is no longer available"); err != nil {
					return true
				}
				return true
			}
			if latest.GetVersion() != intent.GetVersion() {
				if err := s.appendTerminalFallback(intent, "INTENT_SUPERSEDED", "a newer accepted intent replaced this version"); err != nil {
					return true
				}
				return true
			}
			if _, valid := s.store.LatestValidIntent(intent.GetExecutionId(), intent.GetStageId()); !valid {
				if err := s.appendTerminalFallback(intent, scheduler.FallbackReasonIntentExpired, "intent expired before execution"); err != nil {
					return true
				}
				return true
			}
			if watchErr := s.providerWatchFailure(); watchErr != nil {
				if err := s.appendTerminalFallback(intent, "PROVIDER_UNAVAILABLE", watchErr.Error()); err != nil {
					return true
				}
				return true
			}
			if satisfied, err := s.intentAlreadySatisfied(); err != nil {
				if appendErr := s.appendFailureDecision(intent, nil, "SATISFACTION_CHECK_FAILED", err); appendErr != nil {
					return true
				}
				return true
			} else if satisfied(intent) {
				return true
			}
			snapshot, err := s.store.GetSnapshot(ctx, 0, true)
			if err != nil {
				if appendErr := s.appendFailureDecision(intent, nil, "SNAPSHOT_READ_FAILED", err); appendErr != nil {
					return true
				}
				return true
			}
			evaluationStarted := time.Now()
			plan, decision, err := s.scheduler.Evaluate(snapshot, intent)
			s.recordScheduleLatency(intent, time.Since(evaluationStarted))
			if err != nil {
				if appendErr := s.appendFailureDecision(intent, snapshot, "EVALUATION_FAILED", err); appendErr != nil {
					return true
				}
				return true
			}
			fillDecisionMetadataFromIntent(decision, intent)
			if decision.GetFallback() || len(plan.GetActions()) == 0 {
				if err := s.appendDecision(intent.GetJobId(), decision); err != nil {
					return true
				}
				return true
			}
			if complete, ok := s.provider.(provider.CompleteResourceProvider); ok {
				health, healthErr := complete.Health(ctx)
				if healthErr != nil || health == nil || !health.Healthy {
					reason := "provider health check failed"
					if healthErr != nil {
						reason = healthErr.Error()
					} else if health != nil && health.Reason != "" {
						reason = health.Reason
					}
					if err := s.appendTerminalFallback(intent, "PROVIDER_UNAVAILABLE", reason); err != nil {
						return true
					}
					return true
				}
			}
			beforeReservation := s.store.ExportDurableState()
			if _, err := s.store.ReservePlan(plan); err != nil {
				if errors.Is(err, state.ErrPlanRevisionConflict) {
					retry = true
					return false
				}
				if errors.Is(err, state.ErrPlanIntentConflict) || errors.Is(err, state.ErrIntentExpired) {
					if appendErr := s.appendTerminalFallback(intent, "INTENT_SUPERSEDED", err.Error()); appendErr != nil {
						return true
					}
					return true
				}
				decision.Fallback = true
				decision.FallbackReason = "RESERVATION_FAILED"
				decision.SelectedPlan.Actions = nil
				if appendErr := s.appendDecision(intent.GetJobId(), decision); appendErr != nil {
					return true
				}
				return true
			}
			if err := s.checkpointPendingDecision(decision); err != nil {
				if restoreErr := s.store.Restore(beforeReservation); restoreErr != nil {
					slog.Error("reservation rollback failed after checkpoint failure", "error", restoreErr, "job_id", intent.GetJobId(), "run_id", intent.GetRunId(), "trace_id", intent.GetTraceId())
				}
				slog.Error("reservation checkpoint failed; intent remains available for restart retry", "error", err, "job_id", intent.GetJobId(), "run_id", intent.GetRunId(), "trace_id", intent.GetTraceId())
				return true
			}
			s.planMu.Unlock()
			results, executeErr := s.provider.ExecutePlan(ctx, plan)
			s.planMu.Lock()
			if executeErr != nil {
				slog.Warn("provider plan execution failed", "error", executeErr, "job_id", intent.GetJobId(), "run_id", intent.GetRunId(), "trace_id", intent.GetTraceId(), "decision_id", decision.GetDecisionId(), "plan_id", plan.GetPlanId())
			}
			decision.ActionResults = cloneActionResults(results)
			s.recordActionResults(intent, decision.ActionResults)
			if executeErr != nil && len(decision.ActionResults) == 0 {
				if telemetry, ok := s.recorder.(observability.SchedulerTelemetry); ok {
					telemetry.RecordAction(intentCorrelation(intent), "failed")
				}
			}
			succeeded := executeErr == nil && allActionsSucceeded(results, len(plan.GetActions()))
			beforeFinalize := s.store.ExportDurableState()
			finalSnapshot, finalizeErr := s.store.FinalizePlanResults(plan, succeeded, results)
			if finalizeErr != nil {
				decision.Fallback = true
				decision.FallbackReason = "STATE_FINALIZE_FAILED"
			} else if !succeeded {
				decision.Fallback = true
				decision.FallbackReason = "PROVIDER_EXECUTION_FAILED"
			}
			if finalSnapshot != nil {
				for _, result := range decision.ActionResults {
					if result != nil {
						result.ObservedRevision = finalSnapshot.GetRevision()
					}
				}
			}
			if err := s.appendDecision(intent.GetJobId(), decision); err != nil {
				if restoreErr := s.store.Restore(beforeFinalize); restoreErr != nil {
					slog.Error("finalized state rollback failed after decision checkpoint failure", "error", restoreErr, "job_id", intent.GetJobId(), "run_id", intent.GetRunId(), "trace_id", intent.GetTraceId(), "decision_id", decision.GetDecisionId())
				}
			}
			return true
		}()
		if completed {
			return
		}
		if retry {
			continue
		}
	}
	if err := s.appendFailureDecision(intent, nil, "REVISION_CONFLICT_RETRY_EXHAUSTED", state.ErrPlanRevisionConflict); err != nil {
		return
	}
}

func (s *Server) intentAlreadySatisfied() (func(*tgsrlv1.SchedulingIntent) bool, error) {
	snapshot, err := s.store.GetSnapshot(s.backgroundContext(), 0, false)
	if err != nil {
		return nil, err
	}
	activeCounts := make(map[workKey]map[uint64]uint32)
	for _, allocation := range snapshot.GetAllocations() {
		if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
			continue
		}
		key := workKey{executionID: allocation.GetExecutionId(), stageID: allocation.GetStageId()}
		versions := activeCounts[key]
		if versions == nil {
			versions = make(map[uint64]uint32)
			activeCounts[key] = versions
		}
		versions[allocation.GetIntentVersion()]++
	}
	return func(intent *tgsrlv1.SchedulingIntent) bool {
		if intent == nil {
			return false
		}
		key := workKey{executionID: intent.GetExecutionId(), stageID: intent.GetStageId()}
		return activeCounts[key][intent.GetVersion()] >= intent.GetUnitCount()
	}, nil
}

func (s *Server) appendFailureDecision(intent *tgsrlv1.SchedulingIntent, snapshot *tgsrlv1.ClusterSnapshot, reason string, err error) error {
	revision := uint64(0)
	if snapshot != nil {
		revision = snapshot.GetRevision()
	}
	decision := &tgsrlv1.DecisionRecord{
		DecisionId:       fmt.Sprintf("failure-%s-%s-%d-%s", intent.GetExecutionId(), intent.GetStageId(), intent.GetVersion(), reason),
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: revision,
		Fallback:         true,
		FallbackReason:   reason + ": " + err.Error(),
		PolicyVersion:    intent.GetPolicyVersion(),
		DecidedAt:        timestamppb.Now(),
		RunId:            intent.GetRunId(),
		TraceId:          intent.GetTraceId(),
		DataKind:         intent.GetDataKind(),
		SemanticContext:  cloneSemanticEnvelope(intent.GetSemanticContext()),
	}
	return s.appendDecision(intent.GetJobId(), decision)
}

func (s *Server) appendTerminalFallback(intent *tgsrlv1.SchedulingIntent, reason, detail string) error {
	return s.appendFailureDecision(intent, nil, reason, errors.New(detail))
}

func allActionsSucceeded(results []*tgsrlv1.ActionResult, expected int) bool {
	if len(results) != expected {
		return false
	}
	for _, result := range results {
		if result == nil || result.GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED {
			return false
		}
	}
	return true
}
