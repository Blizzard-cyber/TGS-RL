package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/eventloop"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/protection"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scheduler"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const adaptiveDecisionWindow = 8

const (
	adaptiveLabelTargetShare    = "tgsrl.io/adaptive.target-share"
	adaptiveLabelTargetPriority = "tgsrl.io/adaptive.target-priority"
	adaptiveLabelTargetState    = "tgsrl.io/adaptive.target-state"
	adaptiveLabelAllowResize    = "tgsrl.io/adaptive.allow-resize"
	adaptiveLabelAllowScaleIn   = "tgsrl.io/adaptive.allow-scale-in"
	adaptiveLabelAllowRebind    = "tgsrl.io/adaptive.allow-rebind"
	adaptiveLabelAllowRecreate  = "tgsrl.io/adaptive.allow-recreate"
	adaptiveLabelAllowOffload   = "tgsrl.io/adaptive.allow-offload"
	adaptiveLabelSleepAfter     = "tgsrl.io/adaptive.sleep-after"
	adaptiveLabelOffloadAfter   = "tgsrl.io/adaptive.offload-after"
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

func (s *Server) enqueue(ctx context.Context, intent *tgsrlv1.SchedulingIntent, trigger *eventloop.Trigger) {
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started {
		return
	}
	if intent == nil {
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
	work.pending = mergeReconcileWork(work.pending, intent, trigger)
	depth := 0
	if work.pending != nil {
		depth = 1
	}
	s.recordQueueDepthLocked(intent, depth)
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

func (s *Server) triggerReconcile(ctx context.Context, trigger *eventloop.Trigger) {
	if trigger == nil {
		return
	}
	intent, ok := s.store.LatestValidIntent(trigger.ExecutionID, trigger.StageID)
	if !ok {
		return
	}
	s.enqueue(s.workerContext(ctx), intent, trigger)
}

func (s *Server) enqueueThroughRuntime(ctx context.Context, intent *tgsrlv1.SchedulingIntent, trigger *eventloop.Trigger) {
	if intent == nil {
		return
	}
	if s.eventLoop != nil {
		s.eventLoop.PublishIntent(intent)
	}
	if s.runtime != nil {
		runtimeTrigger := cloneRuntimeTrigger(trigger)
		if runtimeTrigger == nil {
			runtimeTrigger = &eventloop.Trigger{}
		}
		runtimeTrigger.ExecutionID = intent.GetExecutionId()
		runtimeTrigger.StageID = intent.GetStageId()
		if runtimeTrigger.ContractObservation == nil {
			runtimeTrigger.ContractObservation = cloneContractObservation(intent.GetContractObservation())
		}
		s.runtime.Enqueue(runtimeTrigger)
		return
	}
	legacy := cloneRuntimeTrigger(trigger)
	if legacy == nil {
		legacy = &eventloop.Trigger{}
	}
	legacy.ExecutionID = intent.GetExecutionId()
	legacy.StageID = intent.GetStageId()
	if legacy.TickKind == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		legacy.TickKind = tgsrlv1.TickKind_TICK_KIND_FAST
	}
	if legacy.Cause == "" {
		legacy.Cause = "legacy_direct"
	}
	if legacy.ContractObservation == nil {
		legacy.ContractObservation = cloneContractObservation(intent.GetContractObservation())
	}
	s.enqueue(s.workerContext(ctx), intent, legacy)
}

func (s *Server) runtimeTriggerForIntent(intent *tgsrlv1.SchedulingIntent, cause string, tick tgsrlv1.TickKind) *eventloop.Trigger {
	if intent == nil {
		return nil
	}
	return &eventloop.Trigger{
		ExecutionID:                  intent.GetExecutionId(),
		StageID:                      intent.GetStageId(),
		TickKind:                     tick,
		Cause:                        cause,
		ContractObservation:          cloneContractObservation(intent.GetContractObservation()),
		CompatibilityDefaultsApplied: false,
	}
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
		if work == nil || work.pending == nil {
			if work != nil {
				work.running = false
			}
			s.mu.Unlock()
			return
		}
		pending := work.pending
		work.pending = nil
		s.recordQueueDepthLocked(pending.intent, 0)
		s.mu.Unlock()
		s.processAccepted(ctx, pending)
	}
}

func (s *Server) processAccepted(ctx context.Context, workItem *reconcileWork) {
	if workItem == nil || workItem.intent == nil {
		return
	}
	intent := workItem.intent
	triggers := workItem.triggers
	if len(triggers) == 0 {
		triggers = []*eventloop.Trigger{s.runtimeTriggerForIntent(intent, "legacy_direct", tgsrlv1.TickKind_TICK_KIND_FAST)}
	}
	key := workKey{executionID: intent.GetExecutionId(), stageID: intent.GetStageId()}
	work := s.workState(key)
	for _, trigger := range triggers {
		if !s.processAcceptedTrigger(ctx, work, intent, trigger) {
			return
		}
	}
}

func (s *Server) processAcceptedTrigger(ctx context.Context, work *workState, intent *tgsrlv1.SchedulingIntent, trigger *eventloop.Trigger) bool {
	evaluationNow := s.now()
	evaluationContext := buildEvaluationContext(evaluationNow, intent, trigger)
	guardKey := protectionKey(intent)
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
				if s.runtime != nil {
					s.runtime.Forget(intent.GetExecutionId(), intent.GetStageId())
				}
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
				if s.runtime != nil {
					s.runtime.Forget(intent.GetExecutionId(), intent.GetStageId())
				}
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
			if readinessErr := s.projectionReadinessFailure(); readinessErr != nil {
				if err := s.appendTerminalFallback(intent, "PROVIDER_UNAVAILABLE", readinessErr.Error()); err != nil {
					return true
				}
				return true
			}
			snapshot, err := s.store.GetSnapshot(ctx, 0, true)
			if err != nil {
				if appendErr := s.appendFailureDecision(intent, nil, "SNAPSHOT_READ_FAILED", err); appendErr != nil {
					return true
				}
				return true
			}
			evaluationStarted := evaluationNow
			plan, decision, err := s.evaluateIntent(snapshot, intent, evaluationContext)
			s.recordScheduleLatency(intent, s.now().Sub(evaluationStarted))
			if err != nil {
				if appendErr := s.appendFailureDecision(intent, snapshot, "EVALUATION_FAILED", err); appendErr != nil {
					return true
				}
				return true
			}
			fillDecisionMetadataFromIntent(decision, intent)
			applyDecisionEvaluationContext(decision, evaluationContext)
			applyDecisionPlanTickDefaults(decision, evaluationContext.GetTickKind())
			applyPlanTickDefaults(plan, evaluationContext.GetTickKind())
			recentDecisions := s.recentDecisionsForIntent(intent, adaptiveDecisionWindow)
			if shouldSuppressDecision(snapshot, intent, plan, decision, recentDecisions) {
				if triggerCauseContains(evaluationContext.GetCause(), "provider_event") {
					if err := s.checkpoint(); err != nil {
						return true
					}
				}
				return true
			}
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
			if err := actionpolicy.ValidatePlan(plan, evaluationContext.GetTickKind(), actionpolicy.ValidationOptions{
				AllowLegacyUnknownTickL1: true,
			}); err != nil {
				decision.Fallback = true
				decision.FallbackReason = "PLAN_POLICY_REJECTED"
				decision.SelectedPlan.Actions = nil
				if appendErr := s.appendDecision(intent.GetJobId(), decision); appendErr != nil {
					return true
				}
				return true
			}
			if plan.GetPurpose() != tgsrlv1.PlanPurpose_PLAN_PURPOSE_UNKNOWN {
				if err := provider.ValidatePlanCapabilities(s.provider, plan); err != nil {
					decision.Fallback = true
					decision.FallbackReason = "PLAN_POLICY_REJECTED"
					decision.SelectedPlan.Actions = nil
					if appendErr := s.appendDecision(intent.GetJobId(), decision); appendErr != nil {
						return true
					}
					return true
				}
			}
			beforeReservation := s.store.ExportDurableState()
			protectionBeforeReservation := s.ExportProtectionState()
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
			if !s.commitProtectionAdmission(guardKey, decision) {
				if restoreErr := s.store.Restore(beforeReservation); restoreErr != nil {
					slog.Error("reservation rollback failed after protection admission rejection", "error", restoreErr, "job_id", intent.GetJobId(), "run_id", intent.GetRunId(), "trace_id", intent.GetTraceId())
				}
				s.restoreProtectionSnapshot(protectionBeforeReservation)
				decision.Fallback = true
				decision.FallbackReason = "PROTECTION_RACE_REJECTED"
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
				s.restoreProtectionSnapshot(protectionBeforeReservation)
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
			protectionBeforeFinalize := s.ExportProtectionState()
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
			s.recordProtectionOutcome(guardKey, succeeded)
			if err := s.appendDecision(intent.GetJobId(), decision); err != nil {
				if restoreErr := s.store.Restore(beforeFinalize); restoreErr != nil {
					slog.Error("finalized state rollback failed after decision checkpoint failure", "error", restoreErr, "job_id", intent.GetJobId(), "run_id", intent.GetRunId(), "trace_id", intent.GetTraceId(), "decision_id", decision.GetDecisionId())
				}
				s.restoreProtectionSnapshot(protectionBeforeFinalize)
			}
			return true
		}()
		if completed {
			return true
		}
		if retry {
			continue
		}
	}
	if err := s.appendFailureDecision(intent, nil, "REVISION_CONFLICT_RETRY_EXHAUSTED", state.ErrPlanRevisionConflict); err != nil {
		return false
	}
	return true
}

func protectionKey(intent *tgsrlv1.SchedulingIntent) string {
	if intent == nil {
		return ""
	}
	return intent.GetExecutionId() + "/" + intent.GetStageId()
}

func (s *Server) commitProtectionAdmission(key string, decision *tgsrlv1.DecisionRecord) bool {
	if s == nil || s.guard == nil || key == "" || decision == nil || decision.GetSelectedPlan() == nil {
		return true
	}
	return s.guard.CommitN(key, protectionScore(decision), len(decision.GetSelectedPlan().GetActions())).Allowed
}

func (s *Server) recordProtectionOutcome(key string, succeeded bool) {
	if s == nil || s.guard == nil || key == "" {
		return
	}
	if succeeded {
		s.guard.Accept(key)
		return
	}
	s.guard.Reject(key)
}

func (s *Server) restoreProtectionSnapshot(snapshot protection.State) {
	if s == nil || s.guard == nil {
		return
	}
	s.guard.RestoreState(snapshot)
}

func protectionScore(decision *tgsrlv1.DecisionRecord) float64 {
	if decision == nil {
		return 0
	}
	for _, evidence := range decision.GetPlannerEvidence() {
		if evidence.GetDisposition() == tgsrlv1.PlannerDisposition_PLANNER_DISPOSITION_SELECTED {
			return float64(evidence.GetUtilityNanos()) / 1e9
		}
	}
	return decision.GetScore()
}

func (s *Server) evaluateIntent(
	snapshot *tgsrlv1.ClusterSnapshot,
	intent *tgsrlv1.SchedulingIntent,
	evaluationContext *tgsrlv1.EvaluationContext,
) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	if evaluator, ok := s.scheduler.(AdaptiveEvaluator); ok {
		projectedSandboxes := s.store.ListProjectedSandboxes()
		if shouldEvaluateAdaptively(snapshot, intent, evaluationContext, projectedSandboxes) {
			if err := s.projectionReadinessFailure(); err != nil {
				return nil, nil, fmt.Errorf("adaptive projection is not ready: %w", err)
			}
			return evaluator.EvaluateAdaptive(&scheduler.AdaptiveEvaluationInput{
				Snapshot:          cloneSnapshot(snapshot),
				Intent:            cloneIntent(intent),
				EvaluationContext: cloneEvaluationContext(evaluationContext),
				Sandboxes:         projectedSandboxes,
				RecentDecisions:   s.recentDecisionsForIntent(intent, adaptiveDecisionWindow),
			})
		}
	}
	if evaluator, ok := s.scheduler.(ContextualEvaluator); ok {
		return evaluator.EvaluateWithContext(snapshot, intent, evaluationContext)
	}
	return s.scheduler.Evaluate(snapshot, intent)
}

func triggerCauseContains(cause, expected string) bool {
	for _, item := range strings.Split(cause, "|") {
		if strings.TrimSpace(item) == expected {
			return true
		}
	}
	return false
}

func shouldEvaluateAdaptively(
	snapshot *tgsrlv1.ClusterSnapshot,
	intent *tgsrlv1.SchedulingIntent,
	evaluationContext *tgsrlv1.EvaluationContext,
	projectedSandboxes []*tgsrlv1.Sandbox,
) bool {
	if intent == nil || evaluationContext == nil {
		return false
	}
	switch evaluationContext.GetCause() {
	case "", "legacy_direct", "publish_intent", "startup_replay":
		return false
	}
	if hasAdaptiveIntentLabels(intent) {
		return true
	}
	return hasProjectedSandboxObservation(snapshot, intent, projectedSandboxes) &&
		(strings.HasPrefix(evaluationContext.GetCause(), "manual_") || evaluationContext.GetCause() == "provider_event")
}

func hasAdaptiveIntentLabels(intent *tgsrlv1.SchedulingIntent) bool {
	if intent == nil {
		return false
	}
	labels := intent.GetLabels()
	for _, key := range []string{
		adaptiveLabelTargetShare,
		adaptiveLabelTargetPriority,
		adaptiveLabelTargetState,
		adaptiveLabelAllowResize,
		adaptiveLabelAllowScaleIn,
		adaptiveLabelAllowRebind,
		adaptiveLabelAllowRecreate,
		adaptiveLabelAllowOffload,
		adaptiveLabelSleepAfter,
		adaptiveLabelOffloadAfter,
	} {
		if strings.TrimSpace(labels[key]) != "" {
			return true
		}
	}
	return false
}

func hasProjectedSandboxObservation(
	snapshot *tgsrlv1.ClusterSnapshot,
	intent *tgsrlv1.SchedulingIntent,
	projectedSandboxes []*tgsrlv1.Sandbox,
) bool {
	if snapshot == nil || intent == nil || len(projectedSandboxes) == 0 {
		return false
	}
	pendingUnits := make(map[string]struct{})
	runtimeUnits := make(map[string]struct{})
	for _, allocation := range snapshot.GetAllocations() {
		if allocation == nil ||
			allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE ||
			allocation.GetExecutionId() != intent.GetExecutionId() ||
			allocation.GetStageId() != intent.GetStageId() ||
			allocation.GetIntentVersion() != intent.GetVersion() {
			continue
		}
		if allocation.GetPendingUnitId() != "" {
			pendingUnits[allocation.GetPendingUnitId()] = struct{}{}
		}
		if allocation.GetRuntimeUnitId() != "" {
			runtimeUnits[allocation.GetRuntimeUnitId()] = struct{}{}
		}
	}
	if len(pendingUnits) == 0 && len(runtimeUnits) == 0 {
		return false
	}
	for _, sandbox := range projectedSandboxes {
		if sandbox == nil || sandbox.GetBinding() == nil {
			continue
		}
		binding := sandbox.GetBinding()
		if binding.GetRuntimeUnitId() != "" {
			if _, ok := runtimeUnits[binding.GetRuntimeUnitId()]; ok {
				return true
			}
		}
		if binding.GetPendingUnitId() != "" {
			if _, ok := pendingUnits[binding.GetPendingUnitId()]; ok {
				return true
			}
		}
	}
	return false
}

func applyPlanTickDefaults(plan *tgsrlv1.PlacementPlan, tick tgsrlv1.TickKind) {
	if plan == nil || tick == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		return
	}
	for _, action := range plan.GetActions() {
		if action != nil && action.GetTickKind() == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
			action.TickKind = tick
		}
	}
}

func applyDecisionPlanTickDefaults(decision *tgsrlv1.DecisionRecord, tick tgsrlv1.TickKind) {
	if decision == nil || tick == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		return
	}
	if decision.GetTickKind() == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		decision.TickKind = tick
	}
	applyPlanTickDefaults(decision.GetSelectedPlan(), tick)
	for _, candidate := range decision.GetCandidates() {
		if candidate == nil {
			continue
		}
		applyPlanTickDefaults(candidate.GetPlan(), tick)
	}
}

func applyDecisionEvaluationContext(decision *tgsrlv1.DecisionRecord, evaluationContext *tgsrlv1.EvaluationContext) {
	if decision == nil || evaluationContext == nil {
		return
	}
	if decision.GetTickKind() == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		decision.TickKind = evaluationContext.GetTickKind()
	}
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
