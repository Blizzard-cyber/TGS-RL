package service

import (
	"context"
	"errors"
	"fmt"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/persistence"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/protobuf/proto"
)

// DurableRepository is the atomic checkpoint boundary used for every critical
// scheduler write. A failed save closes the mutation gate and prevents further
// provider actions until the process is restarted after repairing storage.
type DurableRepository interface {
	SaveCheckpoint(persistence.SchedulerState) error
}

type durableStateExporter interface {
	ExportDurableState() state.DurableState
}

func (s *Server) checkpointLocked() error {
	return s.checkpointStateLocked(s.decisions, s.sequence)
}

func (s *Server) checkpointStateLocked(entries []decisionEntry, cursor uint64, exporters ...durableStateExporter) error {
	if s.persistenceErr != nil {
		return s.persistenceErr
	}
	if s.repository == nil {
		return nil
	}
	// Caller must hold s.mu so decisions and cursor are captured atomically.
	decisions := make([]*tgsrlv1.DecisionRecord, 0, len(entries))
	for _, entry := range entries {
		decisions = append(decisions, cloneDecision(entry.decision))
	}
	store := durableStateExporter(s.store)
	if len(exporters) > 0 && exporters[0] != nil {
		store = exporters[0]
	}
	checkpoint, err := persistence.Capture(store, decisions, cursor)
	if err != nil {
		return s.recordPersistenceFailureLocked(fmt.Errorf("service: capture scheduler checkpoint: %w", err))
	}
	if err := s.repository.SaveCheckpoint(checkpoint); err != nil {
		return s.recordPersistenceFailureLocked(fmt.Errorf("service: persist scheduler checkpoint: %w", err))
	}
	s.recorder.IncCounter("persistence_checkpoints", 1)
	return nil
}

func (s *Server) recordPersistenceFailureLocked(err error) error {
	if s.persistenceErr == nil {
		s.persistenceErr = err
		s.recorder.IncCounter("persistence_failures", 1)
	}
	return s.persistenceErr
}

// RestoreDecisions installs recovered decision history and cursor before the
// service starts accepting requests. Sequence-zero records are durable pending
// audits and remain hidden until recovery commits their provider outcome.
func (s *Server) RestoreDecisions(decisions []*tgsrlv1.DecisionRecord, cursor uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.decisions) != 0 || s.sequence != 0 {
		return fmt.Errorf("service: decisions already initialized")
	}
	committed := make([]*tgsrlv1.DecisionRecord, 0, len(decisions))
	pending := make([]*tgsrlv1.DecisionRecord, 0, len(decisions))
	seen := make(map[string]struct{}, len(decisions))
	for _, decision := range decisions {
		if decision == nil || decision.GetDecisionId() == "" {
			return fmt.Errorf("service: recovered decision identity is invalid")
		}
		if _, duplicate := seen[decision.GetDecisionId()]; duplicate {
			return fmt.Errorf("service: duplicate recovered decision %q", decision.GetDecisionId())
		}
		seen[decision.GetDecisionId()] = struct{}{}
		if decision.GetSequence() == 0 {
			if decision.GetSelectedPlan() == nil || decision.GetSelectedPlan().GetPlanId() == "" {
				return fmt.Errorf("service: recovered pending decision %q requires a selected plan", decision.GetDecisionId())
			}
			pending = append(pending, cloneDecision(decision))
			continue
		}
		committed = append(committed, cloneDecision(decision))
	}
	sort.Slice(committed, func(i, j int) bool { return committed[i].GetSequence() < committed[j].GetSequence() })
	for _, decision := range committed {
		if decision.GetSequence() > cursor {
			return fmt.Errorf("service: recovered decision sequence exceeds cursor")
		}
		s.decisions = append(s.decisions, decisionEntry{jobID: decision.GetJobId(), decision: decision})
	}
	for _, decision := range pending {
		s.decisions = append(s.decisions, decisionEntry{jobID: decision.GetJobId(), decision: decision})
	}
	s.decisions = retainDecisions(s.decisions, s.retention)
	s.sequence = cursor
	return nil
}

// ResumeRecoveredState reconciles recovered scheduler authority back into the
// live event loop and provider-backed store before the server starts serving.
func (s *Server) ResumeRecoveredState(ctx context.Context, recovered *persistence.SchedulerState) error {
	if recovered == nil {
		return nil
	}
	if ctx == nil {
		ctx = s.backgroundContext()
	}
	if err := s.RestoreDecisions(recovered.Decisions, recovered.Cursor); err != nil {
		return err
	}
	if err := s.reconcileRecoveredReservations(ctx, recovered); err != nil {
		return err
	}
	if err := s.checkpoint(); err != nil {
		return err
	}
	for key, intent := range recovered.Intents {
		if intent == nil {
			return fmt.Errorf("service: recovered intent %q is nil", key)
		}
	}
	// Start enqueues the restored intent set only after reservation and decision
	// recovery has committed, so no worker can race recovery.
	return nil
}

func (s *Server) reconcileRecoveredReservations(ctx context.Context, recovered *persistence.SchedulerState) error {
	pending := make([]persistence.ReservationRecord, 0, len(recovered.Reservations))
	for _, reservation := range recovered.Reservations {
		if reservation.Plan == nil || reservation.Plan.GetPlanId() == "" {
			return fmt.Errorf("service: recovered reservation requires plan_id")
		}
		if !reservation.Finalized {
			pending = append(pending, reservation)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	completeProvider, ok := s.provider.(provider.CompleteResourceProvider)
	if !ok {
		return errors.New("service: provider does not support recovered plan reconciliation")
	}
	recoveredPlans, err := completeProvider.RecoverInFlightPlans(ctx)
	if err != nil {
		return fmt.Errorf("service: recover in-flight plans: %w", err)
	}
	recordsByPlanID := make(map[string]*provider.PlanRecord, len(recoveredPlans))
	for _, record := range recoveredPlans {
		if record == nil || record.Plan == nil || record.Plan.GetPlanId() == "" {
			continue
		}
		recordsByPlanID[record.Plan.GetPlanId()] = record
	}
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].Plan.GetPlanId() < pending[j].Plan.GetPlanId()
	})
	for _, reservation := range pending {
		record, err := s.recoveredPlanRecord(ctx, completeProvider, recordsByPlanID, reservation.Plan)
		if err != nil {
			return err
		}
		_, alreadyCommitted := s.decisionForPlan(reservation.Plan)
		pendingDecision, hasPendingAudit := s.pendingDecisionForPlan(reservation.Plan)
		if !alreadyCommitted && !hasPendingAudit {
			// Legacy checkpoints may have recorded a final decision before the
			// reservation checkpoint caught up. Accept that complete audit as the
			// reconciliation source, but never invent a partial audit otherwise.
			alreadyCommitted = recoveredDecisionForPlan(recovered.Decisions, reservation.Plan) != nil
		}
		if !alreadyCommitted && !hasPendingAudit {
			return fmt.Errorf("service: recovered reservation %q is missing its pending decision audit; restore the pre-execution checkpoint and retry startup", reservation.Plan.GetPlanId())
		}
		succeeded := record.Status == provider.PlanStatusSucceeded
		beforeFinalize := s.store.ExportDurableState()
		finalSnapshot, err := s.store.FinalizePlanResults(reservation.Plan, succeeded, record.Results)
		if err != nil {
			return fmt.Errorf("service: finalize recovered reservation %q: %w", reservation.Plan.GetPlanId(), err)
		}
		if alreadyCommitted {
			if err := s.checkpoint(); err != nil {
				if restoreErr := s.store.Restore(beforeFinalize); restoreErr != nil {
					return fmt.Errorf("service: rollback recovered reservation %q: %v (checkpoint error: %w)", reservation.Plan.GetPlanId(), restoreErr, err)
				}
				return err
			}
			continue
		}
		decision, err := recoveredReservationDecision(pendingDecision, reservation.Plan, record, finalSnapshot)
		if err != nil {
			_ = s.store.Restore(beforeFinalize)
			return err
		}
		if err := s.appendDecision(decision.GetJobId(), decision); err != nil {
			if restoreErr := s.store.Restore(beforeFinalize); restoreErr != nil {
				return fmt.Errorf("service: rollback recovered reservation %q: %v (decision checkpoint error: %w)", reservation.Plan.GetPlanId(), restoreErr, err)
			}
			return fmt.Errorf("service: persist recovered decision %q: %w", decision.GetDecisionId(), err)
		}
	}
	return nil
}

func recoveredDecisionForPlan(decisions []*tgsrlv1.DecisionRecord, plan *tgsrlv1.PlacementPlan) *tgsrlv1.DecisionRecord {
	for _, decision := range decisions {
		if decision.GetSequence() != 0 && decisionMatchesPlan(decision, plan) {
			return cloneDecision(decision)
		}
	}
	return nil
}

func recoveredReservationDecision(
	pending *tgsrlv1.DecisionRecord,
	plan *tgsrlv1.PlacementPlan,
	record *provider.PlanRecord,
	finalSnapshot *tgsrlv1.ClusterSnapshot,
) (*tgsrlv1.DecisionRecord, error) {
	if plan == nil || plan.GetPlanId() == "" || plan.GetDecisionId() == "" {
		return nil, errors.New("service: recovered reservation requires plan_id and decision_id for audit retry")
	}
	if record == nil {
		return nil, fmt.Errorf("service: recovered reservation %q has no provider outcome", plan.GetPlanId())
	}
	if pending == nil || !decisionMatchesPlan(pending, plan) {
		return nil, fmt.Errorf("service: recovered reservation %q has no matching decision audit", plan.GetPlanId())
	}
	decision := cloneDecision(pending)
	decision.Sequence = 0
	decision.SelectedPlan = proto.Clone(plan).(*tgsrlv1.PlacementPlan)
	decision.ActionResults = cloneActionResults(record.Results)
	decision.Fallback = record.Status != provider.PlanStatusSucceeded
	decision.FallbackReason = ""
	if decision.GetFallback() {
		decision.FallbackReason = "PROVIDER_EXECUTION_FAILED"
	}
	if finalSnapshot != nil {
		for _, result := range decision.ActionResults {
			if result != nil {
				result.ObservedRevision = finalSnapshot.GetRevision()
			}
		}
	}
	return decision, nil
}

func (s *Server) recoveredPlanRecord(
	ctx context.Context,
	completeProvider provider.CompleteResourceProvider,
	recordsByPlanID map[string]*provider.PlanRecord,
	plan *tgsrlv1.PlacementPlan,
) (*provider.PlanRecord, error) {
	if plan == nil || plan.GetPlanId() == "" {
		return nil, fmt.Errorf("service: recovered plan requires plan_id")
	}
	record := recordsByPlanID[plan.GetPlanId()]
	if record == nil || record.Status == provider.PlanStatusInFlight || record.Status == provider.PlanStatusUnknown {
		reconciled, err := completeProvider.ReconcilePlan(ctx, plan)
		switch {
		case err == nil:
			record = reconciled
		case errors.Is(err, provider.ErrNotFound):
			if record == nil {
				record = &provider.PlanRecord{Plan: plan, Status: provider.PlanStatusFailed}
			}
		default:
			return nil, fmt.Errorf("service: reconcile recovered plan %q: %w", plan.GetPlanId(), err)
		}
	}
	if record == nil {
		record = &provider.PlanRecord{Plan: plan, Status: provider.PlanStatusFailed}
	}
	if record.Plan == nil {
		record.Plan = plan
	}
	switch record.Status {
	case provider.PlanStatusSucceeded, provider.PlanStatusFailed:
		return record, nil
	case provider.PlanStatusInFlight, provider.PlanStatusUnknown, "":
		// Startup must converge any restored reservation into a checkpointed
		// store outcome instead of leaving a stale in-flight fence behind.
		record.Status = provider.PlanStatusFailed
		return record, nil
	default:
		record.Status = provider.PlanStatusFailed
		return record, nil
	}
}

func (s *Server) persistenceFailure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistenceErr
}

func (s *Server) checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpointLocked()
}
