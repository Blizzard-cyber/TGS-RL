package planexecutor

import (
	"context"
	"errors"
	"fmt"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/protobuf/proto"
)

// Executor drives the provider transaction protocol while state.Store remains
// the single durable authority for reservations and transaction progress.
type Executor struct {
	store      *state.Store
	provider   provider.TransactionExecutor
	checkpoint CheckpointFunc
}

func NewExecutor(store *state.Store, transactionalProvider provider.TransactionExecutor, checkpoint CheckpointFunc) (*Executor, error) {
	switch {
	case store == nil:
		return nil, fmt.Errorf("transaction store is required")
	case transactionalProvider == nil:
		return nil, ErrTransactionalProvider
	case checkpoint == nil:
		return nil, ErrCheckpointRequired
	default:
		return &Executor{store: store, provider: transactionalProvider, checkpoint: checkpoint}, nil
	}
}

// Execute creates or resumes the durable transaction identified by plan_id.
func (e *Executor) Execute(ctx context.Context, plan *tgsrlv1.PlacementPlan) (*Outcome, error) {
	record, err := e.Stage(ctx, plan)
	if err != nil {
		return nil, err
	}
	if err := e.persist(ctx, record); err != nil {
		return nil, err
	}
	return e.Resume(ctx, record.TransactionID)
}

// Stage creates and reserves a plan without calling the provider. Callers use
// this boundary to persist their pending audit record before external effects.
func (e *Executor) Stage(ctx context.Context, plan *tgsrlv1.PlacementPlan) (*state.TransactionRecord, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := validatePlan(plan); err != nil {
		return nil, err
	}
	if existing, ok := e.store.GetTransaction(plan.GetPlanId()); ok {
		if !proto.Equal(existing.Plan, plan) {
			return nil, fmt.Errorf("%w: transaction %q plan changed", state.ErrTransactionConflict, plan.GetPlanId())
		}
		return existing, nil
	}
	reserved, _, err := e.store.BeginTransaction(plan, 1)
	if err != nil {
		return nil, err
	}
	return reserved, nil
}

// Resume advances one non-terminal transaction from its durable Store state.
func (e *Executor) Resume(ctx context.Context, transactionID string) (*Outcome, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	record, ok := e.store.GetTransaction(transactionID)
	if !ok {
		return nil, state.ErrTransactionNotFound
	}
	if record.IsTerminal() {
		return &Outcome{Transaction: record, Terminal: true}, nil
	}
	if record.State == state.TransactionStatePrepared {
		prepared, err := e.ensureProviderPrepared(ctx, record)
		if err != nil {
			return nil, err
		}
		record = prepared
	}
	if record.State == state.TransactionStateApplying {
		reconciled, terminal, err := e.resumeFromProviderReceipt(ctx, record)
		if err != nil {
			return nil, err
		}
		record = reconciled
		if terminal {
			return &Outcome{Transaction: record, Terminal: true}, nil
		}
	}
	for {
		next, terminal, err := e.step(ctx, record)
		if err != nil {
			return nil, err
		}
		record = next
		if terminal {
			return &Outcome{Transaction: record, Terminal: true}, nil
		}
	}
}

// ResumeAll deterministically resumes every durable non-terminal transaction.
func (e *Executor) ResumeAll(ctx context.Context) ([]*Outcome, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	records := e.store.ListInFlightTransactions()
	sort.Slice(records, func(i, j int) bool {
		return records[i].TransactionID < records[j].TransactionID
	})
	outcomes := make([]*Outcome, 0, len(records))
	for _, record := range records {
		if record == nil {
			return outcomes, fmt.Errorf("planexecutor: state store returned nil transaction")
		}
		outcome, err := e.Resume(ctx, record.TransactionID)
		if err != nil {
			return outcomes, err
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes, nil
}

// Get returns a detached Store-owned transaction record.
func (e *Executor) Get(ctx context.Context, transactionID string) (*state.TransactionRecord, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	record, ok := e.store.GetTransaction(transactionID)
	if !ok {
		return nil, state.ErrTransactionNotFound
	}
	return record, nil
}

func (e *Executor) step(ctx context.Context, record *state.TransactionRecord) (*state.TransactionRecord, bool, error) {
	switch record.State {
	case state.TransactionStateProposed:
		reserved, _, err := e.store.ReserveTransaction(record.TransactionID, record.Generation)
		if err != nil {
			return nil, false, err
		}
		return reserved, false, e.persist(ctx, reserved)
	case state.TransactionStateReserved:
		return e.prepare(ctx, record)
	case state.TransactionStatePrepared, state.TransactionStateApplying:
		return e.apply(ctx, record)
	case state.TransactionStateApplyFailed, state.TransactionStateCompensating:
		return e.abort(ctx, record)
	default:
		return nil, false, fmt.Errorf("%w: resume from %s", ErrInvalidTransition, record.State)
	}
}

func (e *Executor) ensureProviderPrepared(ctx context.Context, record *state.TransactionRecord) (*state.TransactionRecord, error) {
	receipt, err := e.provider.PreparePlan(ctx, record.TransactionID, record.ProviderGeneration, record.Plan)
	if err != nil {
		return nil, err
	}
	if err := validateReceiptForPlan(receipt, record.ProviderGeneration, record.Plan); err != nil {
		return nil, err
	}
	return e.advance(ctx, record, state.TransactionStatePrepared, state.FailureClassUnknown, "", receipt)
}

func (e *Executor) prepare(ctx context.Context, record *state.TransactionRecord) (*state.TransactionRecord, bool, error) {
	receipt, err := e.provider.PreparePlan(ctx, record.TransactionID, record.ProviderGeneration, record.Plan)
	if err != nil {
		failed, finalizeErr := e.finalize(ctx, record, false, state.TransactionStatePrepareFailed, state.FailureClassProvider, err.Error(), nil)
		return failed, true, finalizeErr
	}
	if err := validateReceiptForPlan(receipt, record.ProviderGeneration, record.Plan); err != nil {
		return nil, false, err
	}
	prepared, err := e.advance(ctx, record, state.TransactionStatePrepared, state.FailureClassUnknown, "", receipt)
	return prepared, false, err
}

func (e *Executor) apply(ctx context.Context, record *state.TransactionRecord) (*state.TransactionRecord, bool, error) {
	current := record
	var err error
	if current.State == state.TransactionStatePrepared {
		current, err = e.advance(ctx, current, state.TransactionStateApplying, state.FailureClassUnknown, "", nil)
		if err != nil {
			return nil, false, err
		}
	}
	for index := range current.Plan.GetActions() {
		if effectStatus(current.ProviderReceipt, index) == state.EffectStatusSucceeded {
			continue
		}
		receipt, executeErr := e.provider.ExecuteStep(ctx, current.TransactionID, current.ProviderGeneration, index)
		if executeErr != nil {
			return e.handleApplyFailure(ctx, current, receipt, executeErr)
		}
		if err := validateReceiptForPlan(receipt, current.ProviderGeneration, current.Plan); err != nil {
			return nil, false, err
		}
		status, ambiguous := classifyStepReceipt(receipt, index)
		if ambiguous {
			return e.handleApplyFailure(ctx, current, receipt, ErrReconcileRequired)
		}
		if status == state.EffectStatusFailed {
			failed, err := e.advance(ctx, current, state.TransactionStateApplyFailed, state.FailureClassProvider, firstNonEmpty(receipt.ErrorMessage, "provider reported an apply failure"), receipt)
			return failed, false, err
		}
		current, err = e.advance(ctx, current, state.TransactionStateApplying, state.FailureClassUnknown, "", receipt)
		if err != nil {
			return nil, false, err
		}
	}
	receipt, err := e.provider.CommitPlan(ctx, current.TransactionID, current.ProviderGeneration)
	if err != nil {
		return e.handleCommitFailure(ctx, current, receipt, err)
	}
	if err := validateReceiptForPlan(receipt, current.ProviderGeneration, current.Plan); err != nil {
		return nil, false, err
	}
	if isAmbiguousPhase(receipt.Phase) {
		return e.handleCommitFailure(ctx, current, receipt, ErrReconcileRequired)
	}
	committed, err := e.finalize(ctx, current, true, state.TransactionStateCommitted, state.FailureClassUnknown, "", receipt)
	return committed, true, err
}

func (e *Executor) abort(ctx context.Context, record *state.TransactionRecord) (*state.TransactionRecord, bool, error) {
	current := record
	var err error
	if current.State == state.TransactionStateApplyFailed {
		current, err = e.advance(ctx, current, state.TransactionStateCompensating, current.FailureClass, current.FailureReason, nil)
		if err != nil {
			return nil, false, err
		}
	}
	receipt, abortErr := e.provider.AbortPlan(ctx, current.TransactionID, current.ProviderGeneration)
	if abortErr != nil {
		return e.handleAbortFailure(ctx, current, receipt, abortErr)
	}
	if err := validateReceiptForPlan(receipt, current.ProviderGeneration, current.Plan); err != nil {
		return nil, false, err
	}
	if isAmbiguousPhase(receipt.Phase) {
		return e.handleAbortFailure(ctx, current, receipt, ErrReconcileRequired)
	}
	aborted, err := e.finalize(ctx, current, false, state.TransactionStateAborted, current.FailureClass, current.FailureReason, receipt)
	return aborted, true, err
}

func (e *Executor) resumeFromProviderReceipt(ctx context.Context, record *state.TransactionRecord) (*state.TransactionRecord, bool, error) {
	prepared, err := e.provider.PreparePlan(ctx, record.TransactionID, record.ProviderGeneration, record.Plan)
	if err != nil {
		degraded, finalizeErr := e.finalize(ctx, record, false, state.TransactionStateDegraded, state.FailureClassInfrastructure, err.Error(), nil)
		return degraded, true, finalizeErr
	}
	if err := validateReceiptForPlan(prepared, record.ProviderGeneration, record.Plan); err != nil {
		degraded, finalizeErr := e.finalize(ctx, record, false, state.TransactionStateDegraded, state.FailureClassInfrastructure, err.Error(), nil)
		return degraded, true, finalizeErr
	}
	receipt, err := e.reconcile(ctx, record)
	if err != nil {
		degraded, finalizeErr := e.finalize(ctx, record, false, state.TransactionStateDegraded, state.FailureClassInfrastructure, err.Error(), nil)
		return degraded, true, finalizeErr
	}
	return e.applyReconciledReceipt(ctx, record, receipt)
}

func (e *Executor) handleApplyFailure(ctx context.Context, record *state.TransactionRecord, receipt *provider.TransactionReceipt, cause error) (*state.TransactionRecord, bool, error) {
	reconciled, err := e.reconcile(ctx, record)
	if err != nil {
		reason := fmt.Sprintf("%v; provider reconciliation failed: %v", cause, err)
		degraded, finalizeErr := e.finalize(ctx, record, false, state.TransactionStateDegraded, state.FailureClassInfrastructure, reason, receipt)
		return degraded, true, finalizeErr
	}
	switch reconciled.Phase {
	case provider.TransactionPhaseCommitted, provider.TransactionPhaseAborted:
		return e.applyReconciledReceipt(ctx, record, reconciled)
	default:
		failureClass := state.FailureClassProvider
		if errors.Is(cause, ErrReconcileRequired) {
			failureClass = state.FailureClassInfrastructure
		}
		if receipt == nil {
			receipt = reconciled
		}
		failed, saveErr := e.advance(ctx, record, state.TransactionStateApplyFailed, failureClass, cause.Error(), receipt)
		return failed, false, saveErr
	}
}

func (e *Executor) handleCommitFailure(ctx context.Context, record *state.TransactionRecord, receipt *provider.TransactionReceipt, cause error) (*state.TransactionRecord, bool, error) {
	reconciled, err := e.reconcile(ctx, record)
	if err != nil {
		reason := fmt.Sprintf("%v; provider reconciliation failed: %v", cause, err)
		degraded, finalizeErr := e.finalize(ctx, record, false, state.TransactionStateDegraded, state.FailureClassInfrastructure, reason, receipt)
		return degraded, true, finalizeErr
	}
	if reconciled.Phase == provider.TransactionPhaseCommitted {
		committed, finalizeErr := e.finalize(ctx, record, true, state.TransactionStateCommitted, state.FailureClassInfrastructure, cause.Error(), reconciled)
		return committed, true, finalizeErr
	}
	return e.handleApplyFailure(ctx, record, receipt, cause)
}

func (e *Executor) handleAbortFailure(ctx context.Context, record *state.TransactionRecord, receipt *provider.TransactionReceipt, cause error) (*state.TransactionRecord, bool, error) {
	reconciled, err := e.reconcile(ctx, record)
	if err != nil {
		reason := fmt.Sprintf("%v; provider reconciliation failed: %v", cause, err)
		degraded, finalizeErr := e.finalize(ctx, record, false, state.TransactionStateDegraded, state.FailureClassCompensation, reason, receipt)
		return degraded, true, finalizeErr
	}
	if reconciled.Phase == provider.TransactionPhaseAborted {
		aborted, finalizeErr := e.finalize(ctx, record, false, state.TransactionStateAborted, state.FailureClassCompensation, cause.Error(), reconciled)
		return aborted, true, finalizeErr
	}
	if receipt == nil {
		receipt = reconciled
	}
	degraded, finalizeErr := e.finalize(ctx, record, false, state.TransactionStateDegraded, state.FailureClassCompensation, cause.Error(), receipt)
	return degraded, true, finalizeErr
}

func (e *Executor) applyReconciledReceipt(ctx context.Context, record *state.TransactionRecord, receipt *provider.TransactionReceipt) (*state.TransactionRecord, bool, error) {
	switch receipt.Phase {
	case provider.TransactionPhaseCommitted:
		committed, err := e.finalize(ctx, record, true, state.TransactionStateCommitted, state.FailureClassInfrastructure, "", receipt)
		return committed, true, err
	case provider.TransactionPhaseAborted:
		aborted, err := e.finalize(ctx, record, false, state.TransactionStateAborted, state.FailureClassInfrastructure, "", receipt)
		return aborted, true, err
	case provider.TransactionPhaseDegraded, provider.TransactionPhaseUnknown:
		degraded, err := e.finalize(ctx, record, false, state.TransactionStateDegraded, state.FailureClassInfrastructure, firstNonEmpty(receipt.ErrorMessage, "provider transaction remains ambiguous"), receipt)
		return degraded, true, err
	case provider.TransactionPhasePrepared:
		return record, false, nil
	default:
		if receiptHasAmbiguousEffect(receipt, len(record.Plan.GetActions())) {
			degraded, err := e.finalize(ctx, record, false, state.TransactionStateDegraded, state.FailureClassInfrastructure, "provider transaction contains an unknown effect", receipt)
			return degraded, true, err
		}
		updated, err := e.advance(ctx, record, state.TransactionStateApplying, state.FailureClassUnknown, "", receipt)
		return updated, false, err
	}
}

func (e *Executor) reconcile(ctx context.Context, record *state.TransactionRecord) (*provider.TransactionReceipt, error) {
	receipt, err := e.provider.ReconcilePlanTransaction(ctx, record.TransactionID, record.ProviderGeneration)
	if err != nil {
		return nil, err
	}
	if err := validateReceiptForPlan(receipt, record.ProviderGeneration, record.Plan); err != nil {
		return nil, err
	}
	return receipt, nil
}

func (e *Executor) advance(ctx context.Context, record *state.TransactionRecord, nextState state.TransactionState, failureClass state.FailureClass, reason string, receipt *provider.TransactionReceipt) (*state.TransactionRecord, error) {
	mergedReceipt := mergeStateReceipt(record.ProviderReceipt, receipt)
	next, err := e.store.AdvanceTransaction(state.TransactionAdvanceRequest{
		TransactionID: record.TransactionID, ExpectedGeneration: record.Generation,
		NextState: nextState, FailureClass: failureClass, FailureReason: reason,
		ProviderReceipt: mergedReceipt,
	})
	if err != nil {
		return nil, err
	}
	if err := e.persist(ctx, next); err != nil {
		return nil, err
	}
	return next, nil
}

func (e *Executor) finalize(ctx context.Context, record *state.TransactionRecord, succeeded bool, finalState state.TransactionState, failureClass state.FailureClass, reason string, receipt *provider.TransactionReceipt) (*state.TransactionRecord, error) {
	mergedReceipt := mergeStateReceipt(record.ProviderReceipt, receipt)
	result, _, err := e.store.FinalizeTransaction(state.TransactionFinalizeRequest{
		TransactionID: record.TransactionID, ExpectedGeneration: record.Generation,
		Succeeded: succeeded, Results: transactionResults(record.Plan, finalState, mergedReceipt), FinalState: finalState,
		FailureClass: failureClass, FailureReason: reason, ProviderReceipt: mergedReceipt,
	})
	if err != nil {
		return nil, err
	}
	if err := e.persist(ctx, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Executor) persist(ctx context.Context, record *state.TransactionRecord) error {
	return e.checkpoint(ctx, record)
}

func validatePlan(plan *tgsrlv1.PlacementPlan) error {
	if plan == nil || plan.GetPlanId() == "" {
		return fmt.Errorf("plan_id is required")
	}
	for index, action := range plan.GetActions() {
		if action == nil || action.GetActionId() == "" {
			return fmt.Errorf("plan action %d is required", index)
		}
	}
	return nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func effectStatus(receipt *state.Receipt, stepIndex int) state.EffectStatus {
	if receipt == nil {
		return state.EffectStatusUnknown
	}
	for _, step := range receipt.Steps {
		if step.StepIndex == stepIndex {
			return step.Status
		}
	}
	return state.EffectStatusUnknown
}

func classifyStepReceipt(receipt *provider.TransactionReceipt, stepIndex int) (state.EffectStatus, bool) {
	if receipt == nil {
		return state.EffectStatusFailed, true
	}
	for _, effect := range receipt.Effects {
		if effect.StepIndex != stepIndex {
			continue
		}
		switch effect.Status {
		case provider.EffectStatusApplied:
			return state.EffectStatusSucceeded, false
		case provider.EffectStatusFailed:
			return state.EffectStatusFailed, false
		default:
			return state.EffectStatusFailed, true
		}
	}
	return state.EffectStatusFailed, true
}

func receiptHasAmbiguousEffect(receipt *provider.TransactionReceipt, stepCount int) bool {
	if receipt == nil || len(receipt.Effects) != stepCount {
		return true
	}
	seen := make(map[int]struct{}, stepCount)
	for _, effect := range receipt.Effects {
		if effect.StepIndex < 0 || effect.StepIndex >= stepCount {
			return true
		}
		if _, duplicate := seen[effect.StepIndex]; duplicate {
			return true
		}
		seen[effect.StepIndex] = struct{}{}
		switch effect.Status {
		case provider.EffectStatusNotApplied, provider.EffectStatusApplied, provider.EffectStatusCompensated, provider.EffectStatusFailed:
		default:
			return true
		}
	}
	return false
}

func isAmbiguousPhase(phase provider.TransactionPhase) bool {
	switch phase {
	case provider.TransactionPhaseUnknown, provider.TransactionPhaseExecuting, provider.TransactionPhaseReconciled:
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
