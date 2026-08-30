package nvidia

import (
	"context"
	"fmt"
	"sort"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	base "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"google.golang.org/protobuf/proto"
)

var _ base.TransactionalResourceProvider = (*Provider)(nil)

// PreparePlan validates and durably identifies a provider transaction.
func (p *Provider) PreparePlan(ctx context.Context, transactionID string, generation uint64, plan *tgsrlv1.PlacementPlan) (*base.TransactionReceipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(transactionID) == "" || generation == 0 || plan == nil || plan.GetPlanId() != transactionID {
		return nil, fmt.Errorf("%w: valid transaction_id, generation, and matching plan are required", base.ErrInvalidArgument)
	}
	if err := validatePlanAt(plan); err != nil {
		return nil, err
	}
	if err := base.ValidateExecutionCapabilities(ctx, p, plan); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.txPlans[transactionID]; existing != nil {
		if !proto.Equal(existing, plan) {
			return nil, fmt.Errorf("%w: transaction_id %q was reused with different content", base.ErrIdempotencyConflict, transactionID)
		}
		receipt := p.txReceipts[transactionID]
		if receipt == nil || receipt.Generation != generation {
			return nil, fmt.Errorf("%w: transaction %q generation mismatch", base.ErrGenerationFenced, transactionID)
		}
		return cloneTransactionReceipt(receipt), nil
	}
	actions := orderedPlanActions(plan)
	receipt := &base.TransactionReceipt{TransactionID: transactionID, PlanID: plan.GetPlanId(), Generation: generation, Phase: base.TransactionPhasePrepared, PreparedAt: p.now(), UpdatedAt: p.now(), Effects: make([]base.TransactionEffect, len(actions))}
	for index, action := range actions {
		receipt.Effects[index] = base.TransactionEffect{StepIndex: index, ActionID: action.GetActionId(), IdempotencyKey: action.GetIdempotencyKey(), Status: base.EffectStatusNotApplied, Generation: generation}
	}
	p.txPlans[transactionID] = clonePlacementPlan(plan)
	p.txReceipts[transactionID] = cloneTransactionReceipt(receipt)
	p.plans[plan.GetPlanId()] = &base.PlanRecord{Plan: clonePlacementPlan(plan), Status: base.PlanStatusInFlight, ObservedRevision: p.revision, UpdatedAt: p.now()}
	return cloneTransactionReceipt(receipt), nil
}

// ExecuteStep applies one ordered provider transaction step.
func (p *Provider) ExecuteStep(ctx context.Context, transactionID string, generation uint64, stepIndex int) (*base.TransactionReceipt, error) {
	p.mu.Lock()
	plan, receipt, err := p.transactionStateLocked(transactionID, generation)
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	actions := orderedPlanActions(plan)
	if stepIndex < 0 || stepIndex >= len(actions) {
		return nil, fmt.Errorf("%w: step_index %d out of range", base.ErrInvalidArgument, stepIndex)
	}
	for index := 0; index < stepIndex; index++ {
		if receipt.Effects[index].Status != base.EffectStatusApplied {
			return nil, fmt.Errorf("%w: step %d is not applied", base.ErrFailedPrecondition, index)
		}
	}
	if receipt.Effects[stepIndex].Status == base.EffectStatusApplied || receipt.Effects[stepIndex].Status == base.EffectStatusCompensated {
		return receipt, nil
	}
	result, _, applied, executeErr := p.executeAction(ctx, actions[stepIndex], actionExecutionPlan)
	p.mu.Lock()
	defer p.mu.Unlock()
	receipt = p.txReceipts[transactionID]
	effect := receipt.Effects[stepIndex]
	effect.Status = base.EffectStatusFailed
	if applied {
		effect.Status = base.EffectStatusApplied
	}
	if result != nil {
		effect.Revision = result.GetObservedRevision()
		receipt.ObservedRevision = result.GetObservedRevision()
	}
	if executeErr != nil {
		effect.ErrorCode, effect.ErrorMessage = errorCode(executeErr), executeErr.Error()
		receipt.Phase, receipt.ErrorCode, receipt.ErrorMessage = base.TransactionPhaseDegraded, effect.ErrorCode, effect.ErrorMessage
	} else {
		receipt.Phase = base.TransactionPhaseExecuting
	}
	receipt.Effects[stepIndex], receipt.UpdatedAt = effect, p.now()
	p.txReceipts[transactionID] = cloneTransactionReceipt(receipt)
	return cloneTransactionReceipt(receipt), executeErr
}

// CommitPlan marks an all-applied provider transaction committed.
func (p *Provider) CommitPlan(ctx context.Context, transactionID string, generation uint64) (*base.TransactionReceipt, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	plan, receipt, err := p.transactionStateLocked(transactionID, generation)
	if err != nil {
		return nil, err
	}
	for index, effect := range receipt.Effects {
		if effect.Status != base.EffectStatusApplied {
			return nil, fmt.Errorf("%w: step %d is not applied", base.ErrFailedPrecondition, index)
		}
	}
	receipt.Phase, receipt.UpdatedAt = base.TransactionPhaseCommitted, p.now()
	p.txReceipts[transactionID] = cloneTransactionReceipt(receipt)
	p.plans[plan.GetPlanId()] = &base.PlanRecord{Plan: clonePlacementPlan(plan), Status: base.PlanStatusSucceeded, Results: transactionResults(receipt), ObservedRevision: p.revision, UpdatedAt: p.now()}
	actions := orderedPlanActions(plan)
	for index, action := range actions {
		if len(actions) == 1 && receipt.Effects[index].Status == base.EffectStatusApplied {
			p.publishConfirmedMutableSandboxLocked(action, "authoritative NVIDIA transaction readback")
		}
	}
	return cloneTransactionReceipt(receipt), nil
}

// AbortPlan compensates applied steps using the existing provider rollback path.
func (p *Provider) AbortPlan(ctx context.Context, transactionID string, generation uint64) (*base.TransactionReceipt, error) {
	p.mu.Lock()
	plan, receipt, err := p.transactionStateLocked(transactionID, generation)
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	actions := orderedPlanActions(plan)
	var abortErr error
	for index := len(actions) - 1; index >= 0; index-- {
		if receipt.Effects[index].Status != base.EffectStatusApplied {
			continue
		}
		p.mu.Lock()
		entry := p.actions[actions[index].GetIdempotencyKey()]
		if entry == nil {
			p.mu.Unlock()
			receipt.Effects[index].CompensationAttempted = true
			receipt.Effects[index].Status = base.EffectStatusDegraded
			abortErr = base.ErrFailedPrecondition
			continue
		}
		_, rollbackErr := p.rollbackActionLocked(ctx, actions[index], entry.before, 0, true)
		p.mu.Unlock()
		if rollbackErr != nil {
			receipt.Effects[index].CompensationAttempted = true
			receipt.Effects[index].Status, receipt.Effects[index].ErrorCode, receipt.Effects[index].ErrorMessage = base.EffectStatusDegraded, errorCode(rollbackErr), rollbackErr.Error()
			abortErr = rollbackErr
			continue
		}
		receipt.Effects[index].Status = base.EffectStatusCompensated
		receipt.Effects[index].CompensationAttempted = true
		receipt.Effects[index].Compensated = true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	receipt.Phase, receipt.UpdatedAt, receipt.ObservedRevision = base.TransactionPhaseAborted, p.now(), p.revision
	if abortErr != nil {
		receipt.Phase, receipt.ErrorCode, receipt.ErrorMessage = base.TransactionPhaseDegraded, errorCode(abortErr), abortErr.Error()
	}
	p.txReceipts[transactionID] = cloneTransactionReceipt(receipt)
	p.plans[plan.GetPlanId()] = &base.PlanRecord{
		Plan:             clonePlacementPlan(plan),
		Status:           base.PlanStatusFailed,
		Results:          transactionResults(receipt),
		ErrorCode:        receipt.ErrorCode,
		ErrorMessage:     receipt.ErrorMessage,
		ObservedRevision: p.revision,
		UpdatedAt:        receipt.UpdatedAt,
	}
	if abortErr != nil {
		p.planErrors[plan.GetPlanId()] = abortErr
	} else {
		delete(p.planErrors, plan.GetPlanId())
	}
	return cloneTransactionReceipt(receipt), abortErr
}

func (p *Provider) publishConfirmedMutableSandboxLocked(action *tgsrlv1.Action, detail string) {
	if action == nil {
		return
	}
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionType_ACTION_TYPE_RESIZE:
	default:
		return
	}
	if sandbox, ok := p.sandboxes[actionSandboxID(action)]; ok {
		p.publishSandboxShareReadbackLocked(action, sandbox.Share, detail)
	}
}

// ReconcilePlanTransaction resolves local or helper-recovered transaction receipts.
func (p *Provider) ReconcilePlanTransaction(ctx context.Context, transactionID string, generation uint64) (*base.TransactionReceipt, error) {
	p.mu.Lock()
	_, receipt, err := p.transactionStateLocked(transactionID, generation)
	p.mu.Unlock()
	if err == nil {
		return receipt, nil
	}
	v2, ok := p.driver.(V2Driver)
	if !ok {
		return nil, err
	}
	var recoveredEffects []base.TransactionEffect
	failed := false
	expectedActions := 0
	committed := false
	planDigest := ""
	for _, recovered := range v2.RecoveredActions() {
		if recovered.PlanID != transactionID || recovered.Generation != generation {
			continue
		}
		if expectedActions == 0 {
			expectedActions, committed, planDigest = recovered.ExpectedActions, recovered.Committed, recovered.PlanDigest
		}
		if recovered.ExpectedActions != expectedActions || recovered.Committed != committed || recovered.PlanDigest == "" || recovered.PlanDigest != planDigest {
			return nil, fmt.Errorf("%w: recovered transaction %q receipt set is inconsistent", base.ErrFailedPrecondition, transactionID)
		}
		status := base.EffectStatusFailed
		if recovered.Succeeded {
			status = base.EffectStatusApplied
		} else {
			failed = true
		}
		recoveredEffects = append(recoveredEffects, base.TransactionEffect{StepIndex: recovered.StepIndex, ActionID: recovered.ActionID, IdempotencyKey: recovered.IdempotencyKey, Status: status, Generation: generation, ErrorCode: recovered.ErrorCode, ErrorMessage: recovered.ErrorMessage})
	}
	if len(recoveredEffects) == 0 {
		return nil, err
	}
	if expectedActions != len(recoveredEffects) {
		return nil, fmt.Errorf("%w: recovered transaction %q has %d of %d action receipts", base.ErrFailedPrecondition, transactionID, len(recoveredEffects), expectedActions)
	}
	sort.Slice(recoveredEffects, func(i, j int) bool {
		return recoveredEffects[i].StepIndex < recoveredEffects[j].StepIndex
	})
	for index, effect := range recoveredEffects {
		if effect.StepIndex != index {
			return nil, fmt.Errorf("%w: recovered transaction %q is missing step %d", base.ErrFailedPrecondition, transactionID, index)
		}
	}
	phase := base.TransactionPhaseReconciled
	if failed {
		phase = base.TransactionPhaseDegraded
	} else if committed {
		phase = base.TransactionPhaseCommitted
	}
	return &base.TransactionReceipt{TransactionID: transactionID, PlanID: transactionID, Generation: generation, Phase: phase, UpdatedAt: p.now(), Effects: recoveredEffects}, nil
}

// DescribeCapabilities reports NVIDIA provider transaction guarantees.
func (*Provider) DescribeCapabilities(ctx context.Context) (base.TransactionCapabilities, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return base.TransactionCapabilities{}, err
	}
	return base.TransactionCapabilities{OrderedStepExecution: true, CompensatingAbort: true, StepIdempotency: true, GenerationFence: true, PartialEffectReporting: true, AtomicReplacement: true}, nil
}

func (p *Provider) transactionStateLocked(transactionID string, generation uint64) (*tgsrlv1.PlacementPlan, *base.TransactionReceipt, error) {
	plan, receipt := p.txPlans[transactionID], p.txReceipts[transactionID]
	if plan == nil || receipt == nil {
		return nil, nil, fmt.Errorf("%w: transaction %q", base.ErrNotFound, transactionID)
	}
	if receipt.Generation != generation {
		return nil, nil, fmt.Errorf("%w: transaction %q generation mismatch", base.ErrGenerationFenced, transactionID)
	}
	return clonePlacementPlan(plan), cloneTransactionReceipt(receipt), nil
}

func orderedPlanActions(plan *tgsrlv1.PlacementPlan) []*tgsrlv1.Action {
	actions := append([]*tgsrlv1.Action(nil), plan.GetActions()...)
	sort.SliceStable(actions, func(i, j int) bool { return actions[i].GetOrder() < actions[j].GetOrder() })
	return actions
}

func cloneTransactionReceipt(receipt *base.TransactionReceipt) *base.TransactionReceipt {
	if receipt == nil {
		return nil
	}
	result := *receipt
	result.Effects = append([]base.TransactionEffect(nil), receipt.Effects...)
	return &result
}

func transactionResults(receipt *base.TransactionReceipt) []*tgsrlv1.ActionResult {
	results := make([]*tgsrlv1.ActionResult, 0, len(receipt.Effects))
	for _, effect := range receipt.Effects {
		status := tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_UNKNOWN
		switch effect.Status {
		case base.EffectStatusNotApplied:
			status = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SKIPPED
		case base.EffectStatusApplied, base.EffectStatusCompensated:
			status = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED
		case base.EffectStatusFailed, base.EffectStatusDegraded:
			status = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED
		}
		results = append(results, &tgsrlv1.ActionResult{ActionId: effect.ActionID, PlanId: receipt.PlanID, IdempotencyKey: effect.IdempotencyKey, Status: status, ObservedRevision: effect.Revision, ErrorCode: effect.ErrorCode, ErrorMessage: effect.ErrorMessage})
		if effect.CompensationAttempted {
			results[len(results)-1].RollbackAttempted = true
			results[len(results)-1].RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED
		}
		if effect.Compensated {
			results[len(results)-1].RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK
		}
	}
	return results
}
