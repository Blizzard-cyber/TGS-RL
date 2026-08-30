package provider

import (
	"context"
	"fmt"
	"sort"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

var _ TransactionalResourceProvider = (*MockResourceProvider)(nil)

func (p *MockResourceProvider) PreparePlan(ctx context.Context, transactionID string, generation uint64, plan *tgsrlv1.PlacementPlan) (*TransactionReceipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(transactionID) == "" {
		return nil, fmt.Errorf("%w: transaction_id is required", ErrInvalidArgument)
	}
	if generation == 0 {
		return nil, fmt.Errorf("%w: generation is required", ErrInvalidArgument)
	}
	if plan == nil || strings.TrimSpace(plan.GetPlanId()) == "" {
		return nil, fmt.Errorf("%w: plan_id is required", ErrInvalidArgument)
	}
	if plan.GetPlanId() != transactionID {
		return nil, fmt.Errorf("%w: transaction_id %q does not match plan_id %q", ErrInvalidArgument, transactionID, plan.GetPlanId())
	}
	if err := validatePlanPolicy(plan); err != nil {
		return nil, err
	}
	if err := ValidateExecutionCapabilities(ctx, p, plan); err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if existingPlan := p.txPlans[transactionID]; existingPlan != nil {
		if !proto.Equal(existingPlan, plan) {
			return nil, fmt.Errorf("%w: transaction_id %q was reused with different content", ErrIdempotencyConflict, transactionID)
		}
		existingReceipt := p.txReceipts[transactionID]
		if existingReceipt == nil {
			return nil, fmt.Errorf("%w: transaction %q receipt missing", ErrNotFound, transactionID)
		}
		if existingReceipt.Generation != generation {
			return nil, fmt.Errorf("%w: transaction %q generation mismatch", ErrGenerationFenced, transactionID)
		}
		return cloneTransactionReceipt(existingReceipt), nil
	}

	ordered, err := prepareOrderedActions(plan)
	if err != nil {
		return nil, err
	}
	receipt := &TransactionReceipt{
		TransactionID: transactionID,
		PlanID:        plan.GetPlanId(),
		Generation:    generation,
		Phase:         TransactionPhasePrepared,
		PreparedAt:    p.now(),
		UpdatedAt:     p.now(),
		Effects:       make([]TransactionEffect, len(ordered)),
	}
	before := make([]actionBeforeImage, len(ordered))
	applied := make([]bool, len(ordered))
	for index, entry := range ordered {
		if err := p.validateRollbackDeclarationLocked(entry.action); err != nil {
			return nil, err
		}
		before[index] = p.captureActionBeforeImageLocked(entry.action)
		receipt.Effects[index] = TransactionEffect{
			StepIndex:      index,
			ActionID:       entry.action.GetActionId(),
			IdempotencyKey: entry.action.GetIdempotencyKey(),
			Status:         EffectStatusNotApplied,
			Generation:     generation,
		}
	}

	p.txPlans[transactionID] = clonePlacementPlan(plan)
	p.txReceipts[transactionID] = cloneTransactionReceipt(receipt)
	p.txBefore[transactionID] = before
	p.txApplied[transactionID] = applied
	p.setPlanRecordLocked(plan, PlanStatusInFlight, nil, nil)
	return cloneTransactionReceipt(receipt), nil
}

func (p *MockResourceProvider) ExecuteStep(ctx context.Context, transactionID string, generation uint64, stepIndex int) (*TransactionReceipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	plan, receipt, applied, before, ordered, err := p.transactionStateLocked(transactionID, generation)
	if err != nil {
		return nil, err
	}
	if stepIndex < 0 || stepIndex >= len(ordered) {
		return nil, fmt.Errorf("%w: step_index %d out of range", ErrInvalidArgument, stepIndex)
	}
	for index := 0; index < stepIndex; index++ {
		if !applied[index] {
			return nil, fmt.Errorf("%w: step %d is not applied yet", ErrFailedPrecondition, index)
		}
	}
	if stepIndex > 0 && applied[stepIndex] {
		return cloneTransactionReceipt(receipt), nil
	}
	entry := ordered[stepIndex]
	effect := receipt.Effects[stepIndex]
	if effect.Status == EffectStatusApplied || effect.Status == EffectStatusCompensated {
		return cloneTransactionReceipt(receipt), nil
	}
	if effect.Status == EffectStatusFailed || effect.Status == EffectStatusDegraded {
		return cloneTransactionReceipt(receipt), nil
	}
	if p.faults.PartialFailureAt > 0 && stepIndex+1 == p.faults.PartialFailureAt {
		now := p.now()
		effect.Status = EffectStatusFailed
		effect.ErrorCode = ErrorCodeInjectedFailure
		effect.ErrorMessage = "configured partial failure"
		effect.Revision = p.revision
		receipt.Effects[stepIndex] = effect
		receipt.Phase = TransactionPhaseDegraded
		receipt.ErrorCode = effect.ErrorCode
		receipt.ErrorMessage = effect.ErrorMessage
		receipt.UpdatedAt = now
		p.txReceipts[transactionID] = cloneTransactionReceipt(receipt)
		p.setPlanRecordLocked(plan, PlanStatusFailed, transactionReceiptResults(receipt), ErrPartialFailure)
		return cloneTransactionReceipt(receipt), ErrPartialFailure
	}
	if err := p.validateRollbackBeforeImageLocked(entry.action, before[stepIndex]); err != nil {
		effect.Status = EffectStatusFailed
		effect.ErrorCode = transactionErrorCode(err)
		effect.ErrorMessage = err.Error()
		effect.Revision = p.revision
		receipt.Effects[stepIndex] = effect
		receipt.Phase = TransactionPhaseDegraded
		receipt.ErrorCode = effect.ErrorCode
		receipt.ErrorMessage = effect.ErrorMessage
		receipt.UpdatedAt = p.now()
		p.txReceipts[transactionID] = cloneTransactionReceipt(receipt)
		p.setPlanRecordLocked(plan, PlanStatusFailed, transactionReceiptResults(receipt), err)
		return cloneTransactionReceipt(receipt), err
	}

	receipt.Phase = TransactionPhaseExecuting
	result, execErr := p.executeActionLocked(ctx, entry.action)
	receipt.UpdatedAt = p.now()
	if result != nil {
		receipt.ObservedRevision = result.GetObservedRevision()
		effect.Revision = result.GetObservedRevision()
	}
	if execErr != nil {
		effect.Status = EffectStatusFailed
		effect.ErrorCode = transactionErrorCode(execErr)
		effect.ErrorMessage = execErr.Error()
		receipt.Phase = TransactionPhaseDegraded
		receipt.ErrorCode = effect.ErrorCode
		receipt.ErrorMessage = effect.ErrorMessage
		receipt.Effects[stepIndex] = effect
		p.txReceipts[transactionID] = cloneTransactionReceipt(receipt)
		p.setPlanRecordLocked(plan, PlanStatusFailed, transactionReceiptResults(receipt), execErr)
		return cloneTransactionReceipt(receipt), execErr
	}

	applied[stepIndex] = true
	effect.Status = EffectStatusApplied
	effect.ErrorCode = ""
	effect.ErrorMessage = ""
	receipt.Effects[stepIndex] = effect
	p.txApplied[transactionID] = append([]bool(nil), applied...)
	p.txReceipts[transactionID] = cloneTransactionReceipt(receipt)
	p.setPlanRecordLocked(plan, PlanStatusInFlight, transactionReceiptResults(receipt), nil)
	return cloneTransactionReceipt(receipt), nil
}

func (p *MockResourceProvider) CommitPlan(ctx context.Context, transactionID string, generation uint64) (*TransactionReceipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	plan, receipt, applied, _, _, err := p.transactionStateLocked(transactionID, generation)
	if err != nil {
		return nil, err
	}
	for index := range receipt.Effects {
		if !applied[index] {
			return nil, fmt.Errorf("%w: step %d is not applied yet", ErrFailedPrecondition, index)
		}
	}
	receipt.Phase = TransactionPhaseCommitted
	receipt.ErrorCode = ""
	receipt.ErrorMessage = ""
	receipt.UpdatedAt = p.now()
	p.txReceipts[transactionID] = cloneTransactionReceipt(receipt)
	p.setPlanRecordLocked(plan, PlanStatusSucceeded, transactionReceiptResults(receipt), nil)
	return cloneTransactionReceipt(receipt), nil
}

func (p *MockResourceProvider) AbortPlan(ctx context.Context, transactionID string, generation uint64) (*TransactionReceipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	plan, receipt, applied, before, ordered, err := p.transactionStateLocked(transactionID, generation)
	if err != nil {
		return nil, err
	}
	receipt.Phase = TransactionPhaseAborted
	receipt.ErrorCode = ""
	receipt.ErrorMessage = ""
	protected := map[string]sandboxMutation{}
	var abortErr error
	for index := len(ordered) - 1; index >= 0; index-- {
		if !applied[index] {
			continue
		}
		action := ordered[index].action
		effect := receipt.Effects[index]
		if effect.Status == EffectStatusCompensated {
			continue
		}
		if failure, fail := p.faults.FailRollbackActionIDs[action.GetActionId()]; fail {
			effect.CompensationAttempted = true
			effect.Status = EffectStatusDegraded
			effect.Compensated = false
			effect.ErrorCode = failure.Code
			if effect.ErrorCode == "" {
				effect.ErrorCode = ErrorCodeInjectedFailure
			}
			effect.ErrorMessage = failure.Message
			if effect.ErrorMessage == "" {
				effect.ErrorMessage = "configured rollback failure"
			}
			receipt.Effects[index] = effect
			protected[before[index].sandboxID] |= before[index].mutations
			if abortErr == nil {
				abortErr = ErrPartialFailure
			}
			continue
		}
		if err := p.restoreActionBeforeImageLocked(action, before[index], protected[before[index].sandboxID]); err != nil {
			effect.CompensationAttempted = true
			effect.Status = EffectStatusDegraded
			effect.Compensated = false
			effect.ErrorCode = transactionErrorCode(err)
			effect.ErrorMessage = err.Error()
			receipt.Effects[index] = effect
			protected[before[index].sandboxID] |= before[index].mutations
			if abortErr == nil {
				abortErr = err
			}
			continue
		}
		p.revision++
		effect.Status = EffectStatusCompensated
		effect.CompensationAttempted = true
		effect.Compensated = true
		effect.Revision = p.revision
		effect.ErrorCode = ""
		effect.ErrorMessage = ""
		receipt.Effects[index] = effect
		applied[index] = false
		if sandbox, ok := p.sandboxes[before[index].sandboxID]; ok {
			p.publishSandboxSnapshotLocked(sandbox, "transaction compensation applied")
		}
		p.publishSnapshotLocked(tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED)
	}
	receipt.UpdatedAt = p.now()
	receipt.ObservedRevision = p.revision
	if abortErr != nil {
		receipt.Phase = TransactionPhaseDegraded
		receipt.ErrorCode = transactionErrorCode(abortErr)
		receipt.ErrorMessage = abortErr.Error()
		p.setPlanRecordLocked(plan, PlanStatusFailed, transactionReceiptResults(receipt), abortErr)
	} else {
		p.setPlanRecordLocked(plan, PlanStatusFailed, transactionReceiptResults(receipt), nil)
	}
	p.txApplied[transactionID] = append([]bool(nil), applied...)
	p.txReceipts[transactionID] = cloneTransactionReceipt(receipt)
	return cloneTransactionReceipt(receipt), abortErr
}

func (p *MockResourceProvider) ReconcilePlanTransaction(ctx context.Context, transactionID string, generation uint64) (*TransactionReceipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	plan, receipt, applied, _, _, err := p.transactionStateLocked(transactionID, generation)
	if err != nil {
		return nil, err
	}
	receipt.Phase = classifyTransactionPhase(receipt, applied)
	receipt.UpdatedAt = p.now()
	p.txReceipts[transactionID] = cloneTransactionReceipt(receipt)
	p.setPlanRecordLocked(plan, statusForTransactionPhase(receipt.Phase), transactionReceiptResults(receipt), nil)
	return cloneTransactionReceipt(receipt), nil
}

func (p *MockResourceProvider) DescribeCapabilities(ctx context.Context) (TransactionCapabilities, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return TransactionCapabilities{}, err
	}
	return TransactionCapabilities{
		OrderedStepExecution:   true,
		CompensatingAbort:      true,
		StepIdempotency:        true,
		GenerationFence:        true,
		PartialEffectReporting: true,
		AtomicReplacement:      true,
	}, nil
}

func prepareOrderedActions(plan *tgsrlv1.PlacementPlan) ([]orderedAction, error) {
	ordered := make([]orderedAction, 0, len(plan.GetActions()))
	for index, action := range plan.GetActions() {
		if action == nil {
			return nil, fmt.Errorf("%w: nil action at index %d", ErrInvalidArgument, index)
		}
		if action.GetPlanId() != plan.GetPlanId() {
			return nil, fmt.Errorf("%w: action %q plan_id mismatch", ErrInvalidArgument, action.GetActionId())
		}
		if action.GetExpectedSnapshotRevision() != plan.GetSnapshotRevision() {
			return nil, fmt.Errorf("%w: action %q revision mismatch", ErrInvalidArgument, action.GetActionId())
		}
		if action.GetRollback() == nil {
			return nil, fmt.Errorf("%w: action %q requires explicit rollback", ErrInvalidArgument, action.GetActionId())
		}
		ordered = append(ordered, orderedAction{index: index, action: cloneAction(action)})
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].action.GetOrder() != ordered[j].action.GetOrder() {
			return ordered[i].action.GetOrder() < ordered[j].action.GetOrder()
		}
		return ordered[i].index < ordered[j].index
	})
	return ordered, nil
}

func (p *MockResourceProvider) transactionStateLocked(transactionID string, generation uint64) (*tgsrlv1.PlacementPlan, *TransactionReceipt, []bool, []actionBeforeImage, []orderedAction, error) {
	if strings.TrimSpace(transactionID) == "" {
		return nil, nil, nil, nil, nil, fmt.Errorf("%w: transaction_id is required", ErrInvalidArgument)
	}
	if generation == 0 {
		return nil, nil, nil, nil, nil, fmt.Errorf("%w: generation is required", ErrInvalidArgument)
	}
	plan := p.txPlans[transactionID]
	if plan == nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("%w: transaction %q", ErrNotFound, transactionID)
	}
	receipt := p.txReceipts[transactionID]
	if receipt == nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("%w: transaction %q receipt missing", ErrNotFound, transactionID)
	}
	if receipt.Generation != generation {
		return nil, nil, nil, nil, nil, fmt.Errorf("%w: transaction %q generation mismatch", ErrGenerationFenced, transactionID)
	}
	applied := append([]bool(nil), p.txApplied[transactionID]...)
	before := append([]actionBeforeImage(nil), p.txBefore[transactionID]...)
	ordered, err := prepareOrderedActions(plan)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	if len(applied) != len(ordered) || len(before) != len(ordered) || len(receipt.Effects) != len(ordered) {
		return nil, nil, nil, nil, nil, fmt.Errorf("%w: transaction %q state is inconsistent", ErrFailedPrecondition, transactionID)
	}
	return clonePlacementPlan(plan), cloneTransactionReceipt(receipt), applied, before, ordered, nil
}

func transactionReceiptResults(receipt *TransactionReceipt) []*tgsrlv1.ActionResult {
	if receipt == nil {
		return nil
	}
	results := make([]*tgsrlv1.ActionResult, 0, len(receipt.Effects))
	for _, effect := range receipt.Effects {
		status := tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SKIPPED
		switch effect.Status {
		case EffectStatusApplied:
			status = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED
		case EffectStatusFailed, EffectStatusDegraded:
			status = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED
		case EffectStatusCompensated:
			status = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED
		}
		result := &tgsrlv1.ActionResult{
			ActionId:         effect.ActionID,
			Status:           status,
			ObservedRevision: effect.Revision,
			PlanId:           receipt.PlanID,
			IdempotencyKey:   effect.IdempotencyKey,
			ErrorCode:        effect.ErrorCode,
			ErrorMessage:     effect.ErrorMessage,
		}
		if effect.CompensationAttempted {
			result.RollbackAttempted = true
			result.RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED
		}
		if effect.Status == EffectStatusCompensated {
			result.RollbackAttempted = true
			result.RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK
		}
		results = append(results, result)
	}
	return results
}

func classifyTransactionPhase(receipt *TransactionReceipt, applied []bool) TransactionPhase {
	if receipt == nil {
		return TransactionPhaseUnknown
	}
	switch receipt.Phase {
	case TransactionPhaseCommitted, TransactionPhaseAborted, TransactionPhaseDegraded:
		return receipt.Phase
	}
	allApplied := len(applied) > 0
	anyApplied := false
	allCompensated := len(receipt.Effects) > 0
	for index, effect := range receipt.Effects {
		if effect.Status == EffectStatusFailed || effect.Status == EffectStatusDegraded {
			return TransactionPhaseDegraded
		}
		if applied[index] {
			anyApplied = true
		} else {
			allApplied = false
		}
		if effect.Status != EffectStatusCompensated {
			allCompensated = false
		}
	}
	switch {
	case allCompensated && len(receipt.Effects) > 0:
		return TransactionPhaseAborted
	case allApplied && len(receipt.Effects) > 0:
		return TransactionPhaseExecuting
	case anyApplied:
		return TransactionPhaseExecuting
	default:
		return TransactionPhasePrepared
	}
}

func statusForTransactionPhase(phase TransactionPhase) PlanStatus {
	switch phase {
	case TransactionPhaseCommitted:
		return PlanStatusSucceeded
	case TransactionPhaseAborted:
		return PlanStatusFailed
	case TransactionPhaseDegraded:
		return PlanStatusFailed
	default:
		return PlanStatusInFlight
	}
}
