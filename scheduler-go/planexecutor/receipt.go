package planexecutor

import (
	"fmt"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func CloneReceipt(receipt *provider.TransactionReceipt) *provider.TransactionReceipt {
	if receipt == nil {
		return nil
	}
	out := *receipt
	out.Effects = append([]provider.TransactionEffect(nil), receipt.Effects...)
	return &out
}

func ValidateReceipt(receipt *provider.TransactionReceipt, providerGeneration uint64) error {
	if receipt == nil {
		return fmt.Errorf("transaction receipt is required")
	}
	if receipt.TransactionID == "" || receipt.PlanID == "" {
		return fmt.Errorf("transaction receipt identity is required")
	}
	if receipt.Generation != providerGeneration {
		return fmt.Errorf("transaction receipt generation %d does not match provider generation %d", receipt.Generation, providerGeneration)
	}
	switch receipt.Phase {
	case provider.TransactionPhaseUnknown, provider.TransactionPhasePrepared, provider.TransactionPhaseExecuting,
		provider.TransactionPhaseCommitted, provider.TransactionPhaseAborted,
		provider.TransactionPhaseReconciled, provider.TransactionPhaseDegraded:
	default:
		return fmt.Errorf("transaction receipt phase %q is invalid", receipt.Phase)
	}
	for index, effect := range receipt.Effects {
		if effect.StepIndex < 0 || effect.ActionID == "" {
			return fmt.Errorf("transaction receipt effect %d is invalid", index)
		}
		switch effect.Status {
		case provider.EffectStatusUnknown, provider.EffectStatusNotApplied, provider.EffectStatusApplied,
			provider.EffectStatusFailed, provider.EffectStatusCompensated, provider.EffectStatusDegraded:
		default:
			return fmt.Errorf("transaction receipt effect %d status %q is invalid", index, effect.Status)
		}
	}
	return nil
}

func stateReceipt(receipt *provider.TransactionReceipt) *state.Receipt {
	if receipt == nil {
		return nil
	}
	out := &state.Receipt{
		Phase: string(receipt.Phase), ObservedRevision: receipt.ObservedRevision,
		ErrorCode: receipt.ErrorCode, ErrorMessage: receipt.ErrorMessage,
	}
	if !receipt.PreparedAt.IsZero() {
		out.PreparedAt = timestamppb.New(receipt.PreparedAt)
	}
	if !receipt.UpdatedAt.IsZero() {
		out.UpdatedAt = timestamppb.New(receipt.UpdatedAt)
	}
	for _, effect := range receipt.Effects {
		out.Steps = append(out.Steps, state.TransactionStep{
			StepIndex: effect.StepIndex, ActionID: effect.ActionID,
			IdempotencyKey: effect.IdempotencyKey, Status: stateEffectStatus(effect.Status),
			Revision: effect.Revision, ErrorCode: effect.ErrorCode,
			ErrorMessage: effect.ErrorMessage, CompensationAttempted: effect.CompensationAttempted, Compensated: effect.Compensated,
		})
	}
	return out
}

func mergeStateReceipt(previous *state.Receipt, latest *provider.TransactionReceipt) *state.Receipt {
	if previous == nil {
		return stateReceipt(latest)
	}
	out := &state.Receipt{
		Phase: previous.Phase, ObservedRevision: previous.ObservedRevision,
		ErrorCode: previous.ErrorCode, ErrorMessage: previous.ErrorMessage,
		PreparedAt: previous.PreparedAt, UpdatedAt: previous.UpdatedAt,
		Steps: append([]state.TransactionStep(nil), previous.Steps...),
	}
	if latest == nil {
		return out
	}
	incoming := stateReceipt(latest)
	out.Phase = incoming.Phase
	out.ObservedRevision = incoming.ObservedRevision
	out.ErrorCode = incoming.ErrorCode
	out.ErrorMessage = incoming.ErrorMessage
	if incoming.PreparedAt != nil {
		out.PreparedAt = incoming.PreparedAt
	}
	if incoming.UpdatedAt != nil {
		out.UpdatedAt = incoming.UpdatedAt
	}
	steps := make(map[int]state.TransactionStep, len(out.Steps)+len(incoming.Steps))
	for _, step := range out.Steps {
		steps[step.StepIndex] = step
	}
	for _, step := range incoming.Steps {
		steps[step.StepIndex] = step
	}
	out.Steps = out.Steps[:0]
	for _, step := range steps {
		out.Steps = append(out.Steps, step)
	}
	sort.Slice(out.Steps, func(i, j int) bool { return out.Steps[i].StepIndex < out.Steps[j].StepIndex })
	return out
}

func stateEffectStatus(status provider.EffectStatus) state.EffectStatus {
	switch status {
	case provider.EffectStatusNotApplied:
		return state.EffectStatusPending
	case provider.EffectStatusApplied:
		return state.EffectStatusSucceeded
	case provider.EffectStatusFailed:
		return state.EffectStatusFailed
	case provider.EffectStatusCompensated:
		return state.EffectStatusRolledBack
	case provider.EffectStatusDegraded:
		return state.EffectStatusDegraded
	default:
		return state.EffectStatusUnknown
	}
}

func transactionResults(plan *tgsrlv1.PlacementPlan, transactionState state.TransactionState, receipt *state.Receipt) []*tgsrlv1.ActionResult {
	if plan == nil {
		return nil
	}
	steps := map[int]state.TransactionStep{}
	if receipt != nil {
		for _, step := range receipt.Steps {
			steps[step.StepIndex] = step
		}
	}
	results := make([]*tgsrlv1.ActionResult, 0, len(plan.GetActions()))
	for index, action := range plan.GetActions() {
		if action == nil {
			continue
		}
		step := steps[index]
		result := &tgsrlv1.ActionResult{
			ActionId: action.GetActionId(), PlanId: plan.GetPlanId(),
			IdempotencyKey: action.GetIdempotencyKey(), ObservedRevision: step.Revision,
			ErrorCode: step.ErrorCode, ErrorMessage: step.ErrorMessage,
		}
		if step.CompensationAttempted {
			result.RollbackAttempted = true
			result.RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED
		}
		if transactionState == state.TransactionStatePrepareFailed {
			result.Status = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SKIPPED
			results = append(results, result)
			continue
		}
		switch step.Status {
		case state.EffectStatusSucceeded:
			result.Status = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED
		case state.EffectStatusRolledBack:
			result.Status = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED
			result.RollbackAttempted = true
			result.RollbackStatus = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK
		case state.EffectStatusFailed, state.EffectStatusDegraded:
			result.Status = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_FAILED
		default:
			result.Status = tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_UNKNOWN
		}
		results = append(results, result)
	}
	return results
}
