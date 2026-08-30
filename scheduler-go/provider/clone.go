package provider

import (
	"errors"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

func cloneCapabilities(capabilities *tgsrlv1.CapabilitySet) *tgsrlv1.CapabilitySet {
	if capabilities == nil {
		return nil
	}
	return proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)
}

func cloneDevice(device *tgsrlv1.Device) *tgsrlv1.Device {
	if device == nil {
		return nil
	}
	return proto.Clone(device).(*tgsrlv1.Device)
}

func cloneDevices(devices []*tgsrlv1.Device) []*tgsrlv1.Device {
	result := make([]*tgsrlv1.Device, len(devices))
	for index, device := range devices {
		result[index] = cloneDevice(device)
	}
	return result
}

func cloneBinding(binding *tgsrlv1.Binding) *tgsrlv1.Binding {
	if binding == nil {
		return nil
	}
	return proto.Clone(binding).(*tgsrlv1.Binding)
}

func cloneSemanticEnvelope(envelope *tgsrlv1.SemanticEnvelope) *tgsrlv1.SemanticEnvelope {
	if envelope == nil {
		return nil
	}
	return proto.Clone(envelope).(*tgsrlv1.SemanticEnvelope)
}

func cloneSandbox(sandbox Sandbox) Sandbox {
	sandbox.Binding = cloneBinding(sandbox.Binding)
	sandbox.SemanticContext = cloneSemanticEnvelope(sandbox.SemanticContext)
	return sandbox
}

func cloneSandboxes(sandboxes []Sandbox) []Sandbox {
	result := make([]Sandbox, len(sandboxes))
	for index, sandbox := range sandboxes {
		result[index] = cloneSandbox(sandbox)
	}
	return result
}

func cloneAction(action *tgsrlv1.Action) *tgsrlv1.Action {
	if action == nil {
		return nil
	}
	return proto.Clone(action).(*tgsrlv1.Action)
}

func cloneActionResult(result *tgsrlv1.ActionResult) *tgsrlv1.ActionResult {
	if result == nil {
		return nil
	}
	return proto.Clone(result).(*tgsrlv1.ActionResult)
}

func cloneActionResults(results []*tgsrlv1.ActionResult) []*tgsrlv1.ActionResult {
	clones := make([]*tgsrlv1.ActionResult, len(results))
	for index, result := range results {
		clones[index] = cloneActionResult(result)
	}
	return clones
}

func cloneTransactionEffects(effects []TransactionEffect) []TransactionEffect {
	clones := make([]TransactionEffect, len(effects))
	copy(clones, effects)
	return clones
}

func cloneTransactionReceipt(receipt *TransactionReceipt) *TransactionReceipt {
	if receipt == nil {
		return nil
	}
	clone := *receipt
	clone.Effects = cloneTransactionEffects(receipt.Effects)
	return &clone
}

func transactionErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var providerError *Error
	if errors.As(err, &providerError) && providerError.Code != "" {
		return providerError.Code
	}
	if errors.Is(err, ErrTransactionalUnavailable) {
		return ErrorCodeUnsupported
	}
	if errors.Is(err, ErrGenerationFenced) {
		return ErrorCodeGenerationConflict
	}
	if errors.Is(err, ErrFailedPrecondition) {
		return ErrorCodeFailedPrecondition
	}
	if errors.Is(err, ErrPartialFailure) {
		return ErrorCodeInjectedFailure
	}
	return ErrorCodeInvalidArgument
}

func cloneFaults(faults FaultOptions) FaultOptions {
	result := FaultOptions{
		Delay:                 faults.Delay,
		PartialFailureAt:      faults.PartialFailureAt,
		DelayByActionType:     make(map[tgsrlv1.ActionType]time.Duration, len(faults.DelayByActionType)),
		FailActionIDs:         make(map[string]InjectedFailure, len(faults.FailActionIDs)),
		FailRollbackActionIDs: make(map[string]InjectedFailure, len(faults.FailRollbackActionIDs)),
	}
	for actionType, delay := range faults.DelayByActionType {
		result.DelayByActionType[actionType] = delay
	}
	for actionID, failure := range faults.FailActionIDs {
		result.FailActionIDs[actionID] = failure
	}
	for actionID, failure := range faults.FailRollbackActionIDs {
		result.FailRollbackActionIDs[actionID] = failure
	}
	return result
}
