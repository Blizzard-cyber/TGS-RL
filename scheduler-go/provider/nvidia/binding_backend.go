package nvidia

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	base "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
)

// CommandBindingBackend delegates binding lifecycle to an argv-only helper.
// Discovery is optional: an unavailable helper means there are no recovered
// bindings, while mutations fail closed.
type CommandBindingBackend struct {
	executor        CommandExecutor
	timeout         time.Duration
	binary          string
	statePath       string
	mpsPIDDirectory string
}

// NewCommandBindingBackend constructs the production binding backend.
func NewCommandBindingBackend(executor CommandExecutor, timeout time.Duration) *CommandBindingBackend {
	return NewCommandBindingBackendWithConfig(executor, timeout, "", "", "")
}

func NewCommandBindingBackendWithConfig(executor CommandExecutor, timeout time.Duration, binary, statePath, mpsPIDDirectory string) *CommandBindingBackend {
	if executor == nil {
		executor = NewExecCommandExecutor()
	}
	if timeout <= 0 {
		timeout = defaultCommandTimeout
	}
	if strings.TrimSpace(binary) == "" {
		binary = "tgsrl-nvidia-binding"
	}
	return &CommandBindingBackend{executor: executor, timeout: timeout, binary: binary, statePath: strings.TrimSpace(statePath), mpsPIDDirectory: strings.TrimSpace(mpsPIDDirectory)}
}

// Discover recovers durable bindings after restart.
func (b *CommandBindingBackend) Discover(ctx context.Context, _ *InventorySnapshot, _ *PartitionSnapshot) (*BindingSnapshot, error) {
	handshake, capabilityErr := b.discoverCapabilities(ctx)
	if isCommandUnavailable(capabilityErr) {
		return &BindingSnapshot{Reason: "nvidia binding helper is unavailable"}, nil
	}
	if capabilityErr != nil {
		return &BindingSnapshot{Reason: capabilityErr.Error()}, nil
	}
	if len(handshake.actions) == 0 {
		return &BindingSnapshot{Reason: "nvidia binding helper reported no supported actions"}, nil
	}
	command := b.command([]string{b.binary, "discover", "--format=csv"})
	executionContext, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	result, err := b.executor.Execute(executionContext, command)
	if isCommandUnavailable(err) {
		return &BindingSnapshot{Reason: "nvidia binding helper is unavailable"}, nil
	}
	if err != nil {
		return nil, err
	}
	bindings, err := parseDiscoveredBindings(result.Stdout)
	if err != nil {
		return nil, err
	}
	return &BindingSnapshot{Available: true, Bindings: bindings, SupportedActions: handshake.actions, SupportsMPSProfiles: handshake.features["mps_profile_pid"] && handshake.features["durable_receipts"] && handshake.features["generation_fence"] && handshake.features["idempotency"]}, nil
}

// DiscoverReceipts recovers completed helper operations after restart.
func (b *CommandBindingBackend) DiscoverReceipts(ctx context.Context) ([]RecoveredAction, error) {
	if _, err := b.discoverCapabilities(ctx); err != nil {
		if isCommandUnavailable(err) {
			return nil, nil
		}
		return nil, err
	}
	command := b.command([]string{b.binary, "receipts", "--format=csv"})
	executionContext, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	result, err := b.executor.Execute(executionContext, command)
	if isCommandUnavailable(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return parseRecoveredActions(result.Stdout)
}

// Apply creates or releases one binding through the helper.
func (b *CommandBindingBackend) Apply(ctx context.Context, request BackendActionRequest) (*BackendActionResult, error) {
	action := request.Action
	if action == nil {
		return nil, fmt.Errorf("%w: action is required", base.ErrInvalidArgument)
	}
	if err := validateHelperReceiptContext(action, request.Receipt); err != nil {
		return nil, err
	}
	var command Command
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_BIND:
		if action.GetBinding() == nil || len(action.GetBinding().GetDeviceIds()) == 0 {
			return nil, fmt.Errorf("%w: bind requires device_ids", base.ErrInvalidArgument)
		}
		if action.GetBinding().GetGeneration() == 0 || action.GetBinding().GetSandboxId() == "" || action.GetBinding().GetSandboxId() != actionSandboxID(action) || !validShare(action.GetShare()) {
			return nil, fmt.Errorf("%w: bind requires matching sandbox identity, non-zero binding generation, and share within (0,1]", base.ErrInvalidArgument)
		}
		argv := []string{b.binary, "bind", "--sandbox", actionSandboxID(action), "--binding", action.GetBinding().GetBindingId(), "--generation", strconv.FormatUint(action.GetBinding().GetGeneration(), 10), "--idempotency-key", action.GetIdempotencyKey(), "--share", strconv.FormatFloat(action.GetShare(), 'g', -1, 64)}
		for _, deviceID := range action.GetBinding().GetDeviceIds() {
			argv = append(argv, "--device", deviceID)
		}
		command = b.command(argv)
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		if action.GetExpectedGeneration() == 0 || actionSandboxID(action) == "" || strings.TrimSpace(action.GetTargetId()) == "" {
			return nil, fmt.Errorf("%w: release requires sandbox, target, and generation fence", base.ErrInvalidArgument)
		}
		command = b.command([]string{b.binary, "release", "--sandbox", actionSandboxID(action), "--target", action.GetTargetId(), "--generation", strconv.FormatUint(action.GetExpectedGeneration(), 10), "--idempotency-key", action.GetIdempotencyKey()})
	default:
		return nil, fmt.Errorf("%w: binding backend does not support %s", base.ErrUnsupported, action.GetActionType())
	}
	command.Argv = appendHelperReceiptArgs(command.Argv, request.Receipt)
	command.Argv = append(command.Argv, "--action-id", action.GetActionId(), "--plan-id", action.GetPlanId())
	result := &BackendActionResult{Detail: "nvidia binding mutation applied", Commands: []Command{command}}
	if request.DryRun {
		return result, nil
	}
	executionContext, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	commandResult, err := b.executor.Execute(executionContext, command)
	result.Results = []CommandResult{commandResult}
	if err != nil {
		result.MutationMayHaveApplied = bindingMutationMayHaveApplied(err)
		return result, err
	}
	if action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND {
		bindings, parseErr := parseDiscoveredBindings(commandResult.Stdout)
		if parseErr != nil || len(bindings) != 1 {
			if parseErr == nil {
				parseErr = fmt.Errorf("nvidia binding helper returned %d bindings, want 1", len(bindings))
			}
			result.MutationMayHaveApplied = true
			return result, parseErr
		}
		result.Binding = &bindings[0]
	} else if request.Binding != nil {
		result.Binding = cloneDiscoveredBinding(request.Binding)
	}
	return result, nil
}

func (b *CommandBindingBackend) discoverCapabilities(ctx context.Context) (backendHandshake, error) {
	command := b.command([]string{b.binary, "capabilities", "--format=csv"})
	executionContext, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	result, err := b.executor.Execute(executionContext, command)
	if err != nil {
		return backendHandshake{}, err
	}
	handshake, err := parseBackendHandshake(result.Stdout, "tgsrl-nvidia-binding", map[string]tgsrlv1.ActionType{"bind": tgsrlv1.ActionType_ACTION_TYPE_BIND, "release": tgsrlv1.ActionType_ACTION_TYPE_RELEASE})
	if err != nil {
		return backendHandshake{}, err
	}
	if !containsActionType(handshake.actions, tgsrlv1.ActionType_ACTION_TYPE_BIND) || !containsActionType(handshake.actions, tgsrlv1.ActionType_ACTION_TYPE_RELEASE) {
		return backendHandshake{}, errors.New("nvidia binding helper must advertise bind and release")
	}
	for _, feature := range []string{"durable_receipts", "generation_fence", "idempotency"} {
		if !handshake.features[feature] {
			return backendHandshake{}, fmt.Errorf("nvidia binding helper does not advertise required feature %q", feature)
		}
	}
	return handshake, nil
}

func validateHelperReceiptContext(action *tgsrlv1.Action, receipt *HelperReceiptContext) error {
	if receipt == nil {
		return nil
	}
	if receipt.StepIndex < 0 || receipt.ExpectedActions <= 0 || receipt.StepIndex >= receipt.ExpectedActions || receipt.TransactionGeneration == 0 || strings.TrimSpace(receipt.PlanDigest) == "" || strings.TrimSpace(receipt.CommandDigest) == "" || strings.TrimSpace(action.GetActionId()) == "" || strings.TrimSpace(action.GetPlanId()) == "" {
		return fmt.Errorf("%w: helper receipt identity is incomplete", base.ErrInvalidArgument)
	}
	return nil
}

func bindingMutationMayHaveApplied(err error) bool {
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		return true
	}
	switch commandErr.Kind {
	case CommandFailureUnavailable, CommandFailurePermission, CommandFailureInvalid:
		return false
	default:
		return true
	}
}

func (b *CommandBindingBackend) command(argv []string) Command {
	env := map[string]string{}
	if b.statePath != "" {
		env["TGSRL_NVIDIA_BINDING_STATE"] = b.statePath
	}
	if b.mpsPIDDirectory != "" {
		env["TGSRL_NVIDIA_MPS_PID_DIR"] = b.mpsPIDDirectory
	}
	return Command{Argv: argv, Env: env}
}

func appendHelperReceiptArgs(argv []string, receipt *HelperReceiptContext) []string {
	if receipt == nil {
		return argv
	}
	return append(argv,
		"--recoverable",
		"--step-index", strconv.Itoa(receipt.StepIndex),
		"--expected-actions", strconv.Itoa(receipt.ExpectedActions),
		"--transaction-generation", strconv.FormatUint(receipt.TransactionGeneration, 10),
		"--plan-digest", receipt.PlanDigest,
		"--command-digest", receipt.CommandDigest,
	)
}

func parseDiscoveredBindings(output []byte) ([]DiscoveredBinding, error) {
	if strings.TrimSpace(string(output)) == "" {
		return nil, nil
	}
	reader := csv.NewReader(strings.NewReader(strings.TrimSpace(string(output))))
	reader.TrimLeadingSpace = true
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse nvidia bindings: %w", err)
	}
	bindings := make([]DiscoveredBinding, 0, len(records))
	seen := make(map[string]struct{}, len(records))
	for row, record := range records {
		if len(record) != 6 && len(record) != 7 {
			return nil, fmt.Errorf("nvidia binding row %d has %d fields, want 6 or 7", row+1, len(record))
		}
		for index := range record {
			record[index] = strings.TrimSpace(record[index])
		}
		generation, err := strconv.ParseUint(record[2], 10, 64)
		if err != nil || generation == 0 {
			return nil, fmt.Errorf("nvidia binding row %d has invalid generation %q", row+1, record[2])
		}
		share, err := strconv.ParseFloat(record[4], 64)
		if err != nil || !validShare(share) {
			return nil, fmt.Errorf("nvidia binding row %d has invalid share %q", row+1, record[4])
		}
		if record[0] == "" || record[1] == "" || record[3] == "" {
			return nil, fmt.Errorf("nvidia binding row %d has missing identity", row+1)
		}
		if _, duplicate := seen[record[0]]; duplicate {
			return nil, fmt.Errorf("duplicate discovered sandbox %q", record[0])
		}
		seen[record[0]] = struct{}{}
		binding := DiscoveredBinding{SandboxID: record[0], BindingID: record[1], Generation: generation, DeviceIDs: strings.Split(record[3], ";"), Share: share, IdempotencyKey: record[5]}
		if len(record) == 7 {
			serverPID, err := strconv.ParseUint(record[6], 10, 32)
			if err != nil || serverPID == 0 {
				return nil, fmt.Errorf("nvidia binding row %d has invalid MPS server PID %q", row+1, record[6])
			}
			binding.ServerPID = uint32(serverPID)
		}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func parseRecoveredActions(output []byte) ([]RecoveredAction, error) {
	if strings.TrimSpace(string(output)) == "" {
		return nil, nil
	}
	reader := csv.NewReader(strings.NewReader(strings.TrimSpace(string(output))))
	reader.TrimLeadingSpace = true
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse nvidia action receipts: %w", err)
	}
	result := make([]RecoveredAction, 0, len(records))
	seenKeys := make(map[string]struct{}, len(records))
	seenSteps := make(map[string]struct{}, len(records))
	for row, record := range records {
		if len(record) != 13 && len(record) != 14 {
			return nil, fmt.Errorf("nvidia receipt row %d has %d fields, want 13 or 14", row+1, len(record))
		}
		for index := range record {
			record[index] = strings.TrimSpace(record[index])
		}
		stepIndex, err := strconv.Atoi(record[0])
		if err != nil || stepIndex < 0 {
			return nil, fmt.Errorf("nvidia receipt row %d has invalid step index %q", row+1, record[0])
		}
		expectedActions, err := strconv.Atoi(record[1])
		if err != nil || expectedActions <= 0 || stepIndex >= expectedActions {
			return nil, fmt.Errorf("nvidia receipt row %d has invalid expected action count %q", row+1, record[1])
		}
		committed, err := strconv.ParseBool(record[2])
		if err != nil || record[3] == "" {
			return nil, fmt.Errorf("nvidia receipt row %d has invalid commit marker or plan digest", row+1)
		}
		generation, err := strconv.ParseUint(record[8], 10, 64)
		if err != nil || generation == 0 {
			return nil, fmt.Errorf("nvidia receipt row %d has invalid generation %q", row+1, record[8])
		}
		actionGeneration := generation
		successIndex := 9
		if len(record) == 14 {
			actionGeneration, err = strconv.ParseUint(record[9], 10, 64)
			if err != nil || actionGeneration == 0 {
				return nil, fmt.Errorf("nvidia receipt row %d has invalid action generation %q", row+1, record[9])
			}
			successIndex = 10
		}
		succeeded, err := strconv.ParseBool(record[successIndex])
		if err != nil {
			return nil, fmt.Errorf("nvidia receipt row %d has invalid success value %q", row+1, record[successIndex])
		}
		commandDigestIndex := successIndex + 3
		if record[4] == "" || record[5] == "" || record[6] == "" || record[7] == "" || record[commandDigestIndex] == "" {
			return nil, fmt.Errorf("nvidia receipt row %d has missing identity", row+1)
		}
		recovered := RecoveredAction{StepIndex: stepIndex, ExpectedActions: expectedActions, Committed: committed, PlanDigest: record[3], ActionID: record[4], PlanID: record[5], IdempotencyKey: record[6], SandboxID: record[7], Generation: generation, ActionGeneration: actionGeneration, Succeeded: succeeded, ErrorCode: record[successIndex+1], ErrorMessage: record[successIndex+2], CommandDigest: record[commandDigestIndex]}
		if _, duplicate := seenKeys[recovered.IdempotencyKey]; duplicate {
			return nil, fmt.Errorf("nvidia receipt row %d duplicates idempotency key %q", row+1, recovered.IdempotencyKey)
		}
		stepKey := recovered.PlanID + "\x00" + strconv.FormatUint(recovered.Generation, 10) + "\x00" + strconv.Itoa(recovered.StepIndex)
		if _, duplicate := seenSteps[stepKey]; duplicate {
			return nil, fmt.Errorf("nvidia receipt row %d duplicates transaction step %d", row+1, recovered.StepIndex)
		}
		seenKeys[recovered.IdempotencyKey] = struct{}{}
		seenSteps[stepKey] = struct{}{}
		result = append(result, recovered)
	}
	return result, nil
}
