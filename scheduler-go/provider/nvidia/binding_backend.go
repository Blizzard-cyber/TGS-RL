package nvidia

import (
	"context"
	"encoding/csv"
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
	executor CommandExecutor
	timeout  time.Duration
	binary   string
}

// NewCommandBindingBackend constructs the production binding backend.
func NewCommandBindingBackend(executor CommandExecutor, timeout time.Duration) *CommandBindingBackend {
	if executor == nil {
		executor = NewExecCommandExecutor()
	}
	if timeout <= 0 {
		timeout = defaultCommandTimeout
	}
	return &CommandBindingBackend{executor: executor, timeout: timeout, binary: "tgsrl-nvidia-binding"}
}

// Discover recovers durable bindings after restart.
func (b *CommandBindingBackend) Discover(ctx context.Context, _ *InventorySnapshot, _ *PartitionSnapshot) (*BindingSnapshot, error) {
	capabilityCommand := Command{Argv: []string{b.binary, "capabilities", "--format=csv"}}
	executionContext, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	capabilityResult, capabilityErr := b.executor.Execute(executionContext, capabilityCommand)
	if isCommandUnavailable(capabilityErr) {
		return &BindingSnapshot{Reason: "nvidia binding helper is unavailable"}, nil
	}
	if capabilityErr != nil {
		return nil, capabilityErr
	}
	handshake, handshakeErr := parseBackendHandshake(capabilityResult.Stdout, "tgsrl-nvidia-binding", map[string]tgsrlv1.ActionType{"bind": tgsrlv1.ActionType_ACTION_TYPE_BIND, "release": tgsrlv1.ActionType_ACTION_TYPE_RELEASE})
	if handshakeErr != nil {
		return &BindingSnapshot{Reason: handshakeErr.Error()}, nil
	}
	if len(handshake.actions) == 0 {
		return &BindingSnapshot{Reason: "nvidia binding helper reported no supported actions"}, nil
	}
	command := Command{Argv: []string{b.binary, "discover", "--format=csv"}}
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
	command := Command{Argv: []string{b.binary, "receipts", "--format=csv"}}
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
	var command Command
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_BIND:
		if action.GetBinding() == nil || len(action.GetBinding().GetDeviceIds()) == 0 {
			return nil, fmt.Errorf("%w: bind requires device_ids", base.ErrInvalidArgument)
		}
		argv := []string{b.binary, "bind", "--sandbox", actionSandboxID(action), "--binding", action.GetBinding().GetBindingId(), "--generation", strconv.FormatUint(action.GetBinding().GetGeneration(), 10), "--idempotency-key", action.GetIdempotencyKey()}
		for _, deviceID := range action.GetBinding().GetDeviceIds() {
			argv = append(argv, "--device", deviceID)
		}
		command = Command{Argv: argv}
	case tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		command = Command{Argv: []string{b.binary, "release", "--sandbox", actionSandboxID(action), "--target", action.GetTargetId(), "--generation", strconv.FormatUint(action.GetExpectedGeneration(), 10), "--idempotency-key", action.GetIdempotencyKey()}}
	default:
		return nil, fmt.Errorf("%w: binding backend does not support %s", base.ErrUnsupported, action.GetActionType())
	}
	result := &BackendActionResult{Detail: "nvidia binding mutation applied", Commands: []Command{command}}
	if request.DryRun {
		return result, nil
	}
	executionContext, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	commandResult, err := b.executor.Execute(executionContext, command)
	result.Results = []CommandResult{commandResult}
	if err != nil {
		return result, err
	}
	return result, nil
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
	for row, record := range records {
		if len(record) != 13 {
			return nil, fmt.Errorf("nvidia receipt row %d has %d fields, want 13", row+1, len(record))
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
		succeeded, err := strconv.ParseBool(record[9])
		if err != nil {
			return nil, fmt.Errorf("nvidia receipt row %d has invalid success value %q", row+1, record[9])
		}
		if record[4] == "" || record[5] == "" || record[6] == "" || record[7] == "" || record[12] == "" {
			return nil, fmt.Errorf("nvidia receipt row %d has missing identity", row+1)
		}
		recovered := RecoveredAction{StepIndex: stepIndex, ExpectedActions: expectedActions, Committed: committed, PlanDigest: record[3], ActionID: record[4], PlanID: record[5], IdempotencyKey: record[6], SandboxID: record[7], Generation: generation, Succeeded: succeeded, ErrorCode: record[10], ErrorMessage: record[11], CommandDigest: record[12]}
		result = append(result, recovered)
	}
	return result, nil
}
