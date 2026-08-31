package nvidia

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	base "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
)

// CommandRuntimeBackend delegates lifecycle control to an argv-only helper.
type CommandRuntimeBackend struct {
	executor  CommandExecutor
	timeout   time.Duration
	binary    string
	statePath string
}

// NewCommandRuntimeBackend constructs a runtime helper backend.
func NewCommandRuntimeBackend(executor CommandExecutor, timeout time.Duration) *CommandRuntimeBackend {
	return NewCommandRuntimeBackendWithConfig(executor, timeout, "", "")
}

func NewCommandRuntimeBackendWithConfig(executor CommandExecutor, timeout time.Duration, binary, statePath string) *CommandRuntimeBackend {
	if executor == nil {
		executor = NewExecCommandExecutor()
	}
	if timeout <= 0 {
		timeout = defaultCommandTimeout
	}
	if strings.TrimSpace(binary) == "" {
		binary = "tgsrl-nvidia-runtime"
	}
	return &CommandRuntimeBackend{executor: executor, timeout: timeout, binary: binary, statePath: strings.TrimSpace(statePath)}
}

// Discover verifies the helper protocol before advertising lifecycle actions.
func (b *CommandRuntimeBackend) Discover(ctx context.Context) (*RuntimeBackendStatus, error) {
	handshake, err := b.discoverCapabilities(ctx)
	if isCommandUnavailable(err) {
		return &RuntimeBackendStatus{Reason: "nvidia runtime helper is unavailable"}, nil
	}
	if err != nil {
		return nil, err
	}
	sandboxCommand := b.command([]string{b.binary, "discover", "--format=csv"})
	executionContext, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	sandboxResult, sandboxErr := b.executor.Execute(executionContext, sandboxCommand)
	if sandboxErr != nil {
		return nil, sandboxErr
	}
	sandboxes, parseErr := parseRuntimeSandboxes(sandboxResult.Stdout)
	if parseErr != nil {
		return nil, parseErr
	}
	return &RuntimeBackendStatus{Available: true, SupportedActions: handshake.actions, Sandboxes: sandboxes, SandboxesAuthoritative: true}, nil
}

func (b *CommandRuntimeBackend) DiscoverReceipts(ctx context.Context) ([]RecoveredAction, error) {
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

func (b *CommandRuntimeBackend) discoverCapabilities(ctx context.Context) (backendHandshake, error) {
	command := b.command([]string{b.binary, "capabilities", "--format=csv"})
	executionContext, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	result, err := b.executor.Execute(executionContext, command)
	if err != nil {
		return backendHandshake{}, err
	}
	handshake, err := parseBackendHandshake(result.Stdout, "tgsrl-nvidia-runtime", map[string]tgsrlv1.ActionType{
		"pause":   tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
		"resume":  tgsrlv1.ActionType_ACTION_TYPE_RESUME,
		"sleep":   tgsrlv1.ActionType_ACTION_TYPE_SLEEP,
		"offload": tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD,
	})
	if err != nil {
		return backendHandshake{}, err
	}
	if len(handshake.actions) == 0 {
		return backendHandshake{}, errors.New("nvidia runtime helper reported no supported actions")
	}
	for _, feature := range []string{"generation_fence", "idempotency", "durable_receipts", "safe_point", "checkpoint", "reload", "readiness"} {
		if !handshake.features[feature] {
			return backendHandshake{}, fmt.Errorf("nvidia runtime helper does not advertise required feature %q", feature)
		}
	}
	return handshake, nil
}

func parseRuntimeSandboxes(output []byte) ([]base.Sandbox, error) {
	if strings.TrimSpace(string(output)) == "" {
		return nil, nil
	}
	rows := strings.Split(strings.TrimSpace(string(output)), "\n")
	result := make([]base.Sandbox, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for row, raw := range rows {
		fields := strings.Split(raw, ",")
		if len(fields) != 6 && len(fields) != 9 {
			return nil, fmt.Errorf("nvidia runtime row %d has %d fields, want 6 or 9", row+1, len(fields))
		}
		for index := range fields {
			fields[index] = strings.TrimSpace(fields[index])
		}
		generation, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil || generation == 0 {
			return nil, fmt.Errorf("nvidia runtime row %d has invalid generation %q", row+1, fields[1])
		}
		safePoint, err := strconv.ParseBool(fields[3])
		if err != nil {
			return nil, fmt.Errorf("nvidia runtime row %d has invalid safe point %q", row+1, fields[3])
		}
		offloaded, err := strconv.ParseBool(fields[4])
		if err != nil {
			return nil, fmt.Errorf("nvidia runtime row %d has invalid offloaded value %q", row+1, fields[4])
		}
		priority, err := strconv.ParseInt(fields[5], 10, 32)
		if err != nil || fields[0] == "" {
			return nil, fmt.Errorf("nvidia runtime row %d has invalid identity or priority", row+1)
		}
		if _, duplicate := seen[fields[0]]; duplicate {
			return nil, fmt.Errorf("duplicate runtime sandbox %q", fields[0])
		}
		seen[fields[0]] = struct{}{}
		state, ok := runtimeSandboxState(fields[2])
		if !ok {
			return nil, fmt.Errorf("nvidia runtime row %d has invalid state %q", row+1, fields[2])
		}
		sandbox := base.Sandbox{SandboxID: fields[0], Generation: generation, State: state, SafePoint: safePoint, Offloaded: offloaded, Priority: int32(priority)}
		if len(fields) == 9 {
			share, parseErr := strconv.ParseFloat(fields[8], 64)
			deviceIDs := strings.Split(fields[7], ";")
			deviceSet := make(map[string]struct{}, len(deviceIDs))
			validDevices := len(deviceIDs) > 0
			for _, deviceID := range deviceIDs {
				if deviceID == "" {
					validDevices = false
					break
				}
				if _, duplicate := deviceSet[deviceID]; duplicate {
					validDevices = false
					break
				}
				deviceSet[deviceID] = struct{}{}
			}
			if parseErr != nil || fields[6] == "" || !validDevices || !validShare(share) {
				return nil, fmt.Errorf("nvidia runtime row %d has invalid binding metadata", row+1)
			}
			sandbox.Share = share
			sandbox.Binding = &tgsrlv1.Binding{BindingId: fields[6], SandboxId: fields[0], Generation: generation, DeviceIds: deviceIDs, Resources: &tgsrlv1.ResourceVector{AcceleratorUnits: share}}
		}
		result = append(result, sandbox)
	}
	return result, nil
}

func runtimeSandboxState(value string) (base.SandboxState, bool) {
	state := base.SandboxState(strings.ToLower(strings.TrimSpace(value)))
	switch state {
	case base.SandboxStateRequested, base.SandboxStateBound, base.SandboxStateRunning, base.SandboxStatePaused, base.SandboxStateSleeping, base.SandboxStateFailed, base.SandboxStateTerminated:
		return state, true
	default:
		return "", false
	}
}

// Apply performs one generation-fenced lifecycle action.
func (b *CommandRuntimeBackend) Apply(ctx context.Context, request BackendActionRequest) (*BackendActionResult, error) {
	action := request.Action
	if action == nil {
		return nil, fmt.Errorf("%w: action is required", base.ErrInvalidArgument)
	}
	verb := runtimeVerb(action)
	if verb == "" {
		return nil, fmt.Errorf("%w: runtime backend does not support %s", base.ErrUnsupported, action.GetActionType())
	}
	if actionSandboxID(action) == "" || action.GetExpectedGeneration() == 0 || action.GetIdempotencyKey() == "" {
		return nil, fmt.Errorf("%w: runtime action requires sandbox, generation, and idempotency key", base.ErrInvalidArgument)
	}
	argv := []string{b.binary, verb, "--sandbox", actionSandboxID(action), "--generation", strconv.FormatUint(action.GetExpectedGeneration(), 10), "--idempotency-key", action.GetIdempotencyKey()}
	argv = appendHelperReceiptArgs(argv, request.Receipt)
	argv = append(argv, "--action-id", action.GetActionId(), "--plan-id", action.GetPlanId())
	command := b.command(argv)
	result := &BackendActionResult{Detail: "nvidia runtime " + verb + " applied", Commands: []Command{command}}
	if request.DryRun {
		return result, nil
	}
	executionContext, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	commandResult, err := b.executor.Execute(executionContext, command)
	result.Results = []CommandResult{commandResult}
	if err != nil {
		result.MutationMayHaveApplied = runtimeMutationMayHaveApplied(err)
		return result, err
	}
	return result, nil
}

func (b *CommandRuntimeBackend) command(argv []string) Command {
	env := map[string]string{}
	if b.statePath != "" {
		env["TGSRL_NVIDIA_RUNTIME_STATE"] = b.statePath
	}
	return Command{Argv: argv, Env: env}
}

func runtimeMutationMayHaveApplied(err error) bool {
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		return true
	}
	switch commandErr.Kind {
	case CommandFailureTimeout, CommandFailureCanceled:
		return true
	case CommandFailureExit:
		return commandErr.ExitCode == 75
	default:
		return false
	}
}

func runtimeVerb(action *tgsrlv1.Action) string {
	if action == nil {
		return ""
	}
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_PAUSE:
		return "pause"
	case tgsrlv1.ActionType_ACTION_TYPE_RESUME:
		return "resume"
	case tgsrlv1.ActionType_ACTION_TYPE_SLEEP:
		return "sleep"
	case tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		return "offload"
	default:
		return ""
	}
}
