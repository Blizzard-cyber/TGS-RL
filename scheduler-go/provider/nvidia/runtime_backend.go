package nvidia

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	base "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
)

// CommandRuntimeBackend delegates lifecycle control to an argv-only helper.
type CommandRuntimeBackend struct {
	executor CommandExecutor
	timeout  time.Duration
	binary   string
}

// NewCommandRuntimeBackend constructs a runtime helper backend.
func NewCommandRuntimeBackend(executor CommandExecutor, timeout time.Duration) *CommandRuntimeBackend {
	if executor == nil {
		executor = NewExecCommandExecutor()
	}
	if timeout <= 0 {
		timeout = defaultCommandTimeout
	}
	return &CommandRuntimeBackend{executor: executor, timeout: timeout, binary: "tgsrl-nvidia-runtime"}
}

// Discover verifies the helper protocol before advertising lifecycle actions.
func (b *CommandRuntimeBackend) Discover(ctx context.Context) (*RuntimeBackendStatus, error) {
	command := Command{Argv: []string{b.binary, "capabilities", "--format=csv"}}
	executionContext, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	result, err := b.executor.Execute(executionContext, command)
	if isCommandUnavailable(err) {
		return &RuntimeBackendStatus{Reason: "nvidia runtime helper is unavailable"}, nil
	}
	if err != nil {
		return nil, err
	}
	handshake, handshakeErr := parseBackendHandshake(result.Stdout, "tgsrl-nvidia-runtime", map[string]tgsrlv1.ActionType{
		"pause":   tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
		"resume":  tgsrlv1.ActionType_ACTION_TYPE_RESUME,
		"sleep":   tgsrlv1.ActionType_ACTION_TYPE_SLEEP,
		"offload": tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD,
	})
	if handshakeErr != nil {
		return &RuntimeBackendStatus{Reason: handshakeErr.Error()}, nil
	}
	if len(handshake.actions) == 0 || !handshake.features["generation_fence"] || !handshake.features["idempotency"] {
		return &RuntimeBackendStatus{Reason: "nvidia runtime helper reported no supported actions"}, nil
	}
	sandboxCommand := Command{Argv: []string{b.binary, "discover", "--format=csv"}}
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

func parseRuntimeSandboxes(output []byte) ([]base.Sandbox, error) {
	if strings.TrimSpace(string(output)) == "" {
		return nil, nil
	}
	rows := strings.Split(strings.TrimSpace(string(output)), "\n")
	result := make([]base.Sandbox, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for row, raw := range rows {
		fields := strings.Split(raw, ",")
		if len(fields) != 6 {
			return nil, fmt.Errorf("nvidia runtime row %d has %d fields, want 6", row+1, len(fields))
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
		result = append(result, base.Sandbox{SandboxID: fields[0], Generation: generation, State: state, SafePoint: safePoint, Offloaded: offloaded, Priority: int32(priority)})
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
	command := Command{Argv: []string{b.binary, verb, "--sandbox", actionSandboxID(action), "--generation", strconv.FormatUint(action.GetExpectedGeneration(), 10), "--idempotency-key", action.GetIdempotencyKey()}}
	result := &BackendActionResult{Detail: "nvidia runtime " + verb + " applied", Commands: []Command{command}}
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
