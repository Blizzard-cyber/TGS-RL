package nvidia

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
)

const commandDiagnosticLimit = 4096

// CommandResult captures both output streams and the process exit status.
type CommandResult struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
}

// CommandExecutor is the sole process execution boundary used by Driver v2.
type CommandExecutor interface {
	Execute(context.Context, Command) (CommandResult, error)
}

// CommandFailureKind classifies execution failures without parsing them again.
type CommandFailureKind string

const (
	CommandFailureUnavailable CommandFailureKind = "unavailable"
	CommandFailureTimeout     CommandFailureKind = "timeout"
	CommandFailureCanceled    CommandFailureKind = "canceled"
	CommandFailurePermission  CommandFailureKind = "permission"
	CommandFailureExit        CommandFailureKind = "exit"
	CommandFailureInvalid     CommandFailureKind = "invalid"
)

// CommandError preserves bounded stderr and the exit status for classification.
type CommandError struct {
	Kind     CommandFailureKind
	Argv     []string
	ExitCode int
	Stderr   string
	Cause    error
}

func (e *CommandError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := fmt.Sprintf("nvidia command %s failed (%s, exit=%d)", commandName(e.Argv), e.Kind, e.ExitCode)
	if e.Stderr != "" {
		message += ": " + e.Stderr
	}
	return message
}

func (e *CommandError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// ExecCommandExecutor executes commands directly without a shell.
type ExecCommandExecutor struct{}

// NewExecCommandExecutor returns the production argv-only executor.
func NewExecCommandExecutor() *ExecCommandExecutor {
	return &ExecCommandExecutor{}
}

// Execute runs one command and always returns captured stdout, stderr, and exit code.
func (*ExecCommandExecutor) Execute(ctx context.Context, command Command) (CommandResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateCommand(command); err != nil {
		return CommandResult{ExitCode: -1}, err
	}
	cmd := exec.CommandContext(ctx, command.Argv[0], command.Argv[1:]...)
	if len(command.Env) > 0 {
		cmd.Env = append(os.Environ(), stableEnvironment(command.Env)...)
	}
	if len(command.Stdin) > 0 {
		cmd.Stdin = bytes.NewReader(command.Stdin)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	result := CommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes(), ExitCode: 0}
	if err == nil {
		return cloneCommandResult(result), nil
	}
	result.ExitCode = -1
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
	}
	return cloneCommandResult(result), classifyCommandError(ctx, command, result, err)
}

func validateCommand(command Command) error {
	if len(command.Argv) == 0 || strings.TrimSpace(command.Argv[0]) == "" {
		return &CommandError{Kind: CommandFailureInvalid, ExitCode: -1, Cause: errors.New("command binary is required")}
	}
	for _, value := range command.Argv {
		if strings.IndexByte(value, 0) >= 0 {
			return &CommandError{Kind: CommandFailureInvalid, Argv: append([]string(nil), command.Argv...), ExitCode: -1, Cause: errors.New("command argument contains NUL")}
		}
	}
	return nil
}

func classifyCommandError(ctx context.Context, command Command, result CommandResult, err error) error {
	kind := CommandFailureExit
	cause := err
	stderr := boundedDiagnostic(result.Stderr)
	switch {
	case ctx != nil && errors.Is(ctx.Err(), context.Canceled):
		kind = CommandFailureCanceled
		cause = context.Canceled
	case ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded):
		kind = CommandFailureTimeout
		cause = ctx.Err()
	case errors.Is(err, context.Canceled):
		kind = CommandFailureCanceled
	case errors.Is(err, context.DeadlineExceeded):
		kind = CommandFailureTimeout
	case errors.Is(err, exec.ErrNotFound), result.ExitCode == 127, containsAnyFold(stderr, "command not found", "not found", "no such file"):
		kind = CommandFailureUnavailable
	case containsAnyFold(stderr, "permission denied", "not permitted", "insufficient permissions"):
		kind = CommandFailurePermission
	case containsAnyFold(stderr, "no devices were found", "couldn't communicate with the nvidia driver", "driver/library version mismatch"):
		kind = CommandFailureUnavailable
	}
	return &CommandError{Kind: kind, Argv: append([]string(nil), command.Argv...), ExitCode: result.ExitCode, Stderr: stderr, Cause: cause}
}

func commandName(argv []string) string {
	if len(argv) == 0 {
		return "<empty>"
	}
	return argv[0]
}

func boundedDiagnostic(value []byte) string {
	trimmed := strings.TrimSpace(string(value))
	if len(trimmed) <= commandDiagnosticLimit {
		return trimmed
	}
	return trimmed[:commandDiagnosticLimit] + "..."
}

func containsAnyFold(value string, candidates ...string) bool {
	value = strings.ToLower(value)
	for _, candidate := range candidates {
		if strings.Contains(value, strings.ToLower(candidate)) {
			return true
		}
	}
	return false
}

func stableEnvironment(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

func cloneCommand(command Command) Command {
	command.Argv = append([]string(nil), command.Argv...)
	command.Stdin = append([]byte(nil), command.Stdin...)
	if command.Env != nil {
		environment := make(map[string]string, len(command.Env))
		for key, value := range command.Env {
			environment[key] = value
		}
		command.Env = environment
	}
	return command
}

func cloneCommandResult(result CommandResult) CommandResult {
	result.Stdout = append([]byte(nil), result.Stdout...)
	result.Stderr = append([]byte(nil), result.Stderr...)
	return result
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
