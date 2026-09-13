package nvidia

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	base "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
)

// MPSBackend discovers the shared MPS daemon and recovered binding profiles.
// NVIDIA's server-level active-thread command only affects clients created
// after the command. It cannot safely implement an online set_share action for
// an already-running training process, so production mutation fails closed.
type MPSBackend struct {
	executor      CommandExecutor
	timeout       time.Duration
	pipeDirectory string
	logDirectory  string
	now           func() time.Time
	mu            sync.Mutex
	profiles      map[string]MPSProfile
	dryRun        bool
}

// SetDryRun suppresses daemon startup during discovery.
func (b *MPSBackend) SetDryRun(dryRun bool) { b.dryRun = dryRun }

// NewMPSBackend constructs the low-level MPS discovery backend.
func NewMPSBackend(executor CommandExecutor, timeout time.Duration, pipeDirectory, logDirectory string) *MPSBackend {
	if executor == nil {
		executor = NewExecCommandExecutor()
	}
	if timeout <= 0 {
		timeout = defaultCommandTimeout
	}
	if strings.TrimSpace(pipeDirectory) == "" {
		pipeDirectory = defaultMPSPipeDirectory
	}
	if strings.TrimSpace(logDirectory) == "" {
		logDirectory = defaultMPSLogDirectory
	}
	return &MPSBackend{executor: executor, timeout: timeout, pipeDirectory: pipeDirectory, logDirectory: logDirectory, now: time.Now, profiles: make(map[string]MPSProfile)}
}

// Mode identifies MPS without any automatic MIG fallback.
func (*MPSBackend) Mode() PartitionMode { return PartitionModeMPS }

// Discover verifies MPS control availability and creates one shared partition per GPU.
func (b *MPSBackend) Discover(ctx context.Context, inventory *InventorySnapshot) (*PartitionSnapshot, error) {
	if inventory == nil || len(inventory.Devices) == 0 {
		return &PartitionSnapshot{Mode: PartitionModeMPS, Reason: "physical GPU inventory is empty"}, nil
	}
	command := b.controlCommand([]byte("get_default_active_thread_percentage\n"))
	ctx, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	result, err := b.executor.Execute(ctx, command)
	if err != nil {
		if !isMPSDaemonStopped(result, err) {
			return &PartitionSnapshot{Mode: PartitionModeMPS, Reason: boundedDiagnostic(result.Stderr), ObservedAt: inventory.ObservedAt}, err
		}
		if b.dryRun {
			return &PartitionSnapshot{Mode: PartitionModeMPS, Reason: "MPS daemon is not running and dry-run forbids startup", ObservedAt: inventory.ObservedAt}, nil
		}
		start := Command{Argv: []string{commandNvidiaMPSControl, "-d"}, Env: map[string]string{"CUDA_MPS_PIPE_DIRECTORY": b.pipeDirectory, "CUDA_MPS_LOG_DIRECTORY": b.logDirectory}}
		startResult, startErr := b.executor.Execute(ctx, start)
		if startErr != nil {
			reason := boundedDiagnostic(startResult.Stderr)
			if reason == "" {
				reason = startErr.Error()
			}
			return &PartitionSnapshot{Mode: PartitionModeMPS, Reason: reason, ObservedAt: inventory.ObservedAt}, startErr
		}
		verifyResult, verifyErr := b.executor.Execute(ctx, command)
		if verifyErr != nil {
			reason := boundedDiagnostic(verifyResult.Stderr)
			if reason == "" {
				reason = verifyErr.Error()
			}
			return &PartitionSnapshot{Mode: PartitionModeMPS, Reason: reason, ObservedAt: inventory.ObservedAt}, verifyErr
		}
	}
	partitions := make([]Partition, 0, len(inventory.Devices))
	for _, device := range inventory.Devices {
		partitions = append(partitions, Partition{ID: "mps-" + device.UUID, ParentUUID: device.UUID, Profile: "startup-only", MemoryBytes: device.MemoryBytes, Share: 1, Labels: map[string]string{"mode": string(PartitionModeMPS), "daemon": "ready", "online_share_mutation": "unsupported"}})
	}
	return &PartitionSnapshot{Mode: PartitionModeMPS, Available: true, Reason: "MPS daemon is available, but online share mutation is unsupported for existing clients", Partitions: partitions, RequiresBindingMetadata: true, ObservedAt: inventory.ObservedAt}, nil
}

func isMPSDaemonStopped(result CommandResult, err error) bool {
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.Kind != CommandFailureExit {
		return false
	}
	diagnostic := firstNonEmpty(boundedDiagnostic(result.Stderr), commandErr.Stderr)
	return containsAnyFold(diagnostic, "cannot find mps control daemon", "mps control daemon is not running", "control daemon not found")
}

// Apply rejects online MPS share mutation. NVIDIA's server-level percentage
// applies only to future clients; a safe implementation needs a
// checkpoint/recreate workflow that starts a new client with the desired
// CUDA_MPS_ACTIVE_THREAD_PERCENTAGE and then verifies that incarnation.
func (b *MPSBackend) Apply(_ context.Context, request BackendActionRequest) (*BackendActionResult, error) {
	action := request.Action
	if action == nil {
		return nil, fmt.Errorf("%w: action is required", base.ErrInvalidArgument)
	}
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE:
	default:
		return nil, fmt.Errorf("%w: MPS backend does not support %s", base.ErrUnsupported, action.GetActionType())
	}
	share := action.GetShare()
	if !validShare(share) {
		return nil, fmt.Errorf("%w: MPS share must be finite and within (0,1]", base.ErrInvalidArgument)
	}
	percentage := int(math.Round(share * 100))
	if percentage == 0 {
		return nil, fmt.Errorf("%w: MPS share rounds to zero active thread percentage", base.ErrInvalidArgument)
	}
	return nil, v2ActionError(
		action,
		base.ErrorCodeUnsupported,
		fmt.Sprintf("MPS %d%% can only be applied before a new client starts; online set_share requires checkpoint/recreate", percentage),
		base.ErrUnsupported,
	)
}

func (b *MPSBackend) restoreProfiles(bindings []DiscoveredBinding, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, binding := range bindings {
		if binding.ServerPID == 0 || !validShare(binding.Share) {
			continue
		}
		b.profiles[binding.SandboxID] = MPSProfile{SandboxID: binding.SandboxID, Generation: binding.Generation, ActiveThreadPercentage: int(math.Round(binding.Share * 100)), UpdatedAt: now}
	}
}

// Profiles returns detached MPS profiles for audit and reconciliation.
func (b *MPSBackend) Profiles() []MPSProfile {
	b.mu.Lock()
	defer b.mu.Unlock()
	profiles := make([]MPSProfile, 0, len(b.profiles))
	for _, profile := range b.profiles {
		profiles = append(profiles, profile)
	}
	sort.Slice(profiles, func(i, j int) bool { return profiles[i].SandboxID < profiles[j].SandboxID })
	return profiles
}

func (b *MPSBackend) controlCommand(input []byte) Command {
	return Command{
		Argv:  []string{commandNvidiaMPSControl},
		Env:   map[string]string{"CUDA_MPS_PIPE_DIRECTORY": b.pipeDirectory, "CUDA_MPS_LOG_DIRECTORY": b.logDirectory},
		Stdin: append([]byte(nil), input...),
	}
}

func containsActionType(values []tgsrlv1.ActionType, expected tgsrlv1.ActionType) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
