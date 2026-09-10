package nvidia

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	ProviderID            = "nvidia"
	CapabilityName        = "nvidia-gpu"
	ErrorCodeUnavailable  = "UNAVAILABLE"
	commandNvidiaSMI      = "nvidia-smi"
	commandNvidiaModprobe = "nvidia-modprobe"

	// capabilityObservationRevision identifies this capability observation
	// shape. It is an evidence revision, not a component semantic version.
	capabilityObservationRevision uint64 = 1
)

// Command describes one structured argv-only driver command.
type Command struct {
	Argv  []string
	Env   map[string]string
	Stdin []byte
}

// ProbeResult is the evidence-backed driver discovery snapshot.
type ProbeResult struct {
	Available              bool
	Reason                 string
	Devices                []*tgsrlv1.Device
	Capabilities           *tgsrlv1.CapabilitySet
	Sandboxes              []provider.Sandbox
	SandboxesAuthoritative bool
}

// DriverState is the provider-owned runtime state exposed to the driver.
type DriverState struct {
	Healthy      bool
	HealthReason string
	Revision     uint64
	Devices      []*tgsrlv1.Device
	Sandboxes    map[string]provider.Sandbox
}

// ActionExecution records one completed vendor action.
type ActionExecution struct {
	Detail                 string
	Commands               []Command
	DryRun                 bool
	Binding                *DiscoveredBinding
	ObservedShare          *float64
	MutationMayHaveApplied bool
}

// ReconcileState reports the driver's current view of device readiness.
type ReconcileState struct {
	Healthy                bool
	Reason                 string
	Devices                []*tgsrlv1.Device
	Capabilities           *tgsrlv1.CapabilitySet
	Sandboxes              []provider.Sandbox
	SandboxesAuthoritative bool
}

// Driver abstracts the vendor-specific probe and execution boundary.
type Driver interface {
	ID() string
	Probe(context.Context) (*ProbeResult, error)
	ExecuteAction(context.Context, *DriverState, *tgsrlv1.Action) (*ActionExecution, error)
	Reconcile(context.Context, *DriverState, *tgsrlv1.PlacementPlan) (*ReconcileState, error)
}

// Runner executes argv-only commands for local driver discovery.
type Runner interface {
	Run(context.Context, Command) ([]byte, error)
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, command Command) ([]byte, error) {
	result, err := NewExecCommandExecutor().Execute(ctx, command)
	return result.Stdout, err
}

// CommandBuilder constructs argv-only commands without shell interpolation.
type CommandBuilder struct {
	binary string
	args   []string
}

func NewCommandBuilder(binary string) *CommandBuilder {
	return &CommandBuilder{binary: strings.TrimSpace(binary)}
}

func (b *CommandBuilder) Arg(values ...string) *CommandBuilder {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			b.args = append(b.args, trimmed)
		}
	}
	return b
}

func (b *CommandBuilder) Build() (Command, error) {
	if b == nil || b.binary == "" {
		return Command{}, fmt.Errorf("%w: command binary is required", provider.ErrInvalidArgument)
	}
	argv := make([]string, 0, len(b.args)+1)
	argv = append(argv, b.binary)
	argv = append(argv, b.args...)
	return Command{Argv: argv}, nil
}

// UnavailableDriver explicitly reports that no usable NVIDIA GPU is present.
type UnavailableDriver struct {
	reason string
}

func NewUnavailableDriver(reason string) *UnavailableDriver {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "nvidia GPU is unavailable"
	}
	return &UnavailableDriver{reason: reason}
}

func (d *UnavailableDriver) ID() string { return "unavailable" }

func (d *UnavailableDriver) Probe(context.Context) (*ProbeResult, error) {
	return &ProbeResult{
		Available:    false,
		Reason:       d.reason,
		Devices:      nil,
		Capabilities: unavailableCapabilities(d.reason),
	}, nil
}

func (d *UnavailableDriver) ExecuteAction(_ context.Context, _ *DriverState, action *tgsrlv1.Action) (*ActionExecution, error) {
	return nil, unavailableActionError(action, d.reason)
}

func (d *UnavailableDriver) Reconcile(_ context.Context, _ *DriverState, _ *tgsrlv1.PlacementPlan) (*ReconcileState, error) {
	return &ReconcileState{
		Healthy:      false,
		Reason:       d.reason,
		Devices:      unavailableDevices(d.reason),
		Capabilities: unavailableCapabilities(d.reason),
	}, nil
}

// LocalDriver probes the local host conservatively and only reports NVIDIA
// capability when command discovery and device enumeration succeed.
type LocalDriver struct {
	runner Runner
	now    func() time.Time
}

func NewLocalDriver() *LocalDriver {
	return newLocalDriver(execRunner{}, time.Now)
}

func NewLocalDriverWithRunner(runner Runner) *LocalDriver {
	return newLocalDriver(runner, time.Now)
}

func newLocalDriver(runner Runner, now func() time.Time) *LocalDriver {
	if runner == nil {
		runner = execRunner{}
	}
	if now == nil {
		now = time.Now
	}
	return &LocalDriver{runner: runner, now: now}
}

func (d *LocalDriver) ID() string { return "local" }

func (d *LocalDriver) Probe(ctx context.Context) (*ProbeResult, error) {
	observedAt := d.now().UTC()
	command, err := NewCommandBuilder(commandNvidiaSMI).Arg("--query-gpu=index,uuid,memory.total,name,driver_version", "--format=csv,noheader,nounits").Build()
	if err != nil {
		return nil, err
	}
	output, err := d.runner.Run(ctx, command)
	if err != nil {
		reason := "nvidia probe failed"
		var commandErr *CommandError
		if errors.Is(err, exec.ErrNotFound) || errors.As(err, &commandErr) && commandErr.Kind == CommandFailureUnavailable {
			reason = "nvidia-smi not found"
		}
		return &ProbeResult{
			Available:    false,
			Reason:       reason,
			Capabilities: unavailableCapabilities(reason),
		}, nil
	}
	devices, driverVersion, parseErr := parseProbeDevices(output)
	if parseErr != nil {
		reason := "invalid nvidia probe output: " + parseErr.Error()
		return &ProbeResult{
			Available:    false,
			Reason:       reason,
			Capabilities: unavailableCapabilities(reason),
		}, nil
	}
	if len(devices) == 0 {
		return &ProbeResult{
			Available:    false,
			Reason:       "no usable nvidia devices discovered",
			Capabilities: unavailableCapabilities("no usable nvidia devices discovered"),
		}, nil
	}
	capabilities := localCapabilities(driverVersion, observedAt)
	capabilities.Attributes["probe"] = "local"
	for _, device := range devices {
		device.Capabilities = cloneCapabilities(capabilities)
	}
	return &ProbeResult{
		Available:    true,
		Devices:      devices,
		Capabilities: capabilities,
	}, nil
}

func (d *LocalDriver) ExecuteAction(_ context.Context, _ *DriverState, action *tgsrlv1.Action) (*ActionExecution, error) {
	if err := validateDriverAction(action); err != nil {
		return nil, err
	}
	return nil, unsupportedLocalActionError(action)
}

func (d *LocalDriver) Reconcile(ctx context.Context, _ *DriverState, _ *tgsrlv1.PlacementPlan) (*ReconcileState, error) {
	probe, err := d.Probe(ctx)
	if err != nil {
		return nil, err
	}
	devices := cloneDevices(probe.Devices)
	if !probe.Available && len(devices) == 0 {
		devices = unavailableDevices(probe.Reason)
	}
	return &ReconcileState{
		Healthy:      probe.Available,
		Reason:       probe.Reason,
		Devices:      devices,
		Capabilities: cloneCapabilities(probe.Capabilities),
	}, nil
}

func localCapabilities(driverVersion string, observedAt time.Time) *tgsrlv1.CapabilitySet {
	capabilities := defaultCapabilities(true)
	capabilities.SupportedActions = nil
	capabilities.Attributes["execution_mode"] = "conservative"
	capabilities.MeasuredAt = timestamppb.New(observedAt.UTC())
	// This package has no authoritative provider build-version source, so it
	// deliberately advertises only the NVIDIA driver version observed from
	// nvidia-smi instead of inventing a provider semantic version.
	capabilities.ComponentVersions = []*tgsrlv1.ComponentVersion{{
		Kind:       tgsrlv1.ComponentKind_COMPONENT_KIND_CUDA_DRIVER,
		Name:       "nvidia-driver",
		Version:    driverVersion,
		ObservedAt: timestamppb.New(observedAt.UTC()),
		Source:     commandNvidiaSMI,
		Revision:   capabilities.GetRevision(),
		Attributes: map[string]string{
			"query_field": "driver_version",
			"scope":       "nvidia-kernel-driver",
		},
	}}
	return capabilities
}

func unavailableCapabilities(reason string) *tgsrlv1.CapabilitySet {
	capabilities := defaultCapabilities(false)
	capabilities.SupportedActions = nil
	if strings.TrimSpace(reason) != "" {
		capabilities.Attributes["reason"] = reason
	}
	return capabilities
}

func validateDriverAction(action *tgsrlv1.Action) error {
	if action == nil {
		return fmt.Errorf("%w: action is required", provider.ErrInvalidArgument)
	}
	return nil
}

func unavailableActionError(action *tgsrlv1.Action, reason string) error {
	if strings.TrimSpace(reason) == "" {
		reason = "nvidia GPU is unavailable"
	}
	return &provider.Error{
		Code:      ErrorCodeUnavailable,
		Message:   reason,
		PlanID:    action.GetPlanId(),
		ActionID:  action.GetActionId(),
		SandboxID: actionSandboxID(action),
		Cause:     provider.ErrFailedPrecondition,
	}
}

func unsupportedLocalActionError(action *tgsrlv1.Action) error {
	message := "nvidia action is not supported by the generic local driver"
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		message = "bind/release remain infrastructure-owned and the provider will not run destructive GPU reset or host attachment operations"
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionType_ACTION_TYPE_RESIZE:
		message = "set_share/resize depend on MIG or MPS specific infrastructure and are unsupported by the generic nvidia provider"
	case tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY:
		message = "set_priority is not a stable per-sandbox generic nvidia primitive"
	case tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionType_ACTION_TYPE_RESUME, tgsrlv1.ActionType_ACTION_TYPE_SLEEP, tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		message = "runtime pause/resume/offload control is not a generic nvidia driver capability"
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND, tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		message = "rebind/recreate require infrastructure-specific orchestration outside the generic nvidia driver"
	case tgsrlv1.ActionType_ACTION_TYPE_UNKNOWN:
		message = "unknown nvidia action"
	}
	return &provider.Error{
		Code:      provider.ErrorCodeUnsupported,
		Message:   message,
		PlanID:    action.GetPlanId(),
		ActionID:  action.GetActionId(),
		SandboxID: actionSandboxID(action),
		Cause:     provider.ErrUnsupported,
	}
}

func normalizeDriverError(action *tgsrlv1.Action, err error) error {
	if err == nil {
		return nil
	}
	var providerError *provider.Error
	if errors.As(err, &providerError) {
		return err
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled), errors.Is(err, provider.ErrDeadlineExceeded):
		return &provider.Error{
			Code:      provider.ErrorCodeDeadlineExceeded,
			Message:   err.Error(),
			PlanID:    action.GetPlanId(),
			ActionID:  action.GetActionId(),
			SandboxID: actionSandboxID(action),
			Cause:     errors.Join(provider.ErrDeadlineExceeded, err),
		}
	case errors.Is(err, provider.ErrUnsupported):
		return &provider.Error{
			Code:      provider.ErrorCodeUnsupported,
			Message:   err.Error(),
			PlanID:    action.GetPlanId(),
			ActionID:  action.GetActionId(),
			SandboxID: actionSandboxID(action),
			Cause:     provider.ErrUnsupported,
		}
	case errors.Is(err, provider.ErrInvalidArgument):
		return &provider.Error{
			Code:      provider.ErrorCodeInvalidArgument,
			Message:   err.Error(),
			PlanID:    action.GetPlanId(),
			ActionID:  action.GetActionId(),
			SandboxID: actionSandboxID(action),
			Cause:     provider.ErrInvalidArgument,
		}
	case errors.Is(err, provider.ErrNotFound):
		return &provider.Error{
			Code:      provider.ErrorCodeNotFound,
			Message:   err.Error(),
			PlanID:    action.GetPlanId(),
			ActionID:  action.GetActionId(),
			SandboxID: actionSandboxID(action),
			Cause:     provider.ErrNotFound,
		}
	default:
		return &provider.Error{
			Code:      provider.ErrorCodeFailedPrecondition,
			Message:   err.Error(),
			PlanID:    action.GetPlanId(),
			ActionID:  action.GetActionId(),
			SandboxID: actionSandboxID(action),
			Cause:     provider.ErrFailedPrecondition,
		}
	}
}

func defaultCapabilities(available bool) *tgsrlv1.CapabilitySet {
	capabilities := &tgsrlv1.CapabilitySet{
		Names:            []string{CapabilityName},
		Algorithms:       []string{"grpo", "ppo"},
		RolloutModes:     []string{"fully_async", "partially_async", "sync"},
		Source:           ProviderID,
		Revision:         capabilityObservationRevision,
		SupportedActions: supportedActionNames(),
		Limits:           map[string]float64{"max_share": 1},
		Attributes:       map[string]string{"driver": "nvidia"},
	}
	if available {
		capabilities.Attributes["probe"] = "available"
	} else {
		capabilities.Attributes["probe"] = "unavailable"
	}
	return capabilities
}

func supportedActionNames() []string {
	values := make([]string, 0, len(actionNames))
	for _, name := range actionNames {
		values = append(values, name)
	}
	sort.Strings(values)
	return values
}

var actionNames = map[tgsrlv1.ActionType]string{
	tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE:    "set_share",
	tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY: "set_priority",
	tgsrlv1.ActionType_ACTION_TYPE_PAUSE:        "pause",
	tgsrlv1.ActionType_ACTION_TYPE_RESUME:       "resume",
	tgsrlv1.ActionType_ACTION_TYPE_SLEEP:        "sleep",
	tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:      "offload",
	tgsrlv1.ActionType_ACTION_TYPE_REBIND:       "rebind",
	tgsrlv1.ActionType_ACTION_TYPE_RECREATE:     "recreate",
	tgsrlv1.ActionType_ACTION_TYPE_BIND:         "bind",
	tgsrlv1.ActionType_ACTION_TYPE_RELEASE:      "release",
	tgsrlv1.ActionType_ACTION_TYPE_RESIZE:       "resize",
}

func cloneCapabilities(capabilities *tgsrlv1.CapabilitySet) *tgsrlv1.CapabilitySet {
	if capabilities == nil {
		return nil
	}
	return proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)
}

func cloneDevices(devices []*tgsrlv1.Device) []*tgsrlv1.Device {
	clones := make([]*tgsrlv1.Device, len(devices))
	for i, device := range devices {
		if device != nil {
			clones[i] = proto.Clone(device).(*tgsrlv1.Device)
		}
	}
	return clones
}

func cloneProbeResult(result *ProbeResult) *ProbeResult {
	if result == nil {
		return nil
	}
	return &ProbeResult{
		Available:              result.Available,
		Reason:                 result.Reason,
		Devices:                cloneDevices(result.Devices),
		Capabilities:           cloneCapabilities(result.Capabilities),
		Sandboxes:              cloneSandboxSlice(result.Sandboxes),
		SandboxesAuthoritative: result.SandboxesAuthoritative,
	}
}

func parseProbeDevices(output []byte) ([]*tgsrlv1.Device, string, error) {
	reader := csv.NewReader(strings.NewReader(strings.TrimSpace(string(output))))
	reader.TrimLeadingSpace = true
	records, err := reader.ReadAll()
	if err != nil {
		return nil, "", err
	}
	devices := make([]*tgsrlv1.Device, 0, len(records))
	driverVersion := ""
	for index, record := range records {
		if len(record) != 5 {
			return nil, "", fmt.Errorf("row %d has %d fields, want 5", index+1, len(record))
		}
		deviceIndex := strings.TrimSpace(record[0])
		uuid := strings.TrimSpace(record[1])
		memoryMB, _ := strconv.ParseInt(strings.TrimSpace(record[2]), 10, 64)
		name := strings.TrimSpace(record[3])
		observedDriverVersion := strings.TrimSpace(record[4])
		if deviceIndex == "" || uuid == "" {
			return nil, "", fmt.Errorf("row %d is missing index or uuid", index+1)
		}
		if !isAuthoritativeDriverVersion(observedDriverVersion) {
			return nil, "", fmt.Errorf("row %d has unavailable driver_version", index+1)
		}
		if driverVersion == "" {
			driverVersion = observedDriverVersion
		} else if observedDriverVersion != driverVersion {
			return nil, "", fmt.Errorf("conflicting nvidia driver versions %q and %q", driverVersion, observedDriverVersion)
		}
		memoryBytes := uint64(memoryMB) << 20
		devices = append(devices, &tgsrlv1.Device{
			DeviceId:     "nvidia-gpu-" + deviceIndex,
			Kind:         tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
			Health:       tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:     &tgsrlv1.ResourceVector{CpuMillis: ^uint64(0), AcceleratorUnits: 1, MemoryBytes: memoryBytes, EphemeralStorageBytes: ^uint64(0), NetworkBandwidthBps: ^uint64(0)},
			Allocatable:  &tgsrlv1.ResourceVector{CpuMillis: ^uint64(0), AcceleratorUnits: 1, MemoryBytes: memoryBytes, EphemeralStorageBytes: ^uint64(0), NetworkBandwidthBps: ^uint64(0)},
			Capabilities: cloneCapabilities(defaultCapabilities(true)),
			Labels: map[string]string{
				"provider": ProviderID,
				"uuid":     uuid,
				"name":     name,
			},
		})
	}
	return devices, driverVersion, nil
}

func isAuthoritativeDriverVersion(version string) bool {
	switch strings.ToLower(strings.TrimSpace(version)) {
	case "", "n/a", "not available", "not supported", "unknown":
		return false
	default:
		return true
	}
}
