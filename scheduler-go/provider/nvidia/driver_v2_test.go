package nvidia

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	base "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var v2Now = time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)

type FakeCommandResponse struct {
	MatchArgv []string
	Result    CommandResult
	Err       error
	Delay     time.Duration
}

type FakeCommandExecutor struct {
	mu        sync.Mutex
	responses []FakeCommandResponse
	commands  []Command
}

func NewFakeCommandExecutor(responses ...FakeCommandResponse) *FakeCommandExecutor {
	return &FakeCommandExecutor{responses: append([]FakeCommandResponse(nil), responses...)}
}

func (e *FakeCommandExecutor) Enqueue(responses ...FakeCommandResponse) {
	e.mu.Lock()
	e.responses = append(e.responses, responses...)
	e.mu.Unlock()
}

func (e *FakeCommandExecutor) Commands() []Command {
	e.mu.Lock()
	defer e.mu.Unlock()
	commands := make([]Command, len(e.commands))
	for index, command := range e.commands {
		commands[index] = cloneCommand(command)
	}
	return commands
}

func (e *FakeCommandExecutor) Execute(ctx context.Context, command Command) (CommandResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateCommand(command); err != nil {
		return CommandResult{ExitCode: -1}, err
	}
	e.mu.Lock()
	e.commands = append(e.commands, cloneCommand(command))
	if len(e.responses) == 0 {
		e.mu.Unlock()
		err := &CommandError{Kind: CommandFailureUnavailable, Argv: append([]string(nil), command.Argv...), ExitCode: -1, Stderr: "fake response is not configured", Cause: exec.ErrNotFound}
		return CommandResult{ExitCode: -1}, err
	}
	response := e.responses[0]
	e.responses = e.responses[1:]
	e.mu.Unlock()
	if len(response.MatchArgv) > 0 && !equalStringSlices(response.MatchArgv, command.Argv) {
		return CommandResult{ExitCode: -1}, &CommandError{Kind: CommandFailureInvalid, Argv: append([]string(nil), command.Argv...), ExitCode: -1, Stderr: fmt.Sprintf("unexpected argv %q, want %q", command.Argv, response.MatchArgv)}
	}
	if response.Delay > 0 {
		timer := time.NewTimer(response.Delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return CommandResult{ExitCode: -1}, classifyCommandError(ctx, command, CommandResult{ExitCode: -1}, ctx.Err())
		case <-timer.C:
		}
	}
	result := cloneCommandResult(response.Result)
	if response.Err != nil {
		var commandErr *CommandError
		if errors.As(response.Err, &commandErr) {
			return result, response.Err
		}
		return result, classifyCommandError(ctx, command, result, response.Err)
	}
	if result.ExitCode != 0 {
		return result, classifyCommandError(ctx, command, result, errors.New("command exited unsuccessfully"))
	}
	return result, nil
}

func TestClassifyCommandErrorPreservesOperationalFailureKinds(t *testing.T) {
	tests := []struct {
		name   string
		ctx    context.Context
		result CommandResult
		err    error
		want   CommandFailureKind
	}{
		{name: "missing command", ctx: context.Background(), result: CommandResult{ExitCode: -1}, err: &exec.Error{Name: "tool", Err: exec.ErrNotFound}, want: CommandFailureUnavailable},
		{name: "permission denied", ctx: context.Background(), result: CommandResult{ExitCode: 1, Stderr: []byte("permission denied")}, err: errors.New("exit status 1"), want: CommandFailurePermission},
		{name: "ordinary exit", ctx: context.Background(), result: CommandResult{ExitCode: 7}, err: errors.New("exit status 7"), want: CommandFailureExit},
		{name: "deadline exceeded", ctx: context.Background(), result: CommandResult{ExitCode: -1}, err: context.DeadlineExceeded, want: CommandFailureTimeout},
		{name: "canceled", ctx: canceledContext(), result: CommandResult{ExitCode: -1}, err: context.Canceled, want: CommandFailureCanceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var commandErr *CommandError
			err := classifyCommandError(test.ctx, Command{Argv: []string{"tool"}}, test.result, test.err)
			if !errors.As(err, &commandErr) || commandErr.Kind != test.want {
				t.Fatalf("classifyCommandError() = %v, want %s", err, test.want)
			}
		})
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestCommandInventoryDiscoveryUsesStableUUIDIdentityAndTopology(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("3, GPU-aaaa, 00000000:AF:00.0, 81920, NVIDIA H100, 550.54.14, Disabled, Default\n"), ExitCode: 0}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("GPU3 X 0-31 1\n"), ExitCode: 0}},
	)
	backend := NewCommandInventoryBackend(executor, time.Second, func() time.Time { return v2Now })
	inventory, err := backend.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	partition := &PartitionSnapshot{Mode: PartitionModeMPS, Available: true, ObservedAt: v2Now, SupportedActions: []tgsrlv1.ActionType{tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionType_ACTION_TYPE_RESIZE}, Partitions: []Partition{{ID: "mps-GPU-aaaa", ParentUUID: "GPU-aaaa", Share: 1}}}
	capabilities := v2Capabilities(inventory, partition, nil, nil, false)
	devices := inventoryDevices(inventory, partition, capabilities)
	if len(devices) != 1 || devices[0].GetDeviceId() != "GPU-aaaa" {
		t.Fatalf("stable devices = %+v", devices)
	}
	if devices[0].GetLabels()["index"] != "3" || devices[0].GetLabels()["pci_bus_id"] != "00000000:AF:00.0" || devices[0].GetLabels()["partition_mode"] != "mps" || devices[0].GetLabels()["topology.cpu_affinity"] != "0-31" || devices[0].GetLabels()["numa_node"] != "1" {
		t.Fatalf("topology labels = %+v", devices[0].GetLabels())
	}
	commands := executor.Commands()
	if len(commands) != 2 || !strings.Contains(commands[0].Argv[1], "pci.bus_id") || !strings.Contains(commands[0].Argv[1], "mig.mode.current") || commands[1].Argv[1] != "topo" {
		t.Fatalf("inventory commands = %+v", commands)
	}
}

func TestTopologyDiscoveryUsesReportedNonContiguousHeaders(t *testing.T) {
	snapshot := &InventorySnapshot{Devices: []InventoryDevice{{UUID: "GPU-a", Index: "1", Topology: map[string]string{}}, {UUID: "GPU-b", Index: "3", Topology: map[string]string{}}}}
	output := []byte("        GPU1 GPU3 CPU Affinity NUMA Affinity\nGPU1    X    NV4  0-31         0\nGPU3    NV4  X    32-63        1\n")
	if err := applyTopology(snapshot, output); err != nil {
		t.Fatal(err)
	}
	if snapshot.Devices[0].Topology["link.GPU-b"] != "NV4" || snapshot.Devices[1].Topology["link.GPU-a"] != "NV4" || snapshot.Devices[1].NUMANode != "1" {
		t.Fatalf("topology = %+v", snapshot.Devices)
	}
}

func TestCommandInventoryRejectsInvalidCapacityAndDuplicateIdentity(t *testing.T) {
	tests := []struct {
		name   string
		output string
		want   string
	}{
		{name: "invalid memory", output: "0, GPU-a, pci-a, -1, H100, 550.1, Disabled, Default", want: "invalid memory.total"},
		{name: "duplicate UUID", output: "0, GPU-a, pci-a, 1024, H100, 550.1, Disabled, Default\n1, GPU-a, pci-b, 1024, H100, 550.1, Disabled, Default", want: "duplicate uuid"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := NewFakeCommandExecutor(FakeCommandResponse{Result: CommandResult{Stdout: []byte(test.output)}})
			_, err := NewCommandInventoryBackend(executor, time.Second, func() time.Time { return v2Now }).Discover(context.Background())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Discover() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBackendHandshakeRequiresIdentityVersionAndSafetyFeatures(t *testing.T) {
	supported := map[string]tgsrlv1.ActionType{"bind": tgsrlv1.ActionType_ACTION_TYPE_BIND}
	if _, err := parseBackendHandshake([]byte("other,1,bind"), "tgsrl-nvidia-binding", supported); err == nil {
		t.Fatal("identity mismatch was accepted")
	}
	if _, err := parseBackendHandshake([]byte("tgsrl-nvidia-binding,2,bind"), "tgsrl-nvidia-binding", supported); err == nil {
		t.Fatal("protocol version mismatch was accepted")
	}
	handshake, err := parseBackendHandshake([]byte("tgsrl-nvidia-binding,1,bind,durable_receipts,generation_fence,idempotency"), "tgsrl-nvidia-binding", supported)
	if err != nil || len(handshake.actions) != 1 || !handshake.features["durable_receipts"] || !handshake.features["generation_fence"] || !handshake.features["idempotency"] {
		t.Fatalf("valid handshake = (%+v, %v)", handshake, err)
	}
}

func TestCommandRuntimeBackendDiscoversAuthoritativeSandboxState(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("tgsrl-nvidia-runtime,1,pause,resume,generation_fence,idempotency,durable_receipts,safe_point,checkpoint,reload,readiness")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("sandbox-a,4,paused,true,false,7\n")}},
	)
	status, err := NewCommandRuntimeBackend(executor, time.Second).Discover(context.Background())
	if err != nil || !status.Available || !status.SandboxesAuthoritative || len(status.Sandboxes) != 1 {
		t.Fatalf("Discover() = (%+v, %v)", status, err)
	}
	sandbox := status.Sandboxes[0]
	if sandbox.State != base.SandboxStatePaused || sandbox.Generation != 4 || !sandbox.SafePoint || sandbox.Offloaded || sandbox.Priority != 7 {
		t.Fatalf("runtime sandbox = %+v", sandbox)
	}
}

func TestCommandRuntimeBackendDiscoversFullAndMultiDeviceBindings(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("tgsrl-nvidia-runtime,1,pause,resume,generation_fence,idempotency,durable_receipts,safe_point,checkpoint,reload,readiness")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("sandbox-a,4,running,false,false,7,binding-a,GPU-a;GPU-b,1\n")}},
	)
	status, err := NewCommandRuntimeBackend(executor, time.Second).Discover(context.Background())
	if err != nil || len(status.Sandboxes) != 1 {
		t.Fatalf("Discover() = (%+v, %v)", status, err)
	}
	if got := status.Sandboxes[0].Binding.GetDeviceIds(); !reflect.DeepEqual(got, []string{"GPU-a", "GPU-b"}) {
		t.Fatalf("device IDs = %v", got)
	}
}

func TestCommandRuntimeBackendPassesReceiptAndStateEnvironment(t *testing.T) {
	executor := NewFakeCommandExecutor(FakeCommandResponse{Result: CommandResult{}})
	backend := NewCommandRuntimeBackendWithConfig(executor, time.Second, "runtime-helper", "/state/runtime.json")
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_PAUSE)
	receipt := &HelperReceiptContext{StepIndex: 1, ExpectedActions: 3, TransactionGeneration: 7, PlanDigest: "sha256:plan", CommandDigest: "sha256:action"}
	result, err := backend.Apply(context.Background(), BackendActionRequest{Action: action, Receipt: receipt})
	if err != nil || result == nil {
		t.Fatalf("Apply() = (%+v, %v)", result, err)
	}
	commands := executor.Commands()
	if len(commands) != 1 {
		t.Fatalf("commands = %+v", commands)
	}
	joined := strings.Join(commands[0].Argv, " ")
	for _, expected := range []string{"runtime-helper pause", "--recoverable", "--step-index 1", "--expected-actions 3", "--transaction-generation 7", "--plan-digest sha256:plan", "--command-digest sha256:action", "--action-id action-a", "--plan-id plan-a"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("argv %q missing %q", joined, expected)
		}
	}
	if commands[0].Env["TGSRL_NVIDIA_RUNTIME_STATE"] != "/state/runtime.json" {
		t.Fatalf("environment = %+v", commands[0].Env)
	}
}

func TestCommandRuntimeBackendRecoversDurableReceipts(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("tgsrl-nvidia-runtime,1,pause,resume,sleep,offload,durable_receipts,generation_fence,idempotency,safe_point,checkpoint,reload,readiness\n")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("0,1,false,plan-digest,action-a,plan-a,key-a,sandbox-a,7,4,true,,,digest-a\n")}},
	)
	receipts, err := NewCommandRuntimeBackend(executor, time.Second).DiscoverReceipts(context.Background())
	if err != nil || len(receipts) != 1 || receipts[0].Generation != 7 || receipts[0].ActionGeneration != 4 || !receipts[0].Succeeded {
		t.Fatalf("DiscoverReceipts() = (%+v, %v)", receipts, err)
	}
}

func TestCommandRuntimeBackendMarksTimeoutAmbiguous(t *testing.T) {
	executor := NewFakeCommandExecutor(FakeCommandResponse{Err: &CommandError{Kind: CommandFailureTimeout, ExitCode: -1, Cause: context.DeadlineExceeded}})
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_PAUSE)
	result, err := NewCommandRuntimeBackend(executor, time.Second).Apply(context.Background(), BackendActionRequest{Action: action})
	if err == nil || result == nil || !result.MutationMayHaveApplied {
		t.Fatalf("Apply() = (%+v, %v)", result, err)
	}
}

func TestLegacyLocalDriverKeepsReadOnlyBehavior(t *testing.T) {
	runner := &fakeNvidiaSMIRunner{output: []byte("3, GPU-stable, 81920, NVIDIA H100, 550.54.14")}
	driver := newLocalDriver(runner, func() time.Time { return v2Now })
	probe, err := driver.Probe(context.Background())
	if err != nil || !probe.Available || len(probe.Devices) != 1 {
		t.Fatalf("Probe() = (%+v, %v)", probe, err)
	}
	if probe.Devices[0].GetDeviceId() != "nvidia-gpu-3" || len(probe.Capabilities.GetSupportedActions()) != 0 {
		t.Fatalf("legacy discovery = %+v", probe)
	}
	if _, err := driver.ExecuteAction(context.Background(), &DriverState{}, v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)); !errors.Is(err, base.ErrUnsupported) {
		t.Fatalf("legacy ExecuteAction() error = %v, want unsupported", err)
	}
}

func TestMPSBackendDiscoversDaemonAndAppliesDynamicShare(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("100\n")}},
		FakeCommandResponse{Result: CommandResult{}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("38\n")}},
	)
	backend := NewMPSBackend(executor, time.Second, "/run/mps", "/var/log/mps")
	inventory := v2Inventory(false)
	discovery, err := backend.Discover(context.Background(), inventory)
	if err != nil || !discovery.Available || len(discovery.Partitions) != 1 {
		t.Fatalf("Discover() = (%+v, %v)", discovery, err)
	}
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)
	action.Share = 0.375
	result, err := backend.Apply(context.Background(), BackendActionRequest{Action: action, Partitions: discovery, Binding: &DiscoveredBinding{SandboxID: "sandbox-a", Generation: 1, ServerPID: 4242}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Commands) != 2 || result.Commands[0].Argv[0] != commandNvidiaMPSControl || string(result.Commands[0].Stdin) != "set_active_thread_percentage 4242 38\n" || string(result.Commands[1].Stdin) != "get_active_thread_percentage 4242\n" {
		t.Fatalf("MPS commands = %+v", result.Commands)
	}
	if result.ObservedShare == nil || *result.ObservedShare != 0.38 {
		t.Fatalf("MPS observed share = %v, want 0.38", result.ObservedShare)
	}
	commands := executor.Commands()
	if len(commands) != 3 || commands[1].Env["CUDA_MPS_PIPE_DIRECTORY"] != "/run/mps" || commands[2].Env["CUDA_MPS_PIPE_DIRECTORY"] != "/run/mps" {
		t.Fatalf("recorded MPS commands = %+v", commands)
	}
	profiles := backend.Profiles()
	if len(profiles) != 1 || profiles[0].SandboxID != "sandbox-a" || profiles[0].ActiveThreadPercentage != 38 {
		t.Fatalf("MPS profiles = %+v", profiles)
	}
}

func TestMPSBackendFailsWhenShareReadbackDoesNotMatch(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("25\n")}},
	)
	backend := NewMPSBackend(executor, time.Second, "/run/mps", "/var/log/mps")
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)
	action.Share = 0.5

	result, err := backend.Apply(context.Background(), BackendActionRequest{
		Action:     action,
		Partitions: v2MPSPartition().Snapshot,
		Binding:    &DiscoveredBinding{SandboxID: "sandbox-a", Generation: 1, ServerPID: 4242},
	})
	if !errors.Is(err, base.ErrFailedPrecondition) || result == nil || result.ObservedShare != nil || !result.MutationMayHaveApplied {
		t.Fatalf("MPS readback mismatch = result:%+v err:%v", result, err)
	}
	if profiles := backend.Profiles(); len(profiles) != 0 {
		t.Fatalf("mismatched readback was recorded as a profile: %+v", profiles)
	}
}

func TestProviderCompensatesAmbiguousShareReadback(t *testing.T) {
	partition := v2MPSPartition()
	partition.ApplyFunc = func(_ context.Context, request BackendActionRequest) (*BackendActionResult, error) {
		if strings.HasSuffix(request.Action.GetActionId(), "-rollback") {
			share := request.Action.GetShare()
			return &BackendActionResult{Detail: "rollback verified", ObservedShare: &share}, nil
		}
		return &BackendActionResult{Detail: "readback failed", MutationMayHaveApplied: true}, errors.New("readback failed")
	}
	providerImpl, err := New(
		WithNow(func() time.Time { return v2Now }),
		WithDriverV2(LocalDriverV2Options{
			Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}},
			Partition: partition,
			Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{
				Available:           true,
				SupportsMPSProfiles: true,
				Bindings: []DiscoveredBinding{{
					SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 1,
					DeviceIDs: []string{"GPU-aaaa"}, Share: 0.25, ServerPID: 4242,
				}},
			}},
			Now: func() time.Time { return v2Now },
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)
	action.Share = 0.75
	action.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}

	result, err := providerImpl.ExecuteAction(context.Background(), action)
	if err == nil || result == nil || !result.GetRollbackAttempted() || result.GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK {
		t.Fatalf("ambiguous share result = %+v, err = %v", result, err)
	}
	sandbox, getErr := providerImpl.GetSandbox(context.Background(), "sandbox-a")
	if getErr != nil || sandbox.Share != 0.25 {
		t.Fatalf("sandbox after compensation = %+v, err = %v", sandbox, getErr)
	}
}

func TestMPSBackendStartsMissingDaemonAndFailsClosedWhenStartFails(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Err: errors.New("exit status 1"), Result: CommandResult{ExitCode: 1, Stderr: []byte("MPS control daemon is not running")}},
		FakeCommandResponse{Err: errors.New("start failed"), Result: CommandResult{ExitCode: 1, Stderr: []byte("permission denied")}},
	)
	discovery, err := NewMPSBackend(executor, time.Second, "", "").Discover(context.Background(), v2Inventory(false))
	if err == nil || discovery.Available || !strings.Contains(discovery.Reason, "permission denied") {
		t.Fatalf("Discover() = (%+v, %v), want explicit unavailable", discovery, err)
	}
	commands := executor.Commands()
	if len(commands) != 2 || len(commands[1].Argv) != 2 || commands[1].Argv[1] != "-d" {
		t.Fatalf("MPS startup commands = %+v", commands)
	}
}

func TestMPSBackendDryRunNeverStartsMissingDaemon(t *testing.T) {
	executor := NewFakeCommandExecutor(FakeCommandResponse{Err: errors.New("exit status 1"), Result: CommandResult{ExitCode: 1, Stderr: []byte("MPS control daemon is not running")}})
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Executor: executor, Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true}}, DryRun: true, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	probe, err := driver.Probe(context.Background())
	if err != nil || probe.Available || !strings.Contains(probe.Reason, "dry-run forbids startup") {
		t.Fatalf("Probe() = (%+v, %v)", probe, err)
	}
	if commands := executor.Commands(); len(commands) != 1 {
		t.Fatalf("dry-run MPS commands = %+v, want probe only", commands)
	}
}

func TestMPSBackendMissingBinaryIsUnavailableWithoutStartupRetry(t *testing.T) {
	executor := NewFakeCommandExecutor(FakeCommandResponse{Err: &CommandError{Kind: CommandFailureUnavailable, ExitCode: -1, Cause: exec.ErrNotFound}})
	discovery, err := NewMPSBackend(executor, time.Second, "", "").Discover(context.Background(), v2Inventory(false))
	if err == nil || discovery.Available {
		t.Fatalf("Discover() = (%+v, %v), want unavailable", discovery, err)
	}
	if commands := executor.Commands(); len(commands) != 1 {
		t.Fatalf("missing binary commands = %+v, want one probe", commands)
	}
}

func TestMPSBackendStartsAndVerifiesMissingDaemon(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Err: errors.New("exit status 1"), Result: CommandResult{ExitCode: 1, Stderr: []byte("MPS control daemon is not running")}},
		FakeCommandResponse{Result: CommandResult{}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("100\n")}},
	)
	discovery, err := NewMPSBackend(executor, time.Second, "", "").Discover(context.Background(), v2Inventory(false))
	if err != nil || !discovery.Available {
		t.Fatalf("Discover() = (%+v, %v)", discovery, err)
	}
	if commands := executor.Commands(); len(commands) != 3 || commands[1].Argv[1] != "-d" {
		t.Fatalf("MPS startup commands = %+v", commands)
	}
}

func TestMIGBackendRequiresExplicitModeAndL4Slow(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("GPU 0: H100 (UUID: GPU-aaaa)\n  MIG 1g.10gb Device 0: (UUID: MIG-aaaa/1/0)\n")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("MIG-aaaa/1/0, 1g.10gb, 10240\n")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("tgsrl-nvidia-mig,1,rebind,recreate,durable_receipts,generation_fence,idempotency,safe_point,checkpoint,stop,restore,readiness")}},
	)
	backend := NewMIGBackend(executor, time.Second)
	discovery, err := backend.Discover(context.Background(), v2Inventory(true))
	if err != nil || !discovery.Available || len(discovery.Partitions) != 1 || discovery.Partitions[0].ID != "MIG-aaaa/1/0" {
		t.Fatalf("MIG Discover() = (%+v, %v)", discovery, err)
	}
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_REBIND)
	action.Level = tgsrlv1.ActionLevel_ACTION_LEVEL_L1
	action.TickKind = tgsrlv1.TickKind_TICK_KIND_FAST
	if _, err := backend.Apply(context.Background(), BackendActionRequest{Action: action, Partitions: discovery}); err == nil || !strings.Contains(err.Error(), "slow-tick L4") {
		t.Fatalf("MIG Apply() error = %v, want explicit slow/L4 rejection", err)
	}
	disabled, err := backend.Discover(context.Background(), v2Inventory(false))
	if err != nil || disabled.Available || !strings.Contains(disabled.Reason, "disabled") {
		t.Fatalf("MIG disabled discovery = (%+v, %v)", disabled, err)
	}
}

func TestMIGDiscoveryRequiresCompleteLifecycleTransaction(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("GPU 0: H100 (UUID: GPU-aaaa)\n  MIG 1g.10gb Device 0: (UUID: MIG-aaaa/1/0)\n")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("MIG-aaaa/1/0, 1g.10gb, 10240\n")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("tgsrl-nvidia-mig,1,rebind,recreate,durable_receipts,generation_fence,idempotency")}},
	)

	discovery, err := NewMIGBackend(executor, time.Second).Discover(context.Background(), v2Inventory(true))
	if err != nil {
		t.Fatal(err)
	}
	if discovery.Available || len(discovery.SupportedActions) != 0 || !strings.Contains(discovery.Reason, "transactional lifecycle") {
		t.Fatalf("MIG discovery = %+v, want unavailable without lifecycle transaction", discovery)
	}
}

func TestMIGBackendUsesDiscoveredParentProfileAndRediscoveryResult(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("GPU-aaaa,MIG-new/2/0,1g.10gb,2\n")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("GPU 0: H100 (UUID: GPU-aaaa)\n  MIG 1g.10gb Device 1: (UUID: MIG-new/2/0)\n")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("MIG-new/2/0, 1g.10gb, 10240\n")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("tgsrl-nvidia-mig,1,rebind,recreate,durable_receipts,generation_fence,idempotency,safe_point,checkpoint,stop,restore,readiness")}},
	)
	backend := NewMIGBackendWithConfig(executor, time.Second, "/opt/tgsrl-nvidia-mig", "/state/runtime.json", "/state/binding.json")
	initial := &PartitionSnapshot{Mode: PartitionModeMIG, Available: true, SupportedActions: []tgsrlv1.ActionType{tgsrlv1.ActionType_ACTION_TYPE_REBIND}, Partitions: []Partition{{ID: "MIG-old/1/0", ParentUUID: "GPU-aaaa", Profile: "1g.10gb", MemoryBytes: 10 << 30}, {ID: "MIG-new/2/0", ParentUUID: "GPU-aaaa", Profile: "1g.10gb", MemoryBytes: 10 << 30}}}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(true)}}, PartitionMode: PartitionModeMIG, Partition: backend, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true}}, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	driver.rememberDiscovery(v2Inventory(true), initial)
	driver.rememberBackends(&BindingSnapshot{Available: true, Bindings: []DiscoveredBinding{{SandboxID: "sandbox-a", BindingID: "binding-old", Generation: 1, DeviceIDs: []string{"MIG-old/1/0"}, Share: 1}}}, &RuntimeBackendStatus{})
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_REBIND)
	action.Binding.DeviceIds = []string{"MIG-new/2/0"}
	action.Binding.Resources.MemoryBytes = 10 << 30
	execution, err := driver.ExecuteAction(context.Background(), &DriverState{Revision: 1, Sandboxes: map[string]base.Sandbox{"sandbox-a": {SandboxID: "sandbox-a", Generation: 1}}}, action)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Binding == nil || len(execution.Binding.DeviceIDs) != 1 || execution.Binding.DeviceIDs[0] != "MIG-new/2/0" {
		t.Fatalf("rediscovered binding = %+v", execution.Binding)
	}
	commands := executor.Commands()
	if len(commands) != 4 || commands[0].Argv[0] != "/opt/tgsrl-nvidia-mig" || flagValue(commands[0].Argv, "--sandbox") != "sandbox-a" || flagValue(commands[0].Argv, "--parent-uuid") != "GPU-aaaa" || flagValue(commands[0].Argv, "--source-binding") != "binding-old" || flagValue(commands[0].Argv, "--source-uuid") != "MIG-old/1/0" || flagValue(commands[0].Argv, "--target-uuid") != "MIG-new/2/0" || flagValue(commands[0].Argv, "--profile") != "1g.10gb" || flagValue(commands[0].Argv, "--binding") != "binding-a" || flagValue(commands[0].Argv, "--generation") != "1" || flagValue(commands[0].Argv, "--target-generation") != "2" || commands[0].Env["TGSRL_NVIDIA_RUNTIME_STATE"] != "/state/runtime.json" || commands[0].Env["TGSRL_NVIDIA_BINDING_STATE"] != "/state/binding.json" || containsStringValue(commands[0].Argv, "memory-10240MiB") {
		t.Fatalf("MIG mutation commands = %+v", commands)
	}
}

func TestMIGBackendDoesNotDuplicateRuntimeReceipts(t *testing.T) {
	if _, ok := any(NewMIGBackend(NewFakeCommandExecutor(), time.Second)).(ReceiptBackend); ok {
		t.Fatal("MIG backend must not rediscover receipts already owned by the runtime backend")
	}
}

func TestLocalDriverV2ProbeFailsClosedWithoutGPUOrBindingBackend(t *testing.T) {
	tests := []struct {
		name      string
		inventory *InventorySnapshot
		binding   *BindingSnapshot
		want      string
	}{
		{name: "no GPU", inventory: &InventorySnapshot{ObservedAt: v2Now}, binding: &BindingSnapshot{Available: true}, want: "no usable nvidia devices"},
		{name: "binding unavailable", inventory: v2Inventory(false), binding: &BindingSnapshot{Reason: "binding helper missing"}, want: "binding helper missing"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{test.inventory}}, Partition: v2MPSPartition(), Binding: &FakeBindingBackend{Snapshot: test.binding}, Now: func() time.Time { return v2Now }})
			if err != nil {
				t.Fatal(err)
			}
			probe, err := driver.Probe(context.Background())
			if test.name == "no GPU" {
				if err != nil || probe.Available || !strings.Contains(probe.Reason, test.want) || len(probe.Capabilities.GetSupportedActions()) != 0 {
					t.Fatalf("Probe() = (%+v, %v), want unavailable %q", probe, err, test.want)
				}
				return
			}
			if err != nil || !probe.Available || containsSupportedAction(probe.Capabilities.GetSupportedActions(), "bind") {
				t.Fatalf("Probe() = (%+v, %v), want inventory-only availability", probe, err)
			}
		})
	}
}

func TestLocalDriverV2ExplicitMIGDoesNotFallBackToMPS(t *testing.T) {
	executor := NewFakeCommandExecutor()
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Executor: executor, Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, PartitionMode: PartitionModeMIG, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true}}, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	probe, err := driver.Probe(context.Background())
	if err != nil || probe.Available || !strings.Contains(probe.Reason, "MIG mode is disabled") || probe.Capabilities.GetAttributes()["partition_mode"] != "mig" {
		t.Fatalf("MIG Probe() = (%+v, %v)", probe, err)
	}
	if len(executor.Commands()) != 0 {
		t.Fatalf("disabled MIG unexpectedly invoked commands: %+v", executor.Commands())
	}
}

func TestLocalDriverV2DoesNotAdvertiseMPSActionsWithoutProfileDiscovery(t *testing.T) {
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: v2MPSPartition(), Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	probe, err := driver.Probe(context.Background())
	if err != nil || !probe.Available {
		t.Fatalf("Probe() = (%+v, %v)", probe, err)
	}
	if containsSupportedAction(probe.Capabilities.GetSupportedActions(), "bind") || containsSupportedAction(probe.Capabilities.GetSupportedActions(), "release") {
		t.Fatalf("unconfigured binding actions were advertised: %v", probe.Capabilities.GetSupportedActions())
	}
}

func TestMIGDiscoveryRequiresAuthoritativePartitionCapacity(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("GPU 0: H100 (UUID: GPU-aaaa)\n  MIG 1g.10gb Device 0: (UUID: MIG-aaaa/1/0)\n")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("MIG-other/1/0, 1g.10gb, 10240\n")}},
	)
	discovery, err := NewMIGBackend(executor, time.Second).Discover(context.Background(), v2Inventory(true))
	if err != nil || discovery.Available || !strings.Contains(discovery.Reason, "no authoritative capacity") {
		t.Fatalf("Discover() = (%+v, %v)", discovery, err)
	}
}

func TestLocalDriverV2DryRunAuditIdempotencyAndConflict(t *testing.T) {
	audit := NewInMemoryAuditSink(16)
	partition := v2MPSPartition()
	partition.ApplyFunc = nil
	partition.ApplyResult = &BackendActionResult{Detail: "planned", Commands: []Command{{Argv: []string{"mps-helper", "set-share", "--idempotency-key", "key-a"}, Env: map[string]string{"TOKEN": "secret"}, Stdin: []byte("secret input")}}}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: partition, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, Bindings: []DiscoveredBinding{{SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 1, DeviceIDs: []string{"GPU-aaaa"}, Share: 0.25, ServerPID: 4242}}}}, DryRun: true, Audit: audit, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	if probe, err := driver.Probe(context.Background()); err != nil || !probe.Available {
		t.Fatalf("Probe() = (%+v, %v)", probe, err)
	}
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)
	action.Share = 0.5
	state := &DriverState{Sandboxes: map[string]base.Sandbox{"sandbox-a": {SandboxID: "sandbox-a", Generation: 1}}}
	first, err := driver.ExecuteAction(context.Background(), state, action)
	if err != nil || !first.DryRun || len(first.Commands) != 1 {
		t.Fatalf("ExecuteAction() = (%+v, %v)", first, err)
	}
	second, err := driver.ExecuteAction(context.Background(), state, action)
	if err != nil || !second.DryRun {
		t.Fatalf("duplicate ExecuteAction() = (%+v, %v)", second, err)
	}
	conflict := cloneAction(action)
	conflict.Share = 0.75
	if _, err := driver.ExecuteAction(context.Background(), state, conflict); !errors.Is(err, base.ErrIdempotencyConflict) {
		t.Fatalf("conflicting ExecuteAction() error = %v", err)
	}
	records := audit.Records()
	if len(records) != 2 || records[1].Status != AuditStatusPlanned || !records[1].DryRun || records[1].ActionID != action.GetActionId() || records[1].IdempotencyKey != "<redacted>" {
		t.Fatalf("audit records = %+v", records)
	}
	if got := records[1].Commands[0].Argv[len(records[1].Commands[0].Argv)-1]; got == action.GetIdempotencyKey() {
		t.Fatalf("audit retained idempotency key: %+v", records[1].Commands[0])
	}
}

func TestLocalDriverV2FailsClosedOnNilResultAndAuditFailure(t *testing.T) {
	partition := v2MPSPartition()
	partition.ApplyFunc = nil
	partition.ApplyResult = nil
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: partition, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true}}, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.ExecuteAction(context.Background(), &DriverState{Sandboxes: map[string]base.Sandbox{"sandbox-a": {SandboxID: "sandbox-a", Generation: 1}}}, v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)); !errors.Is(err, base.ErrFailedPrecondition) {
		t.Fatalf("nil result error = %v", err)
	}
	records := driver.AuditRecords()
	if len(records) < 2 || records[len(records)-1].Status != AuditStatusFailed {
		t.Fatalf("nil result audit = %+v", records)
	}

	partition = v2MPSPartition()
	driver, err = NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: partition, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true}}, Audit: failingAuditSink{}, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	probe, err := driver.Probe(context.Background())
	if err != nil || probe.Available || !strings.Contains(probe.Reason, "audit write failed") {
		t.Fatalf("Probe(audit failure) = (%+v, %v)", probe, err)
	}
	partition = v2MPSPartition()
	audit := &failSecondAuditSink{}
	driver, err = NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: partition, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, Bindings: []DiscoveredBinding{{SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 1, DeviceIDs: []string{"GPU-aaaa"}, Share: 0.25, ServerPID: 4242}}}}, Audit: audit, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	if probe, err := driver.Probe(context.Background()); err != nil || !probe.Available {
		t.Fatalf("Probe() = (%+v, %v)", probe, err)
	}
	if _, err := driver.ExecuteAction(context.Background(), &DriverState{Sandboxes: map[string]base.Sandbox{"sandbox-a": {SandboxID: "sandbox-a", Generation: 1}}}, v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)); !errors.Is(err, base.ErrFailedPrecondition) {
		t.Fatalf("pre-command audit failure = %v", err)
	}
	if len(partition.Requests()) != 0 {
		t.Fatalf("backend ran after pre-command audit failure: %+v", partition.Requests())
	}
}

func TestProviderV2DryRunDoesNotMutateProjection(t *testing.T) {
	partition := v2MPSPartition()
	partition.ApplyResult = &BackendActionResult{Detail: "planned", Commands: []Command{{Argv: []string{"mps-helper", "set-share"}}}}
	p, err := New(
		WithNow(func() time.Time { return v2Now }),
		WithDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: partition, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, SupportedActions: []tgsrlv1.ActionType{tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionType_ACTION_TYPE_RELEASE}, Bindings: []DiscoveredBinding{{SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 1, DeviceIDs: []string{"GPU-aaaa"}, Share: 0.25, ServerPID: 4242}}}}, DryRun: true, Now: func() time.Time { return v2Now }}),
	)
	if err != nil {
		t.Fatal(err)
	}
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)
	action.Share = 0.75
	result, err := p.ExecuteAction(context.Background(), action)
	if err != nil || result.GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SKIPPED {
		t.Fatalf("ExecuteAction() = (%+v, %v)", result, err)
	}
	sandbox, err := p.GetSandbox(context.Background(), "sandbox-a")
	if err != nil || sandbox.Share != 0.25 {
		t.Fatalf("sandbox after dry run = (%+v, %v)", sandbox, err)
	}
	snapshot, _ := p.Snapshot(context.Background())
	if snapshot.GetRevision() != 1 {
		t.Fatalf("dry run revision = %d, want 1", snapshot.GetRevision())
	}
	inventory, err := p.DriverInventory(context.Background())
	if err != nil || len(inventory.Devices) != 1 || inventory.Devices[0].UUID != "GPU-aaaa" {
		t.Fatalf("DriverInventory() = (%+v, %v)", inventory, err)
	}
	partitions, err := p.DriverPartitions(context.Background())
	if err != nil || partitions.Mode != PartitionModeMPS {
		t.Fatalf("DriverPartitions() = (%+v, %v)", partitions, err)
	}
	planned, err := p.PlanAction(context.Background(), action)
	if err != nil || !planned.DryRun {
		t.Fatalf("PlanAction() = (%+v, %v)", planned, err)
	}
	if records, err := p.DriverAuditRecords(context.Background()); err != nil || len(records) == 0 {
		t.Fatalf("DriverAuditRecords() = (%+v, %v)", records, err)
	}
}

func TestLocalDriverV2RestartDiscoveryAndReconcileRefresh(t *testing.T) {
	inventory := &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false), v2InventoryWithVersion("555.42.02")}}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: inventory, Partition: v2MPSPartition(), Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, Bindings: []DiscoveredBinding{{SandboxID: "recovered", BindingID: "binding-r", Generation: 4, DeviceIDs: []string{"GPU-aaaa"}, Share: 0.5, ServerPID: 4242}}}}, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	probe, err := driver.Probe(context.Background())
	if err != nil || len(probe.Sandboxes) != 1 || probe.Sandboxes[0].Generation != 4 {
		t.Fatalf("restart Probe() = (%+v, %v)", probe, err)
	}
	reconciled, err := driver.Reconcile(context.Background(), &DriverState{}, nil)
	if err != nil || reconciled.Capabilities.GetComponentVersions()[0].GetVersion() != "555.42.02" || reconciled.SandboxesAuthoritative {
		t.Fatalf("Reconcile() = (%+v, %v)", reconciled, err)
	}
}

func TestProviderRediscoverAvoidsTimestampOnlyRevisionChurn(t *testing.T) {
	current := v2Now
	inventory := &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false), v2Inventory(false)}}
	partition := v2MPSPartition()
	binding := &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, Bindings: []DiscoveredBinding{{SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 1, DeviceIDs: []string{"GPU-aaaa"}, Share: 0.5, ServerPID: 4242}}}}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: inventory, Partition: partition, Binding: binding, Now: func() time.Time { return current }})
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(WithNow(func() time.Time { return current }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	before, _ := p.Snapshot(context.Background())
	beforeSandbox, _ := p.GetSandbox(context.Background(), "sandbox-a")
	current = current.Add(time.Minute)
	inventory.Snapshots[0].ObservedAt = current
	partition.Snapshot.ObservedAt = current
	if err := p.Rediscover(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, _ := p.Snapshot(context.Background())
	if after.GetRevision() != before.GetRevision() {
		afterSandbox, _ := p.GetSandbox(context.Background(), "sandbox-a")
		t.Fatalf("timestamp-only rediscovery revision = %d, want %d; before=%+v after=%+v", after.GetRevision(), before.GetRevision(), beforeSandbox, afterSandbox)
	}
}

func TestLocalDriverV2CapabilityRevisionAdvancesOnlyOnContentChange(t *testing.T) {
	current := v2Now
	inventory := &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false), v2Inventory(false), v2InventoryWithVersion("555.42.02")}}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: inventory, Partition: v2MPSPartition(), Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true}}, Now: func() time.Time { return current }})
	if err != nil {
		t.Fatal(err)
	}
	first, err := driver.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	current = current.Add(time.Minute)
	inventory.Snapshots[0].ObservedAt = current
	second, err := driver.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	third, err := driver.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Capabilities.GetRevision() != second.Capabilities.GetRevision() || third.Capabilities.GetRevision() <= second.Capabilities.GetRevision() {
		t.Fatalf("capability revisions = %d, %d, %d", first.Capabilities.GetRevision(), second.Capabilities.GetRevision(), third.Capabilities.GetRevision())
	}
}

func TestProviderRediscoverPublishesRuntimeTombstone(t *testing.T) {
	runtimeBackend := &FakeRuntimeBackend{Status: &RuntimeBackendStatus{Available: true, SandboxesAuthoritative: true, Sandboxes: []base.Sandbox{{SandboxID: "sandbox-a", State: base.SandboxStateRunning, Generation: 1}}}}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: v2MPSPartition(), Runtime: runtimeBackend, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true}}, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(WithNow(func() time.Time { return v2Now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	watchContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch, err := p.WatchSandboxes(watchContext, 0)
	if err != nil {
		t.Fatal(err)
	}
	runtimeBackend.Status.Sandboxes = nil
	if err := p.Rediscover(context.Background()); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case watched := <-watch:
			if watched.Event.GetDetail() == "runtime discovery removed sandbox" {
				if watched.Event.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED {
					t.Fatalf("tombstone = %+v", watched.Event)
				}
				return
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for sandbox tombstone")
		}
	}
}

func TestProviderRediscoverKeepsMissingBindingOnlySandbox(t *testing.T) {
	binding := &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, Bindings: []DiscoveredBinding{{SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 1, DeviceIDs: []string{"GPU-aaaa"}, Share: 0.5, ServerPID: 4242}}}}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: v2MPSPartition(), Binding: binding, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(WithNow(func() time.Time { return v2Now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	binding.Snapshot.Bindings = nil
	if err := p.Rediscover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.GetSandbox(context.Background(), "sandbox-a"); err != nil {
		t.Fatalf("binding-only rediscovery removed runtime state: %v", err)
	}
}

func TestProviderRediscoverMergesBindingWithoutDowngradingRuntimeState(t *testing.T) {
	binding := &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, Bindings: []DiscoveredBinding{{SandboxID: "sandbox-a", BindingID: "binding-new", Generation: 1, DeviceIDs: []string{"GPU-aaaa"}, Share: 0.75, ServerPID: 4242}}}}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: v2MPSPartition(), Binding: binding, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(WithNow(func() time.Time { return v2Now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	safePoint := true
	if _, err := p.ObserveSandbox(context.Background(), observedSandboxEvent("rediscovery-merge", tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, safePoint)); err != nil {
		t.Fatal(err)
	}
	if err := p.Rediscover(context.Background()); err != nil {
		t.Fatal(err)
	}
	sandbox, err := p.GetSandbox(context.Background(), "sandbox-a")
	if err != nil || sandbox.State != base.SandboxStateRunning || !sandbox.SafePoint || sandbox.Binding.GetBindingId() != "binding-new" || sandbox.Share != 0.75 {
		t.Fatalf("merged sandbox = (%+v, %v)", sandbox, err)
	}
}

func TestProviderRediscoverEmitsNoTombstoneWithoutRuntimeAuthority(t *testing.T) {
	binding := &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, Bindings: []DiscoveredBinding{{SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 1, DeviceIDs: []string{"GPU-aaaa"}, Share: 0.5, ServerPID: 4242}}}}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: v2MPSPartition(), Binding: binding, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(WithNow(func() time.Time { return v2Now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	watchContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch, err := p.WatchSandboxes(watchContext, p.sandboxSeq)
	if err != nil {
		t.Fatal(err)
	}
	binding.Snapshot.Bindings = nil
	if err := p.Rediscover(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case watched := <-watch:
		if watched.Event.GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED {
			t.Fatalf("binding-only discovery emitted tombstone: %+v", watched.Event)
		}
	case <-time.After(10 * time.Millisecond):
	}
}

func TestCommandBindingBackendRecoversDurableActionReceipts(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("tgsrl-nvidia-binding,1,bind,release,durable_receipts,generation_fence,idempotency\n")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("0,1,true,plan-digest,action-a,plan-a,key-a,sandbox-a,4,true,,,digest-a\n")}},
	)
	backend := NewCommandBindingBackend(executor, time.Second)
	receipts, err := backend.DiscoverReceipts(context.Background())
	if err != nil || len(receipts) != 1 || !receipts[0].Succeeded || receipts[0].Generation != 4 || receipts[0].CommandDigest != "digest-a" {
		t.Fatalf("DiscoverReceipts() = (%+v, %v)", receipts, err)
	}
}

func TestCommandBindingBackendParsesSeparateTransactionAndActionGenerations(t *testing.T) {
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("tgsrl-nvidia-binding,1,bind,release,durable_receipts,generation_fence,idempotency\n")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("0,1,false,plan-digest,action-a,plan-a,key-a,sandbox-a,7,4,true,,,digest-a\n")}},
	)
	backend := NewCommandBindingBackend(executor, time.Second)
	receipts, err := backend.DiscoverReceipts(context.Background())
	if err != nil || len(receipts) != 1 || receipts[0].Committed || receipts[0].Generation != 7 || receipts[0].ActionGeneration != 4 {
		t.Fatalf("DiscoverReceipts() = (%+v, %v)", receipts, err)
	}
}

func TestCommandBindingBackendRejectsDuplicateReceiptRows(t *testing.T) {
	row := "0,1,false,plan-digest,action-a,plan-a,key-a,sandbox-a,7,4,true,,,digest-a\n"
	executor := NewFakeCommandExecutor(
		FakeCommandResponse{Result: CommandResult{Stdout: []byte("tgsrl-nvidia-binding,1,bind,release,durable_receipts,generation_fence,idempotency\n")}},
		FakeCommandResponse{Result: CommandResult{Stdout: []byte(row + row)}},
	)
	if _, err := NewCommandBindingBackend(executor, time.Second).DiscoverReceipts(context.Background()); err == nil || !strings.Contains(err.Error(), "duplicates") {
		t.Fatalf("duplicate receipt error = %v", err)
	}
}

func TestCommandBindingBackendRejectsUnsafeHandshake(t *testing.T) {
	executor := NewFakeCommandExecutor(FakeCommandResponse{Result: CommandResult{Stdout: []byte("tgsrl-nvidia-binding,1,bind,release,durable_receipts,generation_fence")}})
	snapshot, err := NewCommandBindingBackend(executor, time.Second).Discover(context.Background(), nil, nil)
	if err != nil || snapshot.Available || !strings.Contains(snapshot.Reason, "idempotency") {
		t.Fatalf("Discover() = (%+v, %v)", snapshot, err)
	}
}

func TestCommandBindingBackendPassesReceiptAndEnvironment(t *testing.T) {
	executor := NewFakeCommandExecutor(FakeCommandResponse{Result: CommandResult{Stdout: []byte("sandbox-a,binding-a,2,GPU-aaaa,0.5,key-a,4242\n")}})
	backend := NewCommandBindingBackendWithConfig(executor, time.Second, "binding-helper", "/state/bindings.json", "/run/tgsrl/mps")
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_BIND)
	action.Share = 0.5
	receipt := &HelperReceiptContext{StepIndex: 1, ExpectedActions: 3, TransactionGeneration: 7, PlanDigest: "sha256:plan", CommandDigest: "sha256:action"}
	result, err := backend.Apply(context.Background(), BackendActionRequest{Action: action, Receipt: receipt})
	if err != nil || result.Binding == nil || result.Binding.Generation != 2 || result.Binding.ServerPID != 4242 {
		t.Fatalf("Apply() = (%+v, %v)", result, err)
	}
	commands := executor.Commands()
	if len(commands) != 1 {
		t.Fatalf("commands = %+v", commands)
	}
	joined := strings.Join(commands[0].Argv, " ")
	for _, expected := range []string{"binding-helper bind", "--share 0.5", "--step-index 1", "--expected-actions 3", "--transaction-generation 7", "--plan-digest sha256:plan", "--command-digest sha256:action", "--action-id action-a", "--plan-id plan-a"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("argv %q missing %q", joined, expected)
		}
	}
	if commands[0].Env["TGSRL_NVIDIA_BINDING_STATE"] != "/state/bindings.json" || commands[0].Env["TGSRL_NVIDIA_MPS_PID_DIR"] != "/run/tgsrl/mps" {
		t.Fatalf("environment = %+v", commands[0].Env)
	}
}

func TestCommandBindingBackendMarksAmbiguousFailure(t *testing.T) {
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_BIND)
	action.Share = 0.5
	receipt := &HelperReceiptContext{StepIndex: 0, ExpectedActions: 1, TransactionGeneration: 1, PlanDigest: "sha256:plan", CommandDigest: "sha256:action"}
	tests := []struct {
		name     string
		kind     CommandFailureKind
		mayApply bool
	}{
		{name: "timeout", kind: CommandFailureTimeout, mayApply: true},
		{name: "unavailable", kind: CommandFailureUnavailable, mayApply: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := NewFakeCommandExecutor(FakeCommandResponse{Err: &CommandError{Kind: test.kind, ExitCode: -1, Cause: errors.New(test.name)}})
			result, err := NewCommandBindingBackend(executor, time.Second).Apply(context.Background(), BackendActionRequest{Action: action, Receipt: receipt})
			if err == nil || result == nil || result.MutationMayHaveApplied != test.mayApply {
				t.Fatalf("Apply() = (%+v, %v), may_apply=%v", result, err, test.mayApply)
			}
		})
	}
}

func TestCommandRuntimeBackendRecognizesExplicitUnknownOutcomeExit(t *testing.T) {
	executor := NewFakeCommandExecutor(FakeCommandResponse{Result: CommandResult{ExitCode: 75}, Err: errors.New("exit status 75")})
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_PAUSE)
	result, err := NewCommandRuntimeBackend(executor, time.Second).Apply(context.Background(), BackendActionRequest{Action: action})
	if err == nil || result == nil || !result.MutationMayHaveApplied {
		t.Fatalf("Apply() = (%+v, %v)", result, err)
	}
}

func TestProviderRejectsIncompleteRecoveredTransactionReceiptSet(t *testing.T) {
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_BIND)
	plan := nvidiaPlan("plan-a", 1, action)
	digest, err := placementPlanDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
	p := &Provider{driver: &LocalDriverV2{recoveredActions: map[string]RecoveredAction{"key-a": {StepIndex: 0, ExpectedActions: 2, PlanDigest: digest, ActionID: action.GetActionId(), PlanID: plan.GetPlanId(), IdempotencyKey: action.GetIdempotencyKey(), SandboxID: actionSandboxID(action), Generation: 7, ActionGeneration: receiptActionGeneration(action), Succeeded: true, CommandDigest: "digest"}}}, now: func() time.Time { return v2Now }}
	if _, err := p.recoveredTransactionReceipt(plan.GetPlanId(), 7, plan, orderedPlanActions(plan)); !errors.Is(err, base.ErrFailedPrecondition) {
		t.Fatalf("recoveredTransactionReceipt() error = %v, want incomplete receipt failure", err)
	}
}

func TestLocalDriverV2RecoveredReceiptPreventsReplay(t *testing.T) {
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_BIND)
	digest, err := actionDigest(action)
	if err != nil {
		t.Fatal(err)
	}
	receiptBackend := &fakeReceiptBindingBackend{FakeBindingBackend: FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, SupportedActions: []tgsrlv1.ActionType{tgsrlv1.ActionType_ACTION_TYPE_BIND}}}, receipts: []RecoveredAction{{StepIndex: 0, ExpectedActions: 1, PlanDigest: "plan-digest", ActionID: "action-a", PlanID: "plan-a", IdempotencyKey: "key-a", SandboxID: "sandbox-a", Generation: 7, ActionGeneration: 2, Succeeded: true, CommandDigest: digest}}}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: v2MPSPartition(), Binding: receiptBackend, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	if probe, err := driver.Probe(context.Background()); err != nil || !probe.Available {
		t.Fatalf("Probe() = (%+v, %v)", probe, err)
	}
	execution, err := driver.ExecuteAction(context.Background(), &DriverState{}, action)
	if err != nil || !strings.Contains(execution.Detail, "recovered") || receiptBackend.applyCalls != 0 {
		t.Fatalf("ExecuteAction() = (%+v, %v), apply calls=%d", execution, err, receiptBackend.applyCalls)
	}
	conflict := cloneAction(action)
	conflict.Share = 0.75
	if _, err := driver.ExecuteAction(context.Background(), &DriverState{}, conflict); !errors.Is(err, base.ErrIdempotencyConflict) {
		t.Fatalf("recovered receipt content conflict error = %v", err)
	}
}

func TestProviderPrepareRestoresAppliedBindingStepFromDurableReceipt(t *testing.T) {
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_BIND)
	action.Share = 0.5
	action.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, TargetId: action.GetBinding().GetBindingId()}
	plan := nvidiaPlan("plan-a", 1, action)
	planDigest, err := placementPlanDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
	commandDigest, err := actionDigest(action)
	if err != nil {
		t.Fatal(err)
	}
	binding := &fakeReceiptBindingBackend{FakeBindingBackend: FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, SupportedActions: []tgsrlv1.ActionType{tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionType_ACTION_TYPE_RELEASE}}}, receipts: []RecoveredAction{{StepIndex: 0, ExpectedActions: 1, PlanDigest: planDigest, ActionID: action.GetActionId(), PlanID: plan.GetPlanId(), IdempotencyKey: action.GetIdempotencyKey(), SandboxID: actionSandboxID(action), Generation: 7, ActionGeneration: action.GetBinding().GetGeneration(), Succeeded: true, CommandDigest: commandDigest}}}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: v2MPSPartition(), Binding: binding, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(WithNow(func() time.Time { return v2Now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := p.PreparePlan(context.Background(), plan.GetPlanId(), 7, plan)
	if err != nil || prepared.Phase != base.TransactionPhaseExecuting || len(prepared.Effects) != 1 || prepared.Effects[0].Status != base.EffectStatusApplied {
		t.Fatalf("PreparePlan(recovered) = (%+v, %v)", prepared, err)
	}
	committed, err := p.CommitPlan(context.Background(), plan.GetPlanId(), 7)
	if err != nil || committed.Phase != base.TransactionPhaseCommitted || binding.applyCalls != 0 {
		t.Fatalf("CommitPlan(recovered) = (%+v, %v), apply_calls=%d", committed, err, binding.applyCalls)
	}
}

func TestProviderPrepareRejectsRecoveredReceiptForDifferentAction(t *testing.T) {
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_BIND)
	action.Share = 0.5
	action.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, TargetId: action.GetBinding().GetBindingId()}
	plan := nvidiaPlan("plan-a", 1, action)
	planDigest, err := placementPlanDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{
		Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}},
		Partition: v2MPSPartition(),
		Binding:   &fakeReceiptBindingBackend{FakeBindingBackend: FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, SupportedActions: []tgsrlv1.ActionType{tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionType_ACTION_TYPE_RELEASE}}}, receipts: []RecoveredAction{{StepIndex: 0, ExpectedActions: 1, PlanDigest: planDigest, ActionID: action.GetActionId(), PlanID: plan.GetPlanId(), IdempotencyKey: action.GetIdempotencyKey(), SandboxID: actionSandboxID(action), Generation: 7, ActionGeneration: action.GetBinding().GetGeneration(), Succeeded: true, CommandDigest: "sha256:different-action"}}},
		Now:       func() time.Time { return v2Now },
	})
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(WithNow(func() time.Time { return v2Now }), WithDriver(driver))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.PreparePlan(context.Background(), plan.GetPlanId(), 7, plan); !errors.Is(err, base.ErrIdempotencyConflict) {
		t.Fatalf("PreparePlan() error = %v", err)
	}
}

func TestLocalDriverV2ReceiptDiscoveryIsAuthoritative(t *testing.T) {
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: v2MPSPartition(), Binding: &fakeReceiptBindingBackend{FakeBindingBackend: FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true}}, receipts: []RecoveredAction{{StepIndex: 0, ExpectedActions: 1, PlanDigest: "plan-digest", IdempotencyKey: "stale"}}}, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.rememberReceipts([]RecoveredAction{{IdempotencyKey: "old"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	receipts := driver.RecoveredActions()
	if len(receipts) != 1 || receipts[0].IdempotencyKey != "stale" {
		t.Fatalf("authoritative receipts = %+v", receipts)
	}
}

func TestLocalDriverV2ClassifiesCommandFailureAndGenerationFence(t *testing.T) {
	executor := NewFakeCommandExecutor(FakeCommandResponse{Result: CommandResult{ExitCode: 1, Stderr: []byte("driver/library version mismatch")}, Err: errors.New("exit status 1")})
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: NewMPSBackend(executor, time.Second, "", ""), Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true}}, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	probe, err := driver.Probe(context.Background())
	if err != nil || probe.Available || !strings.Contains(probe.Reason, "unavailable") {
		t.Fatalf("Probe() = (%+v, %v)", probe, err)
	}
	partition := v2MPSPartition()
	driver, _ = NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: partition, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true}}, Now: func() time.Time { return v2Now }})
	if _, err := driver.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)
	action.ExpectedGeneration = 2
	state := &DriverState{Sandboxes: map[string]base.Sandbox{"sandbox-a": {SandboxID: "sandbox-a", Generation: 1}}}
	if _, err := driver.ExecuteAction(context.Background(), state, action); !errors.Is(err, base.ErrFailedPrecondition) {
		t.Fatalf("generation fence error = %v", err)
	}
}

func TestProviderV2PlanRollbackInvokesBackendCompensation(t *testing.T) {
	partition := v2MPSPartition()
	partition.ApplyFunc = func(_ context.Context, request BackendActionRequest) (*BackendActionResult, error) {
		if request.Action.GetActionId() == "pause" {
			return nil, errors.New("pause failed")
		}
		share := request.Action.GetShare()
		return &BackendActionResult{Detail: "applied", ObservedShare: &share}, nil
	}
	runtimeBackend := &FakeRuntimeBackend{Status: &RuntimeBackendStatus{Available: true, SupportedActions: []tgsrlv1.ActionType{tgsrlv1.ActionType_ACTION_TYPE_PAUSE}}, ApplyErr: errors.New("pause failed")}
	p, err := New(WithNow(func() time.Time { return v2Now }), WithDriverV2(LocalDriverV2Options{
		Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: partition, Runtime: runtimeBackend, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, SupportedActions: []tgsrlv1.ActionType{tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionType_ACTION_TYPE_RELEASE}, Bindings: []DiscoveredBinding{{SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 1, DeviceIDs: []string{"GPU-aaaa"}, Share: 0.25, ServerPID: 4242}}}}, Now: func() time.Time { return v2Now },
	}))
	if err != nil {
		t.Fatal(err)
	}
	share := v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)
	share.ActionId, share.PlanId, share.IdempotencyKey, share.Share = "share", "rollback-plan", "share-key", 0.75
	share.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	pause := v2Action(tgsrlv1.ActionType_ACTION_TYPE_PAUSE)
	pause.ActionId, pause.PlanId, pause.IdempotencyKey = "pause", "rollback-plan", "pause-key"
	pause.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RESUME, TargetId: "sandbox-a"}
	plan := nvidiaPlan("rollback-plan", 1, share, pause)
	results, err := executeTestTransaction(context.Background(), p, plan)
	if !errors.Is(err, base.ErrPartialFailure) || len(results) != 2 || results[0].GetRollbackStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_ROLLED_BACK {
		t.Fatalf("ExecutePlan() = (%+v, %v)", results, err)
	}
	requests := partition.Requests()
	if len(requests) != 2 || requests[0].Action.GetActionId() != "share" || requests[1].Action.GetActionId() != "share-rollback" || requests[1].Action.GetShare() != 0.25 {
		t.Fatalf("partition requests = %+v", requests)
	}
	sandbox, err := p.GetSandbox(context.Background(), "sandbox-a")
	if err != nil || sandbox.Share != 0.25 {
		t.Fatalf("sandbox after rollback = (%+v, %v)", sandbox, err)
	}
}

func TestProviderV2TransactionalLifecycle(t *testing.T) {
	partition := v2MPSPartition()
	p, err := New(WithNow(func() time.Time { return v2Now }), WithDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: partition, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, Bindings: []DiscoveredBinding{{SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 1, DeviceIDs: []string{"GPU-aaaa"}, Share: 0.25, ServerPID: 4242}}}}, Now: func() time.Time { return v2Now }}))
	if err != nil {
		t.Fatal(err)
	}
	watchContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch, err := p.WatchSandboxes(watchContext, 0)
	if err != nil {
		t.Fatal(err)
	}
	action := v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)
	action.Share = 0.5
	action.Rollback = &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, TargetId: "sandbox-a"}
	plan := nvidiaPlan("plan-a", 1, action)
	receipt, err := p.PreparePlan(context.Background(), "plan-a", 1, plan)
	if err != nil || receipt.Phase != base.TransactionPhasePrepared {
		t.Fatalf("PreparePlan() = (%+v, %v)", receipt, err)
	}
	receipt, err = p.ExecuteStep(context.Background(), "plan-a", 1, 0)
	if err != nil || receipt.Effects[0].Status != base.EffectStatusApplied {
		t.Fatalf("ExecuteStep() = (%+v, %v)", receipt, err)
	}
	select {
	case event := <-watch:
		t.Fatalf("uncommitted transaction published sandbox observation: %+v", event)
	default:
	}
	receipt, err = p.CommitPlan(context.Background(), "plan-a", 1)
	if err != nil || receipt.Phase != base.TransactionPhaseCommitted {
		t.Fatalf("CommitPlan() = (%+v, %v)", receipt, err)
	}
	select {
	case event := <-watch:
		if event.Event.GetShare() != 0.5 || event.Event.SafePoint != nil || event.Event.GetActionId() != action.GetActionId() {
			t.Fatalf("committed share readback event = %+v", event.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for committed share readback")
	}
	if _, err := p.PreparePlan(context.Background(), "plan-a", 2, plan); !errors.Is(err, base.ErrGenerationFenced) {
		t.Fatalf("PreparePlan(generation conflict) error = %v", err)
	}
	capabilities, err := p.DescribeCapabilities(context.Background())
	if err != nil || !capabilities.OrderedStepExecution || !capabilities.CompensatingAbort || !capabilities.StepIdempotency || !capabilities.GenerationFence || !capabilities.PartialEffectReporting || !capabilities.AtomicReplacement {
		t.Fatalf("transaction capabilities = (%+v, %v)", capabilities, err)
	}
}

func TestProviderRejectsDuplicateRecoveredTransactionSteps(t *testing.T) {
	first := v2Action(tgsrlv1.ActionType_ACTION_TYPE_BIND)
	second := cloneAction(first)
	second.ActionId, second.IdempotencyKey, second.SandboxId = "action-b", "key-b", "sandbox-b"
	second.Binding.SandboxId, second.Binding.BindingId = "sandbox-b", "binding-b"
	plan := nvidiaPlan("plan-a", 1, first, second)
	digest, err := placementPlanDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
	firstDigest, err := actionDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	driver := &LocalDriverV2{recoveredActions: map[string]RecoveredAction{
		"key-a": {StepIndex: 0, ExpectedActions: 2, PlanDigest: digest, ActionID: first.GetActionId(), PlanID: plan.GetPlanId(), IdempotencyKey: first.GetIdempotencyKey(), SandboxID: actionSandboxID(first), Generation: 7, ActionGeneration: receiptActionGeneration(first), Succeeded: true, CommandDigest: firstDigest},
		"key-b": {StepIndex: 0, ExpectedActions: 2, PlanDigest: digest, ActionID: second.GetActionId(), PlanID: plan.GetPlanId(), IdempotencyKey: second.GetIdempotencyKey(), SandboxID: actionSandboxID(second), Generation: 7, ActionGeneration: receiptActionGeneration(second), Succeeded: true, CommandDigest: "action-b-digest"},
	}}
	p := &Provider{driver: driver, now: func() time.Time { return v2Now }}
	if _, err := p.recoveredTransactionReceipt(plan.GetPlanId(), 7, plan, orderedPlanActions(plan)); !errors.Is(err, base.ErrFailedPrecondition) || !strings.Contains(err.Error(), "duplicates step") {
		t.Fatalf("recoveredTransactionReceipt() error = %v", err)
	}
}

func TestLocalDriverV2SerializesSameSandboxMutations(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	partition := v2MPSPartition()
	var mu sync.Mutex
	calls := 0
	partition.ApplyFunc = func(ctx context.Context, request BackendActionRequest) (*BackendActionResult, error) {
		mu.Lock()
		calls++
		call := calls
		mu.Unlock()
		if call == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		share := request.Action.GetShare()
		return &BackendActionResult{Detail: "applied", ObservedShare: &share}, nil
	}
	driver, err := NewLocalDriverV2(LocalDriverV2Options{Inventory: &FakeInventoryBackend{Snapshots: []*InventorySnapshot{v2Inventory(false)}}, Partition: partition, Binding: &FakeBindingBackend{Snapshot: &BindingSnapshot{Available: true, SupportsMPSProfiles: true, Bindings: []DiscoveredBinding{{SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 1, DeviceIDs: []string{"GPU-aaaa"}, Share: 0.25, ServerPID: 4242}}}}, Now: func() time.Time { return v2Now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	state := &DriverState{Sandboxes: map[string]base.Sandbox{"sandbox-a": {SandboxID: "sandbox-a", Generation: 1}}}
	first := v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)
	first.IdempotencyKey, first.Share = "first", 0.25
	second := v2Action(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE)
	second.IdempotencyKey, second.Share = "second", 0.75
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		_, executeErr := driver.ExecuteAction(context.Background(), state, first)
		firstDone <- executeErr
	}()
	<-started
	go func() {
		_, executeErr := driver.ExecuteAction(context.Background(), state, second)
		secondDone <- executeErr
	}()
	time.Sleep(time.Millisecond)
	mu.Lock()
	callsBeforeRelease := calls
	mu.Unlock()
	if callsBeforeRelease != 1 {
		t.Fatalf("concurrent calls before release = %d, want 1", callsBeforeRelease)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second mutation error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("backend calls = %d, want serialized 2", calls)
	}
}

func v2Inventory(mig bool) *InventorySnapshot {
	return v2InventoryWithVersionAndMIG("550.54.14", mig)
}

func v2InventoryWithVersion(version string) *InventorySnapshot {
	return v2InventoryWithVersionAndMIG(version, false)
}

func v2InventoryWithVersionAndMIG(version string, mig bool) *InventorySnapshot {
	return &InventorySnapshot{ObservedAt: v2Now, DriverVersion: version, Devices: []InventoryDevice{{UUID: "GPU-aaaa", Index: "0", PCIAddress: "00000000:AF:00.0", Name: "H100", DriverVersion: version, MemoryBytes: 80 << 30, MIGEnabled: mig}}}
}

func v2MPSPartition() *FakePartitionBackend {
	return &FakePartitionBackend{PartitionMode: PartitionModeMPS, Snapshot: &PartitionSnapshot{Mode: PartitionModeMPS, Available: true, RequiresBindingMetadata: true, ObservedAt: v2Now, SupportedActions: []tgsrlv1.ActionType{tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE}, Partitions: []Partition{{ID: "mps-GPU-aaaa", ParentUUID: "GPU-aaaa", Profile: "dynamic", Share: 1}}}, ApplyFunc: func(_ context.Context, request BackendActionRequest) (*BackendActionResult, error) {
		share := request.Action.GetShare()
		return &BackendActionResult{Detail: "applied", ObservedShare: &share}, nil
	}}
}

func v2Action(actionType tgsrlv1.ActionType) *tgsrlv1.Action {
	definition, _ := actionpolicy.DefinitionForAction(actionType)
	return &tgsrlv1.Action{
		ActionId: "action-a", ActionType: actionType, Level: definition.Level, TargetId: "sandbox-a", SandboxId: "sandbox-a", PlanId: "plan-a", ExpectedGeneration: 1, ExpectedSnapshotRevision: 1, Deadline: timestamppb.New(v2Now.Add(time.Hour)), IdempotencyKey: "key-a", TickKind: tgsrlv1.TickKind_TICK_KIND_SLOW,
		Binding:              &tgsrlv1.Binding{BindingId: "binding-a", SandboxId: "sandbox-a", Generation: 2, DeviceIds: []string{"GPU-aaaa"}, Resources: &tgsrlv1.ResourceVector{AcceleratorUnits: 0.5, MemoryBytes: 40 << 30}},
		RequiredCapabilities: &tgsrlv1.CapabilitySet{SupportedActions: []string{definition.CapabilityName}},
		Preconditions:        actionpolicy.RequiredPreconditions(actionType, false, false),
		ExpectedImpacts:      append([]tgsrlv1.ExpectedImpact(nil), definition.ExpectedImpacts...),
	}
}

type fakeReceiptBindingBackend struct {
	FakeBindingBackend
	receipts   []RecoveredAction
	applyCalls int
}

type failingAuditSink struct{}

func (failingAuditSink) Record(context.Context, AuditRecord) error {
	return errors.New("audit unavailable")
}

type failSecondAuditSink struct{ calls int }

func (s *failSecondAuditSink) Record(context.Context, AuditRecord) error {
	s.calls++
	if s.calls == 2 {
		return errors.New("audit unavailable")
	}
	return nil
}

func containsStringValue(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func flagValue(values []string, flag string) string {
	for index := 0; index+1 < len(values); index++ {
		if values[index] == flag {
			return values[index+1]
		}
	}
	return ""
}

func (b *fakeReceiptBindingBackend) DiscoverReceipts(context.Context) ([]RecoveredAction, error) {
	return append([]RecoveredAction(nil), b.receipts...), nil
}

func (b *fakeReceiptBindingBackend) Apply(ctx context.Context, request BackendActionRequest) (*BackendActionResult, error) {
	b.applyCalls++
	return b.FakeBindingBackend.Apply(ctx, request)
}
