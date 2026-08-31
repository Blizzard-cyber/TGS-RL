package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"

	runtimehelper "github.com/Blizzard-cyber/TGS-RL/internal/managedworker"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider/nvidia/bindinghelper"
)

const migFixture = `GPU 0: NVIDIA H100 (UUID: GPU-aaaa)
  MIG 1g.10gb Device 0: (UUID: MIG-source/1/0)
  MIG 1g.10gb Device 1: (UUID: MIG-target/2/0)
GPU 1: NVIDIA H100 (UUID: GPU-bbbb)
  MIG 2g.20gb Device 0: (UUID: MIG-other/1/0)
`

func TestMIGHelperInventoryAndCapabilities(t *testing.T) {
	originalExecute, originalLookup := executeNVIDIACommand, lookupExecutable
	t.Cleanup(func() { executeNVIDIACommand, lookupExecutable = originalExecute, originalLookup })
	lookupExecutable = func(string) (string, error) { return "/usr/bin/nvidia-smi", nil }
	t.Setenv("TGSRL_NVIDIA_RUNTIME_STATE", filepath.Join(t.TempDir(), "runtime.json"))
	executeNVIDIACommand = func(_ context.Context, argv []string) ([]byte, error) {
		if !reflect.DeepEqual(argv, []string{"nvidia-smi", "-L"}) {
			t.Fatalf("argv = %v", argv)
		}
		return []byte(migFixture), nil
	}
	var output bytes.Buffer
	if err := run([]string{"capabilities", "--format=csv"}, &output); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(output.String()); got != "tgsrl-nvidia-mig,1" {
		t.Fatalf("capabilities without managed worker = %q", got)
	}
	output.Reset()
	if err := run([]string{"inventory", "--format=csv"}, &output); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(output.String()); got != "MIG-source/1/0,1g.10gb,10240\nMIG-target/2/0,1g.10gb,10240\nMIG-other/1/0,2g.20gb,20480" {
		t.Fatalf("inventory = %q", got)
	}
}

func TestMIGHelperDoesNotAdvertiseMutationWithoutNvidiaSMI(t *testing.T) {
	originalLookup := lookupExecutable
	t.Cleanup(func() { lookupExecutable = originalLookup })
	lookupExecutable = func(string) (string, error) { return "", exec.ErrNotFound }
	var output bytes.Buffer
	if err := run([]string{"capabilities", "--format=csv"}, &output); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(output.String()); got != "tgsrl-nvidia-mig,1" {
		t.Fatalf("capabilities = %q", got)
	}
}

func TestHasManagedWorker(t *testing.T) {
	state := runtimehelper.State{Workers: map[string]runtimehelper.Worker{"sandbox-a": {SandboxID: "sandbox-a", ControlSocket: filepath.Join(t.TempDir(), "worker.sock"), State: "running", BindingID: "binding-a", DeviceID: "MIG-a", Share: 1}}}
	if !hasManagedWorker(state) {
		t.Fatal("managed worker was not detected")
	}
	state.Workers["sandbox-a"] = runtimehelper.Worker{SandboxID: "sandbox-a", ControlSocket: "worker.sock", State: "failed"}
	if hasManagedWorker(state) {
		t.Fatal("failed worker advertised MIG lifecycle")
	}
}

func TestMIGMutationRequiresExplicitSourceAndTarget(t *testing.T) {
	base := []string{"--sandbox", "sandbox-a", "--parent-uuid", "GPU-aaaa", "--source-binding", "binding-old", "--source-uuid", "MIG-source/1/0", "--target-uuid", "MIG-target/2/0", "--profile", "1g.10gb", "--binding", "binding-a", "--generation", "4", "--target-generation", "5", "--idempotency-key", "key-a"}
	if _, err := parseMutation("rebind", base); err != nil {
		t.Fatal(err)
	}
	recreate := append([]string(nil), base...)
	if _, err := parseMutation("recreate", recreate); err == nil || !strings.Contains(err.Error(), "inconsistent") {
		t.Fatalf("recreate mismatch error = %v", err)
	}
}

func TestExecuteMutationRequiresExistingTargetOnExpectedParent(t *testing.T) {
	originalExecute := executeNVIDIACommand
	t.Cleanup(func() { executeNVIDIACommand = originalExecute })
	executeNVIDIACommand = func(context.Context, []string) ([]byte, error) { return []byte(migFixture), nil }
	args, err := parseMutation("rebind", []string{"--sandbox", "sandbox-a", "--parent-uuid", "GPU-aaaa", "--source-binding", "binding-old", "--source-uuid", "MIG-source/1/0", "--target-uuid", "MIG-target/2/0", "--profile", "1g.10gb", "--binding", "binding-a", "--generation", "4", "--target-generation", "5", "--idempotency-key", "key-a"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := executeMutation(context.Background(), args); err != nil || got != "MIG-target/2/0" {
		t.Fatalf("executeMutation() = (%q, %v)", got, err)
	}
	args.parentUUID = "GPU-bbbb"
	if _, err := executeMutation(context.Background(), args); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("wrong parent error = %v", err)
	}
}

func TestMIGFixtureValidation(t *testing.T) {
	devices, err := parseMIGList([]byte(migFixture))
	if err != nil || len(devices) != 3 || devices[1].parent != "GPU-aaaa" || devices[2].profile != "2g.20gb" {
		t.Fatalf("devices = %+v error = %v", devices, err)
	}
	if got, err := profileMemoryMiB("1g.10gb"); err != nil || got != 10240 {
		t.Fatalf("profileMemoryMiB() = %d, %v", got, err)
	}
	if _, err := parseInventory([]byte("MIG-a,1g.10gb,10240,GPU-a\nMIG-a,1g.10gb,10240,GPU-a")); err == nil {
		t.Fatal("duplicate MIG inventory was accepted")
	}
	if _, err := executeCommand(context.Background(), []string{"/definitely/missing/tgsrl-nvidia-smi", "-L"}); !errors.Is(err, runtimehelper.ErrRequestNotDelivered) {
		t.Fatalf("missing executable error = %v", err)
	}
}

func TestMIGHelperRebindEndToEndWithManagedWorker(t *testing.T) {
	originalExecute, originalLookup := executeNVIDIACommand, lookupExecutable
	t.Cleanup(func() { executeNVIDIACommand, lookupExecutable = originalExecute, originalLookup })
	executeNVIDIACommand = func(context.Context, []string) ([]byte, error) { return []byte(migFixture), nil }
	lookupExecutable = func(string) (string, error) { return "/usr/bin/nvidia-smi", nil }
	process := exec.Command("sleep", "30")
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = syscall.Kill(process.Process.Pid, syscall.SIGCONT)
		_ = process.Process.Kill()
		_, _ = process.Process.Wait()
	}()
	directory := t.TempDir()
	socketPath := filepath.Join(os.TempDir(), fmt.Sprintf("tgsrl-mig-%d.sock", process.Process.Pid))
	_ = os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	defer os.Remove(socketPath)
	done := make(chan error, 1)
	go serveManagedWorker(listener, done, 6)
	runtimePath, bindingPath := filepath.Join(directory, "runtime.json"), filepath.Join(directory, "binding.json")
	t.Setenv("TGSRL_NVIDIA_RUNTIME_STATE", runtimePath)
	t.Setenv("TGSRL_NVIDIA_BINDING_STATE", bindingPath)
	runtimeStore, _ := runtimehelper.NewStore(runtimePath)
	token, err := runtimehelper.ProcessToken(context.Background(), process.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtimeStore.Register(runtimehelper.Worker{SandboxID: "sandbox-a", Generation: 4, PID: process.Process.Pid, ProcessToken: token, State: "running", Ready: true, BindingID: "binding-old", DeviceID: "MIG-source/1/0", Share: 1, ControlSocket: socketPath}); err != nil {
		t.Fatal(err)
	}
	bindingStore, _ := bindinghelper.NewStore(bindingPath)
	oldBinding := bindinghelper.Binding{SandboxID: "sandbox-a", BindingID: "binding-old", Generation: 4, DeviceIDs: []string{"MIG-source/1/0"}, Share: 1, IdempotencyKey: "old-bind"}
	oldReceipt := bindinghelper.Receipt{StepIndex: 0, ExpectedActions: 1, PlanDigest: "old-plan", ActionID: "old-action", PlanID: "old-plan", IdempotencyKey: "old-bind", SandboxID: "sandbox-a", TransactionGeneration: 1, ActionGeneration: 4, Succeeded: true, CommandDigest: "old-digest"}
	if err := bindingStore.ApplyBinding(oldBinding, oldReceipt); err != nil {
		t.Fatal(err)
	}
	var capabilities bytes.Buffer
	if err := run([]string{"capabilities", "--format=csv"}, &capabilities); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(capabilities.String()); got != "tgsrl-nvidia-mig,1,rebind,recreate,durable_receipts,generation_fence,idempotency,safe_point,checkpoint,stop,restore,readiness" {
		t.Fatalf("managed-worker capabilities = %q", got)
	}
	argv := []string{"rebind", "--sandbox", "sandbox-a", "--parent-uuid", "GPU-aaaa", "--source-binding", "binding-old", "--source-uuid", "MIG-source/1/0", "--target-uuid", "MIG-target/2/0", "--profile", "1g.10gb", "--binding", "binding-new", "--share", "1", "--generation", "4", "--target-generation", "5", "--idempotency-key", "rebind-key", "--recoverable", "--plan-id", "plan-a", "--action-id", "action-a", "--plan-digest", "sha256:plan", "--command-digest", "sha256:action"}
	var output bytes.Buffer
	if err := run(argv, &output); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(output.String()); got != "GPU-aaaa,MIG-target/2/0,1g.10gb,5" {
		t.Fatalf("mutation output = %q", got)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := run(argv, &output); err != nil {
		t.Fatalf("completed mutation retry failed: %v", err)
	}
	if got := strings.TrimSpace(output.String()); got != "GPU-aaaa,MIG-target/2/0,1g.10gb,5" {
		t.Fatalf("retried mutation output = %q", got)
	}
	runtimeState, err := runtimeStore.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	worker := runtimeState.Workers["sandbox-a"]
	if worker.Generation != 5 || worker.BindingID != "binding-new" || worker.DeviceID != "MIG-target/2/0" || !worker.Ready {
		t.Fatalf("runtime worker = %+v", worker)
	}
	bindingState, err := bindingStore.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	binding := bindingState.Bindings["sandbox-a"]
	if binding.Generation != 5 || binding.BindingID != "binding-new" || !reflect.DeepEqual(binding.DeviceIDs, []string{"MIG-target/2/0"}) {
		t.Fatalf("binding state = %+v", binding)
	}
	var receipts bytes.Buffer
	if err := runtimehelper.WriteReceiptsCSV(&receipts, runtimeState); err != nil || !strings.Contains(receipts.String(), "0,1,false,sha256:plan,action-a,plan-a,rebind-key,sandbox-a,1,4,true") {
		t.Fatalf("receipts = %q error = %v", receipts.String(), err)
	}
}

func serveManagedWorker(listener net.Listener, done chan<- error, count int) {
	for index := 0; index < count; index++ {
		connection, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		var request runtimehelper.ControlRequest
		err = json.NewDecoder(connection).Decode(&request)
		if err == nil {
			response := runtimehelper.ControlResponse{Accepted: true, Generation: request.Generation}
			switch request.Action {
			case "status":
				response.State, response.Ready = "running", true
			case "prepare_pause":
				response.SafePoint = true
			case "checkpoint":
				response.CheckpointRef = "checkpoint-a"
			case "stop":
				response.State = "terminated"
			case "reload":
				if request.DeviceID != "MIG-target/2/0" || request.Generation != 5 {
					err = fmt.Errorf("reload request = %+v", request)
				}
			case "resume":
				response.State, response.Ready = "running", true
			default:
				err = fmt.Errorf("unexpected action %q", request.Action)
			}
			if err == nil {
				err = json.NewEncoder(connection).Encode(response)
			}
		}
		_ = connection.Close()
		if err != nil {
			done <- err
			return
		}
	}
	done <- nil
}

func TestMIGTargetGenerationIsExplicit(t *testing.T) {
	args := []string{"--sandbox", "sandbox-a", "--parent-uuid", "GPU-aaaa", "--source-binding", "binding-old", "--source-uuid", "MIG-source/1/0", "--target-uuid", "MIG-target/2/0", "--profile", "1g.10gb", "--binding", "binding-a", "--generation", "4", "--target-generation", strconv.Itoa(6), "--idempotency-key", "key-a"}
	if _, err := parseMutation("rebind", args); err == nil || !strings.Contains(err.Error(), "next generation") {
		t.Fatalf("target generation error = %v", err)
	}
}
