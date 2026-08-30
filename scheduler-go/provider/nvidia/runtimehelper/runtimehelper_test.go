package runtimehelper

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
	"strings"
	"syscall"
	"testing"
	"time"
)

func testWorker(t *testing.T) (Worker, func()) {
	t.Helper()
	command := exec.Command("sleep", "30")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		_ = syscall.Kill(command.Process.Pid, syscall.SIGCONT)
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
	}
	directory := t.TempDir()
	safePoint := filepath.Join(directory, "safe-point")
	readiness := filepath.Join(directory, "readiness")
	if err := os.WriteFile(safePoint, []byte("true"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readiness, []byte("true"), 0o600); err != nil {
		t.Fatal(err)
	}
	return Worker{SandboxID: "sandbox-a", Generation: 4, PID: command.Process.Pid, State: "running", SafePoint: true, Ready: true, SafePointFile: safePoint, ReadinessFile: readiness}, cleanup
}

func actionRequest(operation, key string) ActionRequest {
	return ActionRequest{Operation: operation, SandboxID: "sandbox-a", Generation: 4, IdempotencyKey: key, StepIndex: 0, ExpectedActions: 1, TransactionGeneration: 7, PlanDigest: "sha256:plan-" + key, CommandDigest: "sha256:action-" + key, ActionID: "action-" + key, PlanID: "plan-" + key, Recoverable: true}
}

func TestControllerPausesAndResumesRealProcessAcrossRestart(t *testing.T) {
	worker, cleanup := testWorker(t)
	defer cleanup()
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := NewController(store)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := controller.Register(ctx, worker); err != nil {
		t.Fatal(err)
	}
	paused, err := controller.Apply(ctx, actionRequest("pause", "pause-key"))
	if err != nil || paused.State != "paused" || paused.Ready {
		t.Fatalf("pause = (%+v, %v)", paused, err)
	}
	stopped, err := processStopped(ctx, worker.PID)
	if err != nil || !stopped {
		t.Fatalf("process stopped = %v error = %v", stopped, err)
	}
	restarted, _ := NewController(store)
	state, err := restarted.Discover(ctx)
	if err != nil || state.Workers[worker.SandboxID].State != "paused" {
		t.Fatalf("restart discovery = %+v error = %v", state, err)
	}
	resumed, err := restarted.Apply(ctx, actionRequest("resume", "resume-key"))
	if err != nil || resumed.State != "running" || !resumed.Ready {
		t.Fatalf("resume = (%+v, %v)", resumed, err)
	}
	stopped, err = processStopped(ctx, worker.PID)
	if err != nil || stopped {
		t.Fatalf("process stopped after resume = %v error = %v", stopped, err)
	}
}

func TestControllerManagedOffloadAndReload(t *testing.T) {
	worker, cleanup := testWorker(t)
	defer cleanup()
	worker.ControlSocket = "test.sock"
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := NewController(store)
	controller.control = func(_ context.Context, _ string, request ControlRequest) (ControlResponse, error) {
		if request.Action != "status" {
			t.Fatalf("registration action = %q", request.Action)
		}
		return ControlResponse{Accepted: true, Generation: 4, State: "running", Ready: true}, nil
	}
	if err := controller.Register(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	var calls []ControlRequest
	controller.control = func(_ context.Context, _ string, request ControlRequest) (ControlResponse, error) {
		calls = append(calls, request)
		switch request.Action {
		case "prepare_pause":
			return ControlResponse{Accepted: true, Generation: 4, SafePoint: true}, nil
		case "checkpoint":
			return ControlResponse{Accepted: true, Generation: 4, CheckpointRef: "checkpoint-a"}, nil
		case "offload":
			return ControlResponse{Accepted: true, Generation: 4, Offloaded: true}, nil
		case "reload":
			return ControlResponse{Accepted: true, Generation: 4}, nil
		case "resume":
			return ControlResponse{Accepted: true, Generation: 4, State: "running", Ready: true}, nil
		default:
			return ControlResponse{}, errors.New("unexpected action")
		}
	}
	offloaded, err := controller.Apply(context.Background(), actionRequest("offload", "offload-key"))
	if err != nil || !offloaded.Offloaded || offloaded.CheckpointRef != "checkpoint-a" {
		t.Fatalf("offload = (%+v, %v)", offloaded, err)
	}
	resumed, err := controller.Apply(context.Background(), actionRequest("resume", "resume-key"))
	if err != nil || resumed.Offloaded || resumed.State != "running" || !resumed.Ready {
		t.Fatalf("resume = (%+v, %v)", resumed, err)
	}
	want := []string{"prepare_pause", "checkpoint", "offload", "reload", "resume"}
	if len(calls) != len(want) {
		t.Fatalf("calls = %+v", calls)
	}
	for index := range want {
		if calls[index].Action != want[index] {
			t.Fatalf("call %d = %q, want %q", index, calls[index].Action, want[index])
		}
	}
}

func TestControllerKeepsAmbiguousWorkerCallPending(t *testing.T) {
	worker, cleanup := testWorker(t)
	defer cleanup()
	worker.ControlSocket = "test.sock"
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := NewController(store)
	controller.control = func(_ context.Context, _ string, request ControlRequest) (ControlResponse, error) {
		if request.Action != "status" {
			t.Fatalf("registration action = %q", request.Action)
		}
		return ControlResponse{Accepted: true, Generation: 4, State: "running", Ready: true}, nil
	}
	if err := controller.Register(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	controller.control = func(_ context.Context, _ string, request ControlRequest) (ControlResponse, error) {
		if request.Action == "prepare_pause" {
			return ControlResponse{Accepted: true, SafePoint: true}, nil
		}
		return ControlResponse{}, context.DeadlineExceeded
	}
	request := actionRequest("pause", "pause-key")
	if _, err := controller.Apply(context.Background(), request); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("Apply() error = %v", err)
	}
	state, err := store.Snapshot()
	if err != nil || state.Receipts[request.IdempotencyKey].Completed {
		t.Fatalf("pending receipt = %+v error = %v", state.Receipts[request.IdempotencyKey], err)
	}
	var output bytes.Buffer
	if err := WriteReceiptsCSV(&output, state); err != nil || !strings.Contains(output.String(), "OUTCOME_UNKNOWN") {
		t.Fatalf("receipt output = %q error = %v", output.String(), err)
	}
}

func TestControllerReconcilesPendingManagedWorkerReceiptAfterRestart(t *testing.T) {
	worker, cleanup := testWorker(t)
	defer cleanup()
	worker.ControlSocket = "test.sock"
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := NewController(store)
	controller.control = func(_ context.Context, _ string, request ControlRequest) (ControlResponse, error) {
		if request.Action != "status" {
			t.Fatalf("registration action = %q", request.Action)
		}
		return ControlResponse{Accepted: true, Generation: 4, State: "running", Ready: true}, nil
	}
	if err := controller.Register(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	controller.control = func(_ context.Context, _ string, request ControlRequest) (ControlResponse, error) {
		if request.Action == "prepare_pause" {
			return ControlResponse{Accepted: true, Generation: 4, SafePoint: true}, nil
		}
		return ControlResponse{}, context.DeadlineExceeded
	}
	request := actionRequest("pause", "pause-key")
	if _, err := controller.Apply(context.Background(), request); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("Apply() error = %v", err)
	}
	restarted, _ := NewController(store)
	restarted.control = func(_ context.Context, _ string, request ControlRequest) (ControlResponse, error) {
		if request.Action != "status" {
			t.Fatalf("recovery action = %q", request.Action)
		}
		return ControlResponse{Accepted: true, Generation: 4, State: "paused", SafePoint: true}, nil
	}
	state, err := restarted.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := state.Workers[worker.SandboxID]; got.State != "paused" || !got.SafePoint {
		t.Fatalf("reconciled worker = %+v", got)
	}
	if got := state.Receipts[request.IdempotencyKey]; !got.Completed || !got.Succeeded {
		t.Fatalf("reconciled receipt = %+v", got)
	}
}

func TestUnixControlProtocol(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "worker.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer connection.Close()
		var request ControlRequest
		if decodeErr := json.NewDecoder(connection).Decode(&request); decodeErr != nil {
			done <- decodeErr
			return
		}
		if request.Action != "checkpoint" || request.SandboxID != "sandbox-a" || request.Generation != 4 {
			done <- fmt.Errorf("request = %+v", request)
			return
		}
		done <- json.NewEncoder(connection).Encode(ControlResponse{Accepted: true, Generation: 4, CheckpointRef: "checkpoint-a"})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := callUnixControl(ctx, socketPath, ControlRequest{Action: "checkpoint", SandboxID: "sandbox-a", Generation: 4})
	if err != nil || !response.Accepted || response.CheckpointRef != "checkpoint-a" {
		t.Fatalf("response = %+v error = %v", response, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestControllerReconfigureOrdersLifecycleAndPersistsReplacement(t *testing.T) {
	worker, cleanup := testWorker(t)
	defer cleanup()
	worker.ControlSocket, worker.BindingID, worker.DeviceID, worker.Share = "test.sock", "binding-old", "MIG-source/1/0", 1
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := NewController(store)
	controller.control = func(_ context.Context, _ string, request ControlRequest) (ControlResponse, error) {
		if request.Action != "status" {
			t.Fatalf("registration action = %q", request.Action)
		}
		return ControlResponse{Accepted: true, Generation: 4, State: "running", Ready: true}, nil
	}
	if err := controller.Register(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	var calls []string
	controller.control = func(_ context.Context, _ string, request ControlRequest) (ControlResponse, error) {
		calls = append(calls, request.Action)
		switch request.Action {
		case "prepare_pause":
			return ControlResponse{Accepted: true, Generation: 4, SafePoint: true}, nil
		case "checkpoint":
			return ControlResponse{Accepted: true, Generation: 4, CheckpointRef: "checkpoint-a"}, nil
		case "stop":
			return ControlResponse{Accepted: true, Generation: 4, State: "terminated"}, nil
		case "reload":
			if request.Generation != 5 || request.DeviceID != "MIG-target/2/0" || request.Profile != "1g.10gb" {
				t.Fatalf("reload request = %+v", request)
			}
			return ControlResponse{Accepted: true, Generation: 5, State: "sleeping", Offloaded: false}, nil
		case "resume":
			return ControlResponse{Accepted: true, Generation: 5, State: "running", Ready: true}, nil
		default:
			return ControlResponse{}, fmt.Errorf("unexpected action %q", request.Action)
		}
	}
	request := actionRequest("rebind", "rebind-key")
	request.TargetGeneration, request.TargetDeviceID, request.TargetProfile = 5, "MIG-target/2/0", "1g.10gb"
	request.TargetBindingID, request.TargetShare = "binding-new", 1
	request.TargetParentID = "GPU-aaaa"
	request.SourceBindingID, request.SourceDeviceID = "binding-old", "MIG-source/1/0"
	mutations := 0
	reconfigured, err := controller.Reconfigure(context.Background(), request, func(_ context.Context, current Worker) (string, error) {
		mutations++
		if current.DeviceID != "MIG-source/1/0" {
			t.Fatalf("source worker = %+v", current)
		}
		return "MIG-target/2/0", nil
	})
	if err != nil || reconfigured.Generation != 5 || reconfigured.BindingID != "binding-new" || reconfigured.DeviceID != "MIG-target/2/0" || !reconfigured.Ready || mutations != 1 {
		t.Fatalf("Reconfigure() = (%+v, %v), mutations=%d", reconfigured, err, mutations)
	}
	if want := []string{"prepare_pause", "checkpoint", "stop", "reload", "resume"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
}

func TestControllerReconfigureResumesFromEveryDurablePhase(t *testing.T) {
	phases := []struct {
		name          string
		wantCalls     []string
		wantMutations int
	}{
		{name: "pending", wantCalls: []string{"prepare_pause", "checkpoint", "stop", "reload", "resume"}, wantMutations: 1},
		{name: "prepared", wantCalls: []string{"checkpoint", "stop", "reload", "resume"}, wantMutations: 1},
		{name: "checkpointed", wantCalls: []string{"stop", "reload", "resume"}, wantMutations: 1},
		{name: "stopped", wantCalls: []string{"reload", "resume"}, wantMutations: 1},
		{name: "reconfigured", wantCalls: []string{"reload", "resume"}},
		{name: "reloaded", wantCalls: []string{"resume"}},
	}
	for _, test := range phases {
		t.Run(test.name, func(t *testing.T) {
			worker, cleanup := testWorker(t)
			defer cleanup()
			worker.ControlSocket, worker.BindingID, worker.DeviceID, worker.Share = "test.sock", "binding-old", "MIG-source/1/0", 1
			store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
			controller, _ := NewController(store)
			controller.control = func(_ context.Context, _ string, request ControlRequest) (ControlResponse, error) {
				if request.Action != "status" {
					t.Fatalf("registration action = %q", request.Action)
				}
				return ControlResponse{Accepted: true, Generation: 4, State: "running", Ready: true}, nil
			}
			if err := controller.Register(context.Background(), worker); err != nil {
				t.Fatal(err)
			}
			request := actionRequest("rebind", "rebind-key")
			request.TargetGeneration, request.TargetDeviceID, request.TargetProfile = 5, "MIG-target/2/0", "1g.10gb"
			request.TargetBindingID, request.TargetShare, request.TargetParentID = "binding-new", 1, "GPU-aaaa"
			request.SourceBindingID, request.SourceDeviceID = "binding-old", "MIG-source/1/0"
			persisted, _, _, err := store.Begin(request)
			if err != nil {
				t.Fatal(err)
			}
			phaseOrder := []string{"prepared", "checkpointed", "stopped", "reconfigured", "reloaded"}
			for _, phase := range phaseOrder {
				if phase == "prepared" {
					persisted.SafePoint, persisted.Ready = true, false
				}
				if phase == "checkpointed" {
					persisted.CheckpointRef = "checkpoint-a"
				}
				if phase == "stopped" {
					persisted.State = "terminated"
				}
				if phase == "reconfigured" {
					persisted.Generation, persisted.BindingID, persisted.DeviceID, persisted.Share = 5, "binding-new", "MIG-target/2/0", 1
					persisted.State, persisted.Offloaded = "sleeping", true
				}
				if phase == "reloaded" {
					persisted.Offloaded = false
				}
				if test.name == "pending" || err != nil {
					break
				}
				err = store.Advance(request.IdempotencyKey, phase, persisted)
				if phase == test.name {
					break
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			var calls []string
			controller.control = func(_ context.Context, _ string, controlRequest ControlRequest) (ControlResponse, error) {
				calls = append(calls, controlRequest.Action)
				switch controlRequest.Action {
				case "prepare_pause":
					return ControlResponse{Accepted: true, Generation: 4, SafePoint: true}, nil
				case "checkpoint":
					return ControlResponse{Accepted: true, Generation: 4, CheckpointRef: "checkpoint-a"}, nil
				case "stop":
					return ControlResponse{Accepted: true, Generation: 4, State: "terminated"}, nil
				case "reload":
					return ControlResponse{Accepted: true, Generation: 5, State: "sleeping"}, nil
				case "resume":
					return ControlResponse{Accepted: true, Generation: 5, State: "running", Ready: true}, nil
				default:
					return ControlResponse{}, fmt.Errorf("unexpected action %q", controlRequest.Action)
				}
			}
			mutations := 0
			result, err := controller.Reconfigure(context.Background(), request, func(context.Context, Worker) (string, error) {
				mutations++
				return request.TargetDeviceID, nil
			})
			if err != nil || result.Generation != 5 || !result.Ready || mutations != test.wantMutations || !reflect.DeepEqual(calls, test.wantCalls) {
				t.Fatalf("Reconfigure(%s) = (%+v, %v), calls=%v mutations=%d", test.name, result, err, calls, mutations)
			}
		})
	}
}

func TestControllerRejectsOffloadWithoutManagedWorker(t *testing.T) {
	worker, cleanup := testWorker(t)
	defer cleanup()
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := NewController(store)
	if err := controller.Register(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Apply(context.Background(), actionRequest("offload", "offload-key")); err == nil || !strings.Contains(err.Error(), "managed-worker") {
		t.Fatalf("offload error = %v", err)
	}
}

func TestControllerRejectsConflictingAndSupersededRetries(t *testing.T) {
	worker, cleanup := testWorker(t)
	defer cleanup()
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := NewController(store)
	if err := controller.Register(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	pause := actionRequest("pause", "shared-key")
	if _, err := controller.Apply(context.Background(), pause); err != nil {
		t.Fatal(err)
	}
	conflict := pause
	conflict.CommandDigest = "sha256:different"
	if _, err := controller.Apply(context.Background(), conflict); err == nil || !strings.Contains(err.Error(), "idempotency key") {
		t.Fatalf("conflicting retry error = %v", err)
	}
	if _, err := controller.Apply(context.Background(), actionRequest("resume", "resume-key")); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Apply(context.Background(), pause); err == nil || !strings.Contains(err.Error(), "superseded") {
		t.Fatalf("superseded retry error = %v", err)
	}
}

func TestControllerReregisterPreservesStateAndSupersedesOlderGeneration(t *testing.T) {
	worker, cleanup := testWorker(t)
	defer cleanup()
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := NewController(store)
	if err := controller.Register(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Apply(context.Background(), actionRequest("pause", "pause-key")); err != nil {
		t.Fatal(err)
	}
	if err := controller.Register(context.Background(), worker); err != nil {
		t.Fatalf("same worker registration error = %v", err)
	}
	state, _ := store.Snapshot()
	if state.Workers[worker.SandboxID].State != "paused" {
		t.Fatalf("re-registration overwrote lifecycle state: %+v", state.Workers[worker.SandboxID])
	}
	_ = syscall.Kill(worker.PID, syscall.SIGCONT)
	worker.Generation++
	worker.State, worker.SafePoint, worker.Ready = "running", true, true
	if err := controller.Register(context.Background(), worker); err != nil {
		t.Fatalf("new generation registration error = %v", err)
	}
	state, _ = store.Snapshot()
	if _, exists := state.Heads[worker.SandboxID]; exists {
		t.Fatalf("new generation retained old head: %+v", state.Heads)
	}
	var output bytes.Buffer
	if err := WriteReceiptsCSV(&output, state); err != nil || !strings.Contains(output.String(), "SUPERSEDED") {
		t.Fatalf("superseded receipt = %q error = %v", output.String(), err)
	}
}
