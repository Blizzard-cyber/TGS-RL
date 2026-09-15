package managedworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Blizzard-cyber/TGS-RL/internal/bootstrapauth"
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

func TestControllerManagedGranularLifecycle(t *testing.T) {
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
			return ControlResponse{Accepted: true, Generation: 4, State: "running", SafePoint: true}, nil
		case "pause":
			return ControlResponse{Accepted: true, Generation: 4, State: "paused", SafePoint: true}, nil
		case "checkpoint":
			return ControlResponse{Accepted: true, Generation: 4, State: "paused", SafePoint: true, CheckpointRef: "checkpoint-a"}, nil
		case "offload":
			return ControlResponse{Accepted: true, Generation: 4, State: "sleeping", SafePoint: true, Offloaded: true, CheckpointRef: "checkpoint-a", GPUMemoryObserved: true}, nil
		case "reload":
			return ControlResponse{Accepted: true, Generation: 4, State: "sleeping", SafePoint: true, CheckpointRef: "checkpoint-a", GPUMemoryObserved: true, GPUMemoryAllocated: 536870912, GPUMemoryReserved: 603979776}, nil
		case "resume":
			return ControlResponse{Accepted: true, Generation: 4, State: "running", Ready: true, CheckpointRef: "checkpoint-a", GPUMemoryObserved: true, GPUMemoryAllocated: 536870912, GPUMemoryReserved: 603979776}, nil
		default:
			return ControlResponse{}, errors.New("unexpected action")
		}
	}
	for _, operation := range []string{"pause", "checkpoint", "offload", "reload", "resume"} {
		if _, err := controller.Apply(context.Background(), actionRequest(operation, operation+"-key")); err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
	}
	want := []string{"prepare_pause", "pause", "checkpoint", "offload", "reload", "resume"}
	if len(calls) != len(want) {
		t.Fatalf("calls = %+v", calls)
	}
	for index := range want {
		if calls[index].Action != want[index] {
			t.Fatalf("call %d = %q, want %q", index, calls[index].Action, want[index])
		}
	}
	state, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	current := state.Workers[worker.SandboxID]
	if current.State != "running" || current.Offloaded || !current.Ready || current.CheckpointRef != "checkpoint-a" {
		t.Fatalf("granular lifecycle final worker = %+v", current)
	}
}

func TestRemoteWorkerRegistrationControlAndStaleExitFence(t *testing.T) {
	controlToken := "worker-token"
	workerState := "running"
	controlServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get(WorkerControlTokenHeader) != controlToken {
			t.Fatalf("control token = %q", request.Header.Get(WorkerControlTokenHeader))
		}
		var control ControlRequest
		if err := json.NewDecoder(request.Body).Decode(&control); err != nil {
			t.Fatal(err)
		}
		switch control.Action {
		case "prepare_pause":
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, SafePoint: true, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1})
		case "pause":
			workerState = "paused"
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, SafePoint: true, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1})
		default:
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, Ready: true, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1})
		}
	}))
	defer controlServer.Close()

	store, err := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	if err != nil {
		t.Fatal(err)
	}
	controller, _ := NewController(store)
	worker := Worker{RunID: "run-a", JobID: "job-a", RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 4, PID: 4242, ProcessToken: "process-a", InstanceID: "instance-a", State: "running", Ready: true, DeviceID: "GPU-aaaa", DeviceIDs: []string{"GPU-aaaa"}, Share: 1, ControlURL: controlServer.URL, ControlToken: controlToken}
	if err := controller.Register(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	paused, err := controller.Apply(context.Background(), actionRequest("pause", "remote-pause"))
	if err != nil || paused.State != "paused" {
		t.Fatalf("remote pause = (%+v, %v)", paused, err)
	}
	updated, matched, err := store.ReportExit("sandbox-a", 4, "old-instance", "process-a", "failed", 1, "late")
	if err != nil || updated || matched {
		t.Fatalf("stale exit = (%v, %v, %v)", updated, matched, err)
	}
}

func TestWorkerRegistryAuthenticatesAndPublishesLifecycle(t *testing.T) {
	signingKey := []byte(strings.Repeat("registry-signing-key-", 2))
	controlServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: "running", Ready: true, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1})
	}))
	defer controlServer.Close()
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := NewController(store)
	var observations []Worker
	handler, err := NewRegistryHandler(controller, store, signingKey, func(_ context.Context, worker Worker) error {
		if worker.BindingID != "binding-a" {
			return errors.New("unexpected binding")
		}
		return nil
	}, func(_ context.Context, worker Worker) error {
		observations = append(observations, worker)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := httptest.NewServer(handler)
	defer registry.Close()
	worker := Worker{RunID: "run-a", JobID: "job-a", RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 4, PID: 4242, ProcessToken: "process-a", InstanceID: "instance-a", State: "running", Ready: true, DeviceIDs: []string{"GPU-aaaa"}, DeviceID: "GPU-aaaa", Share: 1, ControlURL: controlServer.URL, ControlToken: "worker-token"}
	registryToken, err := bootstrapauth.Sign(signingKey, registrationClaims(worker))
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterRemoteWorker(context.Background(), registry.URL, "wrong", worker); err == nil {
		t.Fatal("expected registry authentication failure")
	}
	if err := RegisterRemoteWorker(context.Background(), registry.URL, registryToken, worker); err != nil {
		t.Fatal(err)
	}
	state, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	rebound := state.Workers[worker.SandboxID]
	rebound.Generation, rebound.BindingID, rebound.DeviceID, rebound.DeviceIDs = 5, "binding-b", "GPU-bbbb", []string{"GPU-bbbb"}
	authRequest := httptest.NewRequest(http.MethodPost, "/v1/workers/exit", nil)
	authRequest.Header.Set(WorkerRegistryTokenHeader, registryToken)
	if !authorizedRegisteredWorker(authRequest, rebound) {
		t.Fatal("registered process credential stopped authenticating after a legitimate binding generation update")
	}
	worker.State, worker.Ready, worker.ExitCode, worker.Detail = "failed", false, 9, "worker crashed"
	if err := ReportRemoteWorkerExit(context.Background(), registry.URL, registryToken, worker); err != nil {
		t.Fatal(err)
	}
	if len(observations) != 2 || observations[0].State != "running" || observations[1].State != "failed" {
		t.Fatalf("observations = %+v", observations)
	}
}

func TestWorkerRegistryScopesStatusAndLifecycleToRegistrationToken(t *testing.T) {
	signingKey := []byte(strings.Repeat("registry-signing-key-", 2))
	workerState := "running"
	controlServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var control ControlRequest
		if err := json.NewDecoder(request.Body).Decode(&control); err != nil {
			t.Fatal(err)
		}
		switch control.Action {
		case "prepare_pause":
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, SafePoint: true, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1})
		case "pause":
			workerState = "paused"
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, SafePoint: true, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1})
		case "resume":
			workerState = "running"
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, Ready: true, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1})
		case "stop":
			workerState = "terminated"
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1})
		default:
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, Ready: workerState == "running", BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1})
		}
	}))
	defer controlServer.Close()
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := NewController(store)
	var observations []Worker
	handler, err := NewRegistryHandler(controller, store, signingKey, nil, func(_ context.Context, worker Worker) error {
		observations = append(observations, worker)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := httptest.NewServer(handler)
	defer registry.Close()
	worker := Worker{RunID: "run-a", JobID: "job-a", RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 4, PID: 4242, ProcessToken: "process-a", InstanceID: "instance-a", State: "running", Ready: true, DeviceIDs: []string{"GPU-aaaa"}, DeviceID: "GPU-aaaa", Share: 1, ControlURL: controlServer.URL, ControlToken: "worker-token"}
	token, err := bootstrapauth.Sign(signingKey, registrationClaims(worker))
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterRemoteWorker(context.Background(), registry.URL, token, worker); err != nil {
		t.Fatal(err)
	}
	if _, found, err := GetRemoteWorker(context.Background(), registry.URL, "wrong", WorkerStatusRequest{SandboxID: worker.SandboxID, Generation: worker.Generation}); err == nil || found {
		t.Fatalf("wrong token status = (%v, %v), want authentication failure", found, err)
	}
	for index, action := range []string{"pause", "resume", "stop"} {
		updated, err := ApplyRemoteWorkerAction(context.Background(), registry.URL, token, WorkerActionRequest{Action: action, SandboxID: worker.SandboxID, Generation: worker.Generation, IdempotencyKey: fmt.Sprintf("action-%d", index)})
		if err != nil {
			t.Fatalf("%s action error = %v", action, err)
		}
		want := map[string]string{"pause": "paused", "resume": "running", "stop": "terminated"}[action]
		if updated.State != want {
			t.Fatalf("%s state = %q, want %q", action, updated.State, want)
		}
		if updated.ControlToken != "" || updated.ProcessToken != "" || updated.RegistrationTokenHash != "" || updated.ControlURL != "" {
			t.Fatalf("%s response exposed worker credentials: %+v", action, updated)
		}
	}
	worker.State, worker.Ready, worker.ExitCode, worker.Detail = "terminated", false, 0, "workload completed"
	if err := ReportRemoteWorkerExit(context.Background(), registry.URL, token, worker); err != nil {
		t.Fatalf("report commanded stop exit: %v", err)
	}
	terminal, found, err := GetRemoteWorker(context.Background(), registry.URL, token, WorkerStatusRequest{SandboxID: worker.SandboxID, Generation: worker.Generation})
	if err != nil || !found || terminal.State != "terminated" || terminal.Detail != "workload completed" {
		t.Fatalf("terminal worker = (%+v, %v, %v)", terminal, found, err)
	}
	if len(observations) != 1 || observations[0].State != "running" {
		t.Fatalf("commanded stop published an uncorrelated registry observation: %+v", observations)
	}
	if _, err := ApplyRemoteWorkerAction(context.Background(), registry.URL, token, WorkerActionRequest{Action: "pause", SandboxID: worker.SandboxID, Generation: worker.Generation + 1, IdempotencyKey: "future"}); err == nil {
		t.Fatal("future-generation action succeeded")
	}
}

func TestWorkerRegistryAllowsScopedCooperativeOffloadAndResume(t *testing.T) {
	signingKey := []byte(strings.Repeat("registry-signing-key-", 2))
	workerState, offloaded, checkpointRef := "running", false, ""
	var calls []string
	controlServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var control ControlRequest
		if err := json.NewDecoder(request.Body).Decode(&control); err != nil {
			t.Fatal(err)
		}
		calls = append(calls, control.Action)
		switch control.Action {
		case "prepare_pause":
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, SafePoint: true, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1, GPUMemoryObserved: true, GPUMemoryAllocated: 536870912, GPUMemoryReserved: 603979776})
		case "checkpoint":
			checkpointRef = "/tmp/checkpoint-a"
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, SafePoint: true, CheckpointRef: checkpointRef, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1, GPUMemoryObserved: true, GPUMemoryAllocated: 536870912, GPUMemoryReserved: 603979776})
		case "offload":
			workerState, offloaded = "sleeping", true
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, SafePoint: true, Offloaded: true, CheckpointRef: checkpointRef, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1, GPUMemoryObserved: true, GPUMemoryAllocated: 0, GPUMemoryReserved: 0})
		case "reload":
			offloaded = false
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, SafePoint: true, CheckpointRef: checkpointRef, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1})
		case "resume":
			workerState = "running"
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, Ready: true, Offloaded: offloaded, CheckpointRef: checkpointRef, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1, GPUMemoryObserved: true, GPUMemoryAllocated: 536870912, GPUMemoryReserved: 536870912})
		default:
			allocated, reserved := uint64(536870912), uint64(603979776)
			if offloaded {
				allocated, reserved = 0, 0
			}
			_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: workerState, Ready: true, Offloaded: offloaded, CheckpointRef: checkpointRef, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1, GPUMemoryObserved: true, GPUMemoryAllocated: allocated, GPUMemoryReserved: reserved})
		}
	}))
	defer controlServer.Close()
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := NewController(store)
	handler, err := NewRegistryHandler(controller, store, signingKey, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	registry := httptest.NewServer(handler)
	defer registry.Close()
	worker := Worker{RunID: "run-a", JobID: "job-a", RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 4, PID: 4242, ProcessToken: "process-a", InstanceID: "instance-a", State: "running", Ready: true, DeviceIDs: []string{"GPU-aaaa"}, DeviceID: "GPU-aaaa", Share: 1, ControlURL: controlServer.URL, ControlToken: "worker-token"}
	token, err := bootstrapauth.Sign(signingKey, registrationClaims(worker))
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterRemoteWorker(context.Background(), registry.URL, token, worker); err != nil {
		t.Fatal(err)
	}
	offloadedWorker, err := ApplyRemoteWorkerAction(context.Background(), registry.URL, token, WorkerActionRequest{Action: "offload", SandboxID: worker.SandboxID, Generation: 4, IdempotencyKey: "offload-a"})
	if err != nil || !offloadedWorker.Offloaded || offloadedWorker.CheckpointRef == "" {
		t.Fatalf("offload = (%+v, %v)", offloadedWorker, err)
	}
	if offloadedWorker.GPUMemoryAllocated != 0 || offloadedWorker.GPUMemoryReserved != 0 {
		t.Fatalf("offload GPU memory = (%d, %d)", offloadedWorker.GPUMemoryAllocated, offloadedWorker.GPUMemoryReserved)
	}
	var replay workerActionResponse
	if err := postRegistryResponse(context.Background(), registry.URL, token, "/v1/workers/action", WorkerActionRequest{Action: "offload", SandboxID: worker.SandboxID, Generation: 4, IdempotencyKey: "offload-a"}, &replay); err != nil {
		t.Fatal(err)
	}
	if !replay.GPUMemoryObservedBefore || replay.GPUMemoryAllocatedBefore != 536870912 || replay.GPUMemoryReservedBefore != 603979776 {
		t.Fatalf("replayed offload source memory = %+v", replay)
	}
	resumedWorker, err := ApplyRemoteWorkerAction(context.Background(), registry.URL, token, WorkerActionRequest{Action: "resume", SandboxID: worker.SandboxID, Generation: 4, IdempotencyKey: "resume-a"})
	if err != nil || !resumedWorker.Ready || resumedWorker.Offloaded {
		t.Fatalf("resume = (%+v, %v)", resumedWorker, err)
	}
	if resumedWorker.GPUMemoryAllocated != 536870912 || resumedWorker.GPUMemoryReserved != 536870912 {
		t.Fatalf("resume GPU memory = (%d, %d)", resumedWorker.GPUMemoryAllocated, resumedWorker.GPUMemoryReserved)
	}
	if want := []string{"status", "status", "status", "prepare_pause", "checkpoint", "offload", "status", "status", "reload", "resume"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("control calls = %+v, want %+v", calls, want)
	}
}

func TestWorkerRegistryPublishesTraceOnlyForCurrentRegisteredIdentity(t *testing.T) {
	signingKey := []byte(strings.Repeat("registry-signing-key-", 2))
	controlServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(ControlResponse{Accepted: true, Generation: 4, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", State: "running", Ready: true, BindingID: "binding-a", DeviceID: "GPU-aaaa", Share: 1})
	}))
	defer controlServer.Close()
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := NewController(store)
	worker := Worker{RunID: "run-a", JobID: "job-a", TraceID: "trace-a", RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 4, PID: 4242, ProcessToken: "process-a", InstanceID: "instance-a", State: "running", Ready: true, DeviceIDs: []string{"GPU-aaaa"}, DeviceID: "GPU-aaaa", Share: 1, ControlURL: controlServer.URL, ControlToken: "worker-token"}
	var published WorkerTraceRequest
	handler, err := NewRegistryHandler(controller, store, signingKey, nil, nil, func(_ context.Context, authorized Worker, request WorkerTraceRequest) (WorkerTraceResponse, error) {
		if authorized.TraceID != worker.TraceID {
			return WorkerTraceResponse{}, errors.New("unexpected trace identity")
		}
		published = request
		return WorkerTraceResponse{AcceptedEventCount: 1, Cursor: "cursor-a"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := httptest.NewServer(handler)
	defer registry.Close()
	token, err := bootstrapauth.Sign(signingKey, registrationClaims(worker))
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterRemoteWorker(context.Background(), registry.URL, token, worker); err != nil {
		t.Fatal(err)
	}
	trace := WorkerTraceRequest{SandboxID: worker.SandboxID, Generation: worker.Generation, IdempotencyKey: "trace-sha256-a", Batch: []byte("batch-a")}
	response, err := PublishRemoteWorkerTrace(context.Background(), registry.URL, token, trace)
	if err != nil || response.AcceptedEventCount != 1 || response.Cursor != "cursor-a" || !reflect.DeepEqual(published, trace) {
		t.Fatalf("trace publication = (%+v, %v), request=%+v", response, err, published)
	}
	if _, err := PublishRemoteWorkerTrace(context.Background(), registry.URL, "wrong", trace); err == nil {
		t.Fatal("trace publication accepted the wrong registration token")
	}
	stale := trace
	stale.Generation--
	if _, err := PublishRemoteWorkerTrace(context.Background(), registry.URL, token, stale); err == nil {
		t.Fatal("trace publication accepted a stale generation")
	}
}

func TestStoreRejectsRemoteRestartUntilPendingReceiptIsReconciled(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	if err != nil {
		t.Fatal(err)
	}
	worker := Worker{RunID: "run-a", JobID: "job-a", RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 4, PID: 41, ProcessToken: "process-old", InstanceID: "pod-a", State: "running", Ready: true, DeviceIDs: []string{"GPU-a"}, Share: 1, ControlURL: "http://127.0.0.1:50092/v1/control", ControlToken: "token-old"}
	if err := store.Register(worker); err != nil {
		t.Fatal(err)
	}
	request := actionRequest("pause", "pending-a")
	if _, _, _, err := store.Begin(request); err != nil {
		t.Fatal(err)
	}
	restarted := worker
	restarted.PID, restarted.ProcessToken, restarted.ControlToken = 42, "process-new", "token-new"
	if err := store.Register(restarted); err == nil || !strings.Contains(err.Error(), "unresolved lifecycle action") {
		t.Fatalf("restart with pending receipt error = %v", err)
	}
	state, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if receipt := state.Receipts[request.IdempotencyKey]; receipt.Completed {
		t.Fatalf("pending receipt was guessed complete = %+v", receipt)
	}
	updated, matched, err := store.ReportExit(worker.SandboxID, worker.Generation, worker.InstanceID, worker.ProcessToken, "failed", 9, "old worker exited")
	if err != nil || !updated || !matched {
		t.Fatalf("old worker exit = (%v, %v, %v)", updated, matched, err)
	}
	controller, _ := NewController(store)
	state, err = controller.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if receipt := state.Receipts[request.IdempotencyKey]; !receipt.Completed || receipt.Succeeded || receipt.ErrorCode != "WORKER_UNAVAILABLE" {
		t.Fatalf("reconciled receipt = %+v", receipt)
	}
}

func TestStoreRejectsReregistrationAfterTerminalOutcome(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	if err != nil {
		t.Fatal(err)
	}
	worker := Worker{RunID: "run-a", JobID: "job-a", RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 4, PID: 41, ProcessToken: "process-a", InstanceID: "pod-a", State: "running", Ready: true, DeviceIDs: []string{"GPU-a"}, Share: 1, ControlURL: "http://127.0.0.1:50092/v1/control", ControlToken: "token-a"}
	if err := store.Register(worker); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ReportExit(worker.SandboxID, worker.Generation, worker.InstanceID, worker.ProcessToken, "failed", 9, "crashed"); err != nil {
		t.Fatal(err)
	}
	if err := store.Register(worker); err == nil || !strings.Contains(err.Error(), "terminal worker state") {
		t.Fatalf("terminal re-registration error = %v", err)
	}
}

func TestControllerDiscoverMarksUnreachableRemoteWorkerFailedAndReconcilesReceipt(t *testing.T) {
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	worker := Worker{RunID: "run-a", JobID: "job-a", RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 4, PID: 42, ProcessToken: "process-a", InstanceID: "pod-a", State: "running", Ready: true, DeviceIDs: []string{"GPU-a"}, Share: 1, ControlURL: "http://127.0.0.1:1/v1/control", ControlToken: "token-a"}
	if err := store.Register(worker); err != nil {
		t.Fatal(err)
	}
	request := actionRequest("pause", "pending-unreachable")
	if _, _, _, err := store.Begin(request); err != nil {
		t.Fatal(err)
	}
	controller, _ := NewController(store)
	controller.remoteControl = func(context.Context, string, string, ControlRequest) (ControlResponse, error) {
		return ControlResponse{}, ErrRequestNotDelivered
	}
	state, err := controller.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Workers[worker.SandboxID].State != "failed" {
		t.Fatalf("worker state = %+v", state.Workers[worker.SandboxID])
	}
	receipt := state.Receipts[request.IdempotencyKey]
	if !receipt.Completed || receipt.Succeeded || receipt.ErrorCode != "WORKER_UNAVAILABLE" {
		t.Fatalf("receipt = %+v", receipt)
	}
}

func TestStoreRejectsMalformedRemoteWorkerIdentity(t *testing.T) {
	store, _ := NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	worker := Worker{SandboxID: "sandbox-a", Generation: 4, PID: 42, ProcessToken: "process-a", InstanceID: "pod-a", State: "running"}
	if err := store.Register(worker); err == nil || !strings.Contains(err.Error(), "control URL") {
		t.Fatalf("malformed remote worker error = %v", err)
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
			if !request.PreserveProcess {
				t.Fatal("MIG rebind stop must preserve the bootstrap process")
			}
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
