package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	runtimehelper "github.com/Blizzard-cyber/TGS-RL/internal/managedworker"
)

func TestRunWorkerRegistersRealProcessAndReportsExit(t *testing.T) {
	var mu sync.Mutex
	var registered, terminal runtimehelper.Worker
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get(runtimehelper.WorkerRegistryTokenHeader) != "registry-token" {
			t.Fatalf("registry token = %q", request.Header.Get(runtimehelper.WorkerRegistryTokenHeader))
		}
		mu.Lock()
		defer mu.Unlock()
		switch request.URL.Path {
		case "/v1/workers/register":
			if err := json.NewDecoder(request.Body).Decode(&registered); err != nil {
				t.Fatal(err)
			}
		case "/v1/workers/exit":
			if err := json.NewDecoder(request.Body).Decode(&terminal); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unexpected registry path %q", request.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
	}))
	defer registry.Close()
	setWorkerEnvironment(t)
	if err := runWorker(config{registryURL: registry.URL, registryToken: "registry-token", listenAddress: "127.0.0.1:0", advertiseHost: "127.0.0.1", registrationWait: time.Second, command: []string{"/bin/sh", "-c", "sleep 0.1"}}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if registered.PID <= 0 || registered.ProcessToken == "" || registered.ControlURL == "" || registered.InstanceID != "pod-a" {
		t.Fatalf("registered worker = %+v", registered)
	}
	if terminal.State != "terminated" || terminal.Generation != 4 || terminal.InstanceID != registered.InstanceID || terminal.ProcessToken != registered.ProcessToken {
		t.Fatalf("terminal worker = %+v", terminal)
	}
}

func TestRegistrationReadyMarkerIsIdentityScopedAndCleaned(t *testing.T) {
	t.Setenv("TGSRL_SANDBOX_ID", "sandbox-a")
	t.Setenv("TGSRL_GENERATION", "4")
	t.Setenv("TGSRL_POD_UID", "pod-a")
	path := filepath.Join(t.TempDir(), "registered.json")
	worker := runtimehelper.Worker{
		SandboxID:  "sandbox-a",
		Generation: 4,
		InstanceID: "pod-a",
	}

	if _, err := prepareRegistrationReady(path); err != nil {
		t.Fatal(err)
	}
	if err := writeRegistrationReady(path, worker); err != nil {
		t.Fatal(err)
	}
	if err := ready([]string{"--file", path}); err != nil {
		t.Fatalf("ready() error = %v", err)
	}
	t.Setenv("TGSRL_GENERATION", "5")
	if err := ready([]string{"--file", path}); err == nil ||
		!strings.Contains(err.Error(), "does not match") {
		t.Fatalf("stale ready marker error = %v", err)
	}
	t.Setenv("TGSRL_GENERATION", "4")
	cleanupRegistrationReady(path, runtimehelper.Worker{
		SandboxID:  worker.SandboxID,
		Generation: worker.Generation + 1,
		InstanceID: worker.InstanceID,
	})
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stale cleanup removed current marker: %v", err)
	}
	cleanupRegistrationReady(path, worker)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("registration marker still exists: %v", err)
	}
}

func TestPrepareRegistrationReadyRemovesStaleMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registered.json")
	if err := os.WriteFile(path, []byte(`{"sandbox_id":"old"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	clean, err := prepareRegistrationReady(path)
	if err != nil {
		t.Fatal(err)
	}
	if clean != path {
		t.Fatalf("registration marker path = %q, want %q", clean, path)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale registration marker still exists: %v", err)
	}
}

func TestSupervisorTraceProxyUsesDedicatedTokenAndRegistryCredential(t *testing.T) {
	var registryToken string
	var forwarded runtimehelper.WorkerTraceRequest
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		registryToken = request.Header.Get(runtimehelper.WorkerRegistryTokenHeader)
		if err := json.NewDecoder(request.Body).Decode(&forwarded); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(runtimehelper.WorkerTraceResponse{AcceptedEventCount: 1, Cursor: "cursor-a"})
	}))
	defer registry.Close()
	s := &supervisor{worker: runtimehelper.Worker{SandboxID: "sandbox-a", Generation: 4}, traceToken: "trace-token", registryURL: registry.URL, registryToken: "registry-token", controlTimeout: time.Second, registered: true}
	server := httptest.NewServer(s)
	defer server.Close()
	payload, _ := json.Marshal(runtimehelper.WorkerTraceRequest{SandboxID: "forged", Generation: 99, IdempotencyKey: "trace-key", Batch: []byte("batch")})
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/trace", bytes.NewReader(payload))
	request.Header.Set(runtimehelper.WorkerTraceTokenHeader, "trace-token")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || registryToken != "registry-token" {
		t.Fatalf("trace proxy status/token = %d/%q", response.StatusCode, registryToken)
	}
	if forwarded.SandboxID != "sandbox-a" || forwarded.Generation != 4 || forwarded.IdempotencyKey != "trace-key" {
		t.Fatalf("forwarded trace = %+v", forwarded)
	}
	bad, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/trace", bytes.NewReader(payload))
	bad.Header.Set(runtimehelper.WorkerTraceTokenHeader, "registry-token")
	badResponse, err := http.DefaultClient.Do(bad)
	if err != nil {
		t.Fatal(err)
	}
	defer badResponse.Body.Close()
	if badResponse.StatusCode != http.StatusUnauthorized {
		t.Fatalf("registry credential was accepted as workload trace token: %d", badResponse.StatusCode)
	}
}

func TestRunWorkerForwardsTerminationAndReportsCleanExit(t *testing.T) {
	registered := make(chan runtimehelper.Worker, 1)
	terminal := make(chan runtimehelper.Worker, 1)
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var worker runtimehelper.Worker
		if err := json.NewDecoder(request.Body).Decode(&worker); err != nil {
			t.Fatal(err)
		}
		if request.URL.Path == "/v1/workers/register" {
			registered <- worker
		} else {
			terminal <- worker
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
	}))
	defer registry.Close()
	setWorkerEnvironment(t)
	markerDirectory := t.TempDir()
	safePoint := filepath.Join(markerDirectory, "safe-point")
	readiness := filepath.Join(markerDirectory, "readiness")
	for _, path := range []string{safePoint, readiness} {
		if err := os.WriteFile(path, []byte("true"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	signals := make(chan os.Signal, 1)
	done := make(chan error, 1)
	go func() {
		done <- runWorker(config{registryURL: registry.URL, registryToken: "registry-token", listenAddress: "127.0.0.1:0", advertiseHost: "127.0.0.1", safePointFile: safePoint, readinessFile: readiness, registrationWait: time.Second, command: []string{"sleep", "30"}, signals: signals})
	}()
	select {
	case <-registered:
		signals <- syscall.SIGTERM
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not register")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("bootstrap-initiated termination must be reported as an orderly stop: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bootstrap did not terminate after forwarding SIGTERM")
	}
	select {
	case observed := <-terminal:
		if observed.State != "terminated" {
			t.Fatalf("terminal state = %+v", observed)
		}
	case <-time.After(time.Second):
		t.Fatal("bootstrap did not report terminal state")
	}
}

func TestRegisterWithRetryDoesNotRegisterProcessThatAlreadyExited(t *testing.T) {
	registered := make(chan struct{}, 1)
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		registered <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer registry.Close()
	waiter := &processWait{done: make(chan struct{}), err: errors.New("exited")}
	close(waiter.done)
	err := registerWithRetry(context.Background(), registry.URL, "registry-token", runtimehelper.Worker{}, waiter)
	if err == nil || !strings.Contains(err.Error(), "exited before registration") {
		t.Fatalf("registerWithRetry() error = %v", err)
	}
	select {
	case <-registered:
		t.Fatal("registry was called after the workload exited")
	default:
	}
}

func TestSupervisorControlsProcessAndFencesIdentity(t *testing.T) {
	command := execCommand(t, "sleep", "30")
	defer func() {
		_ = signalGroup(command.Process.Pid, syscall.SIGCONT)
		_ = signalGroup(command.Process.Pid, syscall.SIGKILL)
		_, _ = command.Process.Wait()
	}()
	safePoint := filepath.Join(t.TempDir(), "safe-point")
	readiness := filepath.Join(t.TempDir(), "ready")
	if err := os.WriteFile(safePoint, []byte("true"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readiness, []byte("true"), 0o600); err != nil {
		t.Fatal(err)
	}
	processToken, err := runtimehelper.ProcessToken(context.Background(), command.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	s := &supervisor{worker: runtimehelper.Worker{RunID: "run-a", JobID: "job-a", RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", BindingID: "binding-a", DeviceID: "GPU-aaaa", DeviceIDs: []string{"GPU-aaaa"}, Share: 1, Generation: 4, PID: command.Process.Pid, ProcessToken: processToken, InstanceID: "pod-a", State: "running", Ready: true}, controlToken: "control-token", safePointFile: safePoint, readinessFile: readiness, receipts: map[string]receipt{}}
	server := httptest.NewServer(s)
	defer server.Close()
	worker := s.snapshot()
	worker.ControlURL = server.URL + "/v1/control"
	worker.ControlToken = "control-token"
	controllerStore, _ := runtimehelper.NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := runtimehelper.NewController(controllerStore)
	if err := controller.Register(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	paused, err := controller.Apply(context.Background(), runtimehelper.ActionRequest{Operation: "pause", SandboxID: "sandbox-a", Generation: 4, IdempotencyKey: "pause-a", StepIndex: 0, ExpectedActions: 1, TransactionGeneration: 1, PlanDigest: "plan", CommandDigest: "command", ActionID: "action", PlanID: "plan"})
	if err != nil || paused.State != "paused" {
		t.Fatalf("pause = (%+v, %v)", paused, err)
	}
	if status, _ := postControl(t, server.URL, "wrong", runtimehelper.ControlRequest{Action: "status", SandboxID: "sandbox-a", Generation: 4}); status != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d", status)
	}
	if status, body := postControl(t, server.URL, "control-token", runtimehelper.ControlRequest{Action: "status", SandboxID: "sandbox-a", Generation: 3}); status != http.StatusOK || !strings.Contains(body, "generation mismatch") {
		t.Fatalf("stale generation response = %d %s", status, body)
	}
}

func TestSupervisorSnapshotDoesNotBlockOnCooperativeControl(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "tgsrl-bootstrap-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socketPath := filepath.Join(directory, "worker.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	requestReceived := make(chan struct{})
	releaseResponse := make(chan struct{})
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		var request runtimehelper.ControlRequest
		if json.NewDecoder(connection).Decode(&request) != nil {
			return
		}
		close(requestReceived)
		<-releaseResponse
		_ = json.NewEncoder(connection).Encode(runtimehelper.ControlResponse{
			Accepted: true, Generation: 4, State: "running", Ready: true,
		})
	}()
	s := &supervisor{
		worker: runtimehelper.Worker{
			SandboxID: "sandbox-a", Generation: 4, State: "running", Ready: true,
		},
		controlSocket: socketPath, controlTimeout: time.Second, receipts: map[string]receipt{},
	}
	done := make(chan runtimehelper.ControlResponse, 1)
	go func() {
		done <- s.handle(context.Background(), runtimehelper.ControlRequest{
			Action: "prepare_pause", SandboxID: "sandbox-a", Generation: 4,
			IdempotencyKey: "pause-a",
		})
	}()
	select {
	case <-requestReceived:
	case <-time.After(time.Second):
		t.Fatal("cooperative control request was not received")
	}
	snapshotDone := make(chan runtimehelper.Worker, 1)
	go func() { snapshotDone <- s.snapshot() }()
	select {
	case worker := <-snapshotDone:
		if worker.SandboxID != "sandbox-a" {
			t.Fatalf("snapshot = %+v", worker)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("worker snapshot blocked behind cooperative control I/O")
	}
	close(releaseResponse)
	select {
	case response := <-done:
		if !response.Accepted {
			t.Fatalf("control response = %+v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("cooperative control did not finish")
	}
}

func TestVerifyDeviceIdentitiesUsesVisibleUUIDs(t *testing.T) {
	directory := t.TempDir()
	command := filepath.Join(directory, "nvidia-smi")
	script := "#!/bin/sh\nprintf 'GPU 0: test (UUID: GPU-aaaa)\\n  MIG device 0: (UUID: MIG-bbbb)\\n'\n"
	if err := os.WriteFile(command, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := verifyDeviceIdentities(context.Background(), command, []string{"GPU-aaaa"}); err != nil {
		t.Fatal(err)
	}
	if err := verifyDeviceIdentities(context.Background(), command, []string{"MIG-bbbb"}); err != nil {
		t.Fatal(err)
	}
	if err := verifyDeviceIdentities(context.Background(), command, []string{"GPU-missing"}); err == nil {
		t.Fatal("expected missing device identity error")
	}
}

func TestVerifyWorkloadDependenciesUsesWorkloadInterpreter(t *testing.T) {
	if err := verifyWorkloadDependencies([]string{os.Args[0]}, []string{"json"}); err == nil || !strings.Contains(err.Error(), "Python workload command") {
		t.Fatalf("non-Python dependency check error = %v", err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is unavailable")
	}
	if err := verifyWorkloadDependencies([]string{python, "worker.py"}, []string{"json", "pathlib"}); err != nil {
		t.Fatal(err)
	}
	if err := verifyWorkloadDependencies([]string{python}, []string{"module_that_must_not_exist_tgsrl"}); err == nil || !strings.Contains(err.Error(), "missing Python module") {
		t.Fatalf("missing module error = %v", err)
	}
}

func TestMPSPIDCleanupCannotRemoveReplacementGeneration(t *testing.T) {
	directory := t.TempDir()
	if err := writeMPSPID(directory, "sandbox-a", 4, 42, "token-a"); err != nil {
		t.Fatal(err)
	}
	if err := writeMPSPID(directory, "sandbox-a", 5, 43, "token-b"); err != nil {
		t.Fatal(err)
	}
	if err := cleanupMPSPID(directory, "sandbox-a", 4, 42, "token-a"); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(filepath.Join(directory, "sandbox-a.pid"))
	var record mpsPIDRecord
	if decodeErr := json.Unmarshal(payload, &record); err != nil || decodeErr != nil || record != (mpsPIDRecord{Generation: 5, PID: 43, ProcessToken: "token-b"}) {
		t.Fatalf("replacement MPS PID = %q error = %v", payload, err)
	}
	if err := cleanupMPSPID(directory, "sandbox-a", 5, 43, "token-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(directory, "sandbox-a.pid")); !os.IsNotExist(err) {
		t.Fatalf("MPS PID file still exists: %v", err)
	}
}

func setWorkerEnvironment(t *testing.T) {
	t.Helper()
	values := map[string]string{"TGSRL_RUN_ID": "run-a", "TGSRL_JOB_ID": "job-a", "TGSRL_TRACE_ID": "trace-a", "TGSRL_EXECUTION_ID": "execution-a", "TGSRL_RUNTIME_UNIT_ID": "unit-a", "TGSRL_SANDBOX_ID": "sandbox-a", "TGSRL_BINDING_ID": "binding-a", "TGSRL_GENERATION": "4", "TGSRL_DEVICE_IDS": "GPU-aaaa", "TGSRL_ACCELERATOR_SHARE": "1", "TGSRL_POD_UID": "pod-a"}
	for key, value := range values {
		t.Setenv(key, value)
	}
}

func execCommand(t *testing.T, name string, args ...string) *exec.Cmd {
	t.Helper()
	command := exec.Command(name, args...)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	return command
}

func postControl(t *testing.T, serverURL, token string, control runtimehelper.ControlRequest) (int, string) {
	t.Helper()
	payload, _ := json.Marshal(control)
	request, err := http.NewRequest(http.MethodPost, serverURL+"/v1/control", strings.NewReader(string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(runtimehelper.WorkerControlTokenHeader, token)
	result, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Body.Close()
	body, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatal(err)
	}
	return result.StatusCode, string(body)
}
