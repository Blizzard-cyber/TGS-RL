package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	runtimehelper "github.com/Blizzard-cyber/TGS-RL/internal/managedworker"
)

func TestRuntimeHelperControlsRegisteredProcess(t *testing.T) {
	command := exec.Command("sleep", "30")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = syscall.Kill(command.Process.Pid, syscall.SIGCONT)
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
	}()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "runtime.json")
	safePointPath := filepath.Join(directory, "safe-point")
	readyPath := filepath.Join(directory, "ready")
	if err := os.WriteFile(safePointPath, []byte("true"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readyPath, []byte("true"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TGSRL_NVIDIA_RUNTIME_STATE", statePath)
	pid := strconv.Itoa(command.Process.Pid)
	if err := run([]string{"register", "--sandbox", "sandbox-a", "--generation", "4", "--pid", pid, "--safe-point-file", safePointPath, "--readiness-file", readyPath}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"discover", "--format=csv"}, &output); err != nil || strings.TrimSpace(output.String()) != "sandbox-a,4,running,true,false,0" {
		t.Fatalf("discover = %q error = %v", output.String(), err)
	}
	pause := []string{"pause", "--sandbox", "sandbox-a", "--generation", "4", "--idempotency-key", "pause-key", "--recoverable", "--plan-id", "plan-a", "--action-id", "pause-a", "--plan-digest", "sha256:plan", "--command-digest", "sha256:pause", "--timeout", "2s"}
	if err := run(pause, &bytes.Buffer{}); err != nil {
		t.Fatalf("pause error = %v", err)
	}
	if err := run(pause, &bytes.Buffer{}); err != nil {
		t.Fatalf("idempotent pause error = %v", err)
	}
	resume := []string{"resume", "--sandbox", "sandbox-a", "--generation", "4", "--idempotency-key", "resume-key", "--plan-id", "standalone-resume", "--action-id", "resume-a", "--timeout", "2s"}
	if err := run(resume, &bytes.Buffer{}); err != nil {
		t.Fatalf("resume error = %v", err)
	}
	output.Reset()
	if err := run([]string{"receipts", "--format=csv"}, &output); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(output.String()); !strings.Contains(got, "0,1,false,sha256:plan,pause-a,plan-a,pause-key,sandbox-a,1,4,false,SUPERSEDED") {
		t.Fatalf("receipts = %q", got)
	}
}

func TestRuntimeHelperCapabilities(t *testing.T) {
	t.Setenv("TGSRL_NVIDIA_RUNTIME_STATE", filepath.Join(t.TempDir(), "runtime.json"))
	var output bytes.Buffer
	if err := run([]string{"capabilities", "--format=csv"}, &output); err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(output.String())
	for _, value := range []string{"pause", "resume", "sleep", "offload", "durable_receipts", "safe_point", "checkpoint", "reload", "readiness"} {
		if !strings.Contains(got, value) {
			t.Fatalf("capabilities %q missing %q", got, value)
		}
	}
}

func TestParseWorkerAcceptsRemoteBootstrapIdentity(t *testing.T) {
	worker, err := parseWorker([]string{
		"--run", "run-a", "--job", "job-a", "--trace", "trace-a",
		"--runtime-unit", "unit-a", "--sandbox", "sandbox-a", "--binding", "binding-a",
		"--generation", "4", "--pid", "42", "--process-token", "process-a",
		"--instance-id", "pod-a", "--control-url", "http://127.0.0.1:50092/v1/control",
		"--control-token", "control-a", "--device-id", "GPU-a", "--device-id", "GPU-b",
		"--share", "1", "--mps-server-pid", "77",
	})
	if err != nil {
		t.Fatal(err)
	}
	if worker.InstanceID != "pod-a" || worker.ProcessToken != "process-a" || len(worker.DeviceIDs) != 2 || worker.MPSServerPID != 77 {
		t.Fatalf("worker = %+v", worker)
	}
}

func TestRuntimeHelperServeRequiresAuthentication(t *testing.T) {
	store, _ := runtimehelper.NewStore(filepath.Join(t.TempDir(), "runtime.json"))
	controller, _ := runtimehelper.NewController(store)
	handler, err := runtimehelper.NewRegistryHandler(controller, store, []byte(strings.Repeat("s", 32)), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/workers/register", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}
