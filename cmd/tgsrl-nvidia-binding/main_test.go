package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimehelper "github.com/Blizzard-cyber/TGS-RL/internal/managedworker"
)

func TestBindingHelperLifecycleAndRestartDiscovery(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "binding.json")
	t.Setenv("TGSRL_NVIDIA_BINDING_STATE", statePath)
	t.Setenv("TGSRL_NVIDIA_MPS_PID_DIR", "")
	var output bytes.Buffer
	if err := run([]string{"capabilities", "--format=csv"}, &output); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(output.String()); got != "tgsrl-nvidia-binding,1,bind,release,durable_receipts,generation_fence,idempotency" {
		t.Fatalf("capabilities = %q", got)
	}
	bind := []string{"bind", "--sandbox", "sandbox-a", "--binding", "binding-a", "--generation", "4", "--idempotency-key", "key-a", "--share", "0.5", "--device", "GPU-b", "--device", "GPU-a", "--recoverable", "--step-index", "0", "--expected-actions", "1", "--transaction-generation", "7", "--plan-digest", "plan-digest", "--command-digest", "action-digest", "--action-id", "action-a", "--plan-id", "plan-a"}
	output.Reset()
	if err := run(bind, &output); err != nil {
		t.Fatalf("bind error = %v", err)
	}
	if got := strings.TrimSpace(output.String()); got != "sandbox-a,binding-a,4,GPU-a;GPU-b,0.5,key-a" {
		t.Fatalf("bind output = %q", got)
	}
	output.Reset()
	if err := run([]string{"discover", "--format=csv"}, &output); err != nil || strings.TrimSpace(output.String()) != "sandbox-a,binding-a,4,GPU-a;GPU-b,0.5,key-a" {
		t.Fatalf("restart discovery = %q error=%v", strings.TrimSpace(output.String()), err)
	}
	output.Reset()
	if err := run([]string{"receipts", "--format=csv"}, &output); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(output.String()); got != "0,1,false,plan-digest,action-a,plan-a,key-a,sandbox-a,7,4,true,,,action-digest" {
		t.Fatalf("receipt output = %q", got)
	}
	release := []string{"release", "--sandbox", "sandbox-a", "--target", "allocation-a", "--generation", "4", "--idempotency-key", "key-release", "--step-index", "0", "--expected-actions", "1", "--transaction-generation", "8", "--plan-digest", "release-plan-digest", "--command-digest", "release-action-digest", "--action-id", "action-release", "--plan-id", "plan-release"}
	if err := run(release, &bytes.Buffer{}); err != nil {
		t.Fatalf("release error = %v", err)
	}
	output.Reset()
	if err := run([]string{"discover", "--format=csv"}, &output); err != nil || output.Len() != 0 {
		t.Fatalf("post-release discovery = %q error=%v", output.String(), err)
	}
}

func TestBindingHelperRejectsStaleGenerationAndConflictingRetry(t *testing.T) {
	t.Setenv("TGSRL_NVIDIA_BINDING_STATE", filepath.Join(t.TempDir(), "binding.json"))
	base := []string{"bind", "--sandbox", "sandbox-a", "--binding", "binding-a", "--generation", "4", "--idempotency-key", "key-a", "--device", "GPU-a"}
	if err := run(base, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if err := run(base, &bytes.Buffer{}); err != nil {
		t.Fatalf("idempotent retry error = %v", err)
	}
	conflict := append([]string(nil), base...)
	conflict[len(conflict)-1] = "GPU-b"
	if err := run(conflict, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "idempotency key") {
		t.Fatalf("conflicting retry error = %v", err)
	}
	stale := []string{"release", "--sandbox", "sandbox-a", "--target", "allocation-a", "--generation", "3", "--idempotency-key", "release-key"}
	if err := run(stale, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "generation fence") {
		t.Fatalf("stale release error = %v", err)
	}
}

func TestBindingHelperFailsClosedForCorruptState(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "binding.json")
	t.Setenv("TGSRL_NVIDIA_BINDING_STATE", statePath)
	if err := os.WriteFile(statePath, []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"discover", "--format=csv"}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "decode helper state") {
		t.Fatalf("corrupt state error = %v", err)
	}
}

func TestBindingHelperAdvertisesLiveMPSPIDOnly(t *testing.T) {
	directory := t.TempDir()
	t.Setenv("TGSRL_NVIDIA_BINDING_STATE", filepath.Join(t.TempDir(), "binding.json"))
	t.Setenv("TGSRL_NVIDIA_MPS_PID_DIR", directory)
	if err := os.WriteFile(filepath.Join(directory, "sandbox-a.pid"), []byte(fmt.Sprint(os.Getpid())), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"capabilities", "--format=csv"}, &output); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(output.String()); !strings.HasSuffix(got, ",mps_profile_pid") {
		t.Fatalf("capabilities = %q", got)
	}
	processToken, err := runtimehelper.ProcessToken(context.Background(), os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	record, err := json.Marshal(map[string]any{"generation": 4, "pid": os.Getpid(), "process_token": processToken})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "sandbox-a.pid"), append(record, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := resolveMPSPID("sandbox-a", 5); got != 0 {
		t.Fatalf("stale-generation MPS PID = %d, want 0", got)
	}
	if got := resolveMPSPID("sandbox-a", 4); got != uint32(os.Getpid()) {
		t.Fatalf("matching-generation MPS PID = %d, want %d", got, os.Getpid())
	}
	output.Reset()
	if err := run([]string{"capabilities", "--format=csv"}, &output); err != nil || !strings.HasSuffix(strings.TrimSpace(output.String()), ",mps_profile_pid") {
		t.Fatalf("generation-fenced PID capabilities = %q error = %v", output.String(), err)
	}
	record, err = json.Marshal(map[string]any{"generation": 4, "pid": os.Getpid(), "process_token": "stale-token"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "sandbox-a.pid"), append(record, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := run([]string{"capabilities", "--format=csv"}, &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "mps_profile_pid") {
		t.Fatalf("reused MPS PID advertised capability: %q", output.String())
	}
	if err := os.WriteFile(filepath.Join(directory, "sandbox-a.pid"), []byte("999999999"), 0o600); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := run([]string{"capabilities", "--format=csv"}, &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "mps_profile_pid") {
		t.Fatalf("stale PID advertised MPS capability: %q", output.String())
	}
}
