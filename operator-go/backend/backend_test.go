package backend

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

func TestProcessBackendRequiresExecutableAndScopedCredentials(t *testing.T) {
	if _, err := NewProcess(ProcessConfig{BootstrapBinary: "missing-tgsrl-bootstrap", StateDirectory: t.TempDir()}); err == nil {
		t.Fatal("missing bootstrap executable was accepted")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	backend, err := NewProcess(ProcessConfig{BootstrapBinary: executable, StateDirectory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	bundle := testBundle()
	bundle.Job.Spec.Template.Spec.Containers = []api.Container{{
		Env: []api.EnvVar{
			{Name: "TGSRL_SANDBOX_ID", Value: "sandbox-1"},
			{Name: "TGSRL_WORKER_REGISTRY_URL", Value: "http://127.0.0.1:50091"},
			{Name: "TGSRL_WORKER_REGISTRY_TOKEN", Value: "scoped-token"},
		},
	}}
	token, registryURL, err := processCredential(bundle, "sandbox-1")
	if err != nil || token != "scoped-token" || registryURL != "http://127.0.0.1:50091" {
		t.Fatalf("process credential = (%q, %q, %v)", token, registryURL, err)
	}
	if _, _, err := processCredential(bundle, "sandbox-2"); err == nil {
		t.Fatal("cross-sandbox credential lookup succeeded")
	}
	if got := mergedEnvironment(map[string]string{"TGSRL_TEST_OVERRIDE": "new"}); !containsEnvironment(got, "TGSRL_TEST_OVERRIDE=new") {
		t.Fatalf("merged environment = %+v", got)
	}
	t.Setenv("TGSRL_WORKER_REGISTRY_SIGNING_KEY", "master-key-must-not-leak")
	t.Setenv("UNRELATED_PROCESS_SECRET", "must-not-leak")
	if got := mergedEnvironment(nil); containsEnvironment(got, "TGSRL_WORKER_REGISTRY_SIGNING_KEY=master-key-must-not-leak") || containsEnvironment(got, "UNRELATED_PROCESS_SECRET=must-not-leak") {
		t.Fatalf("process environment exposed an inherited secret: %+v", got)
	}
	if backend.bootstrap != executable || !filepath.IsAbs(backend.stateDir) {
		t.Fatalf("process backend = %+v", backend)
	}
}

func TestProcessBackendSnapshotUsesScopedRegistryReadback(t *testing.T) {
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/workers/status" || request.Header.Get("X-TGSRL-Worker-Token") != "scoped-token" {
			http.Error(w, "unexpected request", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "worker": map[string]any{
			"sandbox_id": "sandbox-1", "generation": 1, "pid": 42,
			"process_token": "", "state": "running", "ready": true,
		}})
	}))
	defer registry.Close()
	b := &ProcessBackend{inner: NewFake(), bootstrap: "unused", stateDir: t.TempDir(), processes: make(map[string]*managedProcess)}
	bundle := testBundle()
	bundle.SourceRunID, bundle.SourceJobID = "run-1", "job-1"
	bundle.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", BindingID: "binding-1", Generation: 1}}
	bundle.Job.Spec.Template.Spec.Containers = []api.Container{{Env: []api.EnvVar{
		{Name: "TGSRL_SANDBOX_ID", Value: "sandbox-1"},
		{Name: "TGSRL_WORKER_REGISTRY_URL", Value: registry.URL},
		{Name: "TGSRL_WORKER_REGISTRY_TOKEN", Value: "scoped-token"},
	}}}
	if _, err := b.inner.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	snapshot, terminal, err := b.Snapshot(context.Background(), bundle)
	if err != nil || terminal || !snapshot.WorkerRegistrationRequired || !snapshot.PodReady {
		t.Fatalf("process snapshot = (%+v, %v, %v)", snapshot, terminal, err)
	}
}

func TestProcessBackendRejectsMultipleTargetsBeforeWorkerControl(t *testing.T) {
	calls := 0
	registry := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls++
	}))
	defer registry.Close()
	b := &ProcessBackend{inner: NewFake(), bootstrap: "unused", stateDir: t.TempDir(), processes: make(map[string]*managedProcess)}
	request := ControlRequest{
		Action:         tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE,
		JobID:          "job-1",
		RunID:          "run-1",
		IdempotencyKey: "pause-all",
		Targets: []ControlTarget{
			{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", ExpectedGeneration: 1},
			{RuntimeUnitID: "unit-2", SandboxID: "sandbox-2", ExpectedGeneration: 1},
		},
	}
	if _, err := b.Control(context.Background(), request); err == nil {
		t.Fatal("multi-target process control succeeded")
	}
	if calls != 0 {
		t.Fatalf("multi-target process control made %d worker calls", calls)
	}
}

func containsEnvironment(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestFakeBackendApplyAndIdempotency(t *testing.T) {
	b := NewFake()
	b.SetCapabilities(compiler.CapabilitySet{
		GPUProfiles: map[string]bool{
			compiler.GPUProfileNone:               true,
			compiler.GPUProfileNVIDIADevicePlugin: true,
		},
	})
	bundle := testBundle()
	bundle.Generation = 2
	bundle.Fingerprint = "fp-1"

	first, err := b.Apply(context.Background(), bundle)
	if err != nil {
		t.Fatalf("first apply failed: %v", err)
	}
	if !first.Created {
		t.Fatalf("expected create result")
	}

	second, err := b.Apply(context.Background(), bundle)
	if err != nil {
		t.Fatalf("second apply failed: %v", err)
	}
	if !second.Idempotent {
		t.Fatalf("expected idempotent replay")
	}
}

func TestFakeBackendRejectsGenerationRegressionAndFingerprintDrift(t *testing.T) {
	b := NewFake()
	b.SetCapabilities(compiler.CapabilitySet{
		GPUProfiles: map[string]bool{
			compiler.GPUProfileNone:               true,
			compiler.GPUProfileNVIDIADevicePlugin: true,
		},
	})
	bundle := testBundle()
	bundle.Generation = 3
	bundle.Fingerprint = "fp-1"
	if _, err := b.Apply(context.Background(), bundle); err != nil {
		t.Fatalf("initial apply failed: %v", err)
	}

	regressed, err := api.CloneBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	regressed.Generation = 2
	if _, err := b.Apply(context.Background(), regressed); err != ErrGenerationConflict {
		t.Fatalf("expected ErrGenerationConflict, got %v", err)
	}

	drifted, err := api.CloneBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	drifted.Fingerprint = "fp-2"
	if _, err := b.Apply(context.Background(), drifted); err != ErrFingerprintDrift {
		t.Fatalf("expected ErrFingerprintDrift, got %v", err)
	}
}

func TestFakeBackendSnapshotsReflectObservedAdmissionAndClaimAllocation(t *testing.T) {
	b := NewFake()
	b.SetCapabilities(compiler.CapabilitySet{
		GPUProfiles: map[string]bool{
			compiler.GPUProfileNone:          true,
			compiler.GPUProfileKubernetesDRA: true,
		},
	})
	bundle := testBundle()
	bundle.GPUProfile = compiler.GPUProfileKubernetesDRA
	bundle.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", DeviceIDs: []string{"GPU-aaaa"}, Generation: bundle.Generation}}
	if _, err := b.Apply(context.Background(), bundle); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	snapshots, err := b.Snapshots(context.Background(), bundle)
	if err != nil {
		t.Fatalf("Snapshots() error = %v", err)
	}
	if len(snapshots) != 2 {
		t.Fatalf("snapshots = %+v, want two-step observed stream", snapshots)
	}
	if !snapshots[0].WorkloadAdmitted || !snapshots[0].ResourceClaimsAllocated || len(snapshots[0].AllocatedDeviceIDs) == 0 {
		t.Fatalf("bound snapshot = %+v, want admitted and claim allocated", snapshots[0])
	}
	if !snapshots[1].WorkloadAdmitted || !snapshots[1].ResourceClaimsAllocated || snapshots[1].JobActive == 0 {
		t.Fatalf("running snapshot = %+v, want admitted, claim allocated, and active job", snapshots[1])
	}
}

func TestFakeBackendCleanupRemovesBundleAndNamespacedObjects(t *testing.T) {
	b := NewFake()
	b.SetCapabilities(compiler.CapabilitySet{GPUProfiles: map[string]bool{compiler.GPUProfileNone: true, compiler.GPUProfileKubernetesDRA: true}})
	bundle := testBundle()
	bundle.GPUProfile = compiler.GPUProfileKubernetesDRA
	if _, err := b.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	if err := b.Cleanup(context.Background(), bundle.Key, bundle.Generation); err != nil {
		t.Fatal(err)
	}
	if _, found, err := b.Get(context.Background(), bundle.Key); err != nil || found {
		t.Fatalf("bundle after cleanup = found:%v err:%v", found, err)
	}
	if err := b.Cleanup(context.Background(), bundle.Key, bundle.Generation); err != nil {
		t.Fatalf("idempotent cleanup error = %v", err)
	}
}
