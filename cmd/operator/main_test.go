package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/statuswatch"
)

func TestSelectBackend(t *testing.T) {
	tests := []struct {
		mode       string
		want       string
		ok         bool
		kubeconfig string
	}{
		{mode: "fake", want: "fake", ok: true},
		{mode: "kubernetes", want: "kubernetes", ok: true, kubeconfig: writeTestKubeconfig(t)},
		{mode: "kubernetes", ok: false, kubeconfig: filepath.Join(t.TempDir(), "missing-config")},
		{mode: "bad", ok: false},
	}

	for _, test := range tests {
		backend, got, err := selectBackend(test.mode, "default", test.kubeconfig)
		if test.ok {
			if err != nil {
				t.Fatalf("mode %q returned error: %v", test.mode, err)
			}
			if backend == nil {
				t.Fatalf("mode %q returned nil backend", test.mode)
			}
			if got != test.want {
				t.Fatalf("mode %q returned backend name %q, want %q", test.mode, got, test.want)
			}
			continue
		}
		if err == nil {
			t.Fatalf("mode %q should fail", test.mode)
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	selected, name, err := selectBackend("process", "default", "", backend.ProcessConfig{
		BootstrapBinary: executable,
		StateDirectory:  t.TempDir(),
	})
	if err != nil || selected == nil || name != "process" {
		t.Fatalf("process backend = (%T, %q, %v)", selected, name, err)
	}
	observer, err := selectObserver("process", "default", "", selected)
	if err != nil || observer == nil {
		t.Fatalf("process observer = (%T, %v)", observer, err)
	}
}

func TestSelectFakeObserverPreservesBoundThenRunning(t *testing.T) {
	selected := backend.NewFake()
	bundle := &api.Bundle{Key: "test/bundle", Namespace: "test", Generation: 1, Fingerprint: "fp", Workload: api.Workload{TypeMeta: api.TypeMeta{APIVersion: "kueue.x-k8s.io/v1beta1", Kind: "Workload"}, ObjectMeta: api.ObjectMeta{Name: "workload", Namespace: "test"}}, Job: api.Job{TypeMeta: api.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: api.ObjectMeta{Name: "job", Namespace: "test"}, Spec: api.JobSpec{Parallelism: 1}}}
	if _, err := selected.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	observer, err := selectObserver("fake", "test", "", selected)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := observer.Watch(context.Background(), statuswatch.Request{Bundle: bundle, Bindings: []*tgsrlv1.Binding{{BindingId: "binding"}}})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	second, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if statuswatch.Project(first, false).State != tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND || statuswatch.Project(second, false).State != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("states = %s -> %s, want BOUND -> RUNNING", statuswatch.Project(first, false).State, statuswatch.Project(second, false).State)
	}
}

func writeTestKubeconfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	content := `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://127.0.0.1:6443
users:
- name: test
  user:
    token: token
contexts:
- name: test
  context:
    namespace: default
current-context: test
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return path
}

type capabilityBackend struct {
	backend.Backend
	capabilities compiler.CapabilitySet
}

func (b capabilityBackend) DiscoverCapabilities(context.Context) (compiler.CapabilitySet, error) {
	return b.capabilities, nil
}

func TestValidateGPUProfile(t *testing.T) {
	for _, profile := range []string{
		compiler.GPUProfileNone,
		compiler.GPUProfileNVIDIADevicePlugin,
		compiler.GPUProfileKubernetesDRA,
		compiler.GPUProfileVolcanoHAMI,
	} {
		if err := validateGPUProfile(profile); err != nil {
			t.Fatalf("validateGPUProfile(%q) error = %v", profile, err)
		}
	}
	if err := validateGPUProfile("bad-profile"); err == nil || !strings.Contains(err.Error(), "unsupported gpu profile") {
		t.Fatalf("validateGPUProfile(bad-profile) error = %v, want unsupported gpu profile", err)
	}
}

func TestPreflightBackendValidatesSelectedProfileAndKueue(t *testing.T) {
	fakeBackend := backend.NewFake()
	fakeBackend.SetCapabilities(compiler.CapabilitySet{
		GPUProfiles: map[string]bool{compiler.GPUProfileNone: true},
		KubernetesAPIs: compiler.KubernetesAPIVersions{
			KueueWorkload: compiler.KueueWorkloadV1Beta2,
		},
	})
	if err := preflightBackend(context.Background(), fakeBackend, compiler.GPUProfileNone); err != nil {
		t.Fatalf("preflightBackend() error = %v", err)
	}
	if err := preflightBackend(context.Background(), fakeBackend, compiler.GPUProfileKubernetesDRA); err == nil || !strings.Contains(err.Error(), "not available") {
		t.Fatalf("preflightBackend() error = %v, want unavailable GPU profile", err)
	}

	missingKueue := capabilityBackend{
		Backend: backend.NewFake(),
		capabilities: compiler.CapabilitySet{
			GPUProfiles: map[string]bool{compiler.GPUProfileNone: true},
		},
	}
	if err := preflightBackend(context.Background(), missingKueue, compiler.GPUProfileNone); err == nil || !strings.Contains(err.Error(), "kueue Workload API") {
		t.Fatalf("preflightBackend() error = %v, want missing Kueue API", err)
	}

	draWithoutInventory := capabilityBackend{
		Backend: backend.NewFake(),
		capabilities: compiler.CapabilitySet{
			GPUProfiles:          map[string]bool{compiler.GPUProfileKubernetesDRA: true},
			ExactDevicePlacement: map[string]bool{compiler.GPUProfileKubernetesDRA: true},
			KubernetesAPIs: compiler.KubernetesAPIVersions{
				KueueWorkload:    compiler.KueueWorkloadV1Beta2,
				DRAResourceClaim: compiler.DRAResourceClaimV1,
			},
		},
	}
	if err := preflightBackend(context.Background(), draWithoutInventory, compiler.GPUProfileKubernetesDRA); err == nil || !strings.Contains(err.Error(), "UUID inventory") {
		t.Fatalf("preflightBackend() error = %v, want missing DRA UUID inventory", err)
	}

	countOnly := capabilityBackend{Backend: backend.NewFake(), capabilities: compiler.CapabilitySet{GPUProfiles: map[string]bool{compiler.GPUProfileNVIDIADevicePlugin: true}, KubernetesAPIs: compiler.KubernetesAPIVersions{KueueWorkload: compiler.KueueWorkloadV1Beta2}}}
	if err := preflightBackend(context.Background(), countOnly, compiler.GPUProfileNVIDIADevicePlugin); err == nil || !strings.Contains(err.Error(), "cannot enforce") {
		t.Fatalf("preflightBackend() error = %v, want exact-placement failure", err)
	}
}

func TestStringMapFlagSet(t *testing.T) {
	var flagValue stringMapFlag
	if err := flagValue.Set("gpu=true"); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if err := flagValue.Set("zone=cn"); err != nil {
		t.Fatalf("Set() second error = %v", err)
	}
	if got := map[string]string(flagValue); got["gpu"] != "true" || got["zone"] != "cn" {
		t.Fatalf("flag map = %#v, want both entries", got)
	}
	if err := flagValue.Set("broken"); err == nil {
		t.Fatal("expected key=value validation error")
	}
}

func TestValidateStartupRuntimeConfigFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		profile string
		runtime compiler.RuntimeClassConfig
		wantErr string
	}{
		{name: "valid preconfigured class", profile: compiler.GPUProfileNone, runtime: compiler.RuntimeClassConfig{Name: "sandbox.kata.example"}},
		{name: "valid created class", profile: compiler.GPUProfileNone, runtime: compiler.RuntimeClassConfig{Name: "kata", Handler: "kata-qemu", Create: true}},
		{name: "invalid profile", profile: "invalid", wantErr: "unsupported gpu profile"},
		{name: "invalid runtime class name", profile: compiler.GPUProfileNone, runtime: compiler.RuntimeClassConfig{Name: "bad/name"}, wantErr: "runtime class name"},
		{name: "invalid runtime class handler", profile: compiler.GPUProfileNone, runtime: compiler.RuntimeClassConfig{Name: "kata", Handler: "bad/handler", Create: true}, wantErr: "runtime class handler"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateStartupRuntimeConfig(test.profile, compiler.RuntimeConfig{RuntimeClass: test.runtime})
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("validateStartupRuntimeConfig() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("validateStartupRuntimeConfig() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestValidateStartupRuntimeConfigRequiresBootstrapForDRA(t *testing.T) {
	if _, err := validateStartupRuntimeConfig(compiler.GPUProfileKubernetesDRA, compiler.RuntimeConfig{}); err == nil || !strings.Contains(err.Error(), "managed-worker bootstrap") {
		t.Fatalf("DRA bootstrap error = %v", err)
	}
}

func TestLoadWorkerRegistrySigningKey(t *testing.T) {
	if _, err := loadWorkerRegistrySigningKey("", true); err == nil {
		t.Fatal("required signing key file succeeded when absent")
	}
	path := filepath.Join(t.TempDir(), "signing-key")
	want := strings.Repeat("k", 32)
	if err := os.WriteFile(path, []byte(want+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := loadWorkerRegistrySigningKey(path, true)
	if err != nil || string(got) != want {
		t.Fatalf("loadWorkerRegistrySigningKey() = %q, %v", got, err)
	}
}

func TestRunServicesAllowsGrpcOnlyMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	stopped := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- runServices(ctx, nil, namedService{
			name: "grpc",
			run: func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				close(stopped)
				return nil
			},
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("grpc-only service did not start")
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("grpc-only service did not stop after cancellation")
	}
	if err := <-errCh; err != nil {
		t.Fatalf("runServices() error = %v", err)
	}
}

func TestRunServicesReturnsWorkerError(t *testing.T) {
	want := errors.New("worker failed")
	err := runServices(context.Background(), nil,
		namedService{name: "grpc", run: func(context.Context) error { return nil }},
		namedService{name: "worker", run: func(context.Context) error { return want }},
	)
	if err == nil || !strings.Contains(err.Error(), "worker service") {
		t.Fatalf("runServices() error = %v, want wrapped worker error", err)
	}
}
