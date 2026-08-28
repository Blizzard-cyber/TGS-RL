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

func TestRuntimePublisherPreservesObservedEvent(t *testing.T) {
	runtimeSink := &recordingRuntimePublisher{}
	event := &tgsrlv1.SandboxEvent{
		EventId:    "running-1",
		EventType:  tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_RUNNING,
		SandboxId:  "sandbox-1",
		RunId:      "run-1",
		JobId:      "job-1",
		TraceId:    "trace-1",
		Generation: 1,
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		Binding:    &tgsrlv1.Binding{BindingId: "binding-1", Generation: 1},
	}

	if err := runtimeSink.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if runtimeSink.event.GetGeneration() != 1 || runtimeSink.event.GetBinding().GetGeneration() != 1 {
		t.Fatalf("runtime event generation = %d/%d, want 1/1", runtimeSink.event.GetGeneration(), runtimeSink.event.GetBinding().GetGeneration())
	}
	if event.GetGeneration() != 1 || event.GetBinding().GetGeneration() != 1 {
		t.Fatal("Publish() mutated the caller-owned event")
	}
}

func TestConvergingPublisherKeepsDeterministicGenerationAcrossStateProgression(t *testing.T) {
	runtimeSink := &recordingRuntimePublisher{}
	first := &tgsrlv1.SandboxEvent{
		EventId:    "bound-1",
		EventType:  tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_BOUND,
		SandboxId:  "sandbox-1",
		RunId:      "run-1",
		JobId:      "job-1",
		Generation: 3,
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND,
		Binding:    &tgsrlv1.Binding{BindingId: "binding-1", Generation: 3},
	}
	second := &tgsrlv1.SandboxEvent{
		EventId:    "running-1",
		EventType:  tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_RUNNING,
		SandboxId:  "sandbox-1",
		RunId:      "run-1",
		JobId:      "job-1",
		Generation: 3,
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		Binding:    &tgsrlv1.Binding{BindingId: "binding-1", Generation: 3},
	}

	if err := runtimeSink.Publish(context.Background(), first); err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
	if got := runtimeSink.event.GetGeneration(); got != 3 {
		t.Fatalf("first published generation = %d, want 3", got)
	}
	if err := runtimeSink.Publish(context.Background(), second); err != nil {
		t.Fatalf("second Publish() error = %v", err)
	}
	if got := runtimeSink.event.GetGeneration(); got != 3 {
		t.Fatalf("second published generation = %d, want 3", got)
	}
}

func TestConvergingPublisherRestartReplayKeepsSameGenerationAndEventID(t *testing.T) {
	runtimeSink := &recordingRuntimePublisher{}
	event := &tgsrlv1.SandboxEvent{
		EventId:    "decision-1:SANDBOX_EVENT_TYPE_BOUND:7:binding-1",
		EventType:  tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_BOUND,
		SandboxId:  "sandbox-1",
		RunId:      "run-1",
		JobId:      "job-1",
		Generation: 7,
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND,
		Binding:    &tgsrlv1.Binding{BindingId: "binding-1", Generation: 7},
	}

	if err := runtimeSink.Publish(context.Background(), event); err != nil {
		t.Fatalf("original Publish() error = %v", err)
	}
	firstEventID := runtimeSink.event.GetEventId()
	firstGeneration := runtimeSink.event.GetGeneration()

	if err := runtimeSink.Publish(context.Background(), event); err != nil {
		t.Fatalf("restarted Publish() error = %v", err)
	}
	if runtimeSink.event.GetEventId() != firstEventID {
		t.Fatalf("event ID changed across restart replay: %q vs %q", runtimeSink.event.GetEventId(), firstEventID)
	}
	if runtimeSink.event.GetGeneration() != firstGeneration {
		t.Fatalf("generation changed across restart replay: %d vs %d", runtimeSink.event.GetGeneration(), firstGeneration)
	}
}

type recordingRuntimePublisher struct {
	event *tgsrlv1.SandboxEvent
}

func (p *recordingRuntimePublisher) Publish(_ context.Context, event *tgsrlv1.SandboxEvent) error {
	p.event = event
	return nil
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

func TestRuntimeConfigValidation(t *testing.T) {
	if _, err := compiler.ValidateRuntimeConfig(compiler.RuntimeConfig{
		RuntimeClass: compiler.RuntimeClassConfig{Handler: "nvidia", Create: true},
	}); err == nil {
		t.Fatal("expected runtime class name validation error")
	}
	if _, err := compiler.ValidateRuntimeConfig(compiler.RuntimeConfig{
		RuntimeClass: compiler.RuntimeClassConfig{Name: "nvidia", Create: true},
	}); err == nil {
		t.Fatal("expected runtime class handler validation error")
	}
	config, err := compiler.ValidateRuntimeConfig(compiler.RuntimeConfig{
		RuntimeClass: compiler.RuntimeClassConfig{Name: "kata"},
		NodeSelector: map[string]string{"gpu": "true"},
	})
	if err != nil {
		t.Fatalf("ValidateRuntimeConfig() error = %v", err)
	}
	if config.RuntimeClass.Name != "kata" || config.RuntimeClass.Create {
		t.Fatalf("runtime config = %+v, want preconfigured runtime class reference", config.RuntimeClass)
	}
	if got := config.NodeSelector["gpu"]; got != "true" {
		t.Fatalf("normalized node selector = %+v, want trimmed value", config.NodeSelector)
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

func TestRuntimeConfigValidationTrimsAndRejectsInvalidNodeSelector(t *testing.T) {
	config, err := compiler.ValidateRuntimeConfig(compiler.RuntimeConfig{
		NodeSelector: map[string]string{" example.com/gpu ": " enabled "},
	})
	if err != nil {
		t.Fatalf("ValidateRuntimeConfig() error = %v", err)
	}
	if _, ok := config.NodeSelector[" example.com/gpu "]; ok {
		t.Fatalf("node selector key was not normalized: %+v", config.NodeSelector)
	}
	if got := config.NodeSelector["example.com/gpu"]; got != "enabled" {
		t.Fatalf("node selector = %+v, want trimmed key/value", config.NodeSelector)
	}
	for name, selector := range map[string]map[string]string{
		"empty value": {"gpu": "   "},
		"bad key":     {"bad key": "true"},
		"bad value":   {"gpu": "bad value"},
	} {
		if _, err := compiler.ValidateRuntimeConfig(compiler.RuntimeConfig{NodeSelector: selector}); err == nil {
			t.Fatalf("%s: expected validation error", name)
		}
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
