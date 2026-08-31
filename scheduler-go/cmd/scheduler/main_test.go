package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/internal/bootstrapauth"
	configpkg "github.com/Blizzard-cyber/TGS-RL/scheduler-go/config"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/persistence"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/protection"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider/nvidia/runtimehelper"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scheduler"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func signedWorkerToken(t *testing.T, signingKey []byte, worker runtimehelper.Worker) string {
	t.Helper()
	token, err := bootstrapauth.Sign(signingKey, bootstrapauth.Claims{RunID: worker.RunID, JobID: worker.JobID, RuntimeUnitID: worker.RuntimeUnitID, SandboxID: worker.SandboxID, BindingID: worker.BindingID, Generation: worker.Generation, DeviceIDs: worker.AllDeviceIDs()})
	if err != nil {
		t.Fatal(err)
	}
	return token
}

type recordingResumer struct {
	called    int
	ctx       context.Context
	recovered *persistence.SchedulerState
	err       error
}

type recordingRuntimePublisher struct {
	events    []*tgsrlv1.SandboxEvent
	sandboxes []*tgsrlv1.Sandbox
}

func (p *recordingRuntimePublisher) PublishSandboxEvent(_ context.Context, request *tgsrlv1.PublishSandboxEventRequest, _ ...grpc.CallOption) (*tgsrlv1.PublishSandboxEventResponse, error) {
	event := proto.Clone(request.GetEvent()).(*tgsrlv1.SandboxEvent)
	for _, existing := range p.events {
		if existing.GetEventId() == event.GetEventId() {
			if !proto.Equal(existing, event) {
				return nil, errors.New("event identity conflict")
			}
			return &tgsrlv1.PublishSandboxEventResponse{Event: proto.Clone(existing).(*tgsrlv1.SandboxEvent)}, nil
		}
	}
	p.events = append(p.events, event)
	return &tgsrlv1.PublishSandboxEventResponse{Event: event}, nil
}

func (p *recordingRuntimePublisher) GetRuntimeStatus(_ context.Context, _ *tgsrlv1.GetRuntimeStatusRequest, _ ...grpc.CallOption) (*tgsrlv1.GetRuntimeStatusResponse, error) {
	return &tgsrlv1.GetRuntimeStatusResponse{Sandboxes: p.sandboxes}, nil
}

func (r *recordingResumer) ResumeRecoveredState(ctx context.Context, recovered *persistence.SchedulerState) error {
	r.called++
	r.ctx = ctx
	r.recovered = recovered
	return r.err
}

func TestParseFallback(t *testing.T) {
	tests := []struct {
		input string
		want  scheduler.FallbackMode
		ok    bool
	}{
		{input: "noop", want: scheduler.FallbackNoOp, ok: true},
		{input: "NO_OP", want: scheduler.FallbackNoOp, ok: true},
		{input: "static", want: scheduler.FallbackStatic, ok: true},
		{input: "binpack"},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := parseFallback(test.input)
			if (err == nil) != test.ok {
				t.Fatalf("parseFallback(%q) error = %v, ok = %v", test.input, err, test.ok)
			}
			if got != test.want {
				t.Fatalf("parseFallback(%q) = %q, want %q", test.input, got, test.want)
			}
		})
	}
}

func TestLoadStartupConfigUsesDefaultManifest(t *testing.T) {
	t.Setenv("TGSRL_CONFIG_PROVIDER_KIND", "")
	t.Setenv("TGSRL_CONFIG_STRATEGY", "")
	t.Setenv("TGSRL_CONFIG_TOP_K", "")
	t.Setenv("TGSRL_CONFIG_FAST_INTERVAL", "")
	t.Setenv("TGSRL_CONFIG_MEDIUM_INTERVAL", "")
	t.Setenv("TGSRL_CONFIG_SLOW_INTERVAL", "")

	cfg, err := loadStartupConfig(&cliArgs{
		ListenAddress: defaultListenAddress,
		ConfigRoot:    repoRootFromTest(t),
	})
	if err != nil {
		t.Fatalf("loadStartupConfig() error = %v", err)
	}
	if cfg.FallbackMode != scheduler.FallbackStatic {
		t.Fatalf("FallbackMode = %q, want %q", cfg.FallbackMode, scheduler.FallbackStatic)
	}
	if cfg.StartupConfig.ProviderKind != "MockResourceProvider" {
		t.Fatalf("ProviderKind = %q, want MockResourceProvider", cfg.StartupConfig.ProviderKind)
	}
	if cfg.StartupConfig.SelectionStrategy != "stable-first-fit" {
		t.Fatalf("SelectionStrategy = %q, want stable-first-fit", cfg.StartupConfig.SelectionStrategy)
	}
	if cfg.StartupConfig.TopK != 1 {
		t.Fatalf("TopK = %d, want 1", cfg.StartupConfig.TopK)
	}
	if cfg.RuntimeConfig.FastInterval != 25*time.Millisecond || cfg.RuntimeConfig.MediumInterval != 100*time.Millisecond || cfg.RuntimeConfig.SlowInterval != 250*time.Millisecond {
		t.Fatalf("RuntimeConfig = %#v, want 25ms/100ms/250ms", cfg.RuntimeConfig)
	}
	if cfg.ProviderCaps == nil {
		t.Fatal("ProviderCaps = nil, want projected capability proto")
	}
	if cfg.ProviderCaps.GetSource() != cfg.StartupConfig.Capabilities.Source {
		t.Fatalf("ProviderCaps.Source = %q, want %q", cfg.ProviderCaps.GetSource(), cfg.StartupConfig.Capabilities.Source)
	}
	if cfg.ProviderCaps.GetAttributes()["accelerator_vendor"] != cfg.StartupConfig.Capabilities.Attributes["accelerator_vendor"] {
		t.Fatalf("ProviderCaps attributes = %#v, want accelerator_vendor from startup config", cfg.ProviderCaps.GetAttributes())
	}
	if cfg.ProviderCaps.GetMeasuredAt() == nil || len(cfg.ProviderCaps.GetEvidence()) != 1 {
		t.Fatalf("ProviderCaps evidence = %#v, want measured_at plus one evidence entry", cfg.ProviderCaps)
	}
	providerInstance, err := buildProvider(cfg)
	if err != nil {
		t.Fatalf("buildProvider() error = %v", err)
	}
	if providerInstance == nil {
		t.Fatal("buildProvider() returned nil provider")
	}
	capabilities, err := providerInstance.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities() error = %v", err)
	}
	if !proto.Equal(capabilities, cfg.ProviderCaps) {
		t.Fatalf("Capabilities() = %#v, want %#v", capabilities, cfg.ProviderCaps)
	}
	snapshot, err := providerInstance.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if len(snapshot.GetDevices()) == 0 {
		t.Fatal("Snapshot().Devices = 0, want projected mock device")
	}
	if !proto.Equal(snapshot.GetDevices()[0].GetCapabilities(), cfg.ProviderCaps) {
		t.Fatalf("Snapshot device capabilities = %#v, want %#v", snapshot.GetDevices()[0].GetCapabilities(), cfg.ProviderCaps)
	}
	schedulerConfig, err := buildSchedulerConfig(cfg)
	if err != nil {
		t.Fatalf("buildSchedulerConfig() error = %v", err)
	}
	if schedulerConfig.Policy.ID != cfg.StartupConfig.Policy.PolicyID || schedulerConfig.Guard == nil || schedulerConfig.Preemption == nil {
		t.Fatalf("scheduler config did not receive policy projection: %+v", schedulerConfig)
	}
	if !schedulerConfig.Guard.Config().Enabled || schedulerConfig.Preemption.Name() != "noop" {
		t.Fatalf("scheduler guard/preemption = %+v/%q", schedulerConfig.Guard.Config(), schedulerConfig.Preemption.Name())
	}
	if schedulerConfig.Guard.Config().Cooldown != 500*time.Millisecond ||
		schedulerConfig.Guard.Config().Hysteresis != 0.05 ||
		schedulerConfig.Guard.Config().MaxActionsPerWindow != 8 ||
		schedulerConfig.Guard.Config().Window != time.Minute ||
		schedulerConfig.Guard.Config().BreakerThreshold != 3 ||
		schedulerConfig.Guard.Config().BreakerResetAfter != 5*time.Minute {
		t.Fatalf("scheduler guard config = %+v", schedulerConfig.Guard.Config())
	}
}

func TestLoadStartupConfigHonorsEnvOverrides(t *testing.T) {
	t.Setenv("TGSRL_CONFIG_PROVIDER_KIND", "mock")
	t.Setenv("TGSRL_CONFIG_STRATEGY", "trace_aware")
	t.Setenv("TGSRL_CONFIG_TOP_K", "7")
	t.Setenv("TGSRL_CONFIG_FAST_INTERVAL", "40ms")
	t.Setenv("TGSRL_CONFIG_MEDIUM_INTERVAL", "150ms")
	t.Setenv("TGSRL_CONFIG_SLOW_INTERVAL", "500ms")

	cfg, err := loadStartupConfig(&cliArgs{
		ListenAddress: defaultListenAddress,
		ConfigRoot:    repoRootFromTest(t),
	})
	if err != nil {
		t.Fatalf("loadStartupConfig() error = %v", err)
	}
	if cfg.StartupConfig.ProviderKind != "mock" {
		t.Fatalf("ProviderKind = %q, want mock", cfg.StartupConfig.ProviderKind)
	}
	if cfg.StartupConfig.SelectionStrategy != "trace_aware" {
		t.Fatalf("SelectionStrategy = %q, want trace_aware", cfg.StartupConfig.SelectionStrategy)
	}
	if cfg.StartupConfig.TopK != 7 {
		t.Fatalf("TopK = %d, want 7", cfg.StartupConfig.TopK)
	}
	if cfg.RuntimeConfig.FastInterval != 40*time.Millisecond || cfg.RuntimeConfig.MediumInterval != 150*time.Millisecond || cfg.RuntimeConfig.SlowInterval != 500*time.Millisecond {
		t.Fatalf("RuntimeConfig = %#v, want 40ms/150ms/500ms", cfg.RuntimeConfig)
	}
	schedulerConfig, err := buildSchedulerConfig(cfg)
	if err != nil {
		t.Fatalf("buildSchedulerConfig() error = %v", err)
	}
	if schedulerConfig.Policy.Strategy != "trace_aware" {
		t.Fatalf("scheduler policy strategy = %s, want trace_aware", schedulerConfig.Policy.Strategy)
	}
	if schedulerConfig.Policy.TopK != 7 {
		t.Fatalf("scheduler policy top_k = %d, want 7", schedulerConfig.Policy.TopK)
	}
}

func TestBuildProviderFailsFastForUnsupportedKind(t *testing.T) {
	cfg := &runtimeConfig{
		StartupConfig: &configpkg.StartupConfig{
			ProviderKind: "custom-provider",
		},
	}
	_, err := buildProvider(cfg)
	if err == nil {
		t.Fatal("buildProvider() error = nil, want unsupported provider error")
	}
}

func TestParseArgsReadsConfigFlags(t *testing.T) {
	args, err := parseArgs([]string{"-listen", "127.0.0.1:6000", "-config-root", "/tmp/repo", "-manifest", "configs/manifest.yaml", "-fallback", "noop", "-state-dir", "/tmp/state", "-metrics-listen", "127.0.0.1:0", "-nvidia-driver-v2", "-nvidia-binding-helper", "/opt/tgsrl/bin/tgsrl-nvidia-binding", "-nvidia-binding-state", "/var/lib/tgsrl/bindings.json", "-nvidia-mps-pid-dir", "/run/tgsrl/mps", "-nvidia-runtime-helper", "/opt/tgsrl/bin/tgsrl-nvidia-runtime", "-nvidia-runtime-state", "/var/lib/tgsrl/runtime.json", "-nvidia-mig-helper", "/opt/tgsrl/bin/tgsrl-nvidia-mig"})
	if err != nil {
		t.Fatalf("parseArgs() error = %v", err)
	}
	if args.ListenAddress != "127.0.0.1:6000" || args.ConfigRoot != "/tmp/repo" || args.ManifestPath != "configs/manifest.yaml" || args.FallbackFlag != "noop" || args.StateDirectory != "/tmp/state" || args.MetricsAddress != "127.0.0.1:0" || !args.NVIDIADriverV2 || args.NVIDIABindingHelper != "/opt/tgsrl/bin/tgsrl-nvidia-binding" || args.NVIDIABindingState != "/var/lib/tgsrl/bindings.json" || args.NVIDIAMPSPIDDirectory != "/run/tgsrl/mps" || args.NVIDIARuntimeHelper != "/opt/tgsrl/bin/tgsrl-nvidia-runtime" || args.NVIDIARuntimeState != "/var/lib/tgsrl/runtime.json" || args.NVIDIAMIGHelper != "/opt/tgsrl/bin/tgsrl-nvidia-mig" {
		t.Fatalf("parseArgs() = %#v", args)
	}
}

func TestParseArgsDefaultsNVIDIABindingStateUnderSchedulerState(t *testing.T) {
	args, err := parseArgs([]string{"-state-dir", "relative-state"})
	if err != nil {
		t.Fatal(err)
	}
	if args.NVIDIABindingState != filepath.Join("relative-state", "nvidia-binding.json") {
		t.Fatalf("NVIDIABindingState = %q", args.NVIDIABindingState)
	}
	if args.NVIDIARuntimeState != filepath.Join("relative-state", "nvidia-runtime.json") {
		t.Fatalf("NVIDIARuntimeState = %q", args.NVIDIARuntimeState)
	}
}

func TestParseArgsRejectsIncompleteNVIDIADriverV2Configuration(t *testing.T) {
	if _, err := parseArgs([]string{"-nvidia-driver-v2", "-nvidia-binding-helper="}); err == nil || !strings.Contains(err.Error(), "binding helper") {
		t.Fatalf("empty binding helper error = %v", err)
	}
	if _, err := parseArgs([]string{"-nvidia-driver-v2", "-nvidia-runtime-helper="}); err == nil || !strings.Contains(err.Error(), "runtime helper") {
		t.Fatalf("empty runtime helper error = %v", err)
	}
	if _, err := parseArgs([]string{"-nvidia-driver-v2", "-nvidia-partition-mode=mig", "-nvidia-mig-helper="}); err == nil || !strings.Contains(err.Error(), "MIG helper") {
		t.Fatalf("empty MIG helper error = %v", err)
	}
}

func TestParseArgsRejectsWorkerRegistryWithoutRuntimeTarget(t *testing.T) {
	if _, err := parseArgs([]string{"-worker-registry-listen=127.0.0.1:50091"}); err == nil || !strings.Contains(err.Error(), "runtime target") {
		t.Fatalf("worker registry error = %v", err)
	}
	t.Setenv("TGSRL_WORKER_REGISTRY_SIGNING_KEY", "")
	if _, err := parseArgs([]string{"-worker-registry-listen=127.0.0.1:50091", "-worker-registry-runtime-target=127.0.0.1:50071"}); err == nil || !strings.Contains(err.Error(), "signing-key") {
		t.Fatalf("worker registry signing-key error = %v", err)
	}
	t.Setenv("TGSRL_WORKER_REGISTRY_SIGNING_KEY", strings.Repeat("k", 32))
	if _, err := parseArgs([]string{"-worker-registry-listen=127.0.0.1:50091", "-worker-registry-runtime-target=127.0.0.1:50071"}); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("worker registry default state path error = %v", err)
	}
	if _, err := parseArgs([]string{"-worker-registry-listen=127.0.0.1:50091", "-worker-registry-runtime-target=127.0.0.1:50071", "-worker-registry-state=relative.json"}); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("worker registry state path error = %v", err)
	}
	args, err := parseArgs([]string{"-state-dir=/tmp/tgsrl", "-worker-registry-listen=127.0.0.1:50091", "-worker-registry-runtime-target=127.0.0.1:50071"})
	if err != nil || args.WorkerRegistryState != "/tmp/tgsrl/worker-registry.json" {
		t.Fatalf("generic worker registry args = %+v, error = %v", args, err)
	}
}

func TestStartMetricsServer(t *testing.T) {
	recorder := observability.NewPrometheusRecorder()
	recorder.IncCounter("startup", 1)
	server, listener, err := startMetricsServer("127.0.0.1:0", recorder)
	if err != nil {
		t.Fatalf("startMetricsServer() error = %v", err)
	}
	defer server.Close()
	defer listener.Close()
	response, err := http.Get("http://" + listener.Addr().String() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics error = %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || !strings.Contains(string(body), "tgsrl_startup 1") {
		t.Fatalf("metrics body = %q, error = %v", body, err)
	}
}

func TestWorkerRegistryAuthorizesBindingAndPublishesLifecycle(t *testing.T) {
	binding := &tgsrlv1.Binding{BindingId: "binding-a", PendingUnitId: "unit-a", RuntimeUnitId: "unit-a", SandboxId: "sandbox-a", Generation: 4, DeviceIds: []string{"mock-cpu-0"}, Resources: &tgsrlv1.ResourceVector{AcceleratorUnits: 1}}
	resourceProvider, err := provider.NewMockResourceProvider(provider.WithSandboxes(provider.Sandbox{SandboxID: "sandbox-a", State: provider.SandboxStateBound, Generation: 4, Binding: binding, Share: 1}))
	if err != nil {
		t.Fatal(err)
	}
	signingKey := []byte(strings.Repeat("registry-signing-key-", 2))
	t.Setenv("TGSRL_WORKER_REGISTRY_SIGNING_KEY", string(signingKey))
	statePath := filepath.Join(t.TempDir(), "runtime.json")
	runtimePublisher := &recordingRuntimePublisher{sandboxes: []*tgsrlv1.Sandbox{{SandboxId: "sandbox-a", Generation: 4, State: tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND}}}
	server, listener, err := startWorkerRegistry("127.0.0.1:0", statePath, "", resourceProvider, runtimePublisher)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	defer listener.Close()
	go server.Serve(listener) //nolint:errcheck

	workerControl := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_ = json.NewEncoder(w).Encode(runtimehelper.ControlResponse{Accepted: true, State: "running", Generation: 4, Ready: true, InstanceID: "instance-a", PID: 4242, ProcessToken: "process-a", BindingID: "binding-a", DeviceID: "mock-cpu-0", Share: 1})
	})
	controlServer := httptest.NewServer(workerControl)
	defer controlServer.Close()
	worker := runtimehelper.Worker{RunID: "run-a", JobID: "job-a", RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", Generation: 4, BindingID: "binding-a", DeviceIDs: []string{"mock-cpu-0"}, DeviceID: "mock-cpu-0", Share: 1, PID: 4242, ProcessToken: "process-a", InstanceID: "instance-a", State: "running", Ready: true, ControlURL: controlServer.URL, ControlToken: "control-token"}
	registryURL := "http://" + listener.Addr().String()
	registryToken := signedWorkerToken(t, signingKey, worker)
	runtimePublisher.sandboxes[0].State = tgsrlv1.RuntimeState_RUNTIME_STATE_STARTING
	if err := runtimehelper.RegisterRemoteWorker(context.Background(), registryURL, registryToken, worker); err == nil || !strings.Contains(err.Error(), "bound workload generation") {
		t.Fatalf("registration before Runtime BOUND error = %v", err)
	}
	stateBeforeBound, err := runtimehelper.NewStore(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot, err := stateBeforeBound.Snapshot(); err != nil || len(snapshot.Workers) != 0 {
		t.Fatalf("worker persisted before Runtime BOUND: %+v, error = %v", snapshot.Workers, err)
	}
	runtimePublisher.sandboxes[0].State = tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND
	if err := runtimehelper.RegisterRemoteWorker(context.Background(), registryURL, registryToken, worker); err != nil {
		t.Fatal(err)
	}
	if err := runtimehelper.RegisterRemoteWorker(context.Background(), registryURL, registryToken, worker); err != nil {
		t.Fatal(err)
	}
	observed, err := resourceProvider.GetSandbox(context.Background(), "sandbox-a")
	if err != nil || observed.State != provider.SandboxStateRunning {
		t.Fatalf("registered sandbox = %+v error = %v", observed, err)
	}
	if len(runtimePublisher.events) != 1 || runtimePublisher.events[0].GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("runtime events = %+v", runtimePublisher.events)
	}
	worker.State, worker.Ready, worker.ExitCode, worker.Detail = "failed", false, 7, "worker exited"
	if err := runtimehelper.ReportRemoteWorkerExit(context.Background(), registryURL, registryToken, worker); err != nil {
		t.Fatal(err)
	}
	observed, err = resourceProvider.GetSandbox(context.Background(), "sandbox-a")
	if err != nil || observed.State != provider.SandboxStateFailed {
		t.Fatalf("terminal sandbox = %+v error = %v", observed, err)
	}
	if len(runtimePublisher.events) != 2 || runtimePublisher.events[1].GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED {
		t.Fatalf("runtime terminal events = %+v", runtimePublisher.events)
	}
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("runtime registry state was not persisted: %v", err)
	}
}

func TestPersistenceRestoreAcrossStartupBoundary(t *testing.T) {
	root := t.TempDir()
	repository, recovered, err := persistence.OpenAndRecover(context.Background(), root)
	if err != nil || !persistence.IsEmpty(recovered) {
		t.Fatalf("initial recovery = %+v, error = %v", recovered, err)
	}
	snapshot := &tgsrlv1.ClusterSnapshot{SnapshotId: "snapshot-4", Revision: 4}
	store, err := state.NewStore(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decision := &tgsrlv1.DecisionRecord{DecisionId: "decision-1", Sequence: 5}
	checkpoint, err := persistence.Capture(store, []*tgsrlv1.DecisionRecord{decision}, 5, protection.State{})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveCheckpoint(checkpoint); err != nil {
		t.Fatal(err)
	}
	_, restarted, err := persistence.OpenAndRecover(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Snapshot.GetRevision() != 4 || restarted.Cursor != 5 || len(restarted.Decisions) != 1 {
		t.Fatalf("restarted state = %+v", restarted)
	}
}

func TestResumeRecoveredServer(t *testing.T) {
	ctx := context.Background()
	resumer := &recordingResumer{}
	recovered := &persistence.SchedulerState{Snapshot: &tgsrlv1.ClusterSnapshot{SnapshotId: "snapshot-1", Revision: 1}}
	if err := resumeRecoveredServer(ctx, resumer, recovered); err != nil {
		t.Fatalf("resumeRecoveredServer() error = %v", err)
	}
	if resumer.called != 1 || resumer.ctx != ctx || resumer.recovered != recovered {
		t.Fatalf("resumer call = %+v, want one call with original ctx and recovered pointer", resumer)
	}

	resumer = &recordingResumer{}
	if err := resumeRecoveredServer(ctx, resumer, nil); err != nil {
		t.Fatalf("resumeRecoveredServer(nil) error = %v", err)
	}
	if err := resumeRecoveredServer(ctx, resumer, &persistence.SchedulerState{}); err != nil {
		t.Fatalf("resumeRecoveredServer(empty snapshot) error = %v", err)
	}
	if resumer.called != 0 {
		t.Fatalf("resumer called %d times for nil/incomplete recovery, want 0", resumer.called)
	}

	resumer = &recordingResumer{err: errors.New("boom")}
	err := resumeRecoveredServer(ctx, resumer, recovered)
	if err == nil || !strings.Contains(err.Error(), "resume recovered scheduler state: boom") {
		t.Fatalf("resumeRecoveredServer(error) = %v, want wrapped resume error", err)
	}
}

func repoRootFromTest(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return root
}

func TestLoadStartupConfigLegacyFallbackFlagOverridesConfig(t *testing.T) {
	t.Setenv("TGSRL_CONFIG_PROVIDER_KIND", "")
	cfg, err := loadStartupConfig(&cliArgs{
		ListenAddress: defaultListenAddress,
		ConfigRoot:    repoRootFromTest(t),
		FallbackFlag:  "noop",
	})
	if err != nil {
		t.Fatalf("loadStartupConfig() error = %v", err)
	}
	if cfg.FallbackMode != scheduler.FallbackNoOp {
		t.Fatalf("FallbackMode = %q, want %q", cfg.FallbackMode, scheduler.FallbackNoOp)
	}
}
