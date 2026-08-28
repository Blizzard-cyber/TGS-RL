package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	configpkg "github.com/Blizzard-cyber/TGS-RL/scheduler-go/config"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/persistence"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scheduler"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/protobuf/proto"
)

type recordingResumer struct {
	called    int
	ctx       context.Context
	recovered *persistence.SchedulerState
	err       error
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
	loop := buildEventLoop(nil, cfg)
	if loop == nil {
		t.Fatal("buildEventLoop() returned nil")
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
	args, err := parseArgs([]string{"-listen", "127.0.0.1:6000", "-config-root", "/tmp/repo", "-manifest", "configs/manifest.yaml", "-fallback", "noop", "-state-dir", "/tmp/state", "-metrics-listen", "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("parseArgs() error = %v", err)
	}
	if args.ListenAddress != "127.0.0.1:6000" || args.ConfigRoot != "/tmp/repo" || args.ManifestPath != "configs/manifest.yaml" || args.FallbackFlag != "noop" || args.StateDirectory != "/tmp/state" || args.MetricsAddress != "127.0.0.1:0" {
		t.Fatalf("parseArgs() = %#v", args)
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
	checkpoint, err := persistence.Capture(store, []*tgsrlv1.DecisionRecord{decision}, 5)
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

func TestNormalizeToken(t *testing.T) {
	if got := normalizeToken("stable-first_fit"); got != "stablefirstfit" {
		t.Fatalf("normalizeToken() = %q, want stablefirstfit", got)
	}
}

func TestRepoRootExists(t *testing.T) {
	if _, err := os.Stat(filepath.Join(repoRootFromTest(t), "compatibility", "manifests", "cpu-mock.yaml")); err != nil {
		t.Fatalf("repo root missing expected manifest: %v", err)
	}
}
