// Command scheduler runs the TGS-RL scheduling control plane.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	configpkg "github.com/Blizzard-cyber/TGS-RL/scheduler-go/config"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/eventloop"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/persistence"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	nvidiaprovider "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider/nvidia"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scheduler"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/service"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const defaultListenAddress = "127.0.0.1:50051"

func main() {
	if err := run(); err != nil {
		slog.Error("scheduler stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	args, err := parseArgs(os.Args[1:])
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startup, err := loadStartupConfig(args)
	if err != nil {
		return err
	}
	providerInstance, err := buildProvider(startup)
	if err != nil {
		return err
	}
	initial, err := providerInstance.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("read initial provider snapshot: %w", err)
	}
	repository, recovered, err := persistence.OpenAndRecover(ctx, args.StateDirectory)
	if err != nil {
		return fmt.Errorf("recover scheduler persistence: %w", err)
	}
	if recovered != nil && recovered.Snapshot != nil {
		initial = recovered.Snapshot
	}
	store, err := state.NewStore(initial)
	if err != nil {
		return fmt.Errorf("create state store: %w", err)
	}
	if recovered != nil && recovered.Snapshot != nil {
		if err := persistence.RestoreStore(store, recovered); err != nil {
			return err
		}
	}
	schedulerConfig, err := buildSchedulerConfig(startup)
	if err != nil {
		return err
	}
	evaluator, err := scheduler.New(schedulerConfig)
	if err != nil {
		return fmt.Errorf("create scheduler: %w", err)
	}
	recorder := observability.NewPrometheusRecorder()
	tickRuntime := eventloop.NewRuntime(recorder, startup.RuntimeConfig)
	server, err := service.New(service.Config{
		Store:       store,
		Scheduler:   evaluator,
		Provider:    providerInstance,
		TickRuntime: tickRuntime,
		Recorder:    recorder,
		Repository:  repository,
		DeferStart:  true,
	})
	if err != nil {
		return fmt.Errorf("create scheduling service: %w", err)
	}
	defer server.Close()
	if err := resumeRecoveredServer(ctx, server, recovered); err != nil {
		return err
	}
	if err := server.Start(); err != nil {
		return fmt.Errorf("start scheduling service: %w", err)
	}
	metricsServer, metricsListener, err := startMetricsServer(args.MetricsAddress, recorder)
	if err != nil {
		return err
	}
	if metricsServer != nil {
		defer metricsServer.Close()
		defer metricsListener.Close()
	}

	listener, err := net.Listen("tcp", args.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", args.ListenAddress, err)
	}
	grpcServer := grpc.NewServer()
	tgsrlv1.RegisterSchedulerServiceServer(grpcServer, server)
	tgsrlv1.RegisterSchedulerObservationServiceServer(grpcServer, server)

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("shutdown goroutine panicked", "panic", recovered)
			}
		}()
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	slog.Info(
		"scheduler listening",
		"address", listener.Addr().String(),
		"fallback", startup.FallbackMode,
		"provider", startup.StartupConfig.ProviderKind,
		"strategy", startup.StartupConfig.SelectionStrategy,
		"top_k", startup.StartupConfig.TopK,
		"metrics_address", args.MetricsAddress,
		"state_directory", args.StateDirectory,
	)
	if err := grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("serve gRPC: %w", err)
	}
	return nil
}

func buildSchedulerConfig(startup *runtimeConfig) (scheduler.Config, error) {
	if startup == nil || startup.StartupConfig == nil {
		return scheduler.Config{}, fmt.Errorf("scheduler startup config is required")
	}
	policyBundle := startup.StartupConfig.Policy
	policyBundle.Selection.Strategy = startup.StartupConfig.SelectionStrategy
	policyBundle.Selection.TopK = startup.StartupConfig.TopK
	configured, err := policyBundle.ApplyToScheduler(scheduler.Config{Fallback: startup.FallbackMode})
	if err != nil {
		return scheduler.Config{}, fmt.Errorf("apply scheduler policy: %w", err)
	}
	return configured, nil
}

type cliArgs struct {
	ListenAddress        string
	ConfigRoot           string
	ManifestPath         string
	FallbackFlag         string
	StateDirectory       string
	MetricsAddress       string
	NVIDIADriverV2       bool
	NVIDIAPartitionMode  string
	NVIDIADryRun         bool
	NVIDIACommandTimeout time.Duration
}

type runtimeConfig struct {
	ListenAddress        string
	StartupConfig        *configpkg.StartupConfig
	ProviderCaps         *tgsrlv1.CapabilitySet
	FallbackMode         scheduler.FallbackMode
	Provider             provider.CompleteResourceProvider
	RuntimeConfig        eventloop.RuntimeConfig
	ResolvedConfigRoot   string
	NVIDIADriverV2       bool
	NVIDIAPartitionMode  nvidiaprovider.PartitionMode
	NVIDIADryRun         bool
	NVIDIACommandTimeout time.Duration
}

func parseArgs(argv []string) (*cliArgs, error) {
	fs := flag.NewFlagSet("scheduler", flag.ContinueOnError)
	listenAddress := fs.String("listen", defaultListenAddress, "gRPC listen address")
	configRoot := fs.String("config-root", ".", "configuration repository root")
	manifestPath := fs.String("manifest", "", "compatibility manifest path override")
	fallbackFlag := fs.String("fallback", "", "legacy fallback policy override: noop or static")
	stateDirectory := fs.String("state-dir", ".tmp/scheduler-state", "durable scheduler state directory")
	metricsAddress := fs.String("metrics-listen", "127.0.0.1:9090", "Prometheus metrics listen address; empty disables")
	nvidiaDriverV2 := fs.Bool("nvidia-driver-v2", false, "use the NVIDIA Driver v2 backend")
	nvidiaPartitionMode := fs.String("nvidia-partition-mode", string(nvidiaprovider.PartitionModeMPS), "NVIDIA Driver v2 partition mode: mps or mig")
	nvidiaDryRun := fs.Bool("nvidia-dry-run", false, "plan NVIDIA Driver v2 mutations without applying them")
	nvidiaCommandTimeout := fs.Duration("nvidia-command-timeout", 15*time.Second, "NVIDIA Driver v2 command timeout")
	if err := fs.Parse(argv); err != nil {
		return nil, err
	}
	if *nvidiaDriverV2 {
		mode := nvidiaprovider.PartitionMode(*nvidiaPartitionMode)
		if mode != nvidiaprovider.PartitionModeMPS && mode != nvidiaprovider.PartitionModeMIG {
			return nil, fmt.Errorf("unsupported NVIDIA partition mode %q", *nvidiaPartitionMode)
		}
		if *nvidiaCommandTimeout <= 0 {
			return nil, fmt.Errorf("NVIDIA command timeout must be positive")
		}
	}
	return &cliArgs{
		ListenAddress:        *listenAddress,
		ConfigRoot:           *configRoot,
		ManifestPath:         *manifestPath,
		FallbackFlag:         *fallbackFlag,
		StateDirectory:       *stateDirectory,
		MetricsAddress:       *metricsAddress,
		NVIDIADriverV2:       *nvidiaDriverV2,
		NVIDIAPartitionMode:  *nvidiaPartitionMode,
		NVIDIADryRun:         *nvidiaDryRun,
		NVIDIACommandTimeout: *nvidiaCommandTimeout,
	}, nil
}

func loadStartupConfig(args *cliArgs) (*runtimeConfig, error) {
	if args == nil {
		return nil, fmt.Errorf("scheduler arguments are required")
	}
	startupConfig, err := configpkg.LoadStartupConfig(configpkg.LoadOptions{
		Root:         args.ConfigRoot,
		ManifestPath: args.ManifestPath,
	})
	if err != nil {
		return nil, fmt.Errorf("load scheduler startup config: %w", err)
	}
	fallbackMode, err := parseFallback(startupConfig.FallbackMode)
	if err != nil {
		return nil, fmt.Errorf("config fallback %q: %w", startupConfig.FallbackMode, err)
	}
	if strings.TrimSpace(args.FallbackFlag) != "" {
		fallbackMode, err = parseFallback(args.FallbackFlag)
		if err != nil {
			return nil, err
		}
	}
	providerCaps, err := startupConfig.Capabilities.CapabilityProto(time.Now())
	if err != nil {
		return nil, fmt.Errorf("project startup capabilities: %w", err)
	}
	return &runtimeConfig{
		ListenAddress:        args.ListenAddress,
		StartupConfig:        startupConfig,
		ProviderCaps:         providerCaps,
		FallbackMode:         fallbackMode,
		RuntimeConfig:        eventloop.RuntimeConfig{FastInterval: startupConfig.RuntimeIntervals.Fast, MediumInterval: startupConfig.RuntimeIntervals.Medium, SlowInterval: startupConfig.RuntimeIntervals.Slow},
		ResolvedConfigRoot:   startupConfig.Root,
		NVIDIADriverV2:       args.NVIDIADriverV2,
		NVIDIAPartitionMode:  nvidiaprovider.PartitionMode(args.NVIDIAPartitionMode),
		NVIDIADryRun:         args.NVIDIADryRun,
		NVIDIACommandTimeout: args.NVIDIACommandTimeout,
	}, nil
}

func buildProvider(cfg *runtimeConfig) (provider.CompleteResourceProvider, error) {
	if cfg == nil || cfg.StartupConfig == nil {
		return nil, fmt.Errorf("startup config is required")
	}
	switch normalizeToken(cfg.StartupConfig.ProviderKind) {
	case "mock", "mockresourceprovider":
		options := []provider.MockOption{
			provider.WithProviderIdentity("mock", cfg.StartupConfig.ProviderSource),
		}
		if cfg.ProviderCaps != nil {
			options = append(options, provider.WithCapabilities(cloneCapabilitySet(cfg.ProviderCaps)))
		}
		instance, err := provider.NewMockResourceProvider(options...)
		if err != nil {
			return nil, fmt.Errorf("create mock provider: %w", err)
		}
		cfg.Provider = instance
		return instance, nil
	case nvidiaprovider.ProviderID:
		options := []nvidiaprovider.Option{}
		if cfg.NVIDIADriverV2 {
			options = append(options, nvidiaprovider.WithDriverV2(nvidiaprovider.LocalDriverV2Options{
				PartitionMode:  cfg.NVIDIAPartitionMode,
				CommandTimeout: cfg.NVIDIACommandTimeout,
				DryRun:         cfg.NVIDIADryRun,
			}))
		}
		instance, err := nvidiaprovider.New(options...)
		if err != nil {
			return nil, fmt.Errorf("create nvidia provider: %w", err)
		}
		cfg.Provider = instance
		return instance, nil
	default:
		return nil, fmt.Errorf("unsupported provider kind %q", cfg.StartupConfig.ProviderKind)
	}
}

func cloneCapabilitySet(capabilities *tgsrlv1.CapabilitySet) *tgsrlv1.CapabilitySet {
	if capabilities == nil {
		return nil
	}
	return proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)
}

type recoveredStateResumer interface {
	ResumeRecoveredState(context.Context, *persistence.SchedulerState) error
}

func resumeRecoveredServer(ctx context.Context, server recoveredStateResumer, recovered *persistence.SchedulerState) error {
	if server == nil || recovered == nil || recovered.Snapshot == nil {
		return nil
	}
	if err := server.ResumeRecoveredState(ctx, recovered); err != nil {
		return fmt.Errorf("resume recovered scheduler state: %w", err)
	}
	return nil
}

func startMetricsServer(address string, recorder *observability.PrometheusRecorder) (*http.Server, net.Listener, error) {
	if strings.TrimSpace(address) == "" {
		return nil, nil, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, nil, fmt.Errorf("listen for metrics on %s: %w", address, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", recorder.Handler())
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server stopped", "error", err)
		}
	}()
	return server, listener, nil
}

func normalizeToken(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(strings.TrimSpace(value)), "-", ""), "_", "")
}

func parseFallback(value string) (scheduler.FallbackMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "noop", "no_op":
		return scheduler.FallbackNoOp, nil
	case "static":
		return scheduler.FallbackStatic, nil
	default:
		return "", fmt.Errorf("unsupported fallback policy %q", value)
	}
}
