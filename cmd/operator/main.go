package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
	operatorcontrol "github.com/Blizzard-cyber/TGS-RL/operator-go/control"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/controller"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/cursor"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/kube"
	runtimepub "github.com/Blizzard-cyber/TGS-RL/operator-go/runtime"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/statuswatch"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type stringMapFlag map[string]string

type serviceFunc func(context.Context) error

func (f *stringMapFlag) String() string {
	if f == nil || len(*f) == 0 {
		return ""
	}
	values := make([]string, 0, len(*f))
	for key, value := range *f {
		values = append(values, key+"="+value)
	}
	return strings.Join(values, ",")
}

func (f *stringMapFlag) Set(value string) error {
	parts := strings.SplitN(value, "=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("node selector must be key=value")
	}
	key := strings.TrimSpace(parts[0])
	val := strings.TrimSpace(parts[1])
	if key == "" || val == "" {
		return fmt.Errorf("node selector must be key=value with non-empty parts")
	}
	if *f == nil {
		*f = make(map[string]string)
	}
	(*f)[key] = val
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("operator stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	mode := flag.String("mode", "kubernetes", "backend mode: kubernetes or fake")
	controllerEnabled := flag.Bool("controller", true, "enable controller reconcile loop")
	schedulerAddress := flag.String("scheduler", "127.0.0.1:50051", "scheduler gRPC address")
	controlAddress := flag.String("control", "127.0.0.1:50061", "job control gRPC address")
	runtimeAddress := flag.String("runtime", "127.0.0.1:50071", "runtime control gRPC address")
	listenAddress := flag.String("listen", "127.0.0.1:50081", "operator backend control gRPC listen address")
	namespace := flag.String("namespace", "default", "target namespace for compiled objects")
	cursorDir := flag.String("cursor-dir", filepath.Join(os.TempDir(), "tgsrl-operator"), "directory for durable operator cursors")
	kubeconfig := flag.String("kubeconfig", "", "optional kubeconfig path; defaults to KUBECONFIG or in-cluster config")
	gpuProfile := flag.String("gpu-profile", compiler.GPUProfileNone, "gpu profile: none, nvidia-device-plugin, kubernetes-dra, or volcano-hami")
	runtimeClassName := flag.String("runtime-class-name", "", "optional preconfigured RuntimeClass name")
	runtimeClassHandler := flag.String("runtime-class-handler", "", "RuntimeClass handler to create when runtime-class-create is enabled")
	runtimeClassCreate := flag.Bool("runtime-class-create", false, "create the configured RuntimeClass instead of referencing a preconfigured one")
	var nodeSelector stringMapFlag
	flag.Var(&nodeSelector, "node-selector", "optional pod node selector in key=value form; repeat for multiple entries")
	flag.Parse()
	runtimeConfig, err := validateStartupRuntimeConfig(*gpuProfile, compiler.RuntimeConfig{
		RuntimeClass: compiler.RuntimeClassConfig{
			Name:    *runtimeClassName,
			Handler: *runtimeClassHandler,
			Create:  *runtimeClassCreate,
		},
		NodeSelector: map[string]string(nodeSelector),
	})
	if err != nil {
		return err
	}

	selectedBackend, backendName, err := selectBackend(*mode, *namespace, *kubeconfig)
	if err != nil {
		return err
	}
	if backendName == "kubernetes" {
		if err := preflightBackend(context.Background(), selectedBackend, *gpuProfile); err != nil {
			return fmt.Errorf("backend preflight: %w", err)
		}
	}
	if configurable, ok := selectedBackend.(interface{ SetControlStatePath(string) error }); ok {
		if err := configurable.SetControlStatePath(filepath.Join(*cursorDir, "backend-controls.json")); err != nil {
			return fmt.Errorf("load backend control state: %w", err)
		}
	}
	controlServer, err := operatorcontrol.NewServer(selectedBackend)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		return fmt.Errorf("listen for backend control: %w", err)
	}
	defer listener.Close()
	grpcServer := grpc.NewServer()
	tgsrlv1.RegisterRuntimeBackendControlServiceServer(grpcServer, controlServer)
	services := []namedService{{
		name: "grpc",
		run:  func(context.Context) error { return grpcServer.Serve(listener) },
	}}
	if *controllerEnabled {
		reconciler, err := controller.NewWithRuntimeConfig(selectedBackend, runtimeConfig)
		if err != nil {
			return fmt.Errorf("build reconciler: %w", err)
		}

		schedulerConn, err := grpc.DialContext(context.Background(), *schedulerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial scheduler: %w", err)
		}
		defer schedulerConn.Close()
		controlConn, err := grpc.DialContext(context.Background(), *controlAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial control: %w", err)
		}
		defer controlConn.Close()
		runtimeConn, err := grpc.DialContext(context.Background(), *runtimeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial runtime: %w", err)
		}
		defer runtimeConn.Close()

		schedulerClient := tgsrlv1.NewSchedulerServiceClient(schedulerConn)
		source, err := worker.NewWatchDecisionSourceFromGRPC(schedulerClient, 30*time.Second)
		if err != nil {
			return err
		}
		runtimeClient := tgsrlv1.NewRuntimeControlServiceClient(runtimeConn)
		rpcPublisher, err := runtimepub.NewRPCPublisher(runtimeClient, tgsrlv1.NewSchedulerObservationServiceClient(schedulerConn))
		if err != nil {
			return err
		}
		repo := cursor.NewFileRepository(filepath.Join(*cursorDir, "decision-cursor.json"))
		deliveryRepo := worker.NewFileDeliveryRepository(filepath.Join(*cursorDir, "delivery.json"))
		observer, err := selectObserver(*mode, *namespace, *kubeconfig, selectedBackend)
		if err != nil {
			return err
		}
		w, err := worker.New(worker.Config{
			Source:      source,
			JobRuns:     tgsrlv1.NewJobControlServiceClient(controlConn),
			Manifests:   tgsrlv1.NewRuntimeControlServiceClient(runtimeConn),
			Reconciler:  worker.NewControllerReconciler(reconciler),
			Observer:    observer,
			Publisher:   rpcPublisher,
			Cursors:     repo,
			Deliveries:  deliveryRepo,
			Bundles:     selectedBackend,
			Namespace:   *namespace,
			GPUProfiles: []string{*gpuProfile},
		})
		if err != nil {
			return err
		}
		services = append(services, namedService{
			name: "worker",
			run:  w.Run,
		})
	} else {
		slog.Info("operator started without controller", "mode", backendName)
	}
	ctx, stop := signalContext()
	defer stop()
	runErr := runServices(ctx, grpcServer, services...)
	if runErr != nil {
		return runErr
	}
	slog.Info("operator stopped", "mode", backendName, "controller", *controllerEnabled)
	return nil
}

func preflightBackend(ctx context.Context, selectedBackend backend.Backend, gpuProfile string) error {
	discoverer, ok := selectedBackend.(interface {
		DiscoverCapabilities(context.Context) (compiler.CapabilitySet, error)
	})
	if !ok {
		return nil
	}
	capabilities, err := discoverer.DiscoverCapabilities(ctx)
	if err != nil {
		return fmt.Errorf("discover capabilities: %w", err)
	}
	profile := strings.TrimSpace(gpuProfile)
	if !capabilities.GPUProfiles[profile] {
		return fmt.Errorf("GPU profile %q is not available", profile)
	}
	if profile != compiler.GPUProfileNone && !capabilities.ExactDevicePlacement[profile] {
		return fmt.Errorf("GPU profile %q cannot enforce scheduler-selected device identities", profile)
	}
	if capabilities.KubernetesAPIs.KueueWorkload == "" {
		return fmt.Errorf("Kueue Workload API is not available")
	}
	if profile == compiler.GPUProfileKubernetesDRA && len(capabilities.DRADevices) == 0 {
		return fmt.Errorf("NVIDIA DRA device UUID inventory is not available")
	}
	slog.Info("backend capability preflight passed",
		"gpu_profile", profile,
		"kueue_api", capabilities.KubernetesAPIs.KueueWorkload,
		"dra_api", capabilities.KubernetesAPIs.DRAResourceClaim,
	)
	return nil
}

type namedService struct {
	name string
	run  serviceFunc
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func runServices(ctx context.Context, grpcServer *grpc.Server, services ...namedService) error {
	if len(services) == 0 {
		return nil
	}
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	type serviceResult struct {
		name string
		err  error
	}
	errCh := make(chan serviceResult, len(services))
	for _, service := range services {
		service := service
		go func() {
			errCh <- serviceResult{name: service.name, err: service.run(ctx)}
		}()
	}
	var (
		runErr   error
		received int
	)
	select {
	case <-ctx.Done():
	case result := <-errCh:
		received = 1
		if result.err != nil {
			runErr = fmt.Errorf("%s service: %w", result.name, result.err)
		}
	}
	stop()
	if grpcServer != nil {
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			grpcServer.Stop()
		}
	}
	shutdownDeadline := time.NewTimer(5 * time.Second)
	defer shutdownDeadline.Stop()
	for received < len(services) {
		select {
		case result := <-errCh:
			received++
			if runErr == nil && result.err != nil && result.err != grpc.ErrServerStopped && !isBenignServerClose(result.err) {
				runErr = fmt.Errorf("%s service: %w", result.name, result.err)
			}
		case <-shutdownDeadline.C:
			if runErr == nil {
				runErr = fmt.Errorf("operator services did not stop within 5s")
			}
			return runErr
		}
	}
	return runErr
}

func isBenignServerClose(err error) bool {
	return err == nil || strings.Contains(err.Error(), "use of closed network connection")
}

func validateGPUProfile(profile string) error {
	switch strings.TrimSpace(profile) {
	case compiler.GPUProfileNone, compiler.GPUProfileNVIDIADevicePlugin, compiler.GPUProfileKubernetesDRA, compiler.GPUProfileVolcanoHAMI:
		return nil
	default:
		return fmt.Errorf("unsupported gpu profile %q", profile)
	}
}

func validateStartupRuntimeConfig(profile string, config compiler.RuntimeConfig) (compiler.RuntimeConfig, error) {
	if err := validateGPUProfile(profile); err != nil {
		return compiler.RuntimeConfig{}, err
	}
	validated, err := compiler.ValidateRuntimeConfig(config)
	if err != nil {
		return compiler.RuntimeConfig{}, fmt.Errorf("invalid runtime configuration: %w", err)
	}
	return validated, nil
}

func selectBackend(mode, namespace, kubeconfigPath string) (backend.Backend, string, error) {
	switch mode {
	case "fake":
		return backend.NewFake(), "fake", nil
	case "kubernetes":
		selected, err := newKubernetesBackend(namespace, kubeconfigPath)
		if err != nil {
			return nil, "", err
		}
		return selected, "kubernetes", nil
	default:
		return nil, "", fmt.Errorf("unsupported mode %q", mode)
	}
}

func newKubernetesBackend(namespace, kubeconfigPath string) (backend.Backend, error) {
	config, err := kube.LoadConfig(kube.ResolveKubeconfig(kubeconfigPath), namespace)
	if err != nil {
		return nil, fmt.Errorf("load kubernetes config: %w", err)
	}
	client, err := kube.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("new kubernetes client: %w", err)
	}
	kubeBackend, err := backend.NewKubernetes(client)
	if err != nil {
		return nil, fmt.Errorf("new kubernetes backend: %w", err)
	}
	return kubeBackend, nil
}

func selectObserver(mode, namespace, kubeconfigPath string, selectedBackend backend.Backend) (statuswatch.Observer, error) {
	switch mode {
	case "fake":
		fakeBackend, ok := selectedBackend.(*backend.FakeBackend)
		if !ok {
			return nil, fmt.Errorf("fake observer requires fake backend")
		}
		// Fake mode still reads the same backend authority, but its finite stream
		// deliberately preserves the admission BOUND snapshot before RUNNING.
		return statuswatch.NewFake(fakeBackend), nil
	case "kubernetes":
		kubeBackend, ok := selectedBackend.(*backend.KubernetesBackend)
		if !ok {
			return nil, fmt.Errorf("kubernetes observer requires kubernetes backend")
		}
		return statuswatch.NewBackendObserver(kubeBackend, 2*time.Second)
	default:
		return nil, fmt.Errorf("unsupported mode %q", mode)
	}
}
