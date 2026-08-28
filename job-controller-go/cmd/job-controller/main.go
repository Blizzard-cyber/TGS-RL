// Command job-controller runs the durable JobControlService.
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
	"syscall"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/controller"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/runtimeclient"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/service"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/state"
	"google.golang.org/grpc"
)

const defaultListenAddress = "127.0.0.1:50061"
const defaultRuntimeTarget = "127.0.0.1:50071"

func main() {
	if err := run(); err != nil {
		slog.Error("job controller stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	flags := flag.NewFlagSet("job-controller", flag.ContinueOnError)
	listenAddress := flags.String("listen", defaultListenAddress, "gRPC listen address")
	runtimeTarget := flags.String("runtime-target", defaultRuntimeTarget, "runtime control gRPC target")
	stateDir := flags.String("state-dir", defaultStateDir(), "durable controller state directory")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	repository, err := state.NewFileRepository(*stateDir)
	if err != nil {
		return fmt.Errorf("create repository: %w", err)
	}
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	runtimeDriver, err := runtimeclient.Dial(dialCtx, *runtimeTarget)
	if err != nil {
		return fmt.Errorf("dial runtime target %s: %w", *runtimeTarget, err)
	}
	defer func() {
		if closeErr := runtimeDriver.Close(); closeErr != nil {
			slog.Error("close runtime driver", "error", closeErr)
		}
	}()
	engine, err := controller.New(controller.Config{Repository: repository, Runtime: runtimeDriver})
	if err != nil {
		return fmt.Errorf("create controller: %w", err)
	}
	server, err := service.New(service.Config{Controller: engine})
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listenAddress, err)
	}
	grpcServer := grpc.NewServer()
	tgsrlv1.RegisterJobControlServiceServer(grpcServer, server)

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("shutdown goroutine panicked", "panic", recovered)
			}
		}()
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	slog.Info("job controller listening", "address", listener.Addr().String(), "runtime_target", *runtimeTarget, "state_dir", *stateDir)
	if err := grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("serve gRPC: %w", err)
	}
	return nil
}

func defaultStateDir() string {
	cacheDir, err := os.UserCacheDir()
	if err != nil || cacheDir == "" {
		return filepath.Join(os.TempDir(), "tgs-rl-job-controller")
	}
	return filepath.Join(cacheDir, "tgs-rl", "job-controller")
}
