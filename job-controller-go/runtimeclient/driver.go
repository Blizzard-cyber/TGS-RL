package runtimeclient

import (
	"context"
	"errors"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Driver is the controller-facing runtime execution boundary.
type Driver interface {
	ValidateRuntime(context.Context, *tgsrlv1.ValidateRuntimeRequest) (*tgsrlv1.ValidateRuntimeResponse, error)
	CompileRuntime(context.Context, *tgsrlv1.CompileRuntimeRequest) (*tgsrlv1.CompileRuntimeResponse, error)
	PrepareRuntime(context.Context, *tgsrlv1.PrepareRuntimeRequest) (*tgsrlv1.PrepareRuntimeResponse, error)
	StartRuntime(context.Context, *tgsrlv1.StartRuntimeRequest) (*tgsrlv1.StartRuntimeResponse, error)
	PauseRuntime(context.Context, *tgsrlv1.PauseRuntimeRequest) (*tgsrlv1.PauseRuntimeResponse, error)
	ResumeRuntime(context.Context, *tgsrlv1.ResumeRuntimeRequest) (*tgsrlv1.ResumeRuntimeResponse, error)
	StopRuntime(context.Context, *tgsrlv1.StopRuntimeRequest) (*tgsrlv1.StopRuntimeResponse, error)
	TerminateRuntime(context.Context, *tgsrlv1.TerminateRuntimeRequest) (*tgsrlv1.TerminateRuntimeResponse, error)
	GetRuntimeStatus(context.Context, *tgsrlv1.GetRuntimeStatusRequest) (*tgsrlv1.GetRuntimeStatusResponse, error)
	Close() error
}

// GRPCDriver adapts RuntimeControlService gRPC to Driver.
type GRPCDriver struct {
	connection *grpc.ClientConn
	client     tgsrlv1.RuntimeControlServiceClient
}

// Dial dials a runtime control target.
func Dial(ctx context.Context, target string, options ...grpc.DialOption) (*GRPCDriver, error) {
	if target == "" {
		return nil, errors.New("runtimeclient: target is required")
	}
	if len(options) == 0 {
		options = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		}
	}
	connection, err := grpc.DialContext(ctx, target, options...)
	if err != nil {
		return nil, err
	}
	return &GRPCDriver{
		connection: connection,
		client:     tgsrlv1.NewRuntimeControlServiceClient(connection),
	}, nil
}

func (d *GRPCDriver) ValidateRuntime(ctx context.Context, request *tgsrlv1.ValidateRuntimeRequest) (*tgsrlv1.ValidateRuntimeResponse, error) {
	return d.client.ValidateRuntime(ctx, request)
}

func (d *GRPCDriver) CompileRuntime(ctx context.Context, request *tgsrlv1.CompileRuntimeRequest) (*tgsrlv1.CompileRuntimeResponse, error) {
	return d.client.CompileRuntime(ctx, request)
}

func (d *GRPCDriver) PrepareRuntime(ctx context.Context, request *tgsrlv1.PrepareRuntimeRequest) (*tgsrlv1.PrepareRuntimeResponse, error) {
	return d.client.PrepareRuntime(ctx, request)
}

func (d *GRPCDriver) StartRuntime(ctx context.Context, request *tgsrlv1.StartRuntimeRequest) (*tgsrlv1.StartRuntimeResponse, error) {
	return d.client.StartRuntime(ctx, request)
}

func (d *GRPCDriver) PauseRuntime(ctx context.Context, request *tgsrlv1.PauseRuntimeRequest) (*tgsrlv1.PauseRuntimeResponse, error) {
	return d.client.PauseRuntime(ctx, request)
}

func (d *GRPCDriver) ResumeRuntime(ctx context.Context, request *tgsrlv1.ResumeRuntimeRequest) (*tgsrlv1.ResumeRuntimeResponse, error) {
	return d.client.ResumeRuntime(ctx, request)
}

func (d *GRPCDriver) StopRuntime(ctx context.Context, request *tgsrlv1.StopRuntimeRequest) (*tgsrlv1.StopRuntimeResponse, error) {
	return d.client.StopRuntime(ctx, request)
}

func (d *GRPCDriver) TerminateRuntime(ctx context.Context, request *tgsrlv1.TerminateRuntimeRequest) (*tgsrlv1.TerminateRuntimeResponse, error) {
	return d.client.TerminateRuntime(ctx, request)
}

func (d *GRPCDriver) GetRuntimeStatus(ctx context.Context, request *tgsrlv1.GetRuntimeStatusRequest) (*tgsrlv1.GetRuntimeStatusResponse, error) {
	return d.client.GetRuntimeStatus(ctx, request)
}

// Close closes the underlying gRPC connection.
func (d *GRPCDriver) Close() error {
	if d == nil || d.connection == nil {
		return nil
	}
	return d.connection.Close()
}
