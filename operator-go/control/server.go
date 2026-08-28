package control

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server owns only desired backend lifecycle mutations. Backend observations
// are read and published exclusively by worker.ObservationManager.
type Server struct {
	tgsrlv1.UnimplementedRuntimeBackendControlServiceServer
	backend   backend.Backend
	controlMu sync.Mutex
}

func NewServer(executionBackend backend.Backend) (*Server, error) {
	if executionBackend == nil {
		return nil, fmt.Errorf("backend is required")
	}
	return &Server{backend: executionBackend}, nil
}

func (s *Server) ApplyRuntimeControl(ctx context.Context, request *tgsrlv1.ApplyRuntimeControlRequest) (*tgsrlv1.ApplyRuntimeControlResponse, error) {
	converted, err := convertRequest(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// Serialize calls so backend revisions and whole-request idempotency remain
	// ordered across concurrent unary handlers. This method never publishes a
	// SandboxEvent; acceptance and observation are deliberately separate.
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	result, err := s.backend.Control(ctx, converted)
	if err != nil {
		return nil, mapBackendError(err)
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "backend returned no control result")
	}
	return &tgsrlv1.ApplyRuntimeControlResponse{Accepted: result.Accepted, Idempotent: result.Idempotent, BackendRevision: result.BackendRevision, Detail: result.Detail}, nil
}

func convertRequest(request *tgsrlv1.ApplyRuntimeControlRequest) (backend.ControlRequest, error) {
	if request == nil {
		return backend.ControlRequest{}, fmt.Errorf("request is required")
	}
	converted := backend.ControlRequest{Action: request.GetAction(), JobID: strings.TrimSpace(request.GetJobId()), RunID: strings.TrimSpace(request.GetRunId()), TraceID: strings.TrimSpace(request.GetTraceId()), RequestID: strings.TrimSpace(request.GetRequestId()), IdempotencyKey: strings.TrimSpace(request.GetIdempotencyKey()), Reason: strings.TrimSpace(request.GetReason())}
	if request.GetDeadline() != nil {
		if err := request.GetDeadline().CheckValid(); err != nil {
			return backend.ControlRequest{}, fmt.Errorf("invalid deadline: %w", err)
		}
		converted.Deadline = request.GetDeadline().AsTime()
	}
	for _, target := range request.GetTargets() {
		if target == nil {
			return backend.ControlRequest{}, fmt.Errorf("runtime target is required")
		}
		converted.Targets = append(converted.Targets, backend.ControlTarget{RuntimeUnitID: strings.TrimSpace(target.GetRuntimeUnitId()), SandboxID: strings.TrimSpace(target.GetSandboxId()), ExpectedGeneration: target.GetExpectedGeneration()})
	}
	if converted.RunID == "" || converted.JobID == "" || converted.IdempotencyKey == "" {
		return backend.ControlRequest{}, fmt.Errorf("job_id, run_id, and idempotency_key are required")
	}
	if len(converted.Targets) == 0 {
		return backend.ControlRequest{}, fmt.Errorf("at least one runtime target is required")
	}
	for _, target := range converted.Targets {
		if target.RuntimeUnitID == "" || target.SandboxID == "" || target.ExpectedGeneration == 0 {
			return backend.ControlRequest{}, fmt.Errorf("runtime_unit_id, sandbox_id, and expected_generation are required for every target")
		}
	}
	return converted, nil
}

func mapBackendError(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, backend.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, backend.ErrInvalidControl):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, backend.ErrRuntimeNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, backend.ErrGenerationConflict):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
