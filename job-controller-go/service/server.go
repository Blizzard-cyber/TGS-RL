// Package service exposes the JobControlService gRPC surface.
package service

import (
	"context"
	"errors"
	"io"
	"strconv"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/controller"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/state"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	watchConsumerLaggedReason      = "WATCH_CONSUMER_LAGGED"
	watchTerminationTrailer        = "tgsrl-watch-termination"
	watchLastCursorTrailer         = "tgsrl-watch-last-cursor"
	watchRetryAfterSequenceTrailer = "tgsrl-watch-retry-after-sequence"
)

// Server implements the generated JobControlServiceServer interface.
type Server struct {
	tgsrlv1.UnimplementedJobControlServiceServer

	controller *controller.Controller
}

// Config defines the service dependencies.
type Config struct {
	Controller *controller.Controller
}

// New constructs a new JobControlService server.
func New(config Config) (*Server, error) {
	if config.Controller == nil {
		return nil, errors.New("service: controller is required")
	}
	return &Server{controller: config.Controller}, nil
}

func (s *Server) CreateJob(ctx context.Context, request *tgsrlv1.CreateJobRequest) (*tgsrlv1.CreateJobResponse, error) {
	return s.controller.CreateJob(ctx, request)
}

func (s *Server) ListJobs(ctx context.Context, request *tgsrlv1.ListJobsRequest) (*tgsrlv1.ListJobsResponse, error) {
	return s.controller.ListJobs(ctx, request)
}

func (s *Server) GetJob(ctx context.Context, request *tgsrlv1.GetJobRequest) (*tgsrlv1.GetJobResponse, error) {
	return s.controller.GetJob(ctx, request)
}

func (s *Server) ValidateJob(ctx context.Context, request *tgsrlv1.ValidateJobRequest) (*tgsrlv1.ValidateJobResponse, error) {
	return s.controller.ValidateJob(ctx, request)
}

func (s *Server) AdmitJob(ctx context.Context, request *tgsrlv1.AdmitJobRequest) (*tgsrlv1.AdmitJobResponse, error) {
	return s.controller.AdmitJob(ctx, request)
}

func (s *Server) CreateJobRun(ctx context.Context, request *tgsrlv1.CreateJobRunRequest) (*tgsrlv1.CreateJobRunResponse, error) {
	return s.controller.CreateJobRun(ctx, request)
}

func (s *Server) ListJobRuns(ctx context.Context, request *tgsrlv1.ListJobRunsRequest) (*tgsrlv1.ListJobRunsResponse, error) {
	return s.controller.ListJobRuns(ctx, request)
}

func (s *Server) GetJobRun(ctx context.Context, request *tgsrlv1.GetJobRunRequest) (*tgsrlv1.GetJobRunResponse, error) {
	return s.controller.GetJobRun(ctx, request)
}

func (s *Server) ListJobEvents(ctx context.Context, request *tgsrlv1.ListJobEventsRequest) (*tgsrlv1.ListJobEventsResponse, error) {
	return s.controller.ListJobEvents(ctx, request)
}

func (s *Server) RecordOperation(ctx context.Context, request *tgsrlv1.RecordOperationRequest) (*tgsrlv1.RecordOperationResponse, error) {
	return s.controller.RecordOperation(ctx, request)
}

func (s *Server) ListOperations(ctx context.Context, request *tgsrlv1.ListOperationsRequest) (*tgsrlv1.ListOperationsResponse, error) {
	return s.controller.ListOperations(ctx, request)
}

func (s *Server) ApplyJobCommand(ctx context.Context, request *tgsrlv1.ApplyJobCommandRequest) (*tgsrlv1.ApplyJobCommandResponse, error) {
	return s.controller.ApplyJobCommand(ctx, request)
}

func (s *Server) GetOperation(ctx context.Context, request *tgsrlv1.GetOperationRequest) (*tgsrlv1.GetOperationResponse, error) {
	return s.controller.GetOperation(ctx, request)
}

func (s *Server) WatchJobEvents(request *tgsrlv1.WatchJobEventsRequest, stream tgsrlv1.JobControlService_WatchJobEventsServer) error {
	ch, cancel, err := s.controller.WatchJobEvents(request.GetJobId(), request.GetRunId(), request.GetAfterSequence(), request.GetAfterEventId())
	if err != nil {
		return err
	}
	defer cancel()
	watchResult, hasTypedTermination := state.WatchResultFor(ch)
	lastCursor := strconv.FormatUint(request.GetAfterSequence(), 10)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case event, ok := <-ch:
			if !ok {
				if hasTypedTermination {
					termination, terminated := <-watchResult.Termination
					if !terminated {
						return nil
					}
					switch termination.Reason {
					case state.WatchTerminationConsumerLagged:
						stream.SetTrailer(metadata.Pairs(
							watchTerminationTrailer, string(termination.Reason),
							watchLastCursorTrailer, lastCursor,
							watchRetryAfterSequenceTrailer, lastCursor,
						))
						return watchConsumerLaggedError(lastCursor)
					default:
						return status.Errorf(codes.Internal, "watch terminated for unknown reason %q", termination.Reason)
					}
				}
				return nil
			}
			// Send is governed by the gRPC transport context. Repository overflow
			// cannot interrupt an already blocked Send. If it succeeds, the typed
			// cause is observed next; otherwise the transport error takes precedence.
			if err := stream.Send(&tgsrlv1.WatchJobEventsResponse{
				Event:  event,
				Cursor: event.GetCursor(),
			}); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			lastCursor = event.GetCursor()
		}
	}
}

func watchConsumerLaggedError(lastCursor string) error {
	statusValue := status.New(codes.ResourceExhausted, "watch consumer fell behind; reconnect from the last cursor")
	withDetails, err := statusValue.WithDetails(&errdetails.ErrorInfo{
		Reason: watchConsumerLaggedReason,
		Domain: "tgsrl.v1.JobControlService",
		Metadata: map[string]string{
			"last_cursor":          lastCursor,
			"retry_after_sequence": lastCursor,
		},
	})
	if err != nil {
		return statusValue.Err()
	}
	return withDetails.Err()
}

func (s *Server) ReportComponentStatus(ctx context.Context, request *tgsrlv1.ReportComponentStatusRequest) (*tgsrlv1.ReportComponentStatusResponse, error) {
	return s.controller.ReportComponentStatus(ctx, request)
}
