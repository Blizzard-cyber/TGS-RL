package worker

import (
	"context"
	"io"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/cursor"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

type SchedulerWatchClient interface {
	WatchDecisions(ctx context.Context, in *tgsrlv1.WatchDecisionsRequest, opts ...grpc.CallOption) (SchedulerDecisionStream, error)
}

type SchedulerDecisionStream interface {
	Recv() (*tgsrlv1.WatchDecisionsResponse, error)
}

type WatchDecisionSource struct {
	client            SchedulerWatchClient
	heartbeatInterval time.Duration
}

type schedulerClientAdapter struct {
	client tgsrlv1.SchedulerServiceClient
}

func NewWatchDecisionSource(client SchedulerWatchClient, heartbeatInterval time.Duration) (*WatchDecisionSource, error) {
	if client == nil {
		return nil, io.ErrUnexpectedEOF
	}
	if heartbeatInterval <= 0 {
		heartbeatInterval = 30 * time.Second
	}
	return &WatchDecisionSource{client: client, heartbeatInterval: heartbeatInterval}, nil
}

func NewWatchDecisionSourceFromGRPC(client tgsrlv1.SchedulerServiceClient, heartbeatInterval time.Duration) (*WatchDecisionSource, error) {
	if client == nil {
		return nil, io.ErrUnexpectedEOF
	}
	return NewWatchDecisionSource(&schedulerClientAdapter{client: client}, heartbeatInterval)
}

func (s *WatchDecisionSource) Watch(ctx context.Context, after cursor.Cursor) (DecisionStream, error) {
	stream, err := s.client.WatchDecisions(ctx, &tgsrlv1.WatchDecisionsRequest{
		AfterSequence:     after.Sequence,
		AfterDecisionId:   after.DecisionID,
		HeartbeatInterval: durationpb.New(s.heartbeatInterval),
	})
	if err != nil {
		return nil, err
	}
	return &watchDecisionStream{stream: stream}, nil
}

type watchDecisionStream struct {
	stream SchedulerDecisionStream
}

func (s *watchDecisionStream) Recv() (*tgsrlv1.DecisionRecord, error) {
	for {
		response, err := s.stream.Recv()
		if err != nil {
			return nil, err
		}
		if response == nil || response.GetDecision() == nil {
			continue
		}
		decision := response.GetDecision()
		if decision.Cursor == "" {
			decision.Cursor = response.GetCursor()
		}
		return decision, nil
	}
}

func (a *schedulerClientAdapter) WatchDecisions(ctx context.Context, in *tgsrlv1.WatchDecisionsRequest, opts ...grpc.CallOption) (SchedulerDecisionStream, error) {
	return a.client.WatchDecisions(ctx, in, opts...)
}
