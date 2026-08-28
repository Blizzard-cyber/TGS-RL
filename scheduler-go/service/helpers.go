package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"google.golang.org/protobuf/proto"
)

func (s *Server) recordQueueDepthLocked(intent *tgsrlv1.SchedulingIntent, depth int) {
	if telemetry, ok := s.recorder.(observability.SchedulerTelemetry); ok {
		telemetry.RecordQueueDepth(intentCorrelation(intent), depth)
	}
}

func (s *Server) recordScheduleLatency(intent *tgsrlv1.SchedulingIntent, elapsed time.Duration) {
	if telemetry, ok := s.recorder.(observability.SchedulerTelemetry); ok {
		telemetry.ObserveSchedule(intentCorrelation(intent), elapsed.Seconds())
	}
}

func (s *Server) recordActionResults(intent *tgsrlv1.SchedulingIntent, results []*tgsrlv1.ActionResult) {
	telemetry, ok := s.recorder.(observability.SchedulerTelemetry)
	if !ok {
		return
	}
	correlation := intentCorrelation(intent)
	for _, result := range results {
		if result == nil {
			telemetry.RecordAction(correlation, "unknown")
			continue
		}
		statusName := actionResultStatusName(result.GetStatus())
		telemetry.RecordAction(correlation, statusName)
	}
}

func intentCorrelation(intent *tgsrlv1.SchedulingIntent) observability.Correlation {
	if intent == nil {
		return observability.Correlation{}
	}
	return observability.Correlation{JobID: intent.GetJobId(), RunID: intent.GetRunId(), TraceID: intent.GetTraceId()}
}

func cloneActionResults(results []*tgsrlv1.ActionResult) []*tgsrlv1.ActionResult {
	clones := make([]*tgsrlv1.ActionResult, len(results))
	for index, result := range results {
		if result != nil {
			clones[index] = proto.Clone(result).(*tgsrlv1.ActionResult)
		}
	}
	return clones
}

func cloneSemanticEnvelope(envelope *tgsrlv1.SemanticEnvelope) *tgsrlv1.SemanticEnvelope {
	if envelope == nil {
		return nil
	}
	return proto.Clone(envelope).(*tgsrlv1.SemanticEnvelope)
}

func fillDecisionMetadataFromIntent(decision *tgsrlv1.DecisionRecord, intent *tgsrlv1.SchedulingIntent) {
	if decision == nil || intent == nil {
		return
	}
	if decision.GetRunId() == "" {
		decision.RunId = intent.GetRunId()
	}
	if decision.GetJobId() == "" {
		decision.JobId = intent.GetJobId()
	}
	if decision.GetTraceId() == "" {
		decision.TraceId = intent.GetTraceId()
	}
	if decision.GetDataKind() == tgsrlv1.DataKind_DATA_KIND_UNKNOWN {
		decision.DataKind = intent.GetDataKind()
	}
	if decision.GetSemanticContext() == nil && intent.GetSemanticContext() != nil {
		decision.SemanticContext = cloneSemanticEnvelope(intent.GetSemanticContext())
	}
	if decision.GetGeneration() == 0 {
		decision.Generation = intent.GetGeneration()
	}
	if decision.GetCursor() == "" {
		decision.Cursor = intent.GetCursor()
	}
}

func (s *Server) backgroundContext() context.Context {
	if s != nil && s.lifecycleCtx != nil {
		return s.lifecycleCtx
	}
	return context.Background()
}

func (s *Server) setProviderWatchHealthy(kind string) {
	if s == nil {
		return
	}
	s.providerWatch.mu.Lock()
	switch kind {
	case "resource":
		s.providerWatch.resourceHealthy = true
		s.providerWatch.resourceReason = ""
	case "sandbox":
		s.providerWatch.sandboxHealthy = true
		s.providerWatch.sandboxReason = ""
	default:
		s.providerWatch.resourceHealthy = true
		s.providerWatch.sandboxHealthy = true
		s.providerWatch.resourceReason = ""
		s.providerWatch.sandboxReason = ""
	}
	s.providerWatch.mu.Unlock()
}

func (s *Server) setProviderWatchUnhealthy(kind, reason string) {
	if s == nil {
		return
	}
	s.providerWatch.mu.Lock()
	switch kind {
	case "resource":
		s.providerWatch.resourceHealthy = false
		s.providerWatch.resourceReason = reason
	case "sandbox":
		s.providerWatch.sandboxHealthy = false
		s.providerWatch.sandboxReason = reason
	default:
		s.providerWatch.resourceHealthy = false
		s.providerWatch.sandboxHealthy = false
		s.providerWatch.resourceReason = reason
		s.providerWatch.sandboxReason = reason
	}
	s.providerWatch.mu.Unlock()
}

func (s *Server) providerWatchFailure() error {
	if s == nil {
		return nil
	}
	s.providerWatch.mu.RLock()
	defer s.providerWatch.mu.RUnlock()
	if s.providerWatch.resourceHealthy && s.providerWatch.sandboxHealthy {
		return nil
	}
	switch {
	case !s.providerWatch.resourceHealthy && s.providerWatch.resourceReason != "":
		return fmt.Errorf("provider resource watch is unavailable: %s", s.providerWatch.resourceReason)
	case !s.providerWatch.sandboxHealthy && s.providerWatch.sandboxReason != "":
		return fmt.Errorf("provider sandbox watch is unavailable: %s", s.providerWatch.sandboxReason)
	default:
		return errors.New("provider watch is unavailable")
	}
}

func (s *Server) workerContext(requestCtx context.Context) context.Context {
	base := s.backgroundContext()
	if requestCtx == nil {
		return base
	}
	return serverWorkerContext{
		server: base,
		values: context.WithoutCancel(requestCtx),
	}
}

type serverWorkerContext struct {
	server context.Context
	values context.Context
}

func (c serverWorkerContext) Deadline() (time.Time, bool) {
	return c.server.Deadline()
}

func (c serverWorkerContext) Done() <-chan struct{} {
	return c.server.Done()
}

func (c serverWorkerContext) Err() error {
	return c.server.Err()
}

func (c serverWorkerContext) Value(key any) any {
	if c.values != nil {
		if value := c.values.Value(key); value != nil {
			return value
		}
	}
	return c.server.Value(key)
}

func actionResultStatusName(status tgsrlv1.ActionResultStatus) string {
	name := status.String()
	const prefix = "ACTION_RESULT_STATUS_"
	if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
		name = name[len(prefix):]
	}
	switch name {
	case "":
		return "unknown"
	default:
		return lowerASCII(name)
	}
}

func lowerASCII(value string) string {
	out := make([]byte, len(value))
	for i := range value {
		ch := value[i]
		if 'A' <= ch && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		out[i] = ch
	}
	return string(out)
}
