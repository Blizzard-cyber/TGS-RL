package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/eventloop"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func cloneContractObservation(observation *tgsrlv1.ContractObservation) *tgsrlv1.ContractObservation {
	if observation == nil {
		return nil
	}
	return proto.Clone(observation).(*tgsrlv1.ContractObservation)
}

func cloneRuntimeTrigger(trigger *eventloop.Trigger) *eventloop.Trigger {
	return eventloop.CloneTrigger(trigger)
}

func appendOrMergeTrigger(triggers []*eventloop.Trigger, trigger *eventloop.Trigger) []*eventloop.Trigger {
	if trigger == nil {
		return triggers
	}
	for index, existing := range triggers {
		if existing == nil {
			continue
		}
		if existing.TickKind == trigger.TickKind {
			triggers[index] = eventloop.MergeTrigger(existing, trigger)
			return triggers
		}
	}
	return append(triggers, cloneRuntimeTrigger(trigger))
}

func mergeReconcileWork(current *reconcileWork, intent *tgsrlv1.SchedulingIntent, trigger *eventloop.Trigger) *reconcileWork {
	if intent == nil {
		return current
	}
	if current == nil || current.intent == nil || intent.GetVersion() > current.intent.GetVersion() {
		next := &reconcileWork{intent: proto.Clone(intent).(*tgsrlv1.SchedulingIntent)}
		if trigger != nil {
			next.triggers = appendOrMergeTrigger(next.triggers, trigger)
		}
		return next
	}
	if intent.GetVersion() < current.intent.GetVersion() {
		return current
	}
	if current.intent == nil {
		current.intent = proto.Clone(intent).(*tgsrlv1.SchedulingIntent)
	}
	if trigger != nil {
		current.triggers = appendOrMergeTrigger(current.triggers, trigger)
	}
	return current
}

func buildEvaluationContext(now time.Time, intent *tgsrlv1.SchedulingIntent, trigger *eventloop.Trigger) *tgsrlv1.EvaluationContext {
	evaluationTime := timestamppb.New(now.UTC())
	context := &tgsrlv1.EvaluationContext{
		TickKind:                     tgsrlv1.TickKind_TICK_KIND_UNKNOWN,
		EvaluationTime:               evaluationTime,
		DecisionSequence:             0,
		CompatibilityDefaultsApplied: false,
	}
	if trigger != nil {
		context.TickKind = trigger.TickKind
		context.Cause = trigger.Cause
		context.ObservedRevision = trigger.ObservedRevision
		context.ContractObservation = cloneContractObservation(trigger.ContractObservation)
		context.CompatibilityDefaultsApplied = trigger.CompatibilityDefaultsApplied
	}
	if intent != nil && context.ContractObservation == nil {
		context.ContractObservation = cloneContractObservation(intent.GetContractObservation())
	}
	return context
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

func (s *Server) now() time.Time {
	if s != nil && s.clock != nil {
		return s.clock.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *Server) setProviderWatchHealthy(kind string) {
	if s == nil {
		return
	}
	s.providerWatch.mu.Lock()
	switch kind {
	case "resource":
		if !s.providerWatch.resourcePoisoned {
			s.providerWatch.resourceHealthy = true
			s.providerWatch.resourceReason = ""
		}
	case "sandbox":
		if !s.providerWatch.sandboxPoisoned {
			s.providerWatch.sandboxHealthy = true
			s.providerWatch.sandboxReason = ""
		}
	default:
		if !s.providerWatch.resourcePoisoned {
			s.providerWatch.resourceHealthy = true
			s.providerWatch.resourceReason = ""
		}
		if !s.providerWatch.sandboxPoisoned {
			s.providerWatch.sandboxHealthy = true
			s.providerWatch.sandboxReason = ""
		}
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

func (s *Server) poisonProviderWatch(kind, reason string) {
	if s == nil {
		return
	}
	s.providerWatch.mu.Lock()
	switch kind {
	case "resource":
		s.providerWatch.resourceHealthy = false
		s.providerWatch.resourcePoisoned = true
		s.providerWatch.resourceReason = reason
	case "sandbox":
		s.providerWatch.sandboxHealthy = false
		s.providerWatch.sandboxPoisoned = true
		s.providerWatch.sandboxReason = reason
	default:
		s.providerWatch.resourceHealthy = false
		s.providerWatch.resourcePoisoned = true
		s.providerWatch.resourceReason = reason
		s.providerWatch.sandboxHealthy = false
		s.providerWatch.sandboxPoisoned = true
		s.providerWatch.sandboxReason = reason
	}
	s.providerWatch.mu.Unlock()
}

func (s *Server) markProviderWatchEventHealthy(kind string) {
	if s == nil {
		return
	}
	s.providerWatch.mu.Lock()
	switch kind {
	case "resource":
		s.providerWatch.resourcePoisoned = false
		s.providerWatch.resourceHealthy = true
		s.providerWatch.resourceReason = ""
	case "sandbox":
		s.providerWatch.sandboxPoisoned = false
		s.providerWatch.sandboxHealthy = true
		s.providerWatch.sandboxReason = ""
	default:
		s.providerWatch.resourcePoisoned = false
		s.providerWatch.resourceHealthy = true
		s.providerWatch.resourceReason = ""
		s.providerWatch.sandboxPoisoned = false
		s.providerWatch.sandboxHealthy = true
		s.providerWatch.sandboxReason = ""
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
