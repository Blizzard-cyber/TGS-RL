package service

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
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

func cloneSnapshot(snapshot *tgsrlv1.ClusterSnapshot) *tgsrlv1.ClusterSnapshot {
	if snapshot == nil {
		return nil
	}
	return proto.Clone(snapshot).(*tgsrlv1.ClusterSnapshot)
}

func cloneIntent(intent *tgsrlv1.SchedulingIntent) *tgsrlv1.SchedulingIntent {
	if intent == nil {
		return nil
	}
	return proto.Clone(intent).(*tgsrlv1.SchedulingIntent)
}

func cloneEvaluationContext(context *tgsrlv1.EvaluationContext) *tgsrlv1.EvaluationContext {
	if context == nil {
		return nil
	}
	return proto.Clone(context).(*tgsrlv1.EvaluationContext)
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

func shouldSuppressDecision(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, plan *tgsrlv1.PlacementPlan, decision *tgsrlv1.DecisionRecord, recent []*tgsrlv1.DecisionRecord) bool {
	if plan == nil || len(plan.GetActions()) != 0 || !isNoChangeDecision(decision) || !intentSatisfiedBySnapshot(snapshot, intent) {
		return false
	}
	signature := noChangeDecisionSignature(decision)
	for index := len(recent) - 1; index >= 0; index-- {
		previous := recent[index]
		if previous == nil || previous.GetSequence() == 0 || !sameDecisionScope(previous, decision) {
			continue
		}
		if previous.GetFallback() || len(previous.GetRejectedCandidates()) != 0 || previous.GetSelectedPlan() == nil {
			continue
		}
		if len(previous.GetSelectedPlan().GetActions()) != 0 && successfulActionDecision(previous) {
			if sameContractEvaluationState(previous, decision) {
				return true
			}
			continue
		}
		if noChangeDecisionSignature(previous) == signature {
			return true
		}
	}
	return false
}

func isNoChangeDecision(decision *tgsrlv1.DecisionRecord) bool {
	return decision != nil && !decision.GetFallback() && len(decision.GetRejectedCandidates()) == 0 &&
		decision.GetSelectedPlan() != nil && len(decision.GetSelectedPlan().GetActions()) == 0
}

func sameDecisionScope(left, right *tgsrlv1.DecisionRecord) bool {
	return left != nil && right != nil && left.GetExecutionId() == right.GetExecutionId() &&
		left.GetStageId() == right.GetStageId() && left.GetIntentVersion() == right.GetIntentVersion()
}

func successfulActionDecision(decision *tgsrlv1.DecisionRecord) bool {
	if decision == nil || decision.GetFallback() || decision.GetSelectedPlan() == nil || len(decision.GetSelectedPlan().GetActions()) == 0 {
		return false
	}
	results := decision.GetActionResults()
	if len(results) == 0 {
		// Older durable audits did not necessarily persist per-action results.
		// The satisfied snapshot supplies the authoritative convergence proof.
		return true
	}
	if len(results) != len(decision.GetSelectedPlan().GetActions()) {
		return false
	}
	for _, result := range results {
		if result == nil || result.GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED {
			return false
		}
	}
	return true
}

func intentSatisfiedBySnapshot(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) bool {
	if snapshot == nil || intent == nil || intent.GetUnitCount() == 0 {
		return false
	}
	active := uint32(0)
	for _, allocation := range snapshot.GetAllocations() {
		if allocation != nil && allocation.GetState() == tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE &&
			allocation.GetExecutionId() == intent.GetExecutionId() && allocation.GetStageId() == intent.GetStageId() &&
			allocation.GetIntentVersion() == intent.GetVersion() {
			active++
		}
	}
	return active >= intent.GetUnitCount()
}

func noChangeDecisionSignature(decision *tgsrlv1.DecisionRecord) string {
	if !isNoChangeDecision(decision) {
		return ""
	}
	signature := &tgsrlv1.SemanticObject{Fields: []*tgsrlv1.SemanticField{
		{Key: "decision.plan_purpose", Value: semanticIntValue(int64(decision.GetSelectedPlan().GetPurpose()))},
		{Key: "planner.total_proposals", Value: semanticUintValue(decision.GetTotalPlannerProposalCount())},
		{Key: "planner.truncated", Value: semanticBoolValue(decision.GetPlannerEvidenceTruncated())},
	}}
	plannerSignatures := make([]string, 0, len(decision.GetPlannerEvidence()))
	for _, evidence := range decision.GetPlannerEvidence() {
		if evidence == nil {
			continue
		}
		planner := &tgsrlv1.SemanticObject{Fields: []*tgsrlv1.SemanticField{
			{Key: "reason", Value: semanticStringValue(evidence.GetReason())},
			{Key: "purpose", Value: semanticIntValue(int64(evidence.GetPurpose()))},
			{Key: "disposition", Value: semanticIntValue(int64(evidence.GetDisposition()))},
			{Key: "action_type", Value: semanticIntValue(int64(evidence.GetActionType()))},
			{Key: "target", Value: semanticStringValue(evidence.GetTargetId())},
			{Key: "utility_nanos", Value: semanticIntValue(evidence.GetUtilityNanos())},
		}}
		for _, field := range evidence.GetInputs() {
			if field != nil && !volatilePlannerInput(field.GetKey()) {
				planner.Fields = append(planner.Fields, proto.Clone(field).(*tgsrlv1.SemanticField))
			}
		}
		sortSemanticFields(planner.Fields)
		plannerSignatures = append(plannerSignatures, marshalSemanticObject(planner))
	}
	sort.Strings(plannerSignatures)
	for _, plannerSignature := range plannerSignatures {
		signature.Fields = append(signature.Fields, &tgsrlv1.SemanticField{Key: "planner.evidence", Value: semanticStringValue(plannerSignature)})
	}
	for _, contractSignature := range contractEvaluationSignatures(decision) {
		signature.Fields = append(signature.Fields, &tgsrlv1.SemanticField{Key: "contract.evaluation", Value: semanticStringValue(contractSignature)})
	}
	return marshalSemanticObject(signature)
}

func volatilePlannerInput(key string) bool {
	switch key {
	case "planner.evaluation_time_unix_nanos", "planner.snapshot_revision", "planner.observed_revision",
		"planner.decision_sequence", "planner.recent_decision_count", "planner.tick_kind", "planner.trigger_cause",
		"planner.sandbox_observed_at_unix_nanos":
		return true
	default:
		return false
	}
}

func sameContractEvaluationState(previous, current *tgsrlv1.DecisionRecord) bool {
	if len(previous.GetContractEvaluations()) == 0 {
		return true
	}
	return contractEvaluationStateSignature(previous) == contractEvaluationStateSignature(current)
}

func contractEvaluationStateSignature(decision *tgsrlv1.DecisionRecord) string {
	signature := &tgsrlv1.SemanticObject{}
	for _, evaluationSignature := range contractEvaluationSignatures(decision) {
		signature.Fields = append(signature.Fields, &tgsrlv1.SemanticField{Key: "evaluation", Value: semanticStringValue(evaluationSignature)})
	}
	return marshalSemanticObject(signature)
}

func contractEvaluationSignatures(decision *tgsrlv1.DecisionRecord) []string {
	if decision == nil {
		return nil
	}
	signatures := make([]string, 0, len(decision.GetContractEvaluations()))
	for _, evaluation := range decision.GetContractEvaluations() {
		if evaluation == nil {
			continue
		}
		contract := &tgsrlv1.SemanticObject{Fields: []*tgsrlv1.SemanticField{
			{Key: "contract_id", Value: semanticStringValue(evaluation.GetContractId())},
			{Key: "clause_kind", Value: semanticIntValue(int64(evaluation.GetClauseKind()))},
			{Key: "clause_id", Value: semanticStringValue(evaluation.GetClauseId())},
			{Key: "status", Value: semanticIntValue(int64(evaluation.GetStatus()))},
			{Key: "failure_mode", Value: semanticIntValue(int64(evaluation.GetFailureMode()))},
			{Key: "recommended_action", Value: semanticIntValue(int64(evaluation.GetRecommendedAction()))},
			{Key: "observation_disposition", Value: semanticIntValue(int64(evaluation.GetObservationDisposition()))},
			{Key: "reason", Value: semanticStringValue(evaluation.GetDetail())},
		}}
		missing := append([]string(nil), evaluation.GetMissingKeys()...)
		sort.Strings(missing)
		for _, key := range missing {
			contract.Fields = append(contract.Fields, &tgsrlv1.SemanticField{Key: "missing_key", Value: semanticStringValue(key)})
		}
		signatures = append(signatures, marshalSemanticObject(contract))
	}
	sort.Strings(signatures)
	return signatures
}

func sortSemanticFields(fields []*tgsrlv1.SemanticField) {
	sort.SliceStable(fields, func(i, j int) bool {
		if fields[i].GetKey() != fields[j].GetKey() {
			return fields[i].GetKey() < fields[j].GetKey()
		}
		left, _ := proto.MarshalOptions{Deterministic: true}.Marshal(fields[i].GetValue())
		right, _ := proto.MarshalOptions{Deterministic: true}.Marshal(fields[j].GetValue())
		return string(left) < string(right)
	})
}

func marshalSemanticObject(object *tgsrlv1.SemanticObject) string {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(object)
	if err != nil {
		return ""
	}
	return base64.RawStdEncoding.EncodeToString(encoded)
}

func semanticStringValue(value string) *tgsrlv1.SemanticValue {
	return &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_StringValue{StringValue: value}}
}

func semanticIntValue(value int64) *tgsrlv1.SemanticValue {
	return &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_Int64Value{Int64Value: value}}
}

func semanticUintValue(value uint64) *tgsrlv1.SemanticValue {
	return &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_Uint64Value{Uint64Value: value}}
}

func semanticBoolValue(value bool) *tgsrlv1.SemanticValue {
	return &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_BoolValue{BoolValue: value}}
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

func (s *Server) markProjectionLiveAttempt(kind string) {
	if s == nil {
		return
	}
	s.projectionLive.mu.Lock()
	switch kind {
	case "resource":
		s.projectionLive.resourceAttempt = true
	case "sandbox":
		s.projectionLive.sandboxAttempt = true
	default:
		s.projectionLive.resourceAttempt = true
		s.projectionLive.sandboxAttempt = true
	}
	s.projectionLive.mu.Unlock()
}

func (s *Server) markProjectionLiveReady(kind string) {
	if s == nil {
		return
	}
	s.projectionLive.mu.Lock()
	switch kind {
	case "resource":
		s.projectionLive.resourceAttempt = true
		s.projectionLive.resourceLive = true
	case "sandbox":
		s.projectionLive.sandboxAttempt = true
		s.projectionLive.sandboxLive = true
	default:
		s.projectionLive.resourceAttempt = true
		s.projectionLive.resourceLive = true
		s.projectionLive.sandboxAttempt = true
		s.projectionLive.sandboxLive = true
	}
	s.projectionLive.mu.Unlock()
}

func (s *Server) markProjectionLiveNotReady(kind string) {
	if s == nil {
		return
	}
	s.projectionLive.mu.Lock()
	switch kind {
	case "resource":
		s.projectionLive.resourceAttempt = true
		s.projectionLive.resourceLive = false
	case "sandbox":
		s.projectionLive.sandboxAttempt = true
		s.projectionLive.sandboxLive = false
	default:
		s.projectionLive.resourceAttempt = true
		s.projectionLive.resourceLive = false
		s.projectionLive.sandboxAttempt = true
		s.projectionLive.sandboxLive = false
	}
	s.projectionLive.mu.Unlock()
}

func (s *Server) projectionReady() bool {
	if s == nil {
		return false
	}
	s.projectionLive.mu.RLock()
	defer s.projectionLive.mu.RUnlock()
	return s.projectionLive.resourceLive && s.projectionLive.sandboxLive
}

func (s *Server) projectionReadinessFailure() error {
	if s == nil {
		return nil
	}
	s.projectionLive.mu.RLock()
	defer s.projectionLive.mu.RUnlock()
	switch {
	case !s.projectionLive.resourceAttempt:
		return errors.New("provider resource projection has not completed live bootstrap")
	case !s.projectionLive.resourceLive:
		return errors.New("provider resource projection is not live")
	case !s.projectionLive.sandboxAttempt:
		return errors.New("provider sandbox projection has not completed live bootstrap")
	case !s.projectionLive.sandboxLive:
		return errors.New("provider sandbox projection is not live")
	default:
		return nil
	}
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
