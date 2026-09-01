package service

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// WatchDecisions streams retained and future decisions after the requested cursor.
func (s *Server) WatchDecisions(request *tgsrlv1.WatchDecisionsRequest, stream tgsrlv1.SchedulerService_WatchDecisionsServer) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	after, err := s.resolveCursor(request.AfterSequence, request.AfterDecisionId)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	filter := make(map[string]struct{}, len(request.JobIds))
	for _, jobID := range request.JobIds {
		filter[jobID] = struct{}{}
	}
	var heartbeat *time.Ticker
	if request.GetHeartbeatInterval() != nil && request.GetHeartbeatInterval().CheckValid() == nil && request.GetHeartbeatInterval().AsDuration() > 0 {
		heartbeat = time.NewTicker(request.GetHeartbeatInterval().AsDuration())
		defer heartbeat.Stop()
	}

	for {
		decisions, changed, oldest := s.decisionsAfter(after)
		if after > 0 && oldest > 0 && after+1 < oldest {
			return status.Errorf(codes.OutOfRange, "decision cursor %d predates retained sequence %d", after, oldest)
		}
		for _, entry := range decisions {
			if len(filter) > 0 {
				if _, ok := filter[entry.jobID]; !ok {
					if err := stream.Send(&tgsrlv1.WatchDecisionsResponse{
						Decision: &tgsrlv1.DecisionRecord{Sequence: entry.decision.Sequence},
					}); err != nil {
						return err
					}
					after = entry.decision.Sequence
					continue
				}
			}
			if err := stream.Send(&tgsrlv1.WatchDecisionsResponse{Decision: entry.decision}); err != nil {
				return err
			}
			after = entry.decision.Sequence
		}
		if heartbeat == nil {
			select {
			case <-stream.Context().Done():
				return stream.Context().Err()
			case <-changed:
			}
			continue
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-changed:
		case <-heartbeat.C:
			// Protocol v0.3 has no heartbeat oneof. An empty response is the
			// explicit heartbeat; clients must ignore it and retain their cursor.
			if err := stream.Send(&tgsrlv1.WatchDecisionsResponse{}); err != nil {
				return err
			}
		}
	}
}

func (s *Server) appendDecision(jobID string, decision *tgsrlv1.DecisionRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if decision == nil {
		return nil
	}
	if s.persistenceErr != nil {
		return s.persistenceErr
	}
	for _, entry := range s.decisions {
		if entry.decision.GetSequence() != 0 && entry.decision.GetDecisionId() == decision.GetDecisionId() {
			// Deterministic terminal fallbacks may be produced by more than one
			// queued tick before the key is forgotten. Treat an identical audit
			// identity as an idempotent append instead of persisting duplicate IDs.
			if sameRecoveredFailureDecision(entry.decision, decision) {
				return s.checkpointLocked()
			}
		}
	}

	nextSequence := s.sequence + 1
	committed := cloneDecision(decision)
	committed.Sequence = nextSequence
	nextDecisions := make([]decisionEntry, 0, len(s.decisions)+1)
	for _, entry := range s.decisions {
		if entry.decision.GetSequence() == 0 && (entry.decision.GetDecisionId() == committed.GetDecisionId() || decisionMatchesPlan(entry.decision, committed.GetSelectedPlan())) {
			continue
		}
		nextDecisions = append(nextDecisions, entry)
	}
	nextDecisions = append(nextDecisions, decisionEntry{jobID: jobID, decision: committed})
	nextDecisions = retainDecisions(nextDecisions, s.retention)
	if err := s.checkpointStateLocked(nextDecisions, nextSequence); err != nil {
		slog.Error("decision checkpoint failed", "error", err, "job_id", jobID, "run_id", committed.GetRunId(), "trace_id", committed.GetTraceId(), "decision_id", committed.GetDecisionId())
		return err
	}

	// Publish the decision and cursor only after the checkpoint containing both
	// has committed. Readers hold the same mutex, so they cannot observe a
	// decision that recovery would lose.
	s.sequence = nextSequence
	s.decisions = nextDecisions
	s.recorder.IncCounter("decisions_recorded", 1)
	s.recorder.SetGauge("decision_cursor", int64(s.sequence))
	if telemetry, ok := s.recorder.(observability.SchedulerTelemetry); ok {
		telemetry.RecordDecision(decisionCorrelation(jobID, committed), committed.GetFallback(), committed.GetFallbackReason())
	}
	slog.Info("decision recorded", "job_id", jobID, "run_id", committed.GetRunId(), "trace_id", committed.GetTraceId(), "decision_id", committed.GetDecisionId(), "fallback", committed.GetFallback(), "fallback_reason", committed.GetFallbackReason())
	close(s.changed)
	s.changed = make(chan struct{})
	return nil
}

func (s *Server) checkpointPendingDecision(decision *tgsrlv1.DecisionRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if decision == nil {
		return nil
	}
	if s.persistenceErr != nil {
		return s.persistenceErr
	}
	pending := cloneDecision(decision)
	pending.Sequence = 0
	nextDecisions := make([]decisionEntry, 0, len(s.decisions)+1)
	for _, entry := range s.decisions {
		if entry.decision.GetSequence() == 0 && (entry.decision.GetDecisionId() == pending.GetDecisionId() || decisionMatchesPlan(entry.decision, pending.GetSelectedPlan())) {
			continue
		}
		nextDecisions = append(nextDecisions, entry)
	}
	nextDecisions = append(nextDecisions, decisionEntry{jobID: decision.GetJobId(), decision: pending})
	if err := s.checkpointStateLocked(nextDecisions, s.sequence); err != nil {
		return err
	}
	s.decisions = nextDecisions
	return nil
}

func (s *Server) decisionForPlan(plan *tgsrlv1.PlacementPlan) (*tgsrlv1.DecisionRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.decisions {
		if entry.decision.GetSequence() != 0 && decisionMatchesPlan(entry.decision, plan) {
			return cloneDecision(entry.decision), true
		}
	}
	return nil, false
}

func (s *Server) pendingDecisionForPlan(plan *tgsrlv1.PlacementPlan) (*tgsrlv1.DecisionRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.decisions {
		if entry.decision.GetSequence() == 0 && decisionMatchesPlan(entry.decision, plan) {
			return cloneDecision(entry.decision), true
		}
	}
	return nil, false
}

func decisionMatchesPlan(decision *tgsrlv1.DecisionRecord, plan *tgsrlv1.PlacementPlan) bool {
	if decision == nil || plan == nil {
		return false
	}
	if plan.GetDecisionId() != "" && decision.GetDecisionId() == plan.GetDecisionId() {
		return true
	}
	return plan.GetPlanId() != "" && decision.GetSelectedPlan().GetPlanId() == plan.GetPlanId()
}

func retainDecisions(entries []decisionEntry, retention int) []decisionEntry {
	committed := 0
	for _, entry := range entries {
		if entry.decision.GetSequence() != 0 {
			committed++
		}
	}
	drop := committed - retention
	if drop <= 0 {
		return entries
	}
	retained := make([]decisionEntry, 0, len(entries)-drop)
	for _, entry := range entries {
		if entry.decision.GetSequence() != 0 && drop > 0 {
			drop--
			continue
		}
		retained = append(retained, entry)
	}
	return retained
}

func (s *Server) resolveCursor(sequence uint64, decisionID string) (uint64, error) {
	if decisionID == "" {
		return sequence, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.decisions {
		if entry.decision.GetSequence() == 0 {
			continue
		}
		if entry.decision.DecisionId != decisionID {
			continue
		}
		if sequence != 0 && sequence != entry.decision.Sequence {
			return 0, fmt.Errorf("decision cursor conflicts with sequence %d", sequence)
		}
		return entry.decision.Sequence, nil
	}
	return 0, fmt.Errorf("decision cursor %q was not retained", decisionID)
}

func (s *Server) decisionsAfter(sequence uint64) ([]decisionEntry, <-chan struct{}, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var oldest uint64
	for _, entry := range s.decisions {
		if entry.decision.GetSequence() != 0 {
			oldest = entry.decision.GetSequence()
			break
		}
	}
	result := make([]decisionEntry, 0, len(s.decisions))
	for _, entry := range s.decisions {
		if entry.decision.Sequence > sequence {
			result = append(result, decisionEntry{jobID: entry.jobID, decision: cloneDecision(entry.decision)})
		}
	}
	return result, s.changed, oldest
}

func (s *Server) filteredDecisions(afterSequence uint64, jobID, runID string) ([]decisionEntry, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var oldest uint64
	for _, entry := range s.decisions {
		if entry.decision.GetSequence() != 0 {
			oldest = entry.decision.GetSequence()
			break
		}
	}
	result := make([]decisionEntry, 0, len(s.decisions))
	for _, entry := range s.decisions {
		if entry.decision.Sequence <= afterSequence {
			continue
		}
		if jobID != "" && entry.jobID != jobID {
			continue
		}
		if runID != "" && entry.decision.GetRunId() != runID {
			continue
		}
		result = append(result, decisionEntry{jobID: entry.jobID, decision: cloneDecision(entry.decision)})
	}
	return result, oldest
}

func (s *Server) decisionByID(decisionID string) (*tgsrlv1.DecisionRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.decisions {
		if entry.decision.GetSequence() != 0 && entry.decision.GetDecisionId() == decisionID {
			return cloneDecision(entry.decision), true
		}
	}
	return nil, false
}

func (s *Server) recentDecisionsForIntent(intent *tgsrlv1.SchedulingIntent, limit int) []*tgsrlv1.DecisionRecord {
	if intent == nil || limit <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit > s.retention {
		limit = s.retention
	}
	result := make([]*tgsrlv1.DecisionRecord, 0, limit)
	for index := len(s.decisions) - 1; index >= 0 && len(result) < limit; index-- {
		entry := s.decisions[index]
		decision := entry.decision
		if decision == nil {
			continue
		}
		if decision.GetExecutionId() != intent.GetExecutionId() || decision.GetStageId() != intent.GetStageId() {
			continue
		}
		result = append(result, cloneDecision(decision))
	}
	for left, right := 0, len(result)-1; left < right; left, right = left+1, right-1 {
		result[left], result[right] = result[right], result[left]
	}
	return result
}

func cloneDecision(decision *tgsrlv1.DecisionRecord) *tgsrlv1.DecisionRecord {
	if decision == nil {
		return nil
	}
	return proto.Clone(decision).(*tgsrlv1.DecisionRecord)
}

func decisionCursor(decision *tgsrlv1.DecisionRecord) string {
	if decision == nil {
		return ""
	}
	return fmt.Sprintf("%d:%s", decision.GetSequence(), decision.GetDecisionId())
}

func decodeDecisionPageToken(raw string) (decisionPageToken, error) {
	if strings.TrimSpace(raw) == "" {
		return decisionPageToken{}, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return decisionPageToken{}, fmt.Errorf("invalid page_token encoding")
	}
	var token decisionPageToken
	if err := json.Unmarshal(payload, &token); err != nil {
		return decisionPageToken{}, fmt.Errorf("invalid page_token payload")
	}
	return token, nil
}

func encodeDecisionPageToken(token decisionPageToken) (string, error) {
	payload, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decisionCorrelation(jobID string, decision *tgsrlv1.DecisionRecord) observability.Correlation {
	if decision == nil {
		return observability.Correlation{JobID: jobID}
	}
	if jobID == "" {
		jobID = decision.GetJobId()
	}
	return observability.Correlation{JobID: jobID, RunID: decision.GetRunId(), TraceID: decision.GetTraceId()}
}
