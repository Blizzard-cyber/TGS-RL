package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/compiler"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/lifecycle"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *Controller) normalizeJob(job *tgsrlv1.RLTrainingJob, now time.Time) (*tgsrlv1.RLTrainingJob, []string) {
	return compiler.NormalizeJob(job, now)
}

func (c *Controller) newRun(job *tgsrlv1.RLTrainingJob, attempt uint64, runState tgsrlv1.JobRunState, now time.Time) *tgsrlv1.JobRun {
	return compiler.NewRun(job, attempt, runState, now)
}

func (c *Controller) newOperation(opType tgsrlv1.OperationType, jobID, runID, actor, reason, requestID, idempotencyKey string, kind tgsrlv1.DataKind, semantic *tgsrlv1.SemanticEnvelope, now time.Time) *tgsrlv1.Operation {
	identityParts := []string{opType.String(), jobID, runID, requestID, idempotencyKey, actor, reason}
	if strings.TrimSpace(idempotencyKey) == "" {
		identityParts = append(identityParts, now.UTC().Format(time.RFC3339Nano), fmt.Sprint(c.operationSequence.Add(1)))
	}
	return &tgsrlv1.Operation{
		OperationId:     deterministicID("op", identityParts...),
		Type:            opType,
		State:           tgsrlv1.OperationState_OPERATION_STATE_SUCCEEDED,
		Actor:           strings.TrimSpace(actor),
		Reason:          strings.TrimSpace(reason),
		CreatedAt:       timestamppb.New(now),
		CompletedAt:     timestamppb.New(now),
		JobId:           jobID,
		RunId:           runID,
		TraceId:         deterministicID("trace", jobID, runID, opType.String()),
		DataKind:        kind,
		SemanticContext: semantic,
		RequestId:       requestID,
		IdempotencyKey:  idempotencyKey,
		Cursor:          deterministicID("cursor", jobID, runID, opType.String(), requestID, idempotencyKey),
	}
}

func (c *Controller) newEvent(eventType tgsrlv1.JobEventType, job *tgsrlv1.RLTrainingJob, run *tgsrlv1.JobRun, operation *tgsrlv1.Operation, component *tgsrlv1.ComponentStatus, detail string, now time.Time) *tgsrlv1.JobEvent {
	jobID := ""
	runID := ""
	traceID := ""
	stateValue := tgsrlv1.JobState_JOB_STATE_UNKNOWN
	runState := tgsrlv1.JobRunState_JOB_RUN_STATE_UNKNOWN
	dataKind := tgsrlv1.DataKind_DATA_KIND_UNKNOWN
	semantic := (*tgsrlv1.SemanticEnvelope)(nil)
	if job != nil {
		jobID = job.GetJobId()
		stateValue = job.GetState()
		dataKind = job.GetDataKind()
	}
	if run != nil {
		runID = run.GetRunId()
		traceID = run.GetTraceId()
		runState = run.GetRunState()
		if dataKind == tgsrlv1.DataKind_DATA_KIND_UNKNOWN {
			dataKind = run.GetDataKind()
		}
	}
	if operation != nil && traceID == "" {
		traceID = operation.GetTraceId()
	}
	if component != nil && traceID == "" {
		traceID = component.GetTraceId()
		semantic = component.GetSemanticContext()
	}
	if operation != nil && semantic == nil {
		semantic = operation.GetSemanticContext()
	}
	return &tgsrlv1.JobEvent{
		EventId:         deterministicID("event", eventType.String(), jobID, runID, traceID, detail, fmt.Sprintf("%d", now.UnixNano())),
		EventType:       eventType,
		JobId:           jobID,
		RunId:           runID,
		TraceId:         traceID,
		OccurredAt:      timestamppb.New(now),
		State:           stateValue,
		Operation:       cloneOperation(operation),
		ComponentStatus: cloneComponentStatus(component),
		Detail:          detail,
		DataKind:        dataKind,
		SemanticContext: semantic,
		RunState:        runState,
	}
}

func mapCommand(command tgsrlv1.JobCommandType) (tgsrlv1.JobCommandType, tgsrlv1.JobEventType, tgsrlv1.OperationType, error) {
	eventType, operationType, err := lifecycle.MapCommand(command)
	return command, eventType, operationType, err
}

func invalidTransition(command string, runState tgsrlv1.JobRunState) error {
	return lifecycle.InvalidTransitionError(command, runState)
}

func stateFromRun(runState tgsrlv1.JobRunState) tgsrlv1.JobState {
	switch runState {
	case tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING:
		return tgsrlv1.JobState_JOB_STATE_RUNNING
	case tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSED:
		return tgsrlv1.JobState_JOB_STATE_PAUSED
	case tgsrlv1.JobRunState_JOB_RUN_STATE_SUCCEEDED:
		return tgsrlv1.JobState_JOB_STATE_SUCCEEDED
	case tgsrlv1.JobRunState_JOB_RUN_STATE_FAILED:
		return tgsrlv1.JobState_JOB_STATE_FAILED
	case tgsrlv1.JobRunState_JOB_RUN_STATE_STOPPED, tgsrlv1.JobRunState_JOB_RUN_STATE_TERMINATED:
		return tgsrlv1.JobState_JOB_STATE_CANCELLED
	default:
		return tgsrlv1.JobState_JOB_STATE_PENDING
	}
}

func isTerminalRunState(runState tgsrlv1.JobRunState) bool {
	return lifecycle.IsTerminalRunState(runState)
}

func hashRequest(namespace string, message proto.Message) string {
	return hashStrings(namespace, deterministicProtoHash(message))
}

func hashNormalizedJobRequest(namespace string, normalized, submitted *tgsrlv1.RLTrainingJob) string {
	fingerprint := cloneJob(normalized)
	if fingerprint == nil {
		return hashRequest(namespace, fingerprint)
	}
	// State is controller-owned. A missing/invalid created_at and job_id are
	// also normalized by the controller, so their generated values must not make
	// a delayed retry look like different caller content. Explicit caller values
	// remain part of the fingerprint.
	fingerprint.State = tgsrlv1.JobState_JOB_STATE_UNKNOWN
	if submitted != nil && !submitted.GetCreatedAt().IsValid() {
		fingerprint.CreatedAt = nil
	}
	if submitted != nil && strings.TrimSpace(submitted.GetJobId()) == "" {
		fingerprint.JobId = ""
	}
	return hashRequest(namespace, fingerprint)
}

func hashAdmitRequest(jobID, reason string) string {
	return hashStrings("admit", jobID, reason)
}

func hashCommandRequest(jobID, runID string, command tgsrlv1.JobCommandType, actor, reason string) string {
	return hashStrings("command", jobID, runID, command.String(), actor, reason)
}

func deterministicProtoHash(message proto.Message) string {
	if message == nil {
		return ""
	}
	payload, _ := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func deterministicProtoID(prefix string, message proto.Message) string {
	return prefix + "-" + deterministicProtoHash(message)[:16]
}

func deterministicID(prefix string, parts ...string) string {
	return prefix + "-" + hashStrings(parts...)[:16]
}

func hashStrings(parts ...string) string {
	hasher := sha256.New()
	for _, part := range parts {
		_, _ = hasher.Write([]byte(part))
		_, _ = hasher.Write([]byte{0})
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func mergeMaps(base, overlay map[string]string) map[string]string {
	merged := map[string]string{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overlay {
		merged[key] = value
	}
	return merged
}

func cloneJob(job *tgsrlv1.RLTrainingJob) *tgsrlv1.RLTrainingJob {
	if job == nil {
		return nil
	}
	return proto.Clone(job).(*tgsrlv1.RLTrainingJob)
}

func cloneRun(run *tgsrlv1.JobRun) *tgsrlv1.JobRun {
	if run == nil {
		return nil
	}
	return proto.Clone(run).(*tgsrlv1.JobRun)
}

func cloneOperation(operation *tgsrlv1.Operation) *tgsrlv1.Operation {
	if operation == nil {
		return nil
	}
	return proto.Clone(operation).(*tgsrlv1.Operation)
}

func appendOperation(existing []*tgsrlv1.Operation, operation *tgsrlv1.Operation) []*tgsrlv1.Operation {
	if operation == nil {
		return existing
	}
	output := make([]*tgsrlv1.Operation, 0, len(existing)+1)
	replaced := false
	for _, current := range existing {
		if current.GetOperationId() == operation.GetOperationId() {
			output = append(output, cloneOperation(operation))
			replaced = true
			continue
		}
		output = append(output, cloneOperation(current))
	}
	if !replaced {
		output = append(output, cloneOperation(operation))
	}
	return output
}

func cloneRuntime(runtime *tgsrlv1.FrameworkRuntimeSpec) *tgsrlv1.FrameworkRuntimeSpec {
	if runtime == nil {
		return nil
	}
	return proto.Clone(runtime).(*tgsrlv1.FrameworkRuntimeSpec)
}

func cloneExecutionContract(contract *tgsrlv1.ExecutionContract) *tgsrlv1.ExecutionContract {
	if contract == nil {
		return nil
	}
	return proto.Clone(contract).(*tgsrlv1.ExecutionContract)
}

func cloneComponentStatus(component *tgsrlv1.ComponentStatus) *tgsrlv1.ComponentStatus {
	if component == nil {
		return nil
	}
	return proto.Clone(component).(*tgsrlv1.ComponentStatus)
}
