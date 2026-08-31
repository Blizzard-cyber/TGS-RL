package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/state"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const reconciliationRequiredCode = "RECONCILIATION_REQUIRED"

// ReconcileIncompleteOperations resolves persisted operations that were left
// running across a controller restart. It first trusts converged Runtime
// observation, then retries with the original idempotency key when safe.
func (c *Controller) ReconcileIncompleteOperations(ctx context.Context) error {
	operations, err := c.listRunningOperations()
	if err != nil {
		return err
	}
	errorsSeen := make([]error, 0)
	for _, operation := range operations {
		if err := c.reconcileOperation(ctx, operation); err != nil {
			errorsSeen = append(errorsSeen, fmt.Errorf("reconcile operation %s: %w", operation.GetOperationId(), err))
		}
	}
	return errors.Join(errorsSeen...)
}

func (c *Controller) listRunningOperations() ([]*tgsrlv1.Operation, error) {
	var operations []*tgsrlv1.Operation
	err := c.repository.View(func(query state.Query) error {
		pageToken := ""
		for {
			page, next, err := query.ListOperations("", "", tgsrlv1.OperationType_OPERATION_TYPE_UNKNOWN, tgsrlv1.OperationState_OPERATION_STATE_RUNNING, 100, pageToken)
			if err != nil {
				return err
			}
			operations = append(operations, page...)
			if next == "" {
				break
			}
			pageToken = next
		}
		return nil
	})
	sort.SliceStable(operations, func(i, j int) bool {
		left := operations[i].GetCreatedAt().AsTime()
		right := operations[j].GetCreatedAt().AsTime()
		if !left.Equal(right) {
			return left.Before(right)
		}
		return operations[i].GetOperationId() < operations[j].GetOperationId()
	})
	return operations, err
}

func (c *Controller) reconcileOperation(ctx context.Context, operation *tgsrlv1.Operation) error {
	if operation.GetRunId() == "" || operation.GetJobId() == "" {
		return c.markReconciliationRequired(operation, "operation is missing job or run identity")
	}
	if operation.GetType() == tgsrlv1.OperationType_OPERATION_TYPE_ADMIT || operation.GetType() == tgsrlv1.OperationType_OPERATION_TYPE_RETRY {
		if ok, err := c.hasRecoveryIdentity(operation); err != nil {
			return err
		} else if !ok {
			return c.markReconciliationRequired(operation, "operation outcome is unknown after restart and no matching idempotency record is available")
		}
		if operation.GetType() == tgsrlv1.OperationType_OPERATION_TYPE_RETRY && commandDispatchAccepted(operation) {
			return c.markReconciliationRequired(operation, "retry prepare was accepted before restart but did not reach a durable terminal state")
		}
		return c.replayOperation(ctx, operation)
	}
	statusResponse, statusErr := c.runtime.GetRuntimeStatus(ctx, &tgsrlv1.GetRuntimeStatusRequest{RunId: operation.GetRunId()})
	statusKnown := statusErr == nil && statusResponse != nil && statusResponse.RuntimeStatus != nil && runtimeObservationFollowsOperation(operation, statusResponse.GetRuntimeStatus())
	if statusKnown {
		summary := statusResponse.GetRuntimeStatus()
		_, reportErr := c.ReportComponentStatus(ctx, &tgsrlv1.ReportComponentStatusRequest{
			ComponentStatus: &tgsrlv1.ComponentStatus{
				Component:            "runtime",
				Health:               componentHealthFromRuntime(summary.GetHealth()),
				Detail:               summary.GetDetail(),
				Source:               authoritativeRuntimeObservationSource,
				Revision:             summary.GetRevision(),
				ObservedAt:           summary.GetObservedAt(),
				Annotations:          cloneStringMap(summary.GetAnnotations()),
				JobId:                operation.GetJobId(),
				RunId:                operation.GetRunId(),
				TraceId:              operation.GetTraceId(),
				DataKind:             operation.GetDataKind(),
				SemanticContext:      operation.GetSemanticContext(),
				ObservedRuntimeState: summary.GetObservedRuntimeState(),
				Converged:            summary.GetConverged(),
			},
		})
		if reportErr != nil {
			return reportErr
		}
		if done, err := c.operationCompleted(operation.GetOperationId()); err != nil || done {
			return err
		}
	}

	if ok, err := c.hasRecoveryIdentity(operation); err != nil {
		return err
	} else if !ok {
		detail := "operation outcome is unknown after restart and no matching idempotency record is available"
		if statusErr != nil {
			detail += ": " + statusErr.Error()
		}
		return c.markReconciliationRequired(operation, detail)
	}
	if commandDispatchAccepted(operation) {
		detail := "runtime accepted the operation before restart but current state does not confirm its outcome"
		if statusErr != nil {
			detail += ": " + statusErr.Error()
		}
		return c.markReconciliationRequired(operation, detail)
	}
	return c.replayOperation(ctx, operation)
}

func (c *Controller) hasRecoveryIdentity(operation *tgsrlv1.Operation) (bool, error) {
	if operation == nil || strings.TrimSpace(operation.GetIdempotencyKey()) == "" {
		return false, nil
	}
	scope := "command"
	requestHash := ""
	if operation.GetType() == tgsrlv1.OperationType_OPERATION_TYPE_ADMIT {
		scope = "admit"
		requestHash = hashAdmitRequest(operation.GetJobId(), operation.GetReason())
	} else if command, ok := commandForOperation(operation.GetType()); ok {
		requestHash = hashCommandRequest(operation.GetJobId(), operation.GetRunId(), command, operation.GetActor(), operation.GetReason())
	} else {
		return false, nil
	}
	matched := false
	err := c.repository.View(func(query state.Query) error {
		record, ok := query.GetIdempotency(scope, operation.GetIdempotencyKey())
		matched = ok && record.OperationID == operation.GetOperationId() && record.JobID == operation.GetJobId() && record.RunID == operation.GetRunId() && record.RequestHash == requestHash
		return nil
	})
	return matched, err
}

func runtimeObservationFollowsOperation(operation *tgsrlv1.Operation, summary *tgsrlv1.RuntimeStatusSummary) bool {
	if operation == nil || summary == nil || !summary.GetObservedAt().IsValid() {
		return false
	}
	if !operation.GetCreatedAt().IsValid() {
		return true
	}
	return !summary.GetObservedAt().AsTime().Before(operation.GetCreatedAt().AsTime())
}

func (c *Controller) operationCompleted(operationID string) (bool, error) {
	completed := false
	err := c.repository.View(func(query state.Query) error {
		operation, ok := query.GetOperation(operationID)
		completed = ok && operation.GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING
		return nil
	})
	return completed, err
}

func (c *Controller) replayOperation(ctx context.Context, operation *tgsrlv1.Operation) error {
	switch operation.GetType() {
	case tgsrlv1.OperationType_OPERATION_TYPE_ADMIT:
		request := &tgsrlv1.AdmitJobRequest{
			JobId:          operation.GetJobId(),
			Reason:         operation.GetReason(),
			RequestId:      operation.GetRequestId(),
			IdempotencyKey: operation.GetIdempotencyKey(),
		}
		response, err := c.resumeAdmitPhase(ctx, request, hashAdmitRequest(request.GetJobId(), request.GetReason()), operation.GetOperationId(), operation.GetRunId(), operation.GetJobId())
		if err == nil && response.GetOperation().GetState() == tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
			return c.markReconciliationRequired(operation, "admission retry outcome remains unknown after restart")
		}
		return err
	case tgsrlv1.OperationType_OPERATION_TYPE_START, tgsrlv1.OperationType_OPERATION_TYPE_PAUSE,
		tgsrlv1.OperationType_OPERATION_TYPE_RESUME, tgsrlv1.OperationType_OPERATION_TYPE_STOP,
		tgsrlv1.OperationType_OPERATION_TYPE_RETRY, tgsrlv1.OperationType_OPERATION_TYPE_TERMINATE:
		command, ok := commandForOperation(operation.GetType())
		if !ok {
			break
		}
		request := &tgsrlv1.ApplyJobCommandRequest{
			JobId:          operation.GetJobId(),
			RunId:          operation.GetRunId(),
			Command:        command,
			Actor:          operation.GetActor(),
			Reason:         operation.GetReason(),
			RequestId:      operation.GetRequestId(),
			IdempotencyKey: operation.GetIdempotencyKey(),
		}
		requestHash := hashCommandRequest(request.GetJobId(), request.GetRunId(), command, request.GetActor(), request.GetReason())
		response, err := c.resumeCommandPhase(ctx, request, requestHash, operation.GetOperationId(), operation.GetRunId(), operation.GetJobId())
		if err == nil && response.GetOperation().GetState() == tgsrlv1.OperationState_OPERATION_STATE_RUNNING && !commandDispatchAccepted(response.GetOperation()) {
			return c.markReconciliationRequired(operation, "runtime retry outcome remains unknown after restart")
		}
		return err
	}
	return c.markReconciliationRequired(operation, "operation type cannot be safely replayed")
}

func (c *Controller) markReconciliationRequired(operation *tgsrlv1.Operation, detail string) error {
	now := c.clock.Now().UTC()
	return c.repository.Update(func(store state.Store) error {
		if operation.GetJobId() == "" || operation.GetRunId() == "" {
			current, ok := store.GetOperation(operation.GetOperationId())
			if !ok || current.GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
				return nil
			}
			current.State = tgsrlv1.OperationState_OPERATION_STATE_FAILED
			current.ErrorCode = reconciliationRequiredCode
			current.ErrorMessage = detail
			current.CompletedAt = timestamppb.New(now)
			if current.Annotations == nil {
				current.Annotations = map[string]string{}
			}
			current.Annotations["reconciliation.manual_required"] = "true"
			store.PutOperation(current)
			return nil
		}
		job, run, current, err := c.loadCommittedOperation(store, operation.GetJobId(), operation.GetRunId(), operation.GetOperationId())
		if err != nil {
			return err
		}
		if current.GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
			return nil
		}
		current.State = tgsrlv1.OperationState_OPERATION_STATE_FAILED
		current.ErrorCode = reconciliationRequiredCode
		current.ErrorMessage = detail
		current.CompletedAt = timestamppb.New(now)
		if current.Annotations == nil {
			current.Annotations = map[string]string{}
		}
		current.Annotations["reconciliation.manual_required"] = "true"
		run.ComponentStatus = upsertAggregatedStatus(run.GetComponentStatus(), &tgsrlv1.ComponentStatus{
			Component:  "reconciliation",
			Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_DEGRADED,
			Detail:     detail,
			Source:     "job-controller-recovery",
			ObservedAt: timestamppb.New(now),
			JobId:      job.GetJobId(),
			RunId:      run.GetRunId(),
			TraceId:    run.GetTraceId(),
			DataKind:   run.GetDataKind(),
		})
		run.Operations = appendOperation(run.GetOperations(), current)
		store.PutRun(run)
		store.PutOperation(current)
		store.AppendEvent(c.newEvent(tgsrlv1.JobEventType_JOB_EVENT_TYPE_OPERATION_RECORDED, job, run, current, nil, detail, now))
		return nil
	})
}

func commandForOperation(operationType tgsrlv1.OperationType) (tgsrlv1.JobCommandType, bool) {
	commands := map[tgsrlv1.OperationType]tgsrlv1.JobCommandType{
		tgsrlv1.OperationType_OPERATION_TYPE_START:     tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		tgsrlv1.OperationType_OPERATION_TYPE_PAUSE:     tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE,
		tgsrlv1.OperationType_OPERATION_TYPE_RESUME:    tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME,
		tgsrlv1.OperationType_OPERATION_TYPE_STOP:      tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP,
		tgsrlv1.OperationType_OPERATION_TYPE_RETRY:     tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RETRY,
		tgsrlv1.OperationType_OPERATION_TYPE_TERMINATE: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE,
	}
	command, ok := commands[operationType]
	return command, ok
}

func componentHealthFromRuntime(health tgsrlv1.RuntimeHealth) tgsrlv1.ComponentHealth {
	switch health {
	case tgsrlv1.RuntimeHealth_RUNTIME_HEALTH_HEALTHY:
		return tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY
	case tgsrlv1.RuntimeHealth_RUNTIME_HEALTH_DEGRADED:
		return tgsrlv1.ComponentHealth_COMPONENT_HEALTH_DEGRADED
	case tgsrlv1.RuntimeHealth_RUNTIME_HEALTH_FAILED:
		return tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED
	case tgsrlv1.RuntimeHealth_RUNTIME_HEALTH_PROGRESSING:
		return tgsrlv1.ComponentHealth_COMPONENT_HEALTH_PROGRESSING
	default:
		return tgsrlv1.ComponentHealth_COMPONENT_HEALTH_UNKNOWN
	}
}

func cloneStringMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
