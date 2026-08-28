package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/state"
	jobstatus "github.com/Blizzard-cyber/TGS-RL/job-controller-go/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const authoritativeRuntimeObservationSource = "runtime-observation"

// ReportComponentStatus stores one component observation and updates the aggregated run view.
func (c *Controller) ReportComponentStatus(_ context.Context, request *tgsrlv1.ReportComponentStatusRequest) (*tgsrlv1.ReportComponentStatusResponse, error) {
	if request == nil || request.ComponentStatus == nil {
		return nil, status.Error(codes.InvalidArgument, "component_status is required")
	}
	incoming := cloneComponentStatus(request.ComponentStatus)
	if strings.TrimSpace(incoming.GetJobId()) == "" || strings.TrimSpace(incoming.GetRunId()) == "" || strings.TrimSpace(incoming.GetComponent()) == "" {
		return nil, status.Error(codes.InvalidArgument, "component_status.job_id, run_id, and component are required")
	}
	if incoming.ObservedAt == nil {
		incoming.ObservedAt = timestamppb.New(c.clock.Now().UTC())
	}
	var response *tgsrlv1.ReportComponentStatusResponse
	err := c.repository.Update(func(store state.Store) error {
		job, ok := store.GetJob(incoming.GetJobId())
		if !ok {
			return status.Errorf(codes.NotFound, "job %q not found", incoming.GetJobId())
		}
		run, ok := store.GetRun(incoming.GetJobId(), incoming.GetRunId())
		if !ok {
			return status.Errorf(codes.NotFound, "run %q not found", incoming.GetRunId())
		}
		aggregated := mergeAggregatedStatus(run.GetComponentStatus(), incoming)
		run.ComponentStatus = upsertAggregatedStatus(run.GetComponentStatus(), aggregated)
		completedOperation, completedEventType, completedDetail := c.advanceRunFromComponent(job, run, incoming, aggregated, incoming.GetObservedAt().AsTime())
		store.PutJob(job)
		store.PutRun(run)
		store.AppendEvent(c.newEvent(tgsrlv1.JobEventType_JOB_EVENT_TYPE_COMPONENT_CHANGED, job, run, nil, incoming, "component status updated", incoming.GetObservedAt().AsTime()))
		if completedOperation != nil {
			store.PutOperation(completedOperation)
			store.AppendEvent(c.newEvent(completedEventType, job, run, completedOperation, aggregated, completedDetail, incoming.GetObservedAt().AsTime()))
			store.AppendEvent(c.newEvent(tgsrlv1.JobEventType_JOB_EVENT_TYPE_OPERATION_RECORDED, job, run, completedOperation, aggregated, "operation completed from component status", incoming.GetObservedAt().AsTime()))
		}
		response = &tgsrlv1.ReportComponentStatusResponse{ComponentStatus: aggregated}
		return nil
	})
	return response, err
}

func (c *Controller) advanceRunFromComponent(job *tgsrlv1.RLTrainingJob, run *tgsrlv1.JobRun, incoming, aggregated *tgsrlv1.ComponentStatus, now time.Time) (*tgsrlv1.Operation, tgsrlv1.JobEventType, string) {
	if aggregated == nil {
		return nil, tgsrlv1.JobEventType_JOB_EVENT_TYPE_UNKNOWN, ""
	}
	observedState, hasObservedState, converged := authoritativeRuntimeSignal(aggregated)
	var (
		completedOperation *tgsrlv1.Operation
		completedEventType tgsrlv1.JobEventType
		completedDetail    string
	)
	if hasObservedState && observedState == tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED {
		run.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_FAILED
		run.State = tgsrlv1.JobState_JOB_STATE_FAILED
		run.CompletedAt = timestamppb.New(now)
		job.State = tgsrlv1.JobState_JOB_STATE_FAILED
		completedOperation = failLatestRunningLifecycleOperation(run, now, "RUNTIME_STATE_FAILED", runtimeFailureDetail(aggregated))
		completedEventType = tgsrlv1.JobEventType_JOB_EVENT_TYPE_OPERATION_RECORDED
		completedDetail = "operation failed from component status"
		return completedOperation, completedEventType, completedDetail
	}
	if !hasObservedState || !converged {
		return nil, tgsrlv1.JobEventType_JOB_EVENT_TYPE_UNKNOWN, ""
	}
	switch run.GetRunState() {
	case tgsrlv1.JobRunState_JOB_RUN_STATE_STARTING:
		if observedState == tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
			run.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING
			run.State = tgsrlv1.JobState_JOB_STATE_RUNNING
			if run.StartedAt == nil {
				run.StartedAt = timestamppb.New(now)
			}
			job.State = tgsrlv1.JobState_JOB_STATE_RUNNING
			completedOperation = completeLatestOperation(run, tgsrlv1.OperationType_OPERATION_TYPE_START, now)
			completedEventType = tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_STARTED
			completedDetail = "job started"
		}
	case tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSING:
		if observedState == tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED {
			run.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSED
			run.State = tgsrlv1.JobState_JOB_STATE_PAUSED
			job.State = tgsrlv1.JobState_JOB_STATE_PAUSED
			completedOperation = completeLatestOperation(run, tgsrlv1.OperationType_OPERATION_TYPE_PAUSE, now)
			completedEventType = tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_PAUSED
			completedDetail = "job paused"
		}
	case tgsrlv1.JobRunState_JOB_RUN_STATE_RESUMING:
		if observedState == tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
			run.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING
			run.State = tgsrlv1.JobState_JOB_STATE_RUNNING
			if run.StartedAt == nil {
				run.StartedAt = timestamppb.New(now)
			}
			job.State = tgsrlv1.JobState_JOB_STATE_RUNNING
			completedOperation = completeLatestOperation(run, tgsrlv1.OperationType_OPERATION_TYPE_RESUME, now)
			completedEventType = tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_RESUMED
			completedDetail = "job resumed"
		}
	case tgsrlv1.JobRunState_JOB_RUN_STATE_STOPPING:
		if observedState == tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED {
			run.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_STOPPED
			run.State = tgsrlv1.JobState_JOB_STATE_CANCELLED
			run.CompletedAt = timestamppb.New(now)
			job.State = tgsrlv1.JobState_JOB_STATE_CANCELLED
			completedOperation = completeLatestOperation(run, tgsrlv1.OperationType_OPERATION_TYPE_STOP, now)
			completedEventType = tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_STOPPED
			completedDetail = "job stopped"
		}
	case tgsrlv1.JobRunState_JOB_RUN_STATE_TERMINATING:
		if observedState == tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED {
			run.RunState = tgsrlv1.JobRunState_JOB_RUN_STATE_TERMINATED
			run.State = tgsrlv1.JobState_JOB_STATE_CANCELLED
			run.CompletedAt = timestamppb.New(now)
			job.State = tgsrlv1.JobState_JOB_STATE_CANCELLED
			completedOperation = completeLatestOperation(run, tgsrlv1.OperationType_OPERATION_TYPE_TERMINATE, now)
			completedEventType = tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_TERMINATED
			completedDetail = "job terminated"
		}
	}
	return completedOperation, completedEventType, completedDetail
}

func (c *Controller) failOperation(job *tgsrlv1.RLTrainingJob, run *tgsrlv1.JobRun, operation *tgsrlv1.Operation, code, message string, now time.Time) {
	operation.State = tgsrlv1.OperationState_OPERATION_STATE_FAILED
	operation.ErrorCode = code
	operation.ErrorMessage = message
	operation.CompletedAt = timestamppb.New(now)
	run.ComponentStatus = upsertAggregatedStatus(run.GetComponentStatus(), &tgsrlv1.ComponentStatus{
		Component:  "runtime",
		Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED,
		Detail:     message,
		Source:     "controller",
		Revision:   0,
		ObservedAt: timestamppb.New(now),
		JobId:      job.GetJobId(),
		RunId:      run.GetRunId(),
		TraceId:    run.GetTraceId(),
		DataKind:   run.GetDataKind(),
	})
	run.Operations = appendOperation(run.GetOperations(), operation)
}

func observedRuntimeState(statusValue *tgsrlv1.ComponentStatus) (tgsrlv1.RuntimeState, bool) {
	if statusValue == nil {
		return tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN, false
	}
	if statusValue.GetObservedRuntimeState() != tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN {
		return statusValue.GetObservedRuntimeState(), true
	}
	if raw := strings.TrimSpace(statusValue.GetAnnotations()["runtime.state"]); raw != "" {
		if value, ok := tgsrlv1.RuntimeState_value[raw]; ok {
			return tgsrlv1.RuntimeState(value), true
		}
	}
	if raw := strings.TrimSpace(statusValue.GetDetail()); raw != "" {
		candidate := strings.ToUpper(strings.TrimSpace(raw))
		if !strings.HasPrefix(candidate, "RUNTIME_STATE_") {
			candidate = "RUNTIME_STATE_" + candidate
		}
		if value, ok := tgsrlv1.RuntimeState_value[candidate]; ok {
			return tgsrlv1.RuntimeState(value), true
		}
	}
	return tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN, false
}

func authoritativeRuntimeSignal(aggregated *tgsrlv1.ComponentStatus) (tgsrlv1.RuntimeState, bool, bool) {
	if aggregated != nil && strings.TrimSpace(aggregated.GetAnnotations()["runtime.source"]) != authoritativeRuntimeObservationSource {
		return tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN, false, false
	}
	if state, ok := observedRuntimeState(aggregated); ok {
		return state, true, aggregated.GetConverged()
	}
	return tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN, false, false
}

func runtimeFailureDetail(aggregated *tgsrlv1.ComponentStatus) string {
	if aggregated != nil && strings.TrimSpace(aggregated.GetDetail()) != "" {
		return aggregated.GetDetail()
	}
	return "runtime reported failed"
}

func completeLatestOperation(run *tgsrlv1.JobRun, opType tgsrlv1.OperationType, now time.Time) *tgsrlv1.Operation {
	var latest *tgsrlv1.Operation
	for _, operation := range run.GetOperations() {
		if operation.GetType() != opType || operation.GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
			continue
		}
		if latest == nil || operation.GetCreatedAt().AsTime().After(latest.GetCreatedAt().AsTime()) {
			latest = cloneOperation(operation)
		}
	}
	if latest == nil {
		return nil
	}
	latest.State = tgsrlv1.OperationState_OPERATION_STATE_SUCCEEDED
	latest.CompletedAt = timestamppb.New(now)
	run.Operations = appendOperation(run.GetOperations(), latest)
	return latest
}

func failLatestRunningLifecycleOperation(run *tgsrlv1.JobRun, now time.Time, code, message string) *tgsrlv1.Operation {
	var latest *tgsrlv1.Operation
	for _, operation := range run.GetOperations() {
		if operation.GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
			continue
		}
		switch operation.GetType() {
		case tgsrlv1.OperationType_OPERATION_TYPE_START, tgsrlv1.OperationType_OPERATION_TYPE_PAUSE, tgsrlv1.OperationType_OPERATION_TYPE_RESUME, tgsrlv1.OperationType_OPERATION_TYPE_STOP, tgsrlv1.OperationType_OPERATION_TYPE_TERMINATE:
		default:
			continue
		}
		if latest == nil || operation.GetCreatedAt().AsTime().After(latest.GetCreatedAt().AsTime()) {
			latest = cloneOperation(operation)
		}
	}
	if latest == nil {
		return nil
	}
	latest.State = tgsrlv1.OperationState_OPERATION_STATE_FAILED
	latest.ErrorCode = code
	latest.ErrorMessage = message
	latest.CompletedAt = timestamppb.New(now)
	run.Operations = appendOperation(run.GetOperations(), latest)
	return latest
}

func mergeAggregatedStatus(existing []*tgsrlv1.ComponentStatus, incoming *tgsrlv1.ComponentStatus) *tgsrlv1.ComponentStatus {
	return jobstatus.MergeAggregatedStatus(existing, incoming)
}

func upsertAggregatedStatus(existing []*tgsrlv1.ComponentStatus, aggregated *tgsrlv1.ComponentStatus) []*tgsrlv1.ComponentStatus {
	return jobstatus.UpsertAggregatedStatus(existing, aggregated)
}

func parseObservationAnnotations(statusValue *tgsrlv1.ComponentStatus) map[string]*tgsrlv1.ComponentStatus {
	observations := map[string]*tgsrlv1.ComponentStatus{}
	for key, value := range statusValue.GetAnnotations() {
		if !strings.HasPrefix(key, "observation.") {
			continue
		}
		parts := strings.Split(key, ".")
		if len(parts) != 3 {
			continue
		}
		source := parts[1]
		field := parts[2]
		if observations[source] == nil {
			observations[source] = &tgsrlv1.ComponentStatus{
				Component:       statusValue.GetComponent(),
				Source:          source,
				JobId:           statusValue.GetJobId(),
				RunId:           statusValue.GetRunId(),
				TraceId:         statusValue.GetTraceId(),
				DataKind:        statusValue.GetDataKind(),
				SemanticContext: statusValue.GetSemanticContext(),
			}
		}
		switch field {
		case "health":
			if enumValue, ok := tgsrlv1.ComponentHealth_value[value]; ok {
				observations[source].Health = tgsrlv1.ComponentHealth(enumValue)
			}
		case "detail":
			observations[source].Detail = value
		case "revision":
			var revision uint64
			fmt.Sscanf(value, "%d", &revision)
			observations[source].Revision = revision
		case "observed_at":
			if timestamp, err := time.Parse(time.RFC3339Nano, value); err == nil {
				observations[source].ObservedAt = timestamppb.New(timestamp)
			}
		}
	}
	return observations
}
