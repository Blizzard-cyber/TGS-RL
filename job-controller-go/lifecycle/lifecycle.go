package lifecycle

import (
	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// InvalidTransitionError returns a stable failed-precondition error.
func InvalidTransitionError(command string, runState tgsrlv1.JobRunState) error {
	return status.Errorf(codes.FailedPrecondition, "cannot %s run in state %s", command, runState.String())
}

// IsTerminalRunState reports whether the run is terminal.
func IsTerminalRunState(runState tgsrlv1.JobRunState) bool {
	switch runState {
	case tgsrlv1.JobRunState_JOB_RUN_STATE_STOPPED,
		tgsrlv1.JobRunState_JOB_RUN_STATE_SUCCEEDED,
		tgsrlv1.JobRunState_JOB_RUN_STATE_FAILED,
		tgsrlv1.JobRunState_JOB_RUN_STATE_TERMINATED:
		return true
	default:
		return false
	}
}

// MapCommand converts a proto command to its event and operation surfaces.
func MapCommand(command tgsrlv1.JobCommandType) (tgsrlv1.JobEventType, tgsrlv1.OperationType, error) {
	switch command {
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START:
		return tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_STARTED, tgsrlv1.OperationType_OPERATION_TYPE_START, nil
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE:
		return tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_PAUSED, tgsrlv1.OperationType_OPERATION_TYPE_PAUSE, nil
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME:
		return tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_RESUMED, tgsrlv1.OperationType_OPERATION_TYPE_RESUME, nil
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP:
		return tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_STOPPED, tgsrlv1.OperationType_OPERATION_TYPE_STOP, nil
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RETRY:
		return tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_RETRIED, tgsrlv1.OperationType_OPERATION_TYPE_RETRY, nil
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE:
		return tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_TERMINATED, tgsrlv1.OperationType_OPERATION_TYPE_TERMINATE, nil
	default:
		return 0, 0, status.Error(codes.InvalidArgument, "command must be set")
	}
}
