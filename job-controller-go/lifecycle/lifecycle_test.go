package lifecycle

import (
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func TestMapCommandAndTerminalState(t *testing.T) {
	t.Parallel()

	eventType, opType, err := MapCommand(tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE)
	if err != nil {
		t.Fatalf("MapCommand() error = %v", err)
	}
	if eventType != tgsrlv1.JobEventType_JOB_EVENT_TYPE_JOB_TERMINATED || opType != tgsrlv1.OperationType_OPERATION_TYPE_TERMINATE {
		t.Fatalf("MapCommand() = (%s, %s), want terminate pair", eventType, opType)
	}
	if !IsTerminalRunState(tgsrlv1.JobRunState_JOB_RUN_STATE_FAILED) {
		t.Fatal("IsTerminalRunState(FAILED) = false, want true")
	}
}
