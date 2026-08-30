package planexecutor

import (
	"context"
	"errors"

	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
)

var (
	ErrCheckpointRequired    = errors.New("planexecutor: checkpoint callback is required")
	ErrInvalidTransition     = errors.New("planexecutor: invalid transaction transition")
	ErrReconcileRequired     = errors.New("planexecutor: reconcile required")
	ErrTransactionalProvider = errors.New("planexecutor: transactional provider is required")
)

type CheckpointFunc func(context.Context, *state.TransactionRecord) error
