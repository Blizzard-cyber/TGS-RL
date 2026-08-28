package worker

import (
	"context"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/admission"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
	opcontroller "github.com/Blizzard-cyber/TGS-RL/operator-go/controller"
)

type ControllerReconciler struct {
	inner *opcontroller.Reconciler
}

func NewControllerReconciler(inner *opcontroller.Reconciler) *ControllerReconciler {
	if inner == nil {
		return nil
	}
	return &ControllerReconciler{inner: inner}
}

func (r *ControllerReconciler) Reconcile(ctx context.Context, input compiler.CompileInput, policy admission.QueuePolicy) (*ReconcileResult, error) {
	result, err := r.inner.Reconcile(ctx, input, policy)
	if err != nil {
		return nil, err
	}
	return &ReconcileResult{
		Bundle:     result.Bundle,
		Applied:    result.Applied,
		Idempotent: result.Idempotent,
	}, nil
}
