package controller

import (
	"context"
	"fmt"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/admission"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

type ReconcileResult struct {
	Bundle        *api.Bundle
	Created       bool
	Updated       bool
	Idempotent    bool
	Admission     admission.Decision
	Applied       bool
	Queued        bool
	PreemptedKeys []string
}

type Reconciler struct {
	compiler  *compiler.Compiler
	admission *admission.Evaluator
	backend   backend.Backend
}

func New(b backend.Backend) *Reconciler {
	return NewWithCompiler(b, compiler.New())
}

func NewWithRuntimeConfig(b backend.Backend, config compiler.RuntimeConfig) (*Reconciler, error) {
	c, err := compiler.NewWithRuntimeConfig(config)
	if err != nil {
		return nil, err
	}
	return NewWithCompiler(b, c), nil
}

func NewWithCompiler(b backend.Backend, c *compiler.Compiler) *Reconciler {
	if c == nil {
		c = compiler.New()
	}
	return &Reconciler{
		compiler:  c,
		admission: admission.New(),
		backend:   b,
	}
}

func (r *Reconciler) Reconcile(ctx context.Context, input compiler.CompileInput, policy admission.QueuePolicy) (*ReconcileResult, error) {
	if r.backend == nil {
		return nil, fmt.Errorf("backend is required")
	}
	bundle, err := r.compiler.Compile(input)
	if err != nil {
		return nil, err
	}

	decision, err := r.admission.Evaluate(bundle, policy)
	if err != nil {
		return nil, err
	}
	bundle.AdmissionStatus = api.AdmissionStatus{
		Allowed:       decision.Allowed,
		Phase:         decision.Phase,
		Reason:        decision.Reason,
		ReservedQuota: api.CloneResourceList(decision.ReservedQuota),
		PreemptedKeys: append([]string(nil), decision.PreemptedKeys...),
	}
	bundle.Admission.AllowPreemption = bundle.Admission.AllowPreemption && policy.AllowPreemption
	bundle.Workload.Status.Admitted = decision.Allowed
	bundle.Workload.Status.Phase = decision.Phase
	bundle.Workload.Status.Reason = decision.Reason
	bundle.Workload.Status.ReservedQuota = api.CloneResourceList(decision.ReservedQuota)
	bundle.Workload.Status.PreemptedKeys = append([]string(nil), decision.PreemptedKeys...)
	bundle.ControllerStatus.ObservedGeneration = bundle.Generation
	bundle.ControllerStatus.BundleFingerprint = bundle.Fingerprint

	if !decision.Allowed {
		bundle.ControllerStatus.Phase = "queued"
		bundle.ControllerStatus.Reason = decision.Reason
		return &ReconcileResult{
			Bundle:        bundle,
			Admission:     decision,
			Queued:        true,
			PreemptedKeys: append([]string(nil), decision.PreemptedKeys...),
		}, nil
	}

	applied, err := r.backend.Apply(ctx, bundle)
	if err != nil {
		return nil, err
	}
	out := applied.Bundle
	out.AdmissionStatus = bundle.AdmissionStatus
	out.Workload.Status = bundle.Workload.Status
	out.StatusProjection = projectStatus(out)
	if applied.Idempotent {
		out.ControllerStatus.Phase = "steady"
		out.ControllerStatus.Reason = "idempotent-replay"
	} else {
		out.ControllerStatus.Phase = "ready"
		out.ControllerStatus.Reason = "applied-and-admitted"
	}
	out.ControllerStatus.ObservedGeneration = bundle.Generation
	out.ControllerStatus.BundleFingerprint = bundle.Fingerprint
	return &ReconcileResult{
		Bundle:        out,
		Created:       applied.Created,
		Updated:       applied.Updated,
		Idempotent:    applied.Idempotent,
		Admission:     decision,
		Applied:       true,
		PreemptedKeys: append([]string(nil), decision.PreemptedKeys...),
	}, nil
}

func projectStatus(bundle *api.Bundle) api.StatusProjection {
	status := bundle.StatusProjection
	if !bundle.AdmissionStatus.Allowed {
		status.Reason = bundle.AdmissionStatus.Reason
		return status
	}
	switch {
	case bundle.Job.Status.Succeeded > 0:
		status.RunState = "JOB_RUN_STATE_SUCCEEDED"
		status.JobState = "JOB_STATE_SUCCEEDED"
		status.Reason = "job-completed"
	case bundle.Job.Status.Failed > 0:
		status.RunState = "JOB_RUN_STATE_FAILED"
		status.JobState = "JOB_STATE_FAILED"
		status.Reason = "job-failed"
	case bundle.Job.Status.Paused:
		status.RunState = "JOB_RUN_STATE_PAUSED"
		status.JobState = "JOB_STATE_PAUSED"
		status.Reason = "job-paused"
	default:
		status.Reason = "job-active"
	}
	return status
}
