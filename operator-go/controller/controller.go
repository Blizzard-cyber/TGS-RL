package controller

import (
	"context"
	"fmt"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

type ReconcileResult struct {
	Bundles    []*api.Bundle
	Created    bool
	Updated    bool
	Idempotent bool
	Applied    bool
}

type Reconciler struct {
	compiler *compiler.Compiler
	backend  backend.Backend
}

type capabilityDiscoverer interface {
	DiscoverCapabilities(context.Context) (compiler.CapabilitySet, error)
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
		compiler: c,
		backend:  b,
	}
}

func (r *Reconciler) Reconcile(ctx context.Context, input compiler.CompileInput) (*ReconcileResult, error) {
	if r.backend == nil {
		return nil, fmt.Errorf("backend is required")
	}
	var bundles []*api.Bundle
	if len(input.PlacementPlan.GetBindings()) > 0 {
		if discoverer, ok := r.backend.(capabilityDiscoverer); ok && r.compiler != nil {
			capabilities, err := discoverer.DiscoverCapabilities(ctx)
			if err != nil {
				return nil, err
			}
			if len(capabilities.GPUProfiles) == 0 {
				capabilities.GPUProfiles = map[string]bool{compiler.GPUProfileNone: true}
			}
			r.compiler.SetCapabilities(capabilities)
		}
		var err error
		bundles, err = r.compiler.Compile(input)
		if err != nil {
			return nil, err
		}
	}
	result := &ReconcileResult{Applied: true, Idempotent: true}
	if request, ok, err := releaseControlRequest(input); err != nil {
		return nil, err
	} else if ok {
		applied, err := r.backend.Control(ctx, request)
		if err != nil {
			return nil, fmt.Errorf("release workloads for plan %q: %w", input.PlacementPlan.GetPlanId(), err)
		}
		if applied == nil || !applied.Accepted {
			return nil, fmt.Errorf("release workloads for plan %q was not accepted", input.PlacementPlan.GetPlanId())
		}
		result.Idempotent = result.Idempotent && applied.Idempotent
	}
	result.Bundles = make([]*api.Bundle, 0, len(bundles))
	for _, bundle := range bundles {
		bundle.ControllerStatus.ObservedGeneration = bundle.Generation
		bundle.ControllerStatus.BundleFingerprint = bundle.Fingerprint

		applied, err := r.backend.Apply(ctx, bundle)
		if err != nil {
			return nil, err
		}
		out := applied.Bundle
		out.StatusProjection = projectStatus(out)
		if applied.Idempotent {
			out.ControllerStatus.Phase = "steady"
			out.ControllerStatus.Reason = "idempotent-replay"
		} else {
			out.ControllerStatus.Phase = "ready"
			out.ControllerStatus.Reason = "applied"
		}
		out.ControllerStatus.ObservedGeneration = bundle.Generation
		out.ControllerStatus.BundleFingerprint = bundle.Fingerprint
		result.Bundles = append(result.Bundles, out)
		result.Created = result.Created || applied.Created
		result.Updated = result.Updated || applied.Updated
		result.Idempotent = result.Idempotent && applied.Idempotent
	}
	return result, nil
}

func releaseControlRequest(input compiler.CompileInput) (backend.ControlRequest, bool, error) {
	plan := input.PlacementPlan
	request := backend.ControlRequest{
		Action:             tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP,
		JobID:              input.JobRun.GetJobId(),
		RunID:              input.JobRun.GetRunId(),
		TraceID:            input.JobRun.GetTraceId(),
		RequestID:          plan.GetDecisionId(),
		IdempotencyKey:     "scheduler-release:" + plan.GetPlanId(),
		Reason:             "scheduler release plan " + plan.GetPlanId(),
		GlobalTargetLookup: true,
	}
	for _, action := range plan.GetActions() {
		if action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_RELEASE {
			continue
		}
		binding := action.GetBinding()
		if binding == nil {
			binding = action.GetRollback().GetRestoreBinding()
		}
		if binding == nil || runtimeUnitID(binding) == "" || binding.GetSandboxId() == "" || action.GetExpectedGeneration() == 0 || action.GetIdempotencyKey() == "" {
			return backend.ControlRequest{}, false, fmt.Errorf("release action %q lacks workload identity, generation, or idempotency key", action.GetActionId())
		}
		request.Targets = append(request.Targets, backend.ControlTarget{RuntimeUnitID: runtimeUnitID(binding), SandboxID: binding.GetSandboxId(), ExpectedGeneration: action.GetExpectedGeneration()})
	}
	if len(request.Targets) == 0 {
		return backend.ControlRequest{}, false, nil
	}
	return request, true, nil
}

func runtimeUnitID(binding *tgsrlv1.Binding) string {
	if binding.GetRuntimeUnitId() != "" {
		return binding.GetRuntimeUnitId()
	}
	return binding.GetPendingUnitId()
}

func projectStatus(bundle *api.Bundle) api.StatusProjection {
	status := bundle.StatusProjection
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
