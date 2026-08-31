package compiler

import (
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"google.golang.org/protobuf/proto"
)

func buildRuntimeTargets(plan *tgsrlv1.PlacementPlan, generation uint64) []api.RuntimeTarget {
	targets := make([]api.RuntimeTarget, 0, len(plan.GetBindings()))
	for _, binding := range plan.GetBindings() {
		if binding == nil {
			continue
		}
		runtimeUnitID := binding.GetRuntimeUnitId()
		if runtimeUnitID == "" {
			runtimeUnitID = binding.GetPendingUnitId()
		}
		targetGeneration := binding.GetGeneration()
		if targetGeneration == 0 {
			targetGeneration = generation
		}
		targets = append(targets, api.RuntimeTarget{
			RuntimeUnitID: runtimeUnitID,
			SandboxID:     binding.GetSandboxId(),
			BindingID:     binding.GetBindingId(),
			DecisionID:    plan.GetDecisionId(),
			PlanID:        plan.GetPlanId(),
			ActionID:      actionIDForBinding(plan, binding),
			DeviceIDs:     append([]string(nil), binding.GetDeviceIds()...),
			CPUMillis:     binding.GetResources().GetCpuMillis(),
			MemoryBytes:   binding.GetResources().GetMemoryBytes(),
			Accelerators:  binding.GetResources().GetAcceleratorUnits(),
			Generation:    targetGeneration,
		})
	}
	return targets
}

func actionIDForBinding(plan *tgsrlv1.PlacementPlan, binding *tgsrlv1.Binding) string {
	for _, action := range plan.GetActions() {
		if action.GetBinding().GetBindingId() == binding.GetBindingId() || action.GetSandboxId() == binding.GetSandboxId() || action.GetTargetId() == binding.GetRuntimeUnitId() || action.GetTargetId() == binding.GetPendingUnitId() {
			return action.GetActionId()
		}
	}
	return ""
}

func primaryImage(manifest *tgsrlv1.RuntimeManifest) string {
	if len(manifest.GetImageDigests()) == 0 {
		return ""
	}
	return manifest.GetImageDigests()[0]
}

func buildLabels(input *normalizedInput) map[string]string {
	labels := map[string]string{
		"tgsrl.io/job-id":       sanitizeLabelValue(input.Run.GetJobId()),
		"tgsrl.io/run-id":       sanitizeLabelValue(input.Run.GetRunId()),
		"tgsrl.io/trace-id":     sanitizeLabelValue(input.Run.GetTraceId()),
		"tgsrl.io/plan-id":      sanitizeLabelValue(input.Plan.GetPlanId()),
		"tgsrl.io/gpu-profile":  sanitizeLabelValue(input.GPUProfile),
		"tgsrl.io/managed-by":   "operator-go",
		"tgsrl.io/run-state":    sanitizeLabelValue(strings.ToLower(strings.TrimPrefix(input.Run.GetRunState().String(), "JOB_RUN_STATE_"))),
		"tgsrl.io/runtime-kind": sanitizeLabelValue(strings.ToLower(input.Manifest.GetExecutionBackend())),
	}
	for _, key := range sortedProtoLabelKeys(input.Run.GetLabels()) {
		labels["tgsrl.io/user-"+sanitizeLabelKey(key)] = sanitizeLabelValue(input.Run.GetLabels()[key])
	}
	return labels
}

func buildAnnotations(input *normalizedInput) map[string]string {
	annotations := map[string]string{
		"tgsrl.io/manifest-id":       input.Manifest.GetManifestId(),
		"tgsrl.io/decision-id":       input.Plan.GetDecisionId(),
		"tgsrl.io/execution-id":      input.Plan.GetExecutionId(),
		"tgsrl.io/stage-id":          input.Plan.GetStageId(),
		"tgsrl.io/intent-version":    fmt.Sprintf("%d", input.Plan.GetIntentVersion()),
		"tgsrl.io/snapshot-revision": fmt.Sprintf("%d", input.Plan.GetSnapshotRevision()),
		"tgsrl.io/generation":        fmt.Sprintf("%d", input.Generation),
	}
	for _, key := range sortedProtoLabelKeys(input.Manifest.GetAnnotations()) {
		annotations["tgsrl.io/manifest-"+sanitizeLabelKey(key)] = input.Manifest.GetAnnotations()[key]
	}
	return annotations
}

func buildEnv(input *normalizedInput) []api.EnvVar {
	values := make([]api.EnvVar, 0, len(input.Run.GetRuntime().GetEnvironment())+7)
	for _, key := range sortedProtoLabelKeys(input.Run.GetRuntime().GetEnvironment()) {
		values = append(values, api.EnvVar{Name: sanitizeEnvName(key), Value: input.Run.GetRuntime().GetEnvironment()[key]})
	}
	values = append(values,
		api.EnvVar{Name: "TGSRL_JOB_ID", Value: input.Run.GetJobId()},
		api.EnvVar{Name: "TGSRL_RUN_ID", Value: input.Run.GetRunId()},
		api.EnvVar{Name: "TGSRL_TRACE_ID", Value: input.Run.GetTraceId()},
		api.EnvVar{Name: "TGSRL_PLAN_ID", Value: input.Plan.GetPlanId()},
		api.EnvVar{Name: "TGSRL_GPU_PROFILE", Value: input.GPUProfile},
	)
	sort.Slice(values, func(i, j int) bool { return values[i].Name < values[j].Name })
	return values
}

func buildResources(input *normalizedInput) api.ResourceRequirements {
	cpuMillis, memoryBytes := input.resourcesPerUnit.GetCpuMillis(), input.resourcesPerUnit.GetMemoryBytes()
	if cpuMillis == 0 {
		cpuMillis = 1000
	}
	requests := api.ResourceList{
		"cpu":    fmt.Sprintf("%dm", cpuMillis),
		"memory": quantityBytes(memoryBytes),
	}
	limits := api.ResourceList{}
	acceleratorUnits := input.resourcesPerUnit.GetAcceleratorUnits()
	if acceleratorUnits > 0 {
		key := "example.com/accelerator"
		switch input.GPUProfile {
		case GPUProfileNVIDIADevicePlugin:
			key = "nvidia.com/gpu"
		case GPUProfileVolcanoHAMI:
			key = "volcano.sh/gpu"
		case GPUProfileKubernetesDRA:
			return api.ResourceRequirements{Requests: requests}
		}
		value := formatAcceleratorQuantity(acceleratorUnits)
		requests[key] = value
		limits[key] = value
	}
	return api.ResourceRequirements{Requests: requests, Limits: limits}
}

func buildNodeSelector(input *normalizedInput) map[string]string {
	return cloneStringMap(input.Runtime.NodeSelector)
}

func cloneResourceVector(value *tgsrlv1.ResourceVector) *tgsrlv1.ResourceVector {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*tgsrlv1.ResourceVector)
}

func requiresResourceClaim(profile string, accelerators float64) bool {
	return accelerators > 0 && profile == GPUProfileKubernetesDRA
}

func statusReason(run *tgsrlv1.JobRun) string {
	for _, component := range run.GetComponentStatus() {
		if component.GetDetail() != "" {
			return component.GetDetail()
		}
	}
	for _, operation := range run.GetOperations() {
		if operation.GetErrorMessage() != "" {
			return operation.GetErrorMessage()
		}
	}
	return ""
}

func queueName(run *tgsrlv1.JobRun) string {
	if queue := run.GetQueue(); queue != "" {
		return queue
	}
	if queue := run.GetLabels()["queue"]; queue != "" {
		return queue
	}
	return "default"
}

func quotaGroup(run *tgsrlv1.JobRun) string {
	if group := run.GetLabels()["quota_group"]; group != "" {
		return group
	}
	return "default"
}

func preemptionAllowed(run *tgsrlv1.JobRun) bool {
	value := strings.TrimSpace(strings.ToLower(run.GetLabels()["allow_preemption"]))
	return value == "1" || value == "true" || value == "yes"
}

func quantityBytes(value uint64) string {
	if value == 0 {
		return "0"
	}
	return fmt.Sprintf("%d", value)
}

func formatAcceleratorQuantity(value float64) string {
	if math.Abs(value-math.Round(value)) < 1e-9 {
		return fmt.Sprintf("%.0f", value)
	}
	rat := new(big.Rat).SetFloat64(value)
	if rat == nil {
		return fmt.Sprintf("%.3f", value)
	}
	return rat.FloatString(3)
}
