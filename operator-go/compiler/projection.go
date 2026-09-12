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
	for _, artifact := range manifest.GetArtifacts() {
		if artifact == nil || !strings.EqualFold(strings.TrimSpace(artifact.GetKind()), "oci_image") || strings.TrimSpace(artifact.GetAttributes()["purpose"]) != "workload" {
			continue
		}
		return strings.TrimSpace(artifact.GetUri())
	}
	return ""
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
	reserved := map[string]struct{}{
		"TGSRL_JOB_ID": struct{}{}, "TGSRL_RUN_ID": struct{}{}, "TGSRL_TRACE_ID": struct{}{}, "TGSRL_PLAN_ID": struct{}{},
		"TGSRL_GPU_PROFILE": struct{}{}, "TGSRL_SANDBOX_ID": struct{}{}, "TGSRL_BINDING_ID": struct{}{},
		"TGSRL_EXECUTION_ID": struct{}{}, "TGSRL_RUNTIME_UNIT_ID": struct{}{}, "TGSRL_GENERATION": struct{}{}, "TGSRL_DEVICE_IDS": struct{}{},
		"TGSRL_ACCELERATOR_SHARE": struct{}{}, "TGSRL_WORKER_REGISTRY_URL": struct{}{},
		"TGSRL_WORKER_REGISTRY_TOKEN": struct{}{}, "TGSRL_POD_IP": struct{}{},
		"TGSRL_POD_UID":           struct{}{},
		"TGSRL_WORKING_DIRECTORY": struct{}{}, "TGSRL_VERIFY_DEVICE_IDENTITIES": struct{}{},
		"TGSRL_POLICY_VERSION": struct{}{}, "TGSRL_ALGORITHM": struct{}{},
		"TGSRL_VERL_CONTROL_SOCKET": struct{}{}, "TGSRL_VERL_TRACE_PATH": struct{}{},
		"TGSRL_VERL_STATE_PATH": struct{}{},
		"TGSRL_WORKER_ID":       struct{}{}, "TGSRL_STAGE_ID": struct{}{},
		"TGSRL_PHASE_KIND": struct{}{}, "TGSRL_ROLLOUT_MODE": struct{}{},
		"TGSRL_DATA_KIND": struct{}{}, "TGSRL_WORKER_TRACE_URL": struct{}{},
		"TGSRL_WORKER_TRACE_TOKEN":      struct{}{},
		"TGSRL_REQUIRED_PYTHON_MODULES": struct{}{},
	}
	values := make([]api.EnvVar, 0, len(input.Manifest.GetEnvironment())+12)
	for _, key := range sortedProtoLabelKeys(input.Manifest.GetEnvironment()) {
		name := sanitizeEnvName(key)
		if _, protected := reserved[name]; protected {
			continue
		}
		values = append(values, api.EnvVar{Name: name, Value: input.Manifest.GetEnvironment()[key]})
	}
	acceleratorShare := input.resourcesPerUnit.GetAcceleratorUnits()
	if input.GPUProfile == GPUProfileKubernetesDRA && acceleratorShare > 0 {
		acceleratorShare = 1
	}
	values = append(values,
		api.EnvVar{Name: "TGSRL_JOB_ID", Value: input.Run.GetJobId()},
		api.EnvVar{Name: "TGSRL_RUN_ID", Value: input.Run.GetRunId()},
		api.EnvVar{Name: "TGSRL_TRACE_ID", Value: input.Run.GetTraceId()},
		api.EnvVar{Name: "TGSRL_EXECUTION_ID", Value: input.Plan.GetExecutionId()},
		api.EnvVar{Name: "TGSRL_PLAN_ID", Value: input.Plan.GetPlanId()},
		api.EnvVar{Name: "TGSRL_GPU_PROFILE", Value: input.GPUProfile},
		api.EnvVar{Name: "TGSRL_SANDBOX_ID", Value: input.binding.GetSandboxId()},
		api.EnvVar{Name: "TGSRL_BINDING_ID", Value: input.binding.GetBindingId()},
		api.EnvVar{Name: "TGSRL_RUNTIME_UNIT_ID", Value: bindingRuntimeUnitID(input.binding)},
		api.EnvVar{Name: "TGSRL_WORKER_ID", Value: bindingRuntimeUnitID(input.binding)},
		api.EnvVar{Name: "TGSRL_GENERATION", Value: fmt.Sprintf("%d", input.Generation)},
		api.EnvVar{Name: "TGSRL_DEVICE_IDS", Value: strings.Join(input.binding.GetDeviceIds(), ",")},
		api.EnvVar{Name: "TGSRL_ACCELERATOR_SHARE", Value: formatAcceleratorQuantity(acceleratorShare)},
		api.EnvVar{Name: "TGSRL_POLICY_VERSION", Value: input.Manifest.GetPolicyVersion()},
		api.EnvVar{Name: "TGSRL_ALGORITHM", Value: input.Manifest.GetAnnotations()["algorithm"]},
		api.EnvVar{Name: "TGSRL_STAGE_ID", Value: input.Plan.GetStageId()},
		api.EnvVar{Name: "TGSRL_PHASE_KIND", Value: fmt.Sprintf("%d", phaseKindForStage(input.Manifest, input.Plan.GetStageId()))},
		api.EnvVar{Name: "TGSRL_ROLLOUT_MODE", Value: fmt.Sprintf("%d", input.Manifest.GetRolloutMode())},
		api.EnvVar{Name: "TGSRL_DATA_KIND", Value: fmt.Sprintf("%d", input.Manifest.GetDataKind())},
	)
	if strings.EqualFold(input.Manifest.GetFramework(), "verl") {
		controlSocket := manifestEnvironmentOrDefault(input.Manifest.GetEnvironment(), "TGSRL_VERL_CONTROL_SOCKET", "/tmp/tgsrl/verl.sock")
		tracePath := manifestEnvironmentOrDefault(input.Manifest.GetEnvironment(), "TGSRL_VERL_TRACE_PATH", "/tmp/tgsrl/verl.ndjson")
		statePath := manifestEnvironmentOrDefault(input.Manifest.GetEnvironment(), "TGSRL_VERL_STATE_PATH", "/tmp/tgsrl/verl-state.json")
		values = append(values,
			api.EnvVar{Name: "TGSRL_VERL_CONTROL_SOCKET", Value: controlSocket},
			api.EnvVar{Name: "TGSRL_VERL_TRACE_PATH", Value: tracePath},
			api.EnvVar{Name: "TGSRL_VERL_STATE_PATH", Value: statePath},
			api.EnvVar{Name: "TGSRL_REQUIRED_PYTHON_MODULES", Value: requiredPythonModules(input)},
		)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Name < values[j].Name })
	return values
}

func requiredPythonModules(input *normalizedInput) string {
	if input == nil || primaryImage(input.Manifest) == "" {
		return ""
	}
	manifest := input.Manifest
	modules := make([]string, 0, 4)
	if strings.EqualFold(manifest.GetFramework(), "verl") {
		modules = append(modules, "verl")
	}
	if strings.EqualFold(manifest.GetExecutionBackend(), "ray") {
		modules = append(modules, "ray")
	}
	if strings.EqualFold(manifest.GetTrainer(), "pytorch") {
		modules = append(modules, "torch")
	}
	if strings.EqualFold(manifest.GetRolloutEngine(), "vllm") {
		modules = append(modules, "vllm")
	}
	if strings.EqualFold(manifest.GetRolloutEngine(), "sglang") {
		modules = append(modules, "sglang")
	}
	sort.Strings(modules)
	return strings.Join(modules, ",")
}

func phaseKindForStage(manifest *tgsrlv1.RuntimeManifest, stageID string) tgsrlv1.PhaseKind {
	for _, phase := range manifest.GetExecutionContract().GetPhaseGraph().GetPhases() {
		if phase.GetPhaseId() == stageID {
			return phase.GetKind()
		}
	}
	return tgsrlv1.PhaseKind_PHASE_KIND_UNKNOWN
}

func manifestEnvironmentOrDefault(values map[string]string, key, fallback string) string {
	if value := strings.TrimSpace(values[key]); value != "" {
		return value
	}
	return fallback
}

func buildResources(input *normalizedInput) api.ResourceRequirements {
	cpuMillis, memoryBytes := input.resourcesPerUnit.GetCpuMillis(), input.resourcesPerUnit.GetMemoryBytes()
	if cpuMillis == 0 {
		cpuMillis = 1000
	}
	requests := api.ResourceList{
		"cpu":    quantityCPU(cpuMillis),
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

func quantityCPU(millis uint64) string {
	if millis%1000 == 0 {
		return fmt.Sprintf("%d", millis/1000)
	}
	return fmt.Sprintf("%dm", millis)
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
