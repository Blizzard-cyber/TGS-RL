package compiler

import (
	"fmt"
	"math"
	"sort"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

func normalize(input CompileInput, runtimeConfig RuntimeConfig) (*normalizedInput, error) {
	if input.JobRun == nil {
		return nil, errNilRun
	}
	if input.RuntimeManifest == nil {
		return nil, errNilManifest
	}
	if input.PlacementPlan == nil {
		return nil, errNilPlan
	}
	if input.Generation == 0 {
		return nil, fmt.Errorf("generation must be positive")
	}
	namespace := strings.TrimSpace(input.Namespace)
	if namespace == "" {
		namespace = "default"
	}
	profile, err := validateGPUProfiles(input.GPUProfiles)
	if err != nil {
		return nil, err
	}
	if input.JobRun.GetRunId() == "" || input.JobRun.GetJobId() == "" {
		return nil, fmt.Errorf("job run must include run_id and job_id")
	}
	if input.RuntimeManifest.GetRunId() == "" || input.RuntimeManifest.GetJobId() == "" {
		return nil, fmt.Errorf("runtime manifest must include run_id and job_id")
	}
	if input.PlacementPlan.GetRunId() == "" || input.PlacementPlan.GetPlanId() == "" {
		return nil, fmt.Errorf("placement plan must include run_id and plan_id")
	}
	if input.JobRun.GetRunId() != input.RuntimeManifest.GetRunId() || input.JobRun.GetRunId() != input.PlacementPlan.GetRunId() {
		return nil, fmt.Errorf("run_id mismatch across compile inputs")
	}
	if input.JobRun.GetJobId() != input.RuntimeManifest.GetJobId() {
		return nil, fmt.Errorf("job_id mismatch between job run and runtime manifest")
	}
	if input.JobRun.GetTraceId() != "" && input.RuntimeManifest.GetTraceId() != "" && input.JobRun.GetTraceId() != input.RuntimeManifest.GetTraceId() {
		return nil, fmt.Errorf("trace_id mismatch between job run and runtime manifest")
	}
	if input.ManifestHasNoImageDigests() {
		return nil, fmt.Errorf("runtime manifest must include at least one image digest")
	}
	if profile == GPUProfileKubernetesDRA {
		image := primaryImage(input.RuntimeManifest)
		if image == "" {
			return nil, fmt.Errorf("DRA workload requires one workload oci_image artifact")
		}
		if len(input.RuntimeManifest.GetImageDigests()) != 1 || !strings.HasSuffix(image, "@"+input.RuntimeManifest.GetImageDigests()[0]) {
			return nil, fmt.Errorf("workload image must end in the manifest sha256 digest")
		}
	}
	binding, err := workloadBinding(input.PlacementPlan)
	if err != nil {
		return nil, err
	}
	resourcesPerUnit := binding.GetResources()
	concreteGeneration := binding.GetGeneration()
	if concreteGeneration == 0 {
		concreteGeneration = input.Generation
	}
	if concreteGeneration == 0 {
		return nil, fmt.Errorf("binding %q generation must be positive", binding.GetBindingId())
	}
	accelerators := resourcesPerUnit.GetAcceleratorUnits()
	if accelerators > 0 && profile == GPUProfileNone {
		return nil, fmt.Errorf("gpu profile none cannot compile accelerator demand")
	}
	if accelerators == 0 && profile != GPUProfileNone {
		return nil, fmt.Errorf("gpu profile %q requires accelerator demand", profile)
	}
	if profile == GPUProfileKubernetesDRA {
		if accelerators != math.Trunc(accelerators) {
			return nil, fmt.Errorf("binding %q: kubernetes-dra requires an integer accelerator count until NVIDIA sharing configuration is wired", binding.GetBindingId())
		}
		deviceIDs, err := concreteDeviceIDs(binding.GetDeviceIds())
		if err != nil {
			return nil, fmt.Errorf("binding %q: %w", binding.GetBindingId(), err)
		}
		if len(deviceIDs) != int(accelerators) {
			return nil, fmt.Errorf("binding %q: DRA device identity count %d does not match accelerator count %d", binding.GetBindingId(), len(deviceIDs), int(accelerators))
		}
	}
	if IsHAMIGPUProfile(profile) {
		if accelerators > 1 || accelerators <= 0 {
			return nil, fmt.Errorf("binding %q: HAMi vGPU requires accelerator_units within (0,1]", binding.GetBindingId())
		}
		deviceIDs, err := concreteDeviceIDs(binding.GetDeviceIds())
		if err != nil {
			return nil, fmt.Errorf("binding %q: %w", binding.GetBindingId(), err)
		}
		if len(deviceIDs) != 1 {
			return nil, fmt.Errorf("binding %q: HAMi vGPU requires exactly one physical GPU UUID", binding.GetBindingId())
		}
	}
	if runtimeConfig.Bootstrap.Enabled && len(input.RuntimeManifest.GetCommand()) == 0 {
		return nil, fmt.Errorf("worker bootstrap requires a non-empty manifest workload command")
	}
	if runtimeConfig.Bootstrap.Enabled && (binding.GetBindingId() == "" || binding.GetSandboxId() == "" || bindingRuntimeUnitID(binding) == "") {
		return nil, fmt.Errorf("worker bootstrap requires binding, sandbox, and runtime unit identities")
	}
	return &normalizedInput{
		Namespace:        namespace,
		GPUProfile:       profile,
		Generation:       concreteGeneration,
		Run:              input.JobRun,
		Manifest:         input.RuntimeManifest,
		Plan:             input.PlacementPlan,
		Runtime:          cloneRuntimeConfig(runtimeConfig),
		priority:         derivePriority(input.JobRun, input.PlacementPlan),
		resourcesPerUnit: cloneResourceVector(resourcesPerUnit),
		workloadUnitID:   binding.GetPendingUnitId(),
		binding:          proto.Clone(binding).(*tgsrlv1.Binding),
	}, nil
}

func concreteDeviceIDs(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("accelerator binding requires concrete device_ids")
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("accelerator binding device_ids must not contain empty values")
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, fmt.Errorf("accelerator binding device_ids contains duplicate %q", value)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func validateGPUProfiles(profiles []string) (string, error) {
	seen := make(map[string]struct{}, len(profiles))
	normalized := make([]string, 0, len(profiles))
	for _, profile := range profiles {
		profile = strings.TrimSpace(profile)
		if profile == "" {
			continue
		}
		if _, ok := supportedGPUProfiles[profile]; !ok {
			return "", fmt.Errorf("unsupported gpu profile %q", profile)
		}
		if _, ok := seen[profile]; ok {
			continue
		}
		seen[profile] = struct{}{}
		normalized = append(normalized, profile)
	}
	if len(normalized) == 0 {
		return GPUProfileNone, nil
	}
	return normalized[0], nil
}

func ValidateRuntimeConfig(config RuntimeConfig) (RuntimeConfig, error) {
	rawName := config.RuntimeClass.Name
	rawHandler := config.RuntimeClass.Handler
	if rawName != strings.TrimSpace(rawName) {
		return RuntimeConfig{}, fmt.Errorf("runtime class name must not contain leading or trailing whitespace")
	}
	if rawHandler != strings.TrimSpace(rawHandler) {
		return RuntimeConfig{}, fmt.Errorf("runtime class handler must not contain leading or trailing whitespace")
	}
	if rawName == "" {
		if rawHandler != "" || config.RuntimeClass.Create {
			return RuntimeConfig{}, fmt.Errorf("runtime class handler/create requires runtime class name")
		}
	} else {
		if err := validateRuntimeClassName(rawName); err != nil {
			return RuntimeConfig{}, err
		}
	}
	if rawHandler != "" {
		if err := validateRuntimeClassHandler(rawHandler); err != nil {
			return RuntimeConfig{}, err
		}
	}
	if config.RuntimeClass.Create && rawHandler == "" {
		return RuntimeConfig{}, fmt.Errorf("runtime class creation requires runtime class handler")
	}
	normalized := normalizeRuntimeConfig(config)
	if normalized.Bootstrap.HostNetwork && !normalized.Bootstrap.Enabled {
		return RuntimeConfig{}, fmt.Errorf("worker host networking requires worker bootstrap")
	}
	if normalized.Bootstrap.Enabled {
		if normalized.Bootstrap.InstallerImage == "" || normalized.Bootstrap.RegistryURL == "" {
			return RuntimeConfig{}, fmt.Errorf("worker bootstrap requires installer image and registry URL")
		}
		if len(normalized.Bootstrap.RegistrySigningKey) < 32 {
			return RuntimeConfig{}, fmt.Errorf("worker bootstrap registry signing key must contain at least 32 bytes")
		}
		if !strings.Contains(normalized.Bootstrap.InstallerImage, "@sha256:") {
			return RuntimeConfig{}, fmt.Errorf("worker bootstrap installer image must use an immutable sha256 digest")
		}
		if !strings.HasPrefix(normalized.Bootstrap.RegistryURL, "http://") && !strings.HasPrefix(normalized.Bootstrap.RegistryURL, "https://") {
			return RuntimeConfig{}, fmt.Errorf("worker bootstrap registry URL must use HTTP or HTTPS")
		}
	}
	for key, value := range normalized.NodeSelector {
		if err := validateNodeSelectorEntry(key, value); err != nil {
			return RuntimeConfig{}, err
		}
	}
	return normalized, nil
}

func workloadBinding(plan *tgsrlv1.PlacementPlan) (*tgsrlv1.Binding, error) {
	var selected *tgsrlv1.Binding
	for _, binding := range plan.GetBindings() {
		if binding == nil || binding.GetPendingUnitId() == "" {
			continue
		}
		if binding.GetResources() == nil {
			return nil, fmt.Errorf("binding %q is missing resources", binding.GetBindingId())
		}
		if selected != nil {
			return nil, fmt.Errorf("operator bundle requires exactly one concrete binding")
		}
		selected = binding
	}
	if selected == nil {
		return nil, fmt.Errorf("operator bundle requires one concrete binding")
	}
	if bindingRuntimeUnitID(selected) == "" {
		return nil, fmt.Errorf("binding %q is missing runtime unit identity", selected.GetBindingId())
	}
	return selected, nil
}

func bindingRuntimeUnitID(binding *tgsrlv1.Binding) string {
	if binding.GetRuntimeUnitId() != "" {
		return binding.GetRuntimeUnitId()
	}
	return binding.GetPendingUnitId()
}

func derivePriority(run *tgsrlv1.JobRun, plan *tgsrlv1.PlacementPlan) int32 {
	for _, operation := range run.GetOperations() {
		if operation.GetAnnotations()["priority"] != "" {
			var p int
			fmt.Sscanf(operation.GetAnnotations()["priority"], "%d", &p)
			return int32(p)
		}
	}
	for _, action := range plan.GetActions() {
		if action.GetPriority() != 0 {
			return action.GetPriority()
		}
	}
	return 0
}
