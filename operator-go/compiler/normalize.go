package compiler

import (
	"fmt"
	"sort"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
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
	parallelism := maxRunUnits(input.PlacementPlan, desiredUnits(input.JobRun))
	if parallelism == 0 {
		parallelism = 1
	}
	accelerators := acceleratorUnits(input.PlacementPlan)
	if accelerators > 0 && profile == GPUProfileNone {
		return nil, fmt.Errorf("gpu profile none cannot compile accelerator demand")
	}
	if accelerators == 0 && profile != GPUProfileNone {
		return nil, fmt.Errorf("gpu profile %q requires accelerator demand", profile)
	}
	return &normalizedInput{
		Namespace:        namespace,
		GPUProfile:       profile,
		Generation:       input.Generation,
		Run:              input.JobRun,
		Manifest:         input.RuntimeManifest,
		Plan:             input.PlacementPlan,
		AdmissionAllowed: input.AdmissionAllowed,
		Runtime:          cloneRuntimeConfig(runtimeConfig),
		parallelism:      parallelism,
		priority:         derivePriority(input.JobRun, input.PlacementPlan),
		acceleratorUnits: accelerators,
	}, nil
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
	if len(normalized) > 1 {
		sort.Strings(normalized)
		return "", fmt.Errorf("gpu profiles are mutually exclusive: %s", strings.Join(normalized, ", "))
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
	for key, value := range normalized.NodeSelector {
		if err := validateNodeSelectorEntry(key, value); err != nil {
			return RuntimeConfig{}, err
		}
	}
	return normalized, nil
}

func maxRunUnits(plan *tgsrlv1.PlacementPlan, fallback uint32) uint32 {
	if plan == nil {
		return fallback
	}
	var max uint32
	for _, binding := range plan.GetBindings() {
		if binding.GetPendingUnitId() != "" {
			max++
		}
	}
	if max > 0 {
		return max
	}
	return fallback
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

func desiredUnits(run *tgsrlv1.JobRun) uint32 {
	if run == nil {
		return 0
	}
	if run.GetDesiredUnits() > 0 {
		return run.GetDesiredUnits()
	}
	if raw := strings.TrimSpace(run.GetLabels()["desired_units"]); raw != "" {
		var v uint32
		if _, err := fmt.Sscanf(raw, "%d", &v); err == nil {
			return v
		}
	}
	return 0
}
