package runtimeclient

import (
	"fmt"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

// BuildManifest derives the runtime-facing immutable manifest from one run.
func BuildManifest(run *tgsrlv1.JobRun) (*tgsrlv1.RuntimeManifest, error) {
	if run == nil {
		return nil, fmt.Errorf("runtimeclient: run is required")
	}
	if run.GetRuntime() == nil {
		return nil, fmt.Errorf("runtimeclient: run.runtime is required")
	}
	if run.GetExecutionContract() == nil {
		return nil, fmt.Errorf("runtimeclient: run.execution_contract is required")
	}
	manifest := &tgsrlv1.RuntimeManifest{
		ManifestId:           fmt.Sprintf("manifest-%s", run.GetRunId()),
		RunId:                run.GetRunId(),
		JobId:                run.GetJobId(),
		TraceId:              run.GetTraceId(),
		Framework:            run.GetRuntime().GetFramework(),
		ExecutionBackend:     run.GetRuntime().GetExecutionBackend(),
		Trainer:              run.GetRuntime().GetTrainer(),
		RolloutEngine:        run.GetRuntime().GetRolloutEngine(),
		ImageDigests:         []string{run.GetRuntime().GetImageDigest()},
		PatchSet:             append([]string(nil), run.GetRuntime().GetPatchSet()...),
		CompatibilityProfile: run.GetRuntime().GetCompatibilityProfile(),
		Annotations:          mergeMaps(run.GetRuntime().GetAnnotations(), run.GetLabels()),
		DataKind:             run.GetDataKind(),
		ExecutionContract:    proto.Clone(run.GetExecutionContract()).(*tgsrlv1.ExecutionContract),
		ResourcesPerUnit:     cloneResources(run.GetResourcesPerUnit()),
		RequiredCapabilities: cloneCapabilities(run.GetRequiredCapabilities()),
		DesiredUnits:         run.GetDesiredUnits(),
		Priority:             run.GetPriority(),
		Queue:                run.GetQueue(),
		RolloutMode:          run.GetRolloutMode(),
		PolicyVersion:        run.GetPolicyVersion(),
		DeterministicSeed:    run.GetRandomSeed(),
		Command:              append([]string(nil), run.GetRuntime().GetCommand()...),
		Args:                 append([]string(nil), run.GetRuntime().GetArgs()...),
		Environment:          mergeMaps(run.GetRuntime().GetEnvironment(), nil),
		WorkingDirectory:     run.GetRuntime().GetWorkingDirectory(),
	}
	return manifest, nil
}

func cloneResources(resources *tgsrlv1.ResourceVector) *tgsrlv1.ResourceVector {
	if resources == nil {
		return nil
	}
	return proto.Clone(resources).(*tgsrlv1.ResourceVector)
}

func cloneCapabilities(capabilities *tgsrlv1.CapabilitySet) *tgsrlv1.CapabilitySet {
	if capabilities == nil {
		return nil
	}
	return proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)
}

func mergeMaps(left, right map[string]string) map[string]string {
	merged := map[string]string{}
	for key, value := range left {
		merged[key] = value
	}
	for key, value := range right {
		merged[key] = value
	}
	return merged
}
