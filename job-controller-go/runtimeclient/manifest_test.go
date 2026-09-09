package runtimeclient

import (
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func TestBuildManifest(t *testing.T) {
	t.Parallel()

	run := &tgsrlv1.JobRun{
		RunId:   "run-1",
		JobId:   "job-1",
		TraceId: "trace-1",
		Runtime: &tgsrlv1.FrameworkRuntimeSpec{
			Framework:            "pytorch",
			ExecutionBackend:     "kubernetes",
			Trainer:              "torchtune",
			RolloutEngine:        "ray",
			ImageDigest:          "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			ArtifactUri:          "registry.example.test/verl@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			CompatibilityProfile: "profile-1",
			PatchSet:             []string{"patch-a"},
			Command:              []string{"python", "train.py"},
			Args:                 []string{"--steps", "10"},
			Environment:          map[string]string{"MODE": "train"},
			WorkingDirectory:     "/workspace",
			Annotations:          map[string]string{"runtime": "1"},
		},
		ExecutionContract:    &tgsrlv1.ExecutionContract{ContractId: "contract-1"},
		DataKind:             tgsrlv1.DataKind_DATA_KIND_SYNTHETIC,
		ResourcesPerUnit:     &tgsrlv1.ResourceVector{CpuMillis: 2000, MemoryBytes: 4096},
		RequiredCapabilities: &tgsrlv1.CapabilitySet{Names: []string{"logical-cpu"}},
		DesiredUnits:         3,
		Priority:             7,
		Queue:                "gold",
		RolloutMode:          tgsrlv1.RolloutMode_ROLLOUT_MODE_PARTIALLY_ASYNC,
		PolicyVersion:        "policy-2",
		RandomSeed:           11,
		Labels:               map[string]string{"job": "1"},
	}

	manifest, err := BuildManifest(run)
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}
	if manifest.GetRunId() != run.GetRunId() || manifest.GetJobId() != run.GetJobId() {
		t.Fatalf("manifest ids = (%q, %q), want (%q, %q)", manifest.GetRunId(), manifest.GetJobId(), run.GetRunId(), run.GetJobId())
	}
	if len(manifest.GetImageDigests()) != 1 || manifest.GetImageDigests()[0] != run.GetRuntime().GetImageDigest() {
		t.Fatalf("manifest image_digests = %v", manifest.GetImageDigests())
	}
	if len(manifest.GetArtifacts()) != 1 || manifest.GetArtifacts()[0].GetUri() != run.GetRuntime().GetArtifactUri() || manifest.GetArtifacts()[0].GetDigest() != run.GetRuntime().GetImageDigest() {
		t.Fatalf("manifest workload image artifact = %v", manifest.GetArtifacts())
	}
	if manifest.GetAnnotations()["runtime"] != "1" || manifest.GetAnnotations()["job"] != "1" {
		t.Fatalf("manifest annotations = %+v", manifest.GetAnnotations())
	}
	if manifest.GetResourcesPerUnit().GetCpuMillis() != 2000 || manifest.GetDesiredUnits() != 3 || manifest.GetPriority() != 7 || manifest.GetQueue() != "gold" || manifest.GetDeterministicSeed() != 11 {
		t.Fatalf("manifest scheduling fields were not preserved: %+v", manifest)
	}
	if len(manifest.GetCommand()) != 2 || manifest.GetCommand()[1] != "train.py" || len(manifest.GetArgs()) != 2 || manifest.GetArgs()[1] != "10" {
		t.Fatalf("manifest command and args were not preserved: %+v %+v", manifest.GetCommand(), manifest.GetArgs())
	}
	if manifest.GetEnvironment()["MODE"] != "train" || manifest.GetWorkingDirectory() != "/workspace" {
		t.Fatalf("manifest execution environment was not preserved: %+v %q", manifest.GetEnvironment(), manifest.GetWorkingDirectory())
	}
}
