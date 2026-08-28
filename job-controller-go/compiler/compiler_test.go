package compiler

import (
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func TestNormalizeJobDeterministicIDAndSorting(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	job := validJob()
	normalizedA, diagnosticsA := NormalizeJob(job, now)
	normalizedB, diagnosticsB := NormalizeJob(job, now)
	if len(diagnosticsA) != 0 || len(diagnosticsB) != 0 {
		t.Fatalf("NormalizeJob() diagnostics = %v / %v, want none", diagnosticsA, diagnosticsB)
	}
	if normalizedA.GetJobId() == "" || normalizedA.GetJobId() != normalizedB.GetJobId() {
		t.Fatalf("NormalizeJob() ids = %q / %q, want same non-empty id", normalizedA.GetJobId(), normalizedB.GetJobId())
	}
	if normalizedA.GetRuntime().GetPatchSet()[0] != "patch-a" {
		t.Fatalf("NormalizeJob() patch_set = %v, want sorted", normalizedA.GetRuntime().GetPatchSet())
	}
}

func TestValidateJobRejectsInvalidDigest(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Runtime.ImageDigest = "latest"
	diagnostics := ValidateJob(job)
	if len(diagnostics) == 0 {
		t.Fatal("ValidateJob() diagnostics empty, want invalid digest error")
	}
}

func TestNewRunDeterministicAttempt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 12, 5, 0, 0, time.UTC)
	job, diagnostics := NormalizeJob(validJob(), now)
	if len(diagnostics) != 0 {
		t.Fatalf("NormalizeJob() diagnostics = %v", diagnostics)
	}
	runA := NewRun(job, 1, tgsrlv1.JobRunState_JOB_RUN_STATE_ADMITTED, now)
	runB := NewRun(job, 1, tgsrlv1.JobRunState_JOB_RUN_STATE_ADMITTED, now)
	runC := NewRun(job, 2, tgsrlv1.JobRunState_JOB_RUN_STATE_ADMITTED, now)
	if runA.GetRunId() != runB.GetRunId() {
		t.Fatalf("run ids differ for same attempt: %q vs %q", runA.GetRunId(), runB.GetRunId())
	}
	if runA.GetRunId() == runC.GetRunId() {
		t.Fatalf("run ids identical across attempts: %q", runA.GetRunId())
	}
}

func validJob() *tgsrlv1.RLTrainingJob {
	return &tgsrlv1.RLTrainingJob{
		DisplayName:     "compile-job",
		ProtocolVersion: "v0.3",
		Algorithm:       "ppo",
		Runtime: &tgsrlv1.FrameworkRuntimeSpec{
			Framework:               "pytorch",
			FrameworkVersion:        "2.4.0",
			ExecutionBackend:        "kubernetes",
			ExecutionBackendVersion: "1.30.0",
			Trainer:                 "torchtune",
			TrainerVersion:          "0.5.0",
			RolloutEngine:           "ray",
			RolloutEngineVersion:    "2.20.0",
			ImageDigest:             "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			PatchSet:                []string{"patch-b", "patch-a"},
		},
		ExecutionContract: &tgsrlv1.ExecutionContract{
			ContractId: "contract",
			Version:    "1.0.0",
			PhaseGraph: &tgsrlv1.PhaseGraph{
				Phases: []*tgsrlv1.Phase{{
					PhaseId:     "phase-1",
					DisplayName: "Train",
					Kind:        tgsrlv1.PhaseKind_PHASE_KIND_ACTOR,
					Parallelism: 1,
					MaxAttempts: 1,
				}},
				EntryPhaseIds: []string{"phase-1"},
			},
		},
		ResourcesPerUnit: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1 << 30, AcceleratorUnits: 1},
		DesiredUnits:     1,
		RolloutMode:      tgsrlv1.RolloutMode_ROLLOUT_MODE_SYNC,
		PolicyRef:        "policy-1",
		DataKind:         tgsrlv1.DataKind_DATA_KIND_SYNTHETIC,
		RequiredCapabilities: &tgsrlv1.CapabilitySet{
			Names:            []string{"gpu"},
			Algorithms:       []string{"ppo"},
			RolloutModes:     []string{"ROLLOUT_MODE_SYNC"},
			SupportedActions: []string{"start", "pause"},
		},
	}
}
