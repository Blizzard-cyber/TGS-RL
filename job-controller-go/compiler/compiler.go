package compiler

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const ProtocolVersion = "v0.3"

var digestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

// NormalizeJob trims, validates, and deterministically assigns a job id when absent.
func NormalizeJob(job *tgsrlv1.RLTrainingJob, now time.Time) (*tgsrlv1.RLTrainingJob, []string) {
	if job == nil {
		return nil, []string{"job is required"}
	}
	normalized := cloneJob(job)
	normalized.DisplayName = strings.TrimSpace(normalized.GetDisplayName())
	normalized.ProtocolVersion = strings.TrimSpace(normalized.GetProtocolVersion())
	normalized.Algorithm = strings.TrimSpace(normalized.GetAlgorithm())
	normalized.Queue = strings.TrimSpace(normalized.GetQueue())
	normalized.ModelRef = strings.TrimSpace(normalized.GetModelRef())
	normalized.DatasetRef = strings.TrimSpace(normalized.GetDatasetRef())
	normalized.PolicyRef = strings.TrimSpace(normalized.GetPolicyRef())
	if normalized.Runtime != nil {
		normalized.Runtime.Framework = strings.TrimSpace(normalized.Runtime.GetFramework())
		normalized.Runtime.FrameworkVersion = strings.TrimSpace(normalized.Runtime.GetFrameworkVersion())
		normalized.Runtime.ExecutionBackend = strings.TrimSpace(normalized.Runtime.GetExecutionBackend())
		normalized.Runtime.ExecutionBackendVersion = strings.TrimSpace(normalized.Runtime.GetExecutionBackendVersion())
		normalized.Runtime.Trainer = strings.TrimSpace(normalized.Runtime.GetTrainer())
		normalized.Runtime.TrainerVersion = strings.TrimSpace(normalized.Runtime.GetTrainerVersion())
		normalized.Runtime.RolloutEngine = strings.TrimSpace(normalized.Runtime.GetRolloutEngine())
		normalized.Runtime.RolloutEngineVersion = strings.TrimSpace(normalized.Runtime.GetRolloutEngineVersion())
		normalized.Runtime.ImageDigest = strings.TrimSpace(normalized.Runtime.GetImageDigest())
		normalized.Runtime.ArtifactUri = strings.TrimSpace(normalized.Runtime.GetArtifactUri())
		sort.Strings(normalized.Runtime.PatchSet)
	}
	if normalized.RequiredCapabilities != nil {
		sort.Strings(normalized.RequiredCapabilities.Names)
		sort.Strings(normalized.RequiredCapabilities.Algorithms)
		sort.Strings(normalized.RequiredCapabilities.RolloutModes)
		sort.Strings(normalized.RequiredCapabilities.SupportedActions)
	}
	diagnostics := ValidateJob(normalized)
	normalized.State = tgsrlv1.JobState_JOB_STATE_PENDING
	if !normalized.GetCreatedAt().IsValid() {
		normalized.CreatedAt = timestamppb.New(now)
	}
	if strings.TrimSpace(normalized.GetJobId()) == "" {
		fingerprint := cloneJob(normalized)
		fingerprint.JobId = ""
		fingerprint.CreatedAt = nil
		fingerprint.State = tgsrlv1.JobState_JOB_STATE_UNKNOWN
		normalized.JobId = deterministicProtoID("job", fingerprint)
	} else {
		normalized.JobId = strings.TrimSpace(normalized.GetJobId())
	}
	return normalized, diagnostics
}

// ValidateJob enforces strict proto contract validation.
func ValidateJob(job *tgsrlv1.RLTrainingJob) []string {
	diagnostics := make([]string, 0)
	if job.GetProtocolVersion() != ProtocolVersion {
		diagnostics = append(diagnostics, fmt.Sprintf("protocol_version must be %q", ProtocolVersion))
	}
	if job.GetDisplayName() == "" {
		diagnostics = append(diagnostics, "display_name is required")
	}
	if job.GetAlgorithm() == "" {
		diagnostics = append(diagnostics, "algorithm is required")
	}
	if job.GetRolloutMode() == tgsrlv1.RolloutMode_ROLLOUT_MODE_UNKNOWN {
		diagnostics = append(diagnostics, "rollout_mode must be set")
	}
	if job.GetDataKind() == tgsrlv1.DataKind_DATA_KIND_UNKNOWN {
		diagnostics = append(diagnostics, "data_kind must be set")
	}
	if job.GetResourcesPerUnit() == nil {
		diagnostics = append(diagnostics, "resources_per_unit is required")
	} else {
		if job.GetResourcesPerUnit().GetCpuMillis() == 0 {
			diagnostics = append(diagnostics, "resources_per_unit.cpu_millis must be > 0")
		}
		if job.GetResourcesPerUnit().GetMemoryBytes() == 0 {
			diagnostics = append(diagnostics, "resources_per_unit.memory_bytes must be > 0")
		}
	}
	if job.GetDesiredUnits() == 0 {
		diagnostics = append(diagnostics, "desired_units must be > 0")
	}
	if job.GetRuntime() == nil {
		diagnostics = append(diagnostics, "runtime is required")
	} else {
		runtime := job.GetRuntime()
		requiredPins := map[string]string{
			"framework":                 runtime.GetFramework(),
			"framework_version":         runtime.GetFrameworkVersion(),
			"execution_backend":         runtime.GetExecutionBackend(),
			"execution_backend_version": runtime.GetExecutionBackendVersion(),
			"trainer":                   runtime.GetTrainer(),
			"trainer_version":           runtime.GetTrainerVersion(),
			"rollout_engine":            runtime.GetRolloutEngine(),
			"rollout_engine_version":    runtime.GetRolloutEngineVersion(),
			"image_digest":              runtime.GetImageDigest(),
		}
		for field, value := range requiredPins {
			if strings.TrimSpace(value) == "" {
				diagnostics = append(diagnostics, fmt.Sprintf("runtime.%s is required", field))
			}
		}
		if runtime.GetImageDigest() != "" && !digestPattern.MatchString(runtime.GetImageDigest()) {
			diagnostics = append(diagnostics, "runtime.image_digest must be an immutable sha256 digest")
		}
		if runtime.GetArtifactUri() != "" {
			expectedSuffix := "@" + runtime.GetImageDigest()
			if !strings.HasSuffix(runtime.GetArtifactUri(), expectedSuffix) || strings.HasPrefix(runtime.GetArtifactUri(), "@") {
				diagnostics = append(diagnostics, "runtime.artifact_uri must be an immutable OCI image reference ending in @runtime.image_digest")
			}
		}
	}
	if job.GetExecutionContract() == nil {
		diagnostics = append(diagnostics, "execution_contract is required")
	} else if job.GetExecutionContract().GetPhaseGraph() == nil || len(job.GetExecutionContract().GetPhaseGraph().GetPhases()) == 0 {
		diagnostics = append(diagnostics, "execution_contract.phase_graph must contain at least one phase")
	}
	return diagnostics
}

// NewRun deterministically compiles one run's immutable execution specification.
func NewRun(job *tgsrlv1.RLTrainingJob, attempt uint64, runState tgsrlv1.JobRunState, now time.Time) *tgsrlv1.JobRun {
	runID := deterministicID("run", job.GetJobId(), fmt.Sprintf("%d", attempt), deterministicProtoHash(job))
	return &tgsrlv1.JobRun{
		RunId:                runID,
		JobId:                job.GetJobId(),
		TraceId:              deterministicID("trace", job.GetJobId(), runID),
		DisplayName:          job.GetDisplayName(),
		State:                JobStateFromRun(runState),
		CreatedAt:            timestamppb.New(now),
		Attempt:              attempt,
		Runtime:              cloneRuntime(job.GetRuntime()),
		ExecutionContract:    cloneExecutionContract(job.GetExecutionContract()),
		RolloutMode:          job.GetRolloutMode(),
		DataKind:             job.GetDataKind(),
		PolicyVersion:        job.GetPolicyRef(),
		RandomSeed:           deterministicSeed(job.GetJobId(), attempt),
		Labels:               mergeMaps(job.GetLabels(), map[string]string{"controller.generation": fmt.Sprintf("%d", attempt)}),
		RunState:             runState,
		ResourcesPerUnit:     cloneResources(job.GetResourcesPerUnit()),
		RequiredCapabilities: cloneCapabilities(job.GetRequiredCapabilities()),
		DesiredUnits:         job.GetDesiredUnits(),
		Priority:             job.GetPriority(),
		Queue:                job.GetQueue(),
	}
}

// JobStateFromRun maps run lifecycle to top-level job state.
func JobStateFromRun(runState tgsrlv1.JobRunState) tgsrlv1.JobState {
	switch runState {
	case tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING:
		return tgsrlv1.JobState_JOB_STATE_RUNNING
	case tgsrlv1.JobRunState_JOB_RUN_STATE_PAUSED:
		return tgsrlv1.JobState_JOB_STATE_PAUSED
	case tgsrlv1.JobRunState_JOB_RUN_STATE_SUCCEEDED:
		return tgsrlv1.JobState_JOB_STATE_SUCCEEDED
	case tgsrlv1.JobRunState_JOB_RUN_STATE_FAILED:
		return tgsrlv1.JobState_JOB_STATE_FAILED
	case tgsrlv1.JobRunState_JOB_RUN_STATE_STOPPED, tgsrlv1.JobRunState_JOB_RUN_STATE_TERMINATED:
		return tgsrlv1.JobState_JOB_STATE_CANCELLED
	default:
		return tgsrlv1.JobState_JOB_STATE_PENDING
	}
}

func deterministicProtoHash(message proto.Message) string {
	if message == nil {
		return ""
	}
	payload, _ := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func deterministicProtoID(prefix string, message proto.Message) string {
	return prefix + "-" + deterministicProtoHash(message)[:16]
}

func deterministicID(prefix string, parts ...string) string {
	hasher := sha256.New()
	for _, part := range parts {
		_, _ = hasher.Write([]byte(part))
		_, _ = hasher.Write([]byte{0})
	}
	return prefix + "-" + hex.EncodeToString(hasher.Sum(nil))[:16]
}

func deterministicSeed(jobID string, attempt uint64) uint64 {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", jobID, attempt)))
	return binary.BigEndian.Uint64(sum[:8])
}

func mergeMaps(base, overlay map[string]string) map[string]string {
	merged := map[string]string{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overlay {
		merged[key] = value
	}
	return merged
}

func cloneJob(job *tgsrlv1.RLTrainingJob) *tgsrlv1.RLTrainingJob {
	if job == nil {
		return nil
	}
	return proto.Clone(job).(*tgsrlv1.RLTrainingJob)
}

func cloneRuntime(runtime *tgsrlv1.FrameworkRuntimeSpec) *tgsrlv1.FrameworkRuntimeSpec {
	if runtime == nil {
		return nil
	}
	return proto.Clone(runtime).(*tgsrlv1.FrameworkRuntimeSpec)
}

func cloneExecutionContract(contract *tgsrlv1.ExecutionContract) *tgsrlv1.ExecutionContract {
	if contract == nil {
		return nil
	}
	return proto.Clone(contract).(*tgsrlv1.ExecutionContract)
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
