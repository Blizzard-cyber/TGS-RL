package compiler

import (
	"errors"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

var (
	errNilRun      = errors.New("job run is required")
	errNilManifest = errors.New("runtime manifest is required")
	errNilPlan     = errors.New("placement plan is required")

	supportedGPUProfiles = map[string]struct{}{
		GPUProfileNone:               {},
		GPUProfileNVIDIADevicePlugin: {},
		GPUProfileKubernetesDRA:      {},
		GPUProfileVolcanoHAMI:        {},
	}
)

type CompileInput struct {
	Namespace       string
	GPUProfiles     []string
	Generation      uint64
	JobRun          *tgsrlv1.JobRun
	RuntimeManifest *tgsrlv1.RuntimeManifest
	PlacementPlan   *tgsrlv1.PlacementPlan
}

type RuntimeClassConfig struct {
	Name    string
	Handler string
	Create  bool
}

type RuntimeConfig struct {
	RuntimeClass RuntimeClassConfig
	NodeSelector map[string]string
}

type CapabilityProfile struct {
	GPUProfile     string
	DRADeviceIDs   map[string]bool
	RuntimeClass   RuntimeClassConfig
	NodeSelector   map[string]string
	KubernetesAPIs KubernetesAPIVersions
}

type normalizedInput struct {
	Namespace        string
	GPUProfile       string
	Generation       uint64
	Run              *tgsrlv1.JobRun
	Manifest         *tgsrlv1.RuntimeManifest
	Plan             *tgsrlv1.PlacementPlan
	Runtime          RuntimeConfig
	KubernetesAPIs   KubernetesAPIVersions
	priority         int32
	resourcesPerUnit *tgsrlv1.ResourceVector
	workloadUnitID   string
	binding          *tgsrlv1.Binding
}

func (in CompileInput) ManifestHasNoImageDigests() bool {
	return len(in.RuntimeManifest.GetImageDigests()) == 0
}
