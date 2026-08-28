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
	Namespace        string
	GPUProfiles      []string
	Generation       uint64
	JobRun           *tgsrlv1.JobRun
	RuntimeManifest  *tgsrlv1.RuntimeManifest
	PlacementPlan    *tgsrlv1.PlacementPlan
	AdmissionAllowed bool
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

type normalizedInput struct {
	Namespace        string
	GPUProfile       string
	Generation       uint64
	Run              *tgsrlv1.JobRun
	Manifest         *tgsrlv1.RuntimeManifest
	Plan             *tgsrlv1.PlacementPlan
	AdmissionAllowed bool
	Runtime          RuntimeConfig
	parallelism      uint32
	priority         int32
	acceleratorUnits float64
}

func (in CompileInput) ManifestHasNoImageDigests() bool {
	return len(in.RuntimeManifest.GetImageDigests()) == 0
}
