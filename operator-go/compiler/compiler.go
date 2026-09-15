package compiler

import (
	"fmt"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"google.golang.org/protobuf/proto"
)

const (
	GPUProfileNone               = "none"
	GPUProfileNVIDIADevicePlugin = "nvidia-device-plugin"
	GPUProfileKubernetesDRA      = "kubernetes-dra"
	GPUProfileHAMIVGPU           = "hami-vgpu"
	// GPUProfileVolcanoHAMI is retained as a compatibility alias for manifests
	// written before TGS-RL adopted HAMi's canonical NVIDIA resource contract.
	GPUProfileVolcanoHAMI         = "volcano-hami"
	NVIDIADRAFullGPUDeviceClass   = "gpu.nvidia.com"
	NVIDIADRAMIGDeviceClass       = "mig.nvidia.com"
	NVIDIADRADriver               = "gpu.nvidia.com"
	HAMINVIDIAResource            = "nvidia.com/gpu"
	HAMINVIDIACoreResource        = "nvidia.com/gpucores"
	HAMINVIDIAMemoryPercent       = "nvidia.com/gpumem-percentage"
	HAMINVIDIAUseUUIDAnnotation   = "nvidia.com/use-gpuuuid"
	HAMINVIDIAModeAnnotation      = "nvidia.com/vgpu-mode"
	HAMINVIDIAAllocatedAnnotation = "hami.io/vgpu-devices-allocated"
	HAMINVIDIARegisterAnnotation  = "hami.io/node-nvidia-register"
	HAMISchedulerName             = "hami-scheduler"
	HAMIExpectedCoreAnnotation    = "tgsrl.io/hami-core-percent"
	HAMIExpectedMemoryAnnotation  = "tgsrl.io/hami-memory-mib"
)

type Compiler struct {
	runtimeConfig RuntimeConfig
	capabilities  CapabilitySet
}

func New() *Compiler {
	return &Compiler{capabilities: DefaultCapabilitySet()}
}

func NewWithRuntimeConfig(config RuntimeConfig) (*Compiler, error) {
	validated, err := ValidateRuntimeConfig(config)
	if err != nil {
		return nil, err
	}
	return &Compiler{runtimeConfig: validated, capabilities: DefaultCapabilitySet()}, nil
}

func (c *Compiler) SetCapabilities(capabilities CapabilitySet) {
	if c == nil {
		return
	}
	c.capabilities = capabilities
}

func (c *Compiler) compileBinding(input CompileInput) (*api.Bundle, error) {
	if _, err := validateGPUProfiles(input.GPUProfiles); err != nil {
		return nil, err
	}
	binding, err := workloadBinding(input.PlacementPlan)
	if err != nil {
		return nil, err
	}
	selected, err := SelectCapabilityProfileForDevices(c.runtimeConfig, input.GPUProfiles, binding.GetDeviceIds(), binding.GetResources().GetAcceleratorUnits(), c.capabilities)
	if err != nil {
		return nil, err
	}
	input.GPUProfiles = []string{selected.GPUProfile}
	normalized, err := normalize(input, RuntimeConfig{RuntimeClass: selected.RuntimeClass, NodeSelector: selected.NodeSelector, Bootstrap: selected.Bootstrap})
	if err != nil {
		return nil, err
	}
	normalized.KubernetesAPIs = selected.KubernetesAPIs
	if normalized.GPUProfile == GPUProfileKubernetesDRA {
		devices, err := selectedDRADevices(normalized.binding.GetDeviceIds(), selected.DRADevices)
		if err != nil {
			return nil, fmt.Errorf("binding %q: %w", normalized.binding.GetBindingId(), err)
		}
		normalized.draDevices = devices
	}
	if IsHAMIGPUProfile(normalized.GPUProfile) {
		devices, err := selectedHAMIDevices(normalized.binding.GetDeviceIds(), selected.HAMIDevices)
		if err != nil {
			return nil, fmt.Errorf("binding %q: %w", normalized.binding.GetBindingId(), err)
		}
		normalized.hamiDevices = devices
	}
	bundle, err := buildBundle(normalized)
	if err != nil {
		return nil, err
	}
	if err := validateBundle(bundle); err != nil {
		return nil, err
	}
	fingerprint, err := bundleFingerprint(bundle)
	if err != nil {
		return nil, err
	}
	bundle.Fingerprint = fingerprint
	return bundle, nil
}

// Compile projects each concrete scheduler binding into one independently
// fenced workload bundle. PlacementPlan.bindings is an incremental mutation
// surface, not a complete stage replica set, so combining several bindings in
// one Job would lose per-unit identity and make later rebind/recreate unsafe.
func (c *Compiler) Compile(input CompileInput) ([]*api.Bundle, error) {
	if input.PlacementPlan == nil {
		return nil, errNilPlan
	}
	bindings := input.PlacementPlan.GetBindings()
	if len(bindings) == 0 {
		return nil, fmt.Errorf("placement plan requires at least one binding")
	}
	orderedBindings := append([]*tgsrlv1.Binding(nil), bindings...)
	sort.Slice(orderedBindings, func(i, j int) bool {
		if orderedBindings[i].GetPendingUnitId() != orderedBindings[j].GetPendingUnitId() {
			return orderedBindings[i].GetPendingUnitId() < orderedBindings[j].GetPendingUnitId()
		}
		return orderedBindings[i].GetBindingId() < orderedBindings[j].GetBindingId()
	})
	ranks := make(map[string]int, len(orderedBindings))
	for rank, binding := range orderedBindings {
		if binding != nil {
			ranks[binding.GetBindingId()] = rank
		}
	}
	bundles := make([]*api.Bundle, 0, len(bindings))
	for index, binding := range bindings {
		if binding == nil {
			return nil, fmt.Errorf("placement plan binding %d is nil", index)
		}
		plan := proto.Clone(input.PlacementPlan).(*tgsrlv1.PlacementPlan)
		plan.Bindings = []*tgsrlv1.Binding{proto.Clone(binding).(*tgsrlv1.Binding)}
		plan.Actions = actionsForBinding(input.PlacementPlan, binding)
		unitInput := input
		unitInput.PlacementPlan = plan
		unitInput.replicaIndex = ranks[binding.GetBindingId()]
		unitInput.replicaCount = len(orderedBindings)
		if binding.GetGeneration() != 0 {
			unitInput.Generation = binding.GetGeneration()
		}
		bundle, err := c.compileBinding(unitInput)
		if err != nil {
			return nil, fmt.Errorf("compile binding %q: %w", binding.GetBindingId(), err)
		}
		bundles = append(bundles, bundle)
	}
	return bundles, nil
}

func actionsForBinding(plan *tgsrlv1.PlacementPlan, binding *tgsrlv1.Binding) []*tgsrlv1.Action {
	actions := make([]*tgsrlv1.Action, 0, 1)
	for _, action := range plan.GetActions() {
		if action == nil {
			continue
		}
		actionBinding := action.GetBinding()
		matches := actionBinding.GetBindingId() == binding.GetBindingId() ||
			action.GetSandboxId() != "" && action.GetSandboxId() == binding.GetSandboxId() ||
			action.GetTargetId() != "" && (action.GetTargetId() == binding.GetRuntimeUnitId() || action.GetTargetId() == binding.GetPendingUnitId())
		if matches {
			actions = append(actions, proto.Clone(action).(*tgsrlv1.Action))
		}
	}
	return actions
}
