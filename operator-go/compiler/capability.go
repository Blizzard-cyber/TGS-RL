package compiler

import "fmt"

type CapabilitySet struct {
	GPUProfiles         map[string]bool
	RuntimeClasses      map[string]string
	NodeSelectors       map[string]map[string]string
	DefaultNodeSelector map[string]string
}

func DefaultCapabilitySet() CapabilitySet {
	return CapabilitySet{
		GPUProfiles: map[string]bool{
			GPUProfileNone: true,
		},
	}
}

func SelectCapabilityProfile(requested RuntimeConfig, preferredGPUProfiles []string, discovered CapabilitySet) (CapabilityProfile, error) {
	if len(discovered.GPUProfiles) == 0 {
		discovered = DefaultCapabilitySet()
	}
	selected := CapabilityProfile{
		GPUProfile:   GPUProfileNone,
		RuntimeClass: requested.RuntimeClass,
		NodeSelector: cloneStringMap(requested.NodeSelector),
	}
	for _, profile := range preferredGPUProfiles {
		if discovered.GPUProfiles[profile] {
			selected.GPUProfile = profile
			break
		}
	}
	if selected.GPUProfile == "" {
		selected.GPUProfile = GPUProfileNone
	}
	if selected.RuntimeClass.Name != "" {
		if selected.RuntimeClass.Create {
			if handler, ok := discovered.RuntimeClasses[selected.RuntimeClass.Name]; ok && handler != "" && handler != selected.RuntimeClass.Handler {
				return CapabilityProfile{}, fmt.Errorf("runtime class %q capability conflict: existing handler %q differs from requested %q", selected.RuntimeClass.Name, handler, selected.RuntimeClass.Handler)
			}
		} else if discovered.RuntimeClasses != nil {
			if _, ok := discovered.RuntimeClasses[selected.RuntimeClass.Name]; !ok {
				return CapabilityProfile{}, fmt.Errorf("runtime class %q is not discoverable", selected.RuntimeClass.Name)
			}
		}
	}
	if len(selected.NodeSelector) == 0 {
		if profileSelector := cloneStringMap(discovered.NodeSelectors[selected.GPUProfile]); len(profileSelector) != 0 {
			selected.NodeSelector = profileSelector
		} else {
			selected.NodeSelector = cloneStringMap(discovered.DefaultNodeSelector)
		}
	}
	return selected, nil
}
