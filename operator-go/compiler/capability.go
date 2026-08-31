package compiler

import (
	"fmt"
	"sort"
)

type CapabilitySet struct {
	GPUProfiles          map[string]bool
	ExactDevicePlacement map[string]bool
	DRADevices           map[string]DRADevice
	RuntimeClasses       map[string]string
	NodeSelectors        map[string]map[string]string
	DefaultNodeSelector  map[string]string
	KubernetesAPIs       KubernetesAPIVersions
}

// DRADevice is the identity metadata required to project and verify one
// scheduler-selected NVIDIA device through Kubernetes DRA.
type DRADevice struct {
	UUID        string
	Type        string
	DeviceClass string
	Driver      string
	Pool        string
	Device      string
	Profile     string
	ParentUUID  string
}

// KubernetesAPIVersions records the concrete wire contracts selected from API
// discovery. These values are kept separate from GPU availability: a cluster
// may serve the DRA API without exposing any GPU DeviceClass.
type KubernetesAPIVersions struct {
	KueueWorkload    string `json:"kueueWorkload"`
	DRAResourceClaim string `json:"draResourceClaim"`
}

const (
	KueueWorkloadV1Beta2    = "kueue.x-k8s.io/v1beta2"
	KueueWorkloadV1Beta1    = "kueue.x-k8s.io/v1beta1"
	DRAResourceClaimV1      = "resource.k8s.io/v1"
	DRAResourceClaimV1Beta2 = "resource.k8s.io/v1beta2"
	DRAResourceClaimV1Beta1 = "resource.k8s.io/v1beta1"
)

func DefaultCapabilitySet() CapabilitySet {
	return CapabilitySet{
		GPUProfiles: map[string]bool{
			GPUProfileNone: true,
		},
		KubernetesAPIs: KubernetesAPIVersions{
			KueueWorkload:    KueueWorkloadV1Beta2,
			DRAResourceClaim: DRAResourceClaimV1,
		},
	}
}

func SelectCapabilityProfile(requested RuntimeConfig, preferredGPUProfiles []string, discovered CapabilitySet) (CapabilityProfile, error) {
	if len(discovered.GPUProfiles) == 0 {
		discovered = DefaultCapabilitySet()
	}
	selected := CapabilityProfile{
		GPUProfile:     GPUProfileNone,
		DRADevices:     cloneDRADevices(discovered.DRADevices),
		RuntimeClass:   requested.RuntimeClass,
		NodeSelector:   cloneStringMap(requested.NodeSelector),
		KubernetesAPIs: discovered.KubernetesAPIs,
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
	if selected.GPUProfile != GPUProfileNone && !discovered.ExactDevicePlacement[selected.GPUProfile] {
		return CapabilityProfile{}, fmt.Errorf("GPU profile %q cannot enforce scheduler-selected device identities", selected.GPUProfile)
	}
	if selected.KubernetesAPIs.KueueWorkload == "" {
		return CapabilityProfile{}, fmt.Errorf("Kueue Workload API is not discoverable")
	}
	if !supportedKueueWorkloadAPI(selected.KubernetesAPIs.KueueWorkload) {
		return CapabilityProfile{}, fmt.Errorf("unsupported Kueue Workload API %q", selected.KubernetesAPIs.KueueWorkload)
	}
	if selected.GPUProfile == GPUProfileKubernetesDRA && selected.KubernetesAPIs.DRAResourceClaim == "" {
		return CapabilityProfile{}, fmt.Errorf("Kubernetes DRA ResourceClaim API is not discoverable")
	}
	if selected.GPUProfile == GPUProfileKubernetesDRA && !supportedDRAResourceClaimAPI(selected.KubernetesAPIs.DRAResourceClaim) {
		return CapabilityProfile{}, fmt.Errorf("unsupported Kubernetes DRA ResourceClaim API %q", selected.KubernetesAPIs.DRAResourceClaim)
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

func selectedDRADevices(deviceIDs []string, inventory map[string]DRADevice) ([]DRADevice, error) {
	result := make([]DRADevice, 0, len(deviceIDs))
	deviceClass := ""
	for _, deviceID := range deviceIDs {
		device, ok := inventory[deviceID]
		if !ok {
			return nil, fmt.Errorf("references NVIDIA DRA device UUID %q that was not discovered", deviceID)
		}
		if device.UUID != deviceID {
			return nil, fmt.Errorf("NVIDIA DRA inventory key %q identifies UUID %q", deviceID, device.UUID)
		}
		wantClass := nvidiaDRAClassForType(device.Type)
		if device.Driver != NVIDIADRADriver || wantClass == "" || device.DeviceClass != wantClass || device.Pool == "" || device.Device == "" {
			return nil, fmt.Errorf("NVIDIA DRA device UUID %q has incomplete identity metadata", deviceID)
		}
		if device.Type == "mig" && (device.Profile == "" || device.ParentUUID == "") {
			return nil, fmt.Errorf("NVIDIA DRA MIG device UUID %q has incomplete profile or parent UUID", deviceID)
		}
		if deviceClass == "" {
			deviceClass = device.DeviceClass
		} else if deviceClass != device.DeviceClass {
			return nil, fmt.Errorf("mixes DRA device classes %q and %q", deviceClass, device.DeviceClass)
		}
		result = append(result, device)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].UUID < result[j].UUID })
	return result, nil
}

func nvidiaDRAClassForType(deviceType string) string {
	switch deviceType {
	case "gpu":
		return NVIDIADRAFullGPUDeviceClass
	case "mig":
		return NVIDIADRAMIGDeviceClass
	default:
		return ""
	}
}

func cloneDRADevices(src map[string]DRADevice) map[string]DRADevice {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]DRADevice, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneBoolMap(src map[string]bool) map[string]bool {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]bool, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func supportedKueueWorkloadAPI(value string) bool {
	return value == KueueWorkloadV1Beta2 || value == KueueWorkloadV1Beta1
}

func supportedDRAResourceClaimAPI(value string) bool {
	return value == DRAResourceClaimV1 || value == DRAResourceClaimV1Beta2 || value == DRAResourceClaimV1Beta1
}
