package compiler

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

type CapabilitySet struct {
	GPUProfiles          map[string]bool
	ExactDevicePlacement map[string]bool
	DRADevices           map[string]DRADevice
	HAMIDevices          map[string]HAMIDevice
	RuntimeClasses       map[string]string
	NodeSelectors        map[string]map[string]string
	DefaultNodeSelector  map[string]string
	KubernetesAPIs       KubernetesAPIVersions
}

// HAMIDevice is the scheduler-visible physical accelerator identity published
// by a HAMi device plugin through the node registration annotation.
type HAMIDevice struct {
	UUID        string
	Node        string
	Model       string
	Mode        string
	MemoryBytes uint64
	CorePercent int32
	SplitCount  int32
	Healthy     bool
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
	return selectCapabilityProfile(requested, preferredGPUProfiles, nil, 0, discovered, false)
}

// SelectCapabilityProfileForDevices chooses the first discovered profile that
// can faithfully materialize the scheduler-selected device identities. This
// makes the preference list useful in heterogeneous clusters: a DRA-backed
// device can use kubernetes-dra while a non-MIG physical device registered by
// HAMi can use hami-vgpu without treating absent MIG support as a node failure.
func SelectCapabilityProfileForDevices(requested RuntimeConfig, preferredGPUProfiles, deviceIDs []string, acceleratorUnits float64, discovered CapabilitySet) (CapabilityProfile, error) {
	return selectCapabilityProfile(requested, preferredGPUProfiles, deviceIDs, acceleratorUnits, discovered, true)
}

func selectCapabilityProfile(requested RuntimeConfig, preferredGPUProfiles, deviceIDs []string, acceleratorUnits float64, discovered CapabilitySet, deviceAware bool) (CapabilityProfile, error) {
	if len(discovered.GPUProfiles) == 0 {
		discovered = DefaultCapabilitySet()
	}
	selected := CapabilityProfile{
		GPUProfile:     GPUProfileNone,
		DRADevices:     cloneDRADevices(discovered.DRADevices),
		HAMIDevices:    cloneHAMIDevices(discovered.HAMIDevices),
		RuntimeClass:   requested.RuntimeClass,
		NodeSelector:   cloneStringMap(requested.NodeSelector),
		Bootstrap:      requested.Bootstrap,
		KubernetesAPIs: discovered.KubernetesAPIs,
	}
	for _, profile := range preferredGPUProfiles {
		compatible := discovered.GPUProfiles[profile]
		if deviceAware {
			compatible = compatible && profileSupportsRequest(profile, deviceIDs, acceleratorUnits, discovered)
		}
		if compatible {
			selected.GPUProfile = profile
			break
		}
	}
	if deviceAware && selected.GPUProfile == GPUProfileNone && acceleratorUnits > 0 {
		return CapabilityProfile{}, fmt.Errorf(
			"none of the preferred GPU profiles %q can materialize device_ids %q with accelerator_units %g",
			strings.Join(preferredGPUProfiles, ","),
			strings.Join(deviceIDs, ","),
			acceleratorUnits,
		)
	}
	if selected.GPUProfile == "" {
		selected.GPUProfile = GPUProfileNone
	}
	if selected.GPUProfile != GPUProfileNone && !discovered.ExactDevicePlacement[selected.GPUProfile] {
		return CapabilityProfile{}, fmt.Errorf("GPU profile %q cannot enforce scheduler-selected device identities", selected.GPUProfile)
	}
	if len(selected.NodeSelector) == 0 {
		if profileSelector := cloneStringMap(discovered.NodeSelectors[selected.GPUProfile]); len(profileSelector) != 0 {
			selected.NodeSelector = profileSelector
		} else {
			selected.NodeSelector = cloneStringMap(discovered.DefaultNodeSelector)
		}
	}
	if IsHAMIGPUProfile(selected.GPUProfile) && len(deviceIDs) == 1 {
		device := discovered.HAMIDevices[strings.TrimSpace(deviceIDs[0])]
		if selected.NodeSelector == nil {
			selected.NodeSelector = make(map[string]string)
		}
		const hostnameLabel = "kubernetes.io/hostname"
		if hostname := strings.TrimSpace(selected.NodeSelector[hostnameLabel]); hostname != "" && hostname != device.Node {
			return CapabilityProfile{}, fmt.Errorf("HAMi device %q belongs to node %q, which conflicts with node selector %q", device.UUID, device.Node, hostname)
		}
		selected.NodeSelector[hostnameLabel] = device.Node
	}
	if selected.KubernetesAPIs.KueueWorkload == "" {
		return CapabilityProfile{}, fmt.Errorf("kueue Workload API is not discoverable")
	}
	if !supportedKueueWorkloadAPI(selected.KubernetesAPIs.KueueWorkload) {
		return CapabilityProfile{}, fmt.Errorf("unsupported Kueue Workload API %q", selected.KubernetesAPIs.KueueWorkload)
	}
	if selected.GPUProfile == GPUProfileKubernetesDRA && selected.KubernetesAPIs.DRAResourceClaim == "" {
		return CapabilityProfile{}, fmt.Errorf("kubernetes DRA ResourceClaim API is not discoverable")
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
	return selected, nil
}

func profileSupportsRequest(profile string, deviceIDs []string, acceleratorUnits float64, discovered CapabilitySet) bool {
	if profile == GPUProfileNone {
		return len(deviceIDs) == 0 && acceleratorUnits == 0
	}
	switch profile {
	case GPUProfileKubernetesDRA:
		if !discovered.ExactDevicePlacement[profile] || len(deviceIDs) == 0 || acceleratorUnits <= 0 || acceleratorUnits != math.Trunc(acceleratorUnits) {
			return false
		}
		for _, deviceID := range deviceIDs {
			if _, ok := discovered.DRADevices[strings.TrimSpace(deviceID)]; !ok {
				return false
			}
		}
		return true
	case GPUProfileHAMIVGPU:
		if !discovered.ExactDevicePlacement[profile] || len(deviceIDs) != 1 || acceleratorUnits <= 0 || acceleratorUnits > 1 {
			return false
		}
		for _, deviceID := range deviceIDs {
			device, ok := discovered.HAMIDevices[strings.TrimSpace(deviceID)]
			if !ok || !usableHAMIVGPUDevice(device) {
				return false
			}
		}
		return true
	default:
		return acceleratorUnits > 0 && (len(deviceIDs) == 0 || discovered.ExactDevicePlacement[profile])
	}
}

func usableHAMIVGPUDevice(device HAMIDevice) bool {
	return strings.HasPrefix(device.UUID, "GPU-") &&
		device.Node != "" &&
		device.Model != "" &&
		device.Mode == "hami-core" &&
		device.MemoryBytes > 0 &&
		device.CorePercent > 0 &&
		device.SplitCount > 0 &&
		device.Healthy
}

// HasUsableHAMIVGPUDevice reports whether discovery contains at least one
// healthy physical NVIDIA device that can satisfy the hami-vgpu contract.
func HasUsableHAMIVGPUDevice(inventory map[string]HAMIDevice) bool {
	for _, device := range inventory {
		if usableHAMIVGPUDevice(device) {
			return true
		}
	}
	return false
}

func selectedHAMIDevices(deviceIDs []string, inventory map[string]HAMIDevice) ([]HAMIDevice, error) {
	ids, err := concreteDeviceIDs(deviceIDs)
	if err != nil {
		return nil, err
	}
	result := make([]HAMIDevice, 0, len(ids))
	for _, deviceID := range ids {
		device, ok := inventory[deviceID]
		if !ok {
			return nil, fmt.Errorf("references HAMi NVIDIA device UUID %q that was not discovered", deviceID)
		}
		if device.UUID != deviceID || !usableHAMIVGPUDevice(device) {
			return nil, fmt.Errorf("HAMi NVIDIA device UUID %q has incomplete identity metadata", deviceID)
		}
		result = append(result, device)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].UUID < result[j].UUID })
	return result, nil
}

func IsHAMIGPUProfile(profile string) bool {
	return profile == GPUProfileHAMIVGPU
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

func cloneHAMIDevices(src map[string]HAMIDevice) map[string]HAMIDevice {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]HAMIDevice, len(src))
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
