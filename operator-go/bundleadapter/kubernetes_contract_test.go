package bundleadapter

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

func TestObserveHAMIAllocationMatchesSchedulerUUID(t *testing.T) {
	bundle := &api.Bundle{
		GPUProfile:     compiler.GPUProfileHAMIVGPU,
		RuntimeTargets: []api.RuntimeTarget{{DeviceIDs: []string{"GPU-a10"}}},
		Job: api.Job{Spec: api.JobSpec{Template: api.PodTemplateSpec{ObjectMeta: api.ObjectMeta{Annotations: map[string]string{
			compiler.HAMIExpectedMemoryAnnotation: "9211",
			compiler.HAMIExpectedCoreAnnotation:   "40",
		}}}}},
	}
	allocated, deviceIDs, err := observeHAMIAllocation(
		bundle,
		[]byte(`{"items":[{"metadata":{"annotations":{"hami.io/vgpu-devices-allocated":"GPU-a10,NVIDIA,9211,40:;"}}}]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !allocated || !slices.Equal(deviceIDs, []string{"GPU-a10"}) {
		t.Fatalf("allocated=%v deviceIDs=%v", allocated, deviceIDs)
	}
}

func TestObserveHAMIAllocationRejectsUUIDMismatch(t *testing.T) {
	bundle := &api.Bundle{
		GPUProfile:     compiler.GPUProfileHAMIVGPU,
		RuntimeTargets: []api.RuntimeTarget{{DeviceIDs: []string{"GPU-expected"}}},
		Job: api.Job{Spec: api.JobSpec{Template: api.PodTemplateSpec{ObjectMeta: api.ObjectMeta{Annotations: map[string]string{
			compiler.HAMIExpectedMemoryAnnotation: "9211",
			compiler.HAMIExpectedCoreAnnotation:   "40",
		}}}}},
	}
	_, _, err := observeHAMIAllocation(
		bundle,
		[]byte(`{"items":[{"metadata":{"annotations":{"hami.io/vgpu-devices-allocated":"GPU-other,NVIDIA,9211,40:;"}}}]}`),
	)
	if err == nil || !strings.Contains(err.Error(), "want binding device_ids") {
		t.Fatalf("HAMi mismatch error = %v", err)
	}
}

func TestObserveHAMIAllocationRejectsMIGIdentity(t *testing.T) {
	bundle := &api.Bundle{
		GPUProfile:     compiler.GPUProfileHAMIVGPU,
		RuntimeTargets: []api.RuntimeTarget{{DeviceIDs: []string{"GPU-expected"}}},
		Job: api.Job{Spec: api.JobSpec{Template: api.PodTemplateSpec{ObjectMeta: api.ObjectMeta{Annotations: map[string]string{
			compiler.HAMIExpectedMemoryAnnotation: "9211",
			compiler.HAMIExpectedCoreAnnotation:   "40",
		}}}}},
	}
	_, _, err := observeHAMIAllocation(
		bundle,
		[]byte(`{"items":[{"metadata":{"annotations":{"hami.io/vgpu-devices-allocated":"MIG-a100/1/0,NVIDIA,9211,40:;"}}}]}`),
	)
	if err == nil || !strings.Contains(err.Error(), "not a physical GPU UUID") {
		t.Fatalf("HAMi MIG identity error = %v", err)
	}
}

func TestObserveHAMIAllocationRejectsShareMismatch(t *testing.T) {
	bundle := &api.Bundle{
		GPUProfile:     compiler.GPUProfileHAMIVGPU,
		RuntimeTargets: []api.RuntimeTarget{{DeviceIDs: []string{"GPU-a10"}}},
		Job: api.Job{Spec: api.JobSpec{Template: api.PodTemplateSpec{ObjectMeta: api.ObjectMeta{Annotations: map[string]string{
			compiler.HAMIExpectedMemoryAnnotation: "9211",
			compiler.HAMIExpectedCoreAnnotation:   "40",
		}}}}},
	}
	_, _, err := observeHAMIAllocation(
		bundle,
		[]byte(`{"items":[{"metadata":{"annotations":{"hami.io/vgpu-devices-allocated":"GPU-a10,NVIDIA,9211,39:;"}}}]}`),
	)
	if err == nil || !strings.Contains(err.Error(), "want 9211/40") {
		t.Fatalf("HAMi share mismatch error = %v", err)
	}
}

func TestMaterializeSanitizesKubernetesDesiredObjects(t *testing.T) {
	bundle := &api.Bundle{
		Key: "test/bundle-a", Namespace: "test", Generation: 7,
		Workload:     api.Workload{TypeMeta: api.TypeMeta{APIVersion: "kueue.x-k8s.io/v1beta1", Kind: "Workload"}, ObjectMeta: api.ObjectMeta{Name: "workload-a", Namespace: "test", UID: "server-uid", Generation: 9}, Spec: api.WorkloadSpec{QueueName: "default", PodSets: []api.PodSet{{Name: "main", Count: 1, Template: testPodTemplate()}}}, Status: api.WorkloadStatus{Admitted: true}},
		Job:          api.Job{TypeMeta: api.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: api.ObjectMeta{Name: "job-a", Namespace: "test", UID: "server-uid", Generation: 9}, Spec: api.JobSpec{Parallelism: 1, Completions: 1, Template: testPodTemplate()}, Status: api.JobStatus{Active: 1}},
		RuntimeClass: &api.RuntimeClass{TypeMeta: api.TypeMeta{APIVersion: "node.k8s.io/v1", Kind: "RuntimeClass"}, ObjectMeta: api.ObjectMeta{Name: "runtime-a", Namespace: "test", UID: "server-uid", Generation: 9}, Handler: "runc"},
	}
	objects, err := NewKubernetes(0).Materialize(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 4 {
		t.Fatalf("materialized objects = %d, want bundle, runtime class, workload, job", len(objects))
	}
	for _, object := range objects[1:] {
		var payload map[string]any
		if err := json.Unmarshal(object.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		metadata, _ := payload["metadata"].(map[string]any)
		for _, field := range []string{"uid", "generation", "resourceVersion", "ownerReferences"} {
			if _, ok := metadata[field]; ok {
				t.Fatalf("%s desired payload contains metadata.%s: %s", object.Kind, field, object.Payload)
			}
		}
		if _, ok := payload["status"]; ok {
			t.Fatalf("%s desired payload contains status: %s", object.Kind, object.Payload)
		}
		if object.Kind == "RuntimeClass" {
			if _, ok := metadata["namespace"]; ok {
				t.Fatalf("RuntimeClass desired payload contains namespace: %s", object.Payload)
			}
		}
	}
}

func TestDiscoverDRADevicesHandlesTypesGenerationsAndAttributeLayouts(t *testing.T) {
	payload := []byte(`{"items":[
		{"spec":{"driver":"gpu.nvidia.com","pool":{"name":"node-a","generation":1},"devices":[{"name":"old","attributes":{"uuid":{"string":"GPU-old"},"type":{"string":"gpu"}}}]}},
		{"spec":{"driver":"gpu.nvidia.com","pool":{"name":"node-a","generation":2},"devices":[
			{"name":"gpu-0","attributes":{"type":{"string":"gpu"}},"basic":{"attributes":{"uuid":{"string":"GPU-aaaa"}}}},
			{"name":"mig-0","basic":{"attributes":{"uuid":{"string":"MIG-aaaa"},"type":{"string":"mig"},"profile":{"string":"1g.10gb"},"parentUUID":{"string":"GPU-aaaa"}}}},
			{"name":"vfio-0","attributes":{"uuid":{"string":"VFIO-aaaa"},"type":{"string":"vfio"}}}
		]}}
	]}`)
	devices, err := DiscoverDRADevices(payload, compiler.NVIDIADRADriver)
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != 2 || devices["GPU-old"].UUID != "" {
		t.Fatalf("devices = %+v, stale pool generation must be ignored", devices)
	}
	if got := devices["GPU-aaaa"].DeviceClass; got != compiler.NVIDIADRAFullGPUDeviceClass {
		t.Fatalf("full GPU device class = %q", got)
	}
	mig := devices["MIG-aaaa"]
	if mig.DeviceClass != compiler.NVIDIADRAMIGDeviceClass || mig.Profile != "1g.10gb" || mig.ParentUUID != "GPU-aaaa" {
		t.Fatalf("MIG metadata = %+v", mig)
	}
}

func TestDiscoverDRADevicesRejectsAttributeConflictsAndDuplicateUUIDs(t *testing.T) {
	conflict := []byte(`{"items":[{"spec":{"driver":"gpu.nvidia.com","pool":{"name":"node-a","generation":1},"devices":[{"name":"gpu-0","attributes":{"uuid":{"string":"GPU-a"}},"basic":{"attributes":{"uuid":{"string":"GPU-b"},"type":{"string":"gpu"}}}}]}}]}`)
	if _, err := DiscoverDRADevices(conflict, compiler.NVIDIADRADriver); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("attribute conflict error = %v", err)
	}
	duplicate := []byte(`{"items":[{"spec":{"driver":"gpu.nvidia.com","pool":{"name":"node-a","generation":1},"devices":[{"name":"gpu-0","attributes":{"uuid":{"string":"GPU-a"},"type":{"string":"gpu"}}},{"name":"gpu-1","attributes":{"uuid":{"string":"GPU-a"},"type":{"string":"gpu"}}}]}}]}`)
	if _, err := DiscoverDRADevices(duplicate, compiler.NVIDIADRADriver); err == nil || !strings.Contains(err.Error(), "multiple devices") {
		t.Fatalf("duplicate UUID error = %v", err)
	}
	unknown := []byte(`{"items":[{"spec":{"driver":"gpu.nvidia.com","pool":{"name":"node-a","generation":1},"devices":[{"name":"future-0","attributes":{"uuid":{"string":"future-a"},"type":{"string":"future"}}}]}}]}`)
	if _, err := DiscoverDRADevices(unknown, compiler.NVIDIADRADriver); err == nil || !strings.Contains(err.Error(), "unsupported type") {
		t.Fatalf("unknown device type error = %v", err)
	}
	missingMIGMetadata := []byte(`{"items":[{"spec":{"driver":"gpu.nvidia.com","pool":{"name":"node-a","generation":1},"devices":[{"name":"mig-0","attributes":{"uuid":{"string":"MIG-a"},"type":{"string":"mig"}}}]}}]}`)
	if _, err := DiscoverDRADevices(missingMIGMetadata, compiler.NVIDIADRADriver); err == nil || !strings.Contains(err.Error(), "incomplete profile or parent UUID") {
		t.Fatalf("incomplete MIG metadata error = %v", err)
	}
}

func TestMaterializeSkipsRuntimeClassWhenBundleOnlyReferencesPreconfiguredName(t *testing.T) {
	bundle := &api.Bundle{
		Key: "test/bundle-b", Namespace: "test", Generation: 8,
		Workload: api.Workload{TypeMeta: api.TypeMeta{APIVersion: "kueue.x-k8s.io/v1beta1", Kind: "Workload"}, ObjectMeta: api.ObjectMeta{Name: "workload-b", Namespace: "test"}, Spec: api.WorkloadSpec{QueueName: "default", PodSets: []api.PodSet{{Name: "main", Count: 1, Template: testPodTemplate()}}}},
		Job:      api.Job{TypeMeta: api.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: api.ObjectMeta{Name: "job-b", Namespace: "test"}, Spec: api.JobSpec{Parallelism: 1, Completions: 1, Template: api.PodTemplateSpec{Spec: api.PodSpec{RuntimeClassName: "kata-preconfigured", Containers: []api.Container{{Name: "main", Image: "example.invalid/image@sha256:abc"}}, RestartPolicy: "Never"}}}},
	}
	objects, err := NewKubernetes(0).Materialize(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 3 {
		t.Fatalf("materialized objects = %d, want bundle, workload, job", len(objects))
	}
	for _, object := range objects {
		if object.Kind == "RuntimeClass" {
			t.Fatalf("unexpected runtime class materialization for preconfigured reference: %+v", object)
		}
	}
}

func testPodTemplate() api.PodTemplateSpec {
	return api.PodTemplateSpec{Spec: api.PodSpec{Containers: []api.Container{{Name: "main", Image: "example.invalid/image@sha256:abc"}}, RestartPolicy: "Never"}}
}
