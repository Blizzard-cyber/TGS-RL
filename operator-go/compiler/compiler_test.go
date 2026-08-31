package compiler

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/internal/bootstrapauth"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"google.golang.org/protobuf/proto"
)

func discoveredGPUCapabilities(profiles ...string) CapabilitySet {
	set := DefaultCapabilitySet()
	for _, profile := range profiles {
		set.GPUProfiles[profile] = true
		if set.ExactDevicePlacement == nil {
			set.ExactDevicePlacement = make(map[string]bool)
		}
		set.ExactDevicePlacement[profile] = true
		if profile == GPUProfileKubernetesDRA {
			set.DRADevices = map[string]DRADevice{
				"GPU-aaaa": {UUID: "GPU-aaaa", Type: "gpu", DeviceClass: NVIDIADRAFullGPUDeviceClass, Driver: NVIDIADRADriver, Pool: "node-a", Device: "gpu-0"},
				"GPU-bbbb": {UUID: "GPU-bbbb", Type: "gpu", DeviceClass: NVIDIADRAFullGPUDeviceClass, Driver: NVIDIADRADriver, Pool: "node-a", Device: "gpu-1"},
			}
		}
	}
	return set
}

func TestCompileDeterministicBundle(t *testing.T) {
	c := New()
	c.SetCapabilities(discoveredGPUCapabilities(GPUProfileNVIDIADevicePlugin))
	input := testCompileInput()

	left, err := c.compileBinding(singleBindingInput(input, 0))
	if err != nil {
		t.Fatalf("first compile failed: %v", err)
	}
	right, err := c.compileBinding(singleBindingInput(input, 0))
	if err != nil {
		t.Fatalf("second compile failed: %v", err)
	}

	if left.Fingerprint != right.Fingerprint {
		t.Fatalf("fingerprints differ: %q vs %q", left.Fingerprint, right.Fingerprint)
	}
	if left.Job.ObjectMeta.Name != right.Job.ObjectMeta.Name {
		t.Fatalf("job names differ: %q vs %q", left.Job.ObjectMeta.Name, right.Job.ObjectMeta.Name)
	}
	if left.Workload.Spec.PodSets[0].Count != 1 {
		t.Fatalf("unexpected pod count: %d", left.Workload.Spec.PodSets[0].Count)
	}
	if got := left.Job.Spec.Template.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"]; got != "1" {
		t.Fatalf("unexpected accelerator request: %q", got)
	}
	if got := left.Job.Spec.Template.Spec.Containers[0].Resources.Requests["cpu"]; got != "2000m" {
		t.Fatalf("unexpected per-pod cpu request: %q", got)
	}
	if got := left.Admission.Requests["nvidia.com/gpu"]; got != "1" {
		t.Fatalf("unexpected aggregate accelerator quota: %q", got)
	}
}

func TestCompileKeepsBundleIdentityAndVersionsObjectsAcrossGenerations(t *testing.T) {
	c := New()
	c.SetCapabilities(discoveredGPUCapabilities(GPUProfileNVIDIADevicePlugin))
	firstInput := testCompileInput()
	first, err := c.compileBinding(singleBindingInput(firstInput, 0))
	if err != nil {
		t.Fatal(err)
	}
	secondInput := testCompileInput()
	secondInput.Generation++
	secondInput.PlacementPlan.Bindings[0].Generation = first.Generation + 1
	secondInput.PlacementPlan.PlanId = "replacement-plan"
	secondInput.PlacementPlan.DecisionId = "replacement-decision"
	second, err := c.compileBinding(singleBindingInput(secondInput, 0))
	if err != nil {
		t.Fatal(err)
	}
	if first.Key != second.Key {
		t.Fatalf("bundle identity changed across generations: first=%s second=%s", first.Key, second.Key)
	}
	if first.Job.ObjectMeta.Name == second.Job.ObjectMeta.Name || first.Workload.ObjectMeta.Name == second.Workload.ObjectMeta.Name {
		t.Fatalf("immutable workload objects were reused across generations: first=%s/%s second=%s/%s", first.Job.ObjectMeta.Name, first.Workload.ObjectMeta.Name, second.Job.ObjectMeta.Name, second.Workload.ObjectMeta.Name)
	}
	if first.Fingerprint == second.Fingerprint {
		t.Fatal("new plan generation must change the desired-state fingerprint")
	}
}

func TestCompileRejectsMutuallyExclusiveGPUProfiles(t *testing.T) {
	c := New()
	c.SetCapabilities(discoveredGPUCapabilities(GPUProfileKubernetesDRA, GPUProfileVolcanoHAMI))
	input := testCompileInput()
	input.GPUProfiles = []string{GPUProfileKubernetesDRA, GPUProfileVolcanoHAMI}

	if _, err := c.Compile(input); err == nil {
		t.Fatalf("expected mutually exclusive gpu profile error")
	}
}

func TestCompileDRAGeneratesResourceClaim(t *testing.T) {
	c := New()
	c.SetCapabilities(discoveredGPUCapabilities(GPUProfileKubernetesDRA))
	input := testCompileInput()
	input.GPUProfiles = []string{GPUProfileKubernetesDRA}

	bundle, err := c.compileBinding(singleBindingInput(input, 0))
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if bundle.ResourceClaim == nil {
		t.Fatalf("expected resource claim")
	}
	if len(bundle.ResourceClaim.Spec.Devices.Requests) != 1 {
		t.Fatalf("expected exactly one DRA device request")
	}
	request := bundle.ResourceClaim.Spec.Devices.Requests[0]
	if request.Exactly == nil {
		t.Fatal("stable DRA API must use requests[].exactly")
	}
	if got := request.Exactly.DeviceClassName; got != NVIDIADRAFullGPUDeviceClass {
		t.Fatalf("unexpected device class: %q", got)
	}
	if request.Name != "accelerator" || request.Exactly.AllocationMode != "ExactCount" || request.Exactly.Count != 1 {
		t.Fatalf("unexpected DRA request: %+v", request)
	}
	if len(request.Exactly.Selectors) != 1 || request.Exactly.Selectors[0].CEL == nil || request.Exactly.Selectors[0].CEL.Expression != `device.driver == "gpu.nvidia.com" && device.attributes["gpu.nvidia.com"].type == "gpu" && device.attributes["gpu.nvidia.com"].uuid in ["GPU-aaaa"]` {
		t.Fatalf("unexpected DRA UUID selector: %+v", request.Exactly.Selectors)
	}
	if len(bundle.Job.Spec.Template.Spec.ResourceClaims) != 1 {
		t.Fatalf("expected exactly one pod resource claim reference")
	}
	if got := bundle.Job.Spec.Template.Spec.ResourceClaims[0].ResourceClaimName; got != bundle.ResourceClaim.ObjectMeta.Name {
		t.Fatalf("resource claim reference mismatch: %q vs %q", got, bundle.ResourceClaim.ObjectMeta.Name)
	}
	if _, ok := bundle.Job.Spec.Template.Spec.Containers[0].Resources.Requests["resource.k8s.io/gpu"]; ok {
		t.Fatalf("DRA must not use a synthetic extended-resource request")
	}
	claims := bundle.Job.Spec.Template.Spec.Containers[0].Resources.Claims
	if len(claims) != 1 || claims[0].Name != "accelerator" || claims[0].Request != "accelerator" {
		t.Fatalf("unexpected container DRA claims: %+v", claims)
	}
	workloadClaims := bundle.Workload.Spec.PodSets[0].Template.Spec.ResourceClaims
	if len(workloadClaims) != 1 || workloadClaims[0].ResourceClaimName != bundle.ResourceClaim.ObjectMeta.Name {
		t.Fatalf("workload pod set did not receive DRA claim: %+v", workloadClaims)
	}
}

func TestCompileWrapsWorkloadWithManagedWorkerBootstrap(t *testing.T) {
	signingKey := []byte(strings.Repeat("registry-signing-key-", 2))
	c, err := NewWithRuntimeConfig(RuntimeConfig{Bootstrap: WorkerBootstrapConfig{
		Enabled:            true,
		InstallerImage:     "registry.example.test/tgsrl/bootstrap@sha256:" + strings.Repeat("1", 64),
		RegistryURL:        "https://scheduler.example.test:50091",
		RegistrySigningKey: signingKey,
		VerifyDeviceIDs:    true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	c.SetCapabilities(discoveredGPUCapabilities(GPUProfileKubernetesDRA))
	input := singleBindingInput(testCompileInput(), 0)
	input.GPUProfiles = []string{GPUProfileKubernetesDRA}
	input.PlacementPlan.Bindings[0].SandboxId = "sandbox-1"
	input.PlacementPlan.Bindings[0].RuntimeUnitId = "unit-1"
	input.JobRun.Runtime.Command = []string{"must-not", "execute"}
	input.JobRun.Runtime.Environment["JOB_ONLY"] = "stale"
	input.JobRun.Runtime.Environment["TGSRL_GENERATION"] = "malicious"
	input.RuntimeManifest.WorkingDirectory = "/workspace"
	input.RuntimeManifest.Framework = "verl"
	input.RuntimeManifest.Environment["MANIFEST_ONLY"] = "frozen"
	input.RuntimeManifest.PolicyVersion = "policy-7"
	input.RuntimeManifest.Annotations["algorithm"] = "grpo"

	bundle, err := c.compileBinding(input)
	if err != nil {
		t.Fatal(err)
	}
	pod := bundle.Job.Spec.Template.Spec
	if bundle.Job.Spec.BackoffLimit == nil || *bundle.Job.Spec.BackoffLimit != 0 {
		t.Fatalf("bootstrap Job backoff limit = %v, want 0", bundle.Job.Spec.BackoffLimit)
	}
	if len(pod.InitContainers) != 1 || pod.InitContainers[0].Name != "install-tgsrl-bootstrap" {
		t.Fatalf("init containers = %+v", pod.InitContainers)
	}
	if pod.SecurityContext == nil || pod.SecurityContext.FSGroup != 65532 || pod.SecurityContext.FSGroupChangePolicy != "OnRootMismatch" {
		t.Fatalf("bootstrap pod security context = %+v", pod.SecurityContext)
	}
	main := pod.Containers[0]
	if len(main.Command) != 1 || main.Command[0] != "/opt/tgsrl/tgsrl-worker-bootstrap" {
		t.Fatalf("bootstrap command = %+v", main.Command)
	}
	wantArgs := []string{"--listen", "0.0.0.0:50092", "--", "python", "train.py", "--steps", "10"}
	if !slices.Equal(main.Args, wantArgs) {
		t.Fatalf("bootstrap args = %+v, want %+v", main.Args, wantArgs)
	}
	environment := make(map[string]api.EnvVar, len(main.Env))
	for _, value := range main.Env {
		if _, duplicate := environment[value.Name]; duplicate {
			t.Fatalf("duplicate environment variable %q", value.Name)
		}
		environment[value.Name] = value
	}
	if environment["TGSRL_GENERATION"].Value != "7" || environment["TGSRL_SANDBOX_ID"].Value != "sandbox-1" || environment["TGSRL_DEVICE_IDS"].Value != "GPU-aaaa" {
		t.Fatalf("bootstrap identity environment = %+v", environment)
	}
	if environment["TGSRL_POLICY_VERSION"].Value != input.RuntimeManifest.GetPolicyVersion() || environment["TGSRL_ALGORITHM"].Value != input.RuntimeManifest.GetAnnotations()["algorithm"] {
		t.Fatalf("veRL execution environment = %+v", environment)
	}
	if environment["TGSRL_WORKER_ID"].Value != "unit-1" || environment["TGSRL_RUNTIME_UNIT_ID"].Value != "unit-1" {
		t.Fatalf("veRL worker identity environment = %+v", environment)
	}
	if environment["MANIFEST_ONLY"].Value != "frozen" {
		t.Fatalf("manifest environment was not projected: %+v", environment)
	}
	if _, present := environment["JOB_ONLY"]; present {
		t.Fatalf("mutable Job runtime environment leaked into the workload: %+v", environment)
	}
	if environment["TGSRL_WORKING_DIRECTORY"].Value != "/workspace" {
		t.Fatalf("manifest working directory was not preserved: %+v", environment)
	}
	if environment["TGSRL_VERL_CONTROL_SOCKET"].Value != "/tmp/tgsrl/verl.sock" || environment["TGSRL_VERL_TRACE_PATH"].Value == "" || environment["TGSRL_VERL_STATE_PATH"].Value == "" {
		t.Fatalf("veRL bridge paths were not projected: %+v", environment)
	}
	if environment["TGSRL_VERIFY_DEVICE_IDENTITIES"].Value != "true" {
		t.Fatalf("DRA workload must verify visible device identities: %+v", environment)
	}
	if len(main.Ports) != 1 || main.Ports[0].ContainerPort != 50092 {
		t.Fatalf("bootstrap control port = %+v", main.Ports)
	}
	registrationToken := environment["TGSRL_WORKER_REGISTRY_TOKEN"]
	if registrationToken.ValueFrom != nil || !bootstrapauth.Verify(signingKey, bootstrapauth.Claims{RunID: input.JobRun.GetRunId(), JobID: input.JobRun.GetJobId(), RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", BindingID: input.PlacementPlan.Bindings[0].GetBindingId(), Generation: 7, DeviceIDs: []string{"GPU-aaaa"}}, registrationToken.Value) {
		t.Fatalf("registry token is not scoped to the compiled binding: %+v", registrationToken)
	}
	if !reflect.DeepEqual(bundle.Workload.Spec.PodSets[0].Template.Spec, pod) {
		t.Fatal("Kueue pod set and Job template diverged after bootstrap injection")
	}
}

func TestCompileDRAV1Beta1UsesLegacyFlatRequest(t *testing.T) {
	c := New()
	capabilities := discoveredGPUCapabilities(GPUProfileKubernetesDRA)
	capabilities.KubernetesAPIs = KubernetesAPIVersions{
		KueueWorkload:    KueueWorkloadV1Beta1,
		DRAResourceClaim: DRAResourceClaimV1Beta1,
	}
	c.SetCapabilities(capabilities)
	input := testCompileInput()
	input.GPUProfiles = []string{GPUProfileKubernetesDRA}

	bundle, err := c.compileBinding(singleBindingInput(input, 0))
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	request := bundle.ResourceClaim.Spec.Devices.Requests[0]
	if bundle.ResourceClaim.APIVersion != DRAResourceClaimV1Beta1 || bundle.Workload.APIVersion != KueueWorkloadV1Beta1 {
		t.Fatalf("selected APIs were not projected: claim=%s workload=%s", bundle.ResourceClaim.APIVersion, bundle.Workload.APIVersion)
	}
	if request.Exactly != nil || request.DeviceClassName != NVIDIADRAFullGPUDeviceClass || request.Count != 1 || len(request.Selectors) != 1 || request.Selectors[0].CEL == nil {
		t.Fatalf("unexpected legacy DRA request: %+v", request)
	}
}

func TestCompileDRASelectsEveryConcreteBindingUUID(t *testing.T) {
	c := New()
	c.SetCapabilities(discoveredGPUCapabilities(GPUProfileKubernetesDRA))
	input := singleBindingInput(testCompileInput(), 0)
	input.GPUProfiles = []string{GPUProfileKubernetesDRA}
	input.PlacementPlan.Bindings[0].DeviceIds = []string{"GPU-bbbb", "GPU-aaaa"}
	input.PlacementPlan.Bindings[0].Resources.AcceleratorUnits = 2

	bundle, err := c.compileBinding(input)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	request := bundle.ResourceClaim.Spec.Devices.Requests[0].Exactly
	if request == nil || request.Count != 2 {
		t.Fatalf("DRA request = %+v, want two exact devices", request)
	}
	want := `device.driver == "gpu.nvidia.com" && device.attributes["gpu.nvidia.com"].type == "gpu" && device.attributes["gpu.nvidia.com"].uuid in ["GPU-aaaa", "GPU-bbbb"]`
	if got := request.Selectors[0].CEL.Expression; got != want {
		t.Fatalf("DRA selector = %q, want %q", got, want)
	}
}

func TestCompileDRASelectsMIGDeviceClass(t *testing.T) {
	c := New()
	c.SetCapabilities(CapabilitySet{
		GPUProfiles:          map[string]bool{GPUProfileNone: true, GPUProfileKubernetesDRA: true},
		ExactDevicePlacement: map[string]bool{GPUProfileKubernetesDRA: true},
		DRADevices: map[string]DRADevice{
			"MIG-aaaa": {UUID: "MIG-aaaa", Type: "mig", DeviceClass: NVIDIADRAMIGDeviceClass, Driver: NVIDIADRADriver, Pool: "node-a", Device: "gpu-0-mig-1g-10gb-0", Profile: "1g.10gb", ParentUUID: "GPU-parent"},
		},
		KubernetesAPIs: DefaultCapabilitySet().KubernetesAPIs,
	})
	input := singleBindingInput(testCompileInput(), 0)
	input.GPUProfiles = []string{GPUProfileKubernetesDRA}
	input.PlacementPlan.Bindings[0].DeviceIds = []string{"MIG-aaaa"}

	bundle, err := c.compileBinding(input)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	request := bundle.ResourceClaim.Spec.Devices.Requests[0].Exactly
	if request == nil || request.DeviceClassName != NVIDIADRAMIGDeviceClass {
		t.Fatalf("MIG request = %+v, want device class %q", request, NVIDIADRAMIGDeviceClass)
	}
	want := `device.driver == "gpu.nvidia.com" && device.attributes["gpu.nvidia.com"].type == "mig" && device.attributes["gpu.nvidia.com"].uuid in ["MIG-aaaa"]`
	if got := request.Selectors[0].CEL.Expression; got != want {
		t.Fatalf("MIG selector = %q, want %q", got, want)
	}
}

func TestCompileDRARejectsMixedDeviceClasses(t *testing.T) {
	c := New()
	c.SetCapabilities(CapabilitySet{
		GPUProfiles:          map[string]bool{GPUProfileNone: true, GPUProfileKubernetesDRA: true},
		ExactDevicePlacement: map[string]bool{GPUProfileKubernetesDRA: true},
		DRADevices: map[string]DRADevice{
			"GPU-aaaa": {UUID: "GPU-aaaa", Type: "gpu", DeviceClass: NVIDIADRAFullGPUDeviceClass, Driver: NVIDIADRADriver, Pool: "node-a", Device: "gpu-0"},
			"MIG-aaaa": {UUID: "MIG-aaaa", Type: "mig", DeviceClass: NVIDIADRAMIGDeviceClass, Driver: NVIDIADRADriver, Pool: "node-a", Device: "gpu-0-mig-1g-10gb-0", Profile: "1g.10gb", ParentUUID: "GPU-aaaa"},
		},
		KubernetesAPIs: DefaultCapabilitySet().KubernetesAPIs,
	})
	input := singleBindingInput(testCompileInput(), 0)
	input.GPUProfiles = []string{GPUProfileKubernetesDRA}
	input.PlacementPlan.Bindings[0].DeviceIds = []string{"GPU-aaaa", "MIG-aaaa"}
	input.PlacementPlan.Bindings[0].Resources.AcceleratorUnits = 2

	if _, err := c.compileBinding(input); err == nil || !strings.Contains(err.Error(), "mixes DRA device classes") {
		t.Fatalf("compile mixed DRA classes error = %v", err)
	}
}

func TestCompileDRARejectsUndiscoveredOrIncompleteDeviceIdentity(t *testing.T) {
	c := New()
	c.SetCapabilities(discoveredGPUCapabilities(GPUProfileKubernetesDRA))
	input := testCompileInput()
	input.GPUProfiles = []string{GPUProfileKubernetesDRA}
	input.PlacementPlan.Bindings[0].DeviceIds = []string{"GPU-missing"}
	if _, err := c.compileBinding(singleBindingInput(input, 0)); err == nil {
		t.Fatal("expected undiscovered DRA device identity to fail closed")
	}

	input.PlacementPlan.Bindings[0].DeviceIds = nil
	if _, err := c.compileBinding(singleBindingInput(input, 0)); err == nil {
		t.Fatal("expected missing DRA device identity to fail closed")
	}

	input.PlacementPlan.Bindings[0].DeviceIds = []string{"GPU-aaaa"}
	input.PlacementPlan.Bindings[0].Resources.AcceleratorUnits = 0.5
	if _, err := c.compileBinding(singleBindingInput(input, 0)); err == nil {
		t.Fatal("expected fractional DRA request without sharing configuration to fail closed")
	}

	capabilities := discoveredGPUCapabilities(GPUProfileKubernetesDRA)
	device := capabilities.DRADevices["GPU-aaaa"]
	device.UUID = "GPU-other"
	capabilities.DRADevices["GPU-aaaa"] = device
	c.SetCapabilities(capabilities)
	input.PlacementPlan.Bindings[0].Resources.AcceleratorUnits = 1
	if _, err := c.compileBinding(singleBindingInput(input, 0)); err == nil || !strings.Contains(err.Error(), "inventory key") {
		t.Fatalf("expected inconsistent DRA inventory identity to fail closed, got %v", err)
	}
}

func TestCompileCreatesOneBundlePerBindingWithIndependentResources(t *testing.T) {
	c := New()
	c.SetCapabilities(discoveredGPUCapabilities(GPUProfileNVIDIADevicePlugin))
	input := testCompileInput()
	input.PlacementPlan.Bindings[1].Resources.CpuMillis = 1000

	bundles, err := c.Compile(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundles) != 2 {
		t.Fatalf("bundles = %d, want one per binding", len(bundles))
	}
	if bundles[0].Key == bundles[1].Key || bundles[0].Job.ObjectMeta.Name == bundles[1].Job.ObjectMeta.Name {
		t.Fatal("runtime-unit bundles must have distinct stable identities")
	}
	if got := bundles[1].Job.Spec.Template.Spec.Containers[0].Resources.Requests["cpu"]; got != "1000m" {
		t.Fatalf("second bundle cpu = %q, want 1000m", got)
	}
}

func TestCompileUsesPendingUnitIdentityForReplicasOfOneRuntimeUnit(t *testing.T) {
	c := New()
	c.SetCapabilities(discoveredGPUCapabilities(GPUProfileNVIDIADevicePlugin))
	input := testCompileInput()
	for _, binding := range input.PlacementPlan.Bindings {
		binding.RuntimeUnitId = "run-1:actor"
		binding.Generation = 3
	}

	bundles, err := c.Compile(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundles) != 2 || bundles[0].Key == bundles[1].Key {
		t.Fatalf("replica bundle keys = %q, %q", bundles[0].Key, bundles[1].Key)
	}
	if bundles[0].RuntimeTargets[0].RuntimeUnitID != "run-1:actor" || bundles[1].RuntimeTargets[0].RuntimeUnitID != "run-1:actor" {
		t.Fatalf("logical runtime identity was not preserved: %+v %+v", bundles[0].RuntimeTargets, bundles[1].RuntimeTargets)
	}
}

func TestCompileProducesKubernetesDesiredState(t *testing.T) {
	c := New()
	c.SetCapabilities(discoveredGPUCapabilities(GPUProfileNVIDIADevicePlugin))
	bundle, err := c.compileBinding(singleBindingInput(testCompileInput(), 0))
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if bundle.Job.Spec.Template.Spec.RestartPolicy != "Never" {
		t.Fatalf("restart policy = %q", bundle.Job.Spec.Template.Spec.RestartPolicy)
	}
	if bundle.RuntimeClass != nil {
		t.Fatalf("default compile must not materialize a runtime class: %+v", bundle.RuntimeClass)
	}
	if bundle.Job.Spec.Template.Spec.RuntimeClassName != "" {
		t.Fatalf("default compile must not set a runtime class name: %q", bundle.Job.Spec.Template.Spec.RuntimeClassName)
	}
	if len(bundle.Job.Spec.Template.Spec.NodeSelector) != 0 {
		t.Fatalf("default compile must not synthesize a node selector: %+v", bundle.Job.Spec.Template.Spec.NodeSelector)
	}
	for name, value := range map[string]any{
		"workload metadata": bundle.Workload.ObjectMeta,
		"job metadata":      bundle.Job.ObjectMeta,
	} {
		payload, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			t.Fatalf("marshal %s: %v", name, marshalErr)
		}
		var object map[string]any
		if unmarshalErr := json.Unmarshal(payload, &object); unmarshalErr != nil {
			t.Fatalf("decode %s: %v", name, unmarshalErr)
		}
		if _, ok := object["uid"]; ok {
			t.Fatalf("%s contains client-owned uid: %s", name, payload)
		}
		if _, ok := object["generation"]; ok {
			t.Fatalf("%s contains client-owned generation: %s", name, payload)
		}
	}
}

func TestCompileRejectsAcceleratorsWithNoneProfile(t *testing.T) {
	c := New()
	c.SetCapabilities(discoveredGPUCapabilities())
	input := testCompileInput()
	input.GPUProfiles = []string{GPUProfileNone}

	if _, err := c.Compile(input); err == nil {
		t.Fatalf("expected accelerator/profile mismatch error")
	}
}

func TestCompileReferencesPreconfiguredRuntimeClassAndNodeSelector(t *testing.T) {
	c, err := NewWithRuntimeConfig(RuntimeConfig{
		RuntimeClass: RuntimeClassConfig{Name: "kata-gpu"},
		NodeSelector: map[string]string{"accelerator": "true"},
	})
	if err != nil {
		t.Fatalf("NewWithRuntimeConfig() error = %v", err)
	}
	c.SetCapabilities(CapabilitySet{
		GPUProfiles:          map[string]bool{GPUProfileNone: true, GPUProfileNVIDIADevicePlugin: true},
		ExactDevicePlacement: map[string]bool{GPUProfileNVIDIADevicePlugin: true},
		RuntimeClasses:       map[string]string{"kata-gpu": "kata-qemu"},
		KubernetesAPIs:       DefaultCapabilitySet().KubernetesAPIs,
	})
	bundle, err := c.compileBinding(singleBindingInput(testCompileInput(), 0))
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if bundle.RuntimeClass != nil {
		t.Fatalf("preconfigured runtime class must not be materialized: %+v", bundle.RuntimeClass)
	}
	if got := bundle.Job.Spec.Template.Spec.RuntimeClassName; got != "kata-gpu" {
		t.Fatalf("runtime class name = %q, want kata-gpu", got)
	}
	if got := bundle.Job.Spec.Template.Spec.NodeSelector["accelerator"]; got != "true" {
		t.Fatalf("node selector = %+v, want explicit selector", bundle.Job.Spec.Template.Spec.NodeSelector)
	}
}

func TestCompileMaterializesExplicitRuntimeClassCreation(t *testing.T) {
	c, err := NewWithRuntimeConfig(RuntimeConfig{
		RuntimeClass: RuntimeClassConfig{Name: "kata-gpu", Handler: "kata-qemu", Create: true},
	})
	if err != nil {
		t.Fatalf("NewWithRuntimeConfig() error = %v", err)
	}
	c.SetCapabilities(discoveredGPUCapabilities(GPUProfileNVIDIADevicePlugin))
	bundle, err := c.compileBinding(singleBindingInput(testCompileInput(), 0))
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if bundle.RuntimeClass == nil {
		t.Fatal("expected explicit runtime class materialization")
	}
	if bundle.RuntimeClass.ObjectMeta.Name != "kata-gpu" || bundle.RuntimeClass.Handler != "kata-qemu" {
		t.Fatalf("runtime class = %+v, want explicit name/handler", bundle.RuntimeClass)
	}
	if bundle.Job.Spec.Template.Spec.RuntimeClassName != "kata-gpu" {
		t.Fatalf("job runtime class = %q, want kata-gpu", bundle.Job.Spec.Template.Spec.RuntimeClassName)
	}
}

func TestCompileSelectsOnlyDiscoveredGPUCapability(t *testing.T) {
	c := New()
	c.SetCapabilities(CapabilitySet{
		GPUProfiles: map[string]bool{
			GPUProfileNone:          true,
			GPUProfileKubernetesDRA: true,
		},
		ExactDevicePlacement: map[string]bool{GPUProfileKubernetesDRA: true},
		DRADevices: map[string]DRADevice{
			"GPU-aaaa": {UUID: "GPU-aaaa", Type: "gpu", DeviceClass: NVIDIADRAFullGPUDeviceClass, Driver: NVIDIADRADriver, Pool: "node-a", Device: "gpu-0"},
			"GPU-bbbb": {UUID: "GPU-bbbb", Type: "gpu", DeviceClass: NVIDIADRAFullGPUDeviceClass, Driver: NVIDIADRADriver, Pool: "node-a", Device: "gpu-1"},
		},
		KubernetesAPIs: DefaultCapabilitySet().KubernetesAPIs,
	})
	input := testCompileInput()
	input.GPUProfiles = []string{GPUProfileNVIDIADevicePlugin, GPUProfileKubernetesDRA}

	if _, err := c.Compile(input); err == nil {
		t.Fatalf("expected mutually exclusive input validation before capability selection")
	}

	input.GPUProfiles = []string{GPUProfileNVIDIADevicePlugin}
	if _, err := c.Compile(input); err == nil {
		t.Fatalf("expected capability mismatch when requested profile is not discovered")
	}

	input.GPUProfiles = []string{GPUProfileKubernetesDRA}
	bundle, err := c.compileBinding(singleBindingInput(input, 0))
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if bundle.GPUProfile != GPUProfileKubernetesDRA || bundle.ResourceClaim == nil {
		t.Fatalf("bundle = %+v", bundle)
	}
}

func TestNewWithRuntimeConfigRejectsInvalidNodeSelector(t *testing.T) {
	for name, selector := range map[string]map[string]string{
		"invalid key":   {"bad key": "true"},
		"invalid value": {"gpu": "bad value"},
	} {
		if _, err := NewWithRuntimeConfig(RuntimeConfig{NodeSelector: selector}); err == nil {
			t.Fatalf("%s: expected validation error", name)
		}
	}
}

func TestNewWithRuntimeConfigNormalizesNodeSelector(t *testing.T) {
	c, err := NewWithRuntimeConfig(RuntimeConfig{
		NodeSelector: map[string]string{" example.com/gpu ": " enabled "},
	})
	if err != nil {
		t.Fatalf("NewWithRuntimeConfig() error = %v", err)
	}
	c.SetCapabilities(discoveredGPUCapabilities(GPUProfileNVIDIADevicePlugin))
	bundle, err := c.compileBinding(singleBindingInput(testCompileInput(), 0))
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if _, ok := bundle.Job.Spec.Template.Spec.NodeSelector[" example.com/gpu "]; ok {
		t.Fatalf("node selector key was not normalized: %+v", bundle.Job.Spec.Template.Spec.NodeSelector)
	}
	if got := bundle.Job.Spec.Template.Spec.NodeSelector["example.com/gpu"]; got != "enabled" {
		t.Fatalf("node selector = %+v, want trimmed key/value", bundle.Job.Spec.Template.Spec.NodeSelector)
	}
}

func testCompileInput() CompileInput {
	return CompileInput{
		Namespace:   "test-ns",
		GPUProfiles: []string{GPUProfileNVIDIADevicePlugin},
		Generation:  7,
		JobRun: &tgsrlv1.JobRun{
			RunId:       "run-1",
			JobId:       "job-1",
			TraceId:     "trace-1",
			DisplayName: "demo",
			State:       tgsrlv1.JobState_JOB_STATE_RUNNING,
			RunState:    tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING,
			Labels: map[string]string{
				"queue":         "train",
				"desired_units": "2",
			},
			Runtime: &tgsrlv1.FrameworkRuntimeSpec{
				Command:     []string{"python", "train.py"},
				Args:        []string{"--steps", "10"},
				Environment: map[string]string{"alpha.beta/value": "1"},
			},
		},
		RuntimeManifest: &tgsrlv1.RuntimeManifest{
			ManifestId:       "manifest-1",
			RunId:            "run-1",
			JobId:            "job-1",
			TraceId:          "trace-1",
			ExecutionBackend: "kubernetes",
			ImageDigests:     []string{"repo/image@sha256:abc"},
			Annotations:      map[string]string{"team": "rl"},
			Command:          []string{"python", "train.py"},
			Args:             []string{"--steps", "10"},
			Environment:      map[string]string{"alpha.beta/value": "1"},
		},
		PlacementPlan: &tgsrlv1.PlacementPlan{
			PlanId:           "plan-1",
			ExecutionId:      "exec-1",
			StageId:          "stage-1",
			IntentVersion:    3,
			SnapshotRevision: 11,
			DecisionId:       "decision-1",
			RunId:            "run-1",
			Bindings: []*tgsrlv1.Binding{
				{
					BindingId:     "binding-1",
					PendingUnitId: "unit-1",
					DeviceIds:     []string{"GPU-aaaa"},
					Resources: &tgsrlv1.ResourceVector{
						CpuMillis:        2000,
						MemoryBytes:      4096,
						AcceleratorUnits: 1,
					},
				},
				{
					BindingId:     "binding-2",
					PendingUnitId: "unit-2",
					DeviceIds:     []string{"GPU-bbbb"},
					Resources: &tgsrlv1.ResourceVector{
						CpuMillis:        2000,
						MemoryBytes:      4096,
						AcceleratorUnits: 1,
					},
				},
			},
		},
	}
}

func singleBindingInput(input CompileInput, index int) CompileInput {
	plan := proto.Clone(input.PlacementPlan).(*tgsrlv1.PlacementPlan)
	plan.Bindings = []*tgsrlv1.Binding{proto.Clone(plan.GetBindings()[index]).(*tgsrlv1.Binding)}
	input.PlacementPlan = plan
	return input
}
