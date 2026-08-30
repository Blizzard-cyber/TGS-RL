package compiler

import (
	"encoding/json"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

func discoveredGPUCapabilities(profiles ...string) CapabilitySet {
	set := CapabilitySet{GPUProfiles: map[string]bool{GPUProfileNone: true}}
	for _, profile := range profiles {
		set.GPUProfiles[profile] = true
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
	if got := request.DeviceClassName; got != "gpu.resource.k8s.io" {
		t.Fatalf("unexpected device class: %q", got)
	}
	if request.Name != "accelerator" || request.AllocationMode != "ExactCount" || request.Count != 1 {
		t.Fatalf("unexpected DRA request: %+v", request)
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
		GPUProfiles:    map[string]bool{GPUProfileNone: true, GPUProfileNVIDIADevicePlugin: true},
		RuntimeClasses: map[string]string{"kata-gpu": "kata-qemu"},
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
					Resources: &tgsrlv1.ResourceVector{
						CpuMillis:        2000,
						MemoryBytes:      4096,
						AcceleratorUnits: 1,
					},
				},
				{
					BindingId:     "binding-2",
					PendingUnitId: "unit-2",
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
