package controller

import (
	"context"
	"fmt"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
	"google.golang.org/protobuf/proto"
)

func TestReconcileAppliesAdmittedBundleAndIsIdempotent(t *testing.T) {
	b := backend.NewFake()
	b.SetCapabilities(compiler.CapabilitySet{
		GPUProfiles: map[string]bool{
			compiler.GPUProfileNone:               true,
			compiler.GPUProfileNVIDIADevicePlugin: true,
		},
	})
	r := New(b)
	input := compiler.CompileInput{
		Namespace:       "test-ns",
		GPUProfiles:     []string{compiler.GPUProfileNVIDIADevicePlugin},
		Generation:      1,
		JobRun:          testJobRun(),
		RuntimeManifest: testManifest(),
		PlacementPlan:   testPlan(),
	}
	first, err := r.Reconcile(context.Background(), input)
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	if !first.Applied || !first.Created {
		t.Fatalf("unexpected first reconcile result: %+v", first)
	}
	if len(first.Bundles) != 2 {
		t.Fatalf("first reconcile bundles = %d, want 2", len(first.Bundles))
	}
	if first.Bundles[0].ControllerStatus.Phase != "ready" {
		t.Fatalf("unexpected first controller phase: %q", first.Bundles[0].ControllerStatus.Phase)
	}

	second, err := r.Reconcile(context.Background(), input)
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if !second.Idempotent {
		t.Fatalf("expected idempotent replay, got %+v", second)
	}
	if len(second.Bundles) != 2 || second.Bundles[0].ControllerStatus.Phase != "steady" {
		t.Fatalf("unexpected replay controller bundles: %+v", second.Bundles)
	}
}

func TestNewWithRuntimeConfigInjectsCompilerConfig(t *testing.T) {
	b := backend.NewFake()
	b.SetCapabilities(compiler.CapabilitySet{
		GPUProfiles:         map[string]bool{compiler.GPUProfileNone: true, compiler.GPUProfileNVIDIADevicePlugin: true},
		RuntimeClasses:      map[string]string{"kata-gpu": "kata-qemu"},
		DefaultNodeSelector: map[string]string{"accelerator": "true"},
	})
	r, err := NewWithRuntimeConfig(b, compiler.RuntimeConfig{
		RuntimeClass: compiler.RuntimeClassConfig{Name: "kata-gpu"},
		NodeSelector: map[string]string{"accelerator": "true"},
	})
	if err != nil {
		t.Fatalf("NewWithRuntimeConfig() error = %v", err)
	}
	input := compiler.CompileInput{
		Namespace:       "test-ns",
		GPUProfiles:     []string{compiler.GPUProfileNVIDIADevicePlugin},
		Generation:      1,
		JobRun:          testJobRun(),
		RuntimeManifest: testManifest(),
		PlacementPlan:   testPlan(),
	}
	result, err := r.Reconcile(context.Background(), input)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(result.Bundles) != 2 {
		t.Fatalf("reconcile bundles = %d, want 2", len(result.Bundles))
	}
	if got := result.Bundles[0].Job.Spec.Template.Spec.RuntimeClassName; got != "kata-gpu" {
		t.Fatalf("runtime class name = %q, want kata-gpu", got)
	}
	if got := result.Bundles[0].Job.Spec.Template.Spec.NodeSelector["accelerator"]; got != "true" {
		t.Fatalf("node selector = %+v, want explicit injected selector", result.Bundles[0].Job.Spec.Template.Spec.NodeSelector)
	}
}

func TestReconcileFailsClosedWhenRequestedGPUCapabilityIsUndiscovered(t *testing.T) {
	b := backend.NewFake()
	b.SetCapabilities(compiler.CapabilitySet{
		GPUProfiles: map[string]bool{compiler.GPUProfileNone: true, compiler.GPUProfileKubernetesDRA: true},
	})
	r := New(b)
	input := compiler.CompileInput{
		Namespace:       "test-ns",
		GPUProfiles:     []string{compiler.GPUProfileNVIDIADevicePlugin},
		Generation:      1,
		JobRun:          testJobRun(),
		RuntimeManifest: testManifest(),
		PlacementPlan:   testPlan(),
	}
	if _, err := r.Reconcile(context.Background(), input); err == nil {
		t.Fatal("expected reconcile to fail closed when requested GPU capability is not discovered")
	}
}

func TestReconcileStopsReleasedWorkloadAndCreatesReplacement(t *testing.T) {
	b := backend.NewFake()
	b.SetCapabilities(compiler.CapabilitySet{GPUProfiles: map[string]bool{
		compiler.GPUProfileNone: true, compiler.GPUProfileNVIDIADevicePlugin: true,
	}})
	r := New(b)
	admission := controllerInput()
	created, err := r.Reconcile(context.Background(), admission)
	if err != nil || len(created.Bundles) != 2 {
		t.Fatalf("initial reconcile = (%+v, %v)", created, err)
	}
	replacementSource := admission.PlacementPlan.GetBindings()[0]
	victim := admission.PlacementPlan.GetBindings()[1]
	replacement := proto.Clone(replacementSource).(*tgsrlv1.Binding)
	replacement.BindingId = "replacement-binding"
	replacement.Generation++
	plan := &tgsrlv1.PlacementPlan{
		PlanId: "replacement-plan", ExecutionId: "exec-1", StageId: "stage-1", RunId: "run-1", Bindings: []*tgsrlv1.Binding{replacement},
		Actions: []*tgsrlv1.Action{
			{ActionId: "release-victim", ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, Binding: proto.Clone(victim).(*tgsrlv1.Binding), ExpectedGeneration: victim.GetGeneration(), IdempotencyKey: "release-victim-key"},
			{ActionId: "bind-replacement", ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND, Binding: proto.Clone(replacement).(*tgsrlv1.Binding), ExpectedGeneration: replacement.GetGeneration(), IdempotencyKey: "bind-replacement-key"},
		},
	}
	input := admission
	input.PlacementPlan = plan
	result, err := r.Reconcile(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Bundles) != 1 || result.Bundles[0].RuntimeTargets[0].BindingID != replacement.GetBindingId() {
		t.Fatalf("replacement bundles = %+v", result.Bundles)
	}
	victimSnapshot, terminal, err := b.Snapshot(context.Background(), created.Bundles[1])
	if err != nil || !terminal || !victimSnapshot.JobDeleted {
		t.Fatalf("victim readback = (%+v, terminal=%v, err=%v)", victimSnapshot, terminal, err)
	}
	if victimSnapshot.ControlRetireRun {
		t.Fatal("scheduler release was mislabeled as whole-run retirement")
	}
}

func TestReconcileValidatesReplacementBeforeStoppingVictim(t *testing.T) {
	b := backend.NewFake()
	b.SetCapabilities(compiler.CapabilitySet{GPUProfiles: map[string]bool{
		compiler.GPUProfileNone: true, compiler.GPUProfileNVIDIADevicePlugin: true,
	}})
	r := New(b)
	admission := controllerInput()
	created, err := r.Reconcile(context.Background(), admission)
	if err != nil || len(created.Bundles) != 2 {
		t.Fatalf("initial reconcile = (%+v, %v)", created, err)
	}
	victim := admission.PlacementPlan.GetBindings()[1]
	replacement := proto.Clone(admission.PlacementPlan.GetBindings()[0]).(*tgsrlv1.Binding)
	replacement.Resources = nil
	input := admission
	input.PlacementPlan = &tgsrlv1.PlacementPlan{
		PlanId: "invalid-replacement", Bindings: []*tgsrlv1.Binding{replacement},
		Actions: []*tgsrlv1.Action{
			{ActionId: "release-victim", ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, Binding: proto.Clone(victim).(*tgsrlv1.Binding), ExpectedGeneration: victim.GetGeneration(), IdempotencyKey: "release-before-invalid"},
			{ActionId: "bind-invalid", ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND, Binding: replacement},
		},
	}
	if _, err := r.Reconcile(context.Background(), input); err == nil {
		t.Fatal("invalid replacement unexpectedly reconciled")
	}
	victimSnapshot, terminal, err := b.Snapshot(context.Background(), created.Bundles[1])
	if err != nil || terminal || victimSnapshot.JobDeleted {
		t.Fatalf("victim changed before replacement validation: (%+v, terminal=%v, err=%v)", victimSnapshot, terminal, err)
	}
}

func controllerInput() compiler.CompileInput {
	plan := testPlan()
	for index, binding := range plan.Bindings {
		binding.RuntimeUnitId = binding.PendingUnitId
		binding.SandboxId = fmt.Sprintf("sandbox-%d", index+1)
		binding.Generation = 1
		plan.Actions = append(plan.Actions, &tgsrlv1.Action{ActionId: fmt.Sprintf("bind-%d", index+1), ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND, Binding: proto.Clone(binding).(*tgsrlv1.Binding)})
	}
	return compiler.CompileInput{Namespace: "test-ns", GPUProfiles: []string{compiler.GPUProfileNVIDIADevicePlugin}, Generation: 1, JobRun: testJobRun(), RuntimeManifest: testManifest(), PlacementPlan: plan}
}

func testJobRun() *tgsrlv1.JobRun {
	return &tgsrlv1.JobRun{
		RunId:       "run-1",
		JobId:       "job-1",
		TraceId:     "trace-1",
		DisplayName: "demo",
		State:       tgsrlv1.JobState_JOB_STATE_RUNNING,
		RunState:    tgsrlv1.JobRunState_JOB_RUN_STATE_RUNNING,
		Labels: map[string]string{
			"queue":            "train",
			"quota_group":      "team-a",
			"desired_units":    "2",
			"allow_preemption": "true",
		},
		Runtime: &tgsrlv1.FrameworkRuntimeSpec{
			Command:     []string{"python", "train.py"},
			Args:        []string{"--steps", "10"},
			Environment: map[string]string{"alpha.beta/value": "1"},
		},
	}
}

func testManifest() *tgsrlv1.RuntimeManifest {
	return &tgsrlv1.RuntimeManifest{
		ManifestId:       "manifest-1",
		RunId:            "run-1",
		JobId:            "job-1",
		TraceId:          "trace-1",
		ExecutionBackend: "kubernetes",
		ImageDigests:     []string{"repo/image@sha256:abc"},
		Annotations:      map[string]string{"team": "rl"},
	}
}

func testPlan() *tgsrlv1.PlacementPlan {
	return &tgsrlv1.PlacementPlan{
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
	}
}
