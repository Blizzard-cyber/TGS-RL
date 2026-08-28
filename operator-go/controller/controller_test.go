package controller

import (
	"context"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/admission"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

func TestReconcileAppliesAdmittedBundleAndIsIdempotent(t *testing.T) {
	b := backend.NewFake()
	r := New(b)
	input := compiler.CompileInput{
		Namespace:        "test-ns",
		GPUProfiles:      []string{compiler.GPUProfileNVIDIADevicePlugin},
		Generation:       1,
		AdmissionAllowed: true,
		JobRun:           testJobRun(),
		RuntimeManifest:  testManifest(),
		PlacementPlan:    testPlan(),
	}
	policy := admission.QueuePolicy{
		Name:            "train",
		QuotaGroup:      "team-a",
		Capacity:        map[string]string{"cpu": "4000", "memory": "8192", "nvidia.com/gpu": "2"},
		AllowPreemption: true,
	}

	first, err := r.Reconcile(context.Background(), input, policy)
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	if !first.Applied || !first.Created {
		t.Fatalf("unexpected first reconcile result: %+v", first)
	}
	if first.Bundle.ControllerStatus.Phase != "ready" {
		t.Fatalf("unexpected first controller phase: %q", first.Bundle.ControllerStatus.Phase)
	}

	second, err := r.Reconcile(context.Background(), input, policy)
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if !second.Idempotent {
		t.Fatalf("expected idempotent replay, got %+v", second)
	}
	if second.Bundle.ControllerStatus.Phase != "steady" {
		t.Fatalf("unexpected replay controller phase: %q", second.Bundle.ControllerStatus.Phase)
	}
}

func TestReconcileQueuesWhenAdmissionRejects(t *testing.T) {
	b := backend.NewFake()
	r := New(b)
	input := compiler.CompileInput{
		Namespace:        "test-ns",
		GPUProfiles:      []string{compiler.GPUProfileNVIDIADevicePlugin},
		Generation:       1,
		AdmissionAllowed: true,
		JobRun:           testJobRun(),
		RuntimeManifest:  testManifest(),
		PlacementPlan:    testPlan(),
	}
	policy := admission.QueuePolicy{
		Name:            "train",
		QuotaGroup:      "team-a",
		Capacity:        map[string]string{"cpu": "1"},
		AllowPreemption: false,
	}

	result, err := r.Reconcile(context.Background(), input, policy)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if !result.Queued || result.Applied {
		t.Fatalf("unexpected queued result: %+v", result)
	}
	if result.Bundle.Workload.Status.Phase != "queued" {
		t.Fatalf("unexpected workload phase: %q", result.Bundle.Workload.Status.Phase)
	}
}

func TestNewWithRuntimeConfigInjectsCompilerConfig(t *testing.T) {
	b := backend.NewFake()
	r, err := NewWithRuntimeConfig(b, compiler.RuntimeConfig{
		RuntimeClass: compiler.RuntimeClassConfig{Name: "kata-gpu"},
		NodeSelector: map[string]string{"accelerator": "true"},
	})
	if err != nil {
		t.Fatalf("NewWithRuntimeConfig() error = %v", err)
	}
	input := compiler.CompileInput{
		Namespace:        "test-ns",
		GPUProfiles:      []string{compiler.GPUProfileNVIDIADevicePlugin},
		Generation:       1,
		AdmissionAllowed: true,
		JobRun:           testJobRun(),
		RuntimeManifest:  testManifest(),
		PlacementPlan:    testPlan(),
	}
	policy := admission.QueuePolicy{
		Name:            "train",
		QuotaGroup:      "team-a",
		Capacity:        map[string]string{"cpu": "4000", "memory": "8192", "nvidia.com/gpu": "2"},
		AllowPreemption: true,
	}

	result, err := r.Reconcile(context.Background(), input, policy)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if got := result.Bundle.Job.Spec.Template.Spec.RuntimeClassName; got != "kata-gpu" {
		t.Fatalf("runtime class name = %q, want kata-gpu", got)
	}
	if got := result.Bundle.Job.Spec.Template.Spec.NodeSelector["accelerator"]; got != "true" {
		t.Fatalf("node selector = %+v, want explicit injected selector", result.Bundle.Job.Spec.Template.Spec.NodeSelector)
	}
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
