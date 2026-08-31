package backend

import (
	"context"
	"testing"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

func TestFakeBackendApplyAndIdempotency(t *testing.T) {
	b := NewFake()
	b.SetCapabilities(compiler.CapabilitySet{
		GPUProfiles: map[string]bool{
			compiler.GPUProfileNone:               true,
			compiler.GPUProfileNVIDIADevicePlugin: true,
		},
	})
	bundle := testBundle()
	bundle.Generation = 2
	bundle.Fingerprint = "fp-1"

	first, err := b.Apply(context.Background(), bundle)
	if err != nil {
		t.Fatalf("first apply failed: %v", err)
	}
	if !first.Created {
		t.Fatalf("expected create result")
	}

	second, err := b.Apply(context.Background(), bundle)
	if err != nil {
		t.Fatalf("second apply failed: %v", err)
	}
	if !second.Idempotent {
		t.Fatalf("expected idempotent replay")
	}
}

func TestFakeBackendRejectsGenerationRegressionAndFingerprintDrift(t *testing.T) {
	b := NewFake()
	b.SetCapabilities(compiler.CapabilitySet{
		GPUProfiles: map[string]bool{
			compiler.GPUProfileNone:               true,
			compiler.GPUProfileNVIDIADevicePlugin: true,
		},
	})
	bundle := testBundle()
	bundle.Generation = 3
	bundle.Fingerprint = "fp-1"
	if _, err := b.Apply(context.Background(), bundle); err != nil {
		t.Fatalf("initial apply failed: %v", err)
	}

	regressed, err := api.CloneBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	regressed.Generation = 2
	if _, err := b.Apply(context.Background(), regressed); err != ErrGenerationConflict {
		t.Fatalf("expected ErrGenerationConflict, got %v", err)
	}

	drifted, err := api.CloneBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	drifted.Fingerprint = "fp-2"
	if _, err := b.Apply(context.Background(), drifted); err != ErrFingerprintDrift {
		t.Fatalf("expected ErrFingerprintDrift, got %v", err)
	}
}

func TestFakeBackendSnapshotsReflectObservedAdmissionAndClaimAllocation(t *testing.T) {
	b := NewFake()
	b.SetCapabilities(compiler.CapabilitySet{
		GPUProfiles: map[string]bool{
			compiler.GPUProfileNone:          true,
			compiler.GPUProfileKubernetesDRA: true,
		},
	})
	bundle := testBundle()
	bundle.GPUProfile = compiler.GPUProfileKubernetesDRA
	bundle.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", DeviceIDs: []string{"GPU-aaaa"}, Generation: bundle.Generation}}
	if _, err := b.Apply(context.Background(), bundle); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	snapshots, err := b.Snapshots(context.Background(), bundle)
	if err != nil {
		t.Fatalf("Snapshots() error = %v", err)
	}
	if len(snapshots) != 2 {
		t.Fatalf("snapshots = %+v, want two-step observed stream", snapshots)
	}
	if !snapshots[0].WorkloadAdmitted || !snapshots[0].ResourceClaimsAllocated || len(snapshots[0].AllocatedDeviceIDs) == 0 {
		t.Fatalf("bound snapshot = %+v, want admitted and claim allocated", snapshots[0])
	}
	if !snapshots[1].WorkloadAdmitted || !snapshots[1].ResourceClaimsAllocated || snapshots[1].JobActive == 0 {
		t.Fatalf("running snapshot = %+v, want admitted, claim allocated, and active job", snapshots[1])
	}
}

func TestFakeBackendCleanupRemovesBundleAndNamespacedObjects(t *testing.T) {
	b := NewFake()
	b.SetCapabilities(compiler.CapabilitySet{GPUProfiles: map[string]bool{compiler.GPUProfileNone: true, compiler.GPUProfileKubernetesDRA: true}})
	bundle := testBundle()
	bundle.GPUProfile = compiler.GPUProfileKubernetesDRA
	if _, err := b.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	if err := b.Cleanup(context.Background(), bundle.Key, bundle.Generation); err != nil {
		t.Fatal(err)
	}
	if _, found, err := b.Get(context.Background(), bundle.Key); err != nil || found {
		t.Fatalf("bundle after cleanup = found:%v err:%v", found, err)
	}
	if err := b.Cleanup(context.Background(), bundle.Key, bundle.Generation); err != nil {
		t.Fatalf("idempotent cleanup error = %v", err)
	}
}
