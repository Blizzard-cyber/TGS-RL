package backend

import (
	"context"
	"testing"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
)

func TestFakeBackendApplyAndIdempotency(t *testing.T) {
	b := NewFake()
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
