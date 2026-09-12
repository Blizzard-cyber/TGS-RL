package provider

import (
	"errors"
	"testing"
)

func TestRegistryBuildsRegisteredProvider(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("mock", func() (CompleteResourceProvider, error) {
		return NewMockResourceProvider()
	}); err != nil {
		t.Fatal(err)
	}

	instance, err := registry.Build(" MOCK ")
	if err != nil {
		t.Fatal(err)
	}
	if instance == nil {
		t.Fatal("Build() returned nil provider")
	}
}

func TestRegistryRejectsInvalidAndUnknownProviders(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("", func() (CompleteResourceProvider, error) {
		return NewMockResourceProvider()
	}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Register(empty) error = %v, want ErrInvalidArgument", err)
	}
	if err := registry.Register("mock", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Register(nil factory) error = %v, want ErrInvalidArgument", err)
	}
	if _, err := registry.Build("unregistered"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Build(unregistered) error = %v, want ErrUnsupported", err)
	}
}

func TestRegistryRejectsDuplicateKindsAndNilInstances(t *testing.T) {
	registry := NewRegistry()
	factory := func() (CompleteResourceProvider, error) {
		return NewMockResourceProvider()
	}
	if err := registry.Register("mock", factory); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(" MOCK ", factory); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Register(duplicate) error = %v, want ErrInvalidArgument", err)
	}

	empty := NewRegistry()
	if err := empty.Register("nil-provider", func() (CompleteResourceProvider, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := empty.Build("nil-provider"); !errors.Is(err, ErrFailedPrecondition) {
		t.Fatalf("Build(nil-provider) error = %v, want ErrFailedPrecondition", err)
	}
}
