package provider

import (
	"fmt"
	"strings"
)

// Factory constructs one complete provider implementation.
type Factory func() (CompleteResourceProvider, error)

// Registry is the composition-root registry for resource provider plug-ins.
// It contains factories only; it does not own provider lifecycle or state.
type Registry struct {
	factories map[string]Factory
}

// NewRegistry creates an empty provider registry.
func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

// Register adds one provider implementation under a stable kind.
func (r *Registry) Register(kind string, factory Factory) error {
	if r == nil {
		return fmt.Errorf("%w: provider registry is nil", ErrInvalidArgument)
	}
	kind = normalizeProviderKind(kind)
	if kind == "" {
		return fmt.Errorf("%w: provider kind is required", ErrInvalidArgument)
	}
	if factory == nil {
		return fmt.Errorf("%w: provider factory for %q is required", ErrInvalidArgument, kind)
	}
	if _, exists := r.factories[kind]; exists {
		return fmt.Errorf("%w: provider kind %q is already registered", ErrInvalidArgument, kind)
	}
	r.factories[kind] = factory
	return nil
}

// Build constructs the provider registered for kind.
func (r *Registry) Build(kind string) (CompleteResourceProvider, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: provider registry is nil", ErrInvalidArgument)
	}
	normalized := normalizeProviderKind(kind)
	factory, exists := r.factories[normalized]
	if !exists {
		return nil, fmt.Errorf("%w: provider kind %q is not registered", ErrUnsupported, kind)
	}
	instance, err := factory()
	if err != nil {
		return nil, err
	}
	if instance == nil {
		return nil, fmt.Errorf("%w: provider factory %q returned nil", ErrFailedPrecondition, normalized)
	}
	return instance, nil
}

func normalizeProviderKind(kind string) string {
	return strings.ToLower(strings.TrimSpace(kind))
}
