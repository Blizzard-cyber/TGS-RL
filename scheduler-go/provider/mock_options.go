package provider

import (
	"fmt"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// MockOption configures a MockResourceProvider.
type MockOption func(*mockConfig) error

type mockConfig struct {
	devices      []*tgsrlv1.Device
	capabilities *tgsrlv1.CapabilitySet
	sandboxes    []Sandbox
	recovered    []*PlanRecord
	faults       FaultOptions
	now          func() time.Time
	retention    int
	providerID   string
	source       string
}

// WithDevices replaces the default logical CPU devices.
func WithDevices(devices ...*tgsrlv1.Device) MockOption {
	return func(config *mockConfig) error {
		config.devices = cloneDevices(devices)
		return nil
	}
}

// WithCapabilities replaces the default mock capability set. Source is always
// normalized to "mock" so the provider can never masquerade as live hardware.
func WithCapabilities(capabilities *tgsrlv1.CapabilitySet) MockOption {
	return func(config *mockConfig) error {
		config.capabilities = cloneCapabilities(capabilities)
		return nil
	}
}

// MockInventory seeds provider state from one config object.
type MockInventory struct {
	Devices      []*tgsrlv1.Device
	Capabilities *tgsrlv1.CapabilitySet
	Sandboxes    []Sandbox
}

// WithInventory applies a whole-provider inventory seed in one option.
func WithInventory(inventory MockInventory) MockOption {
	return func(config *mockConfig) error {
		config.devices = cloneDevices(inventory.Devices)
		config.capabilities = cloneCapabilities(inventory.Capabilities)
		config.sandboxes = cloneSandboxes(inventory.Sandboxes)
		return nil
	}
}

// WithSandboxes seeds provider-local sandbox state.
func WithSandboxes(sandboxes ...Sandbox) MockOption {
	return func(config *mockConfig) error {
		config.sandboxes = cloneSandboxes(sandboxes)
		return nil
	}
}

// WithFaults installs deterministic fault injection settings.
func WithFaults(faults FaultOptions) MockOption {
	return func(config *mockConfig) error {
		config.faults = cloneFaults(faults)
		return nil
	}
}

// WithNow injects the clock used for timestamps and deadline checks.
func WithNow(now func() time.Time) MockOption {
	return func(config *mockConfig) error {
		if now == nil {
			return fmt.Errorf("%w: nil clock", ErrInvalidArgument)
		}
		config.now = now
		return nil
	}
}

// WithRecoveredPlans seeds plan-recovery state for RecoverInFlightPlans and
// ReconcilePlan.
func WithRecoveredPlans(records ...*PlanRecord) MockOption {
	return func(config *mockConfig) error {
		config.recovered = clonePlanRecords(records)
		return nil
	}
}

// WithEventRetention changes the bounded retained watch log size.
func WithEventRetention(retention int) MockOption {
	return func(config *mockConfig) error {
		config.retention = retention
		return nil
	}
}

// WithProviderIdentity overrides the provider metadata used by wrappers that
// share the provider state machine, such as the NVIDIA package in this
// directory tree.
func WithProviderIdentity(providerID, source string) MockOption {
	return func(config *mockConfig) error {
		config.providerID = providerID
		config.source = source
		return nil
	}
}
