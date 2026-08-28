// Package provider defines the vendor-neutral resource provider boundary.
package provider

import (
	"fmt"
	"sort"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NewMockResourceProvider constructs a deterministic local provider.
func NewMockResourceProvider(options ...MockOption) (*MockResourceProvider, error) {
	config := mockConfig{
		devices:      DefaultMockDevices(),
		capabilities: DefaultMockCapabilities(),
		now:          time.Now,
		retention:    128,
		providerID:   "mock",
		source:       "mock",
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil option", ErrInvalidArgument)
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	if config.capabilities == nil {
		return nil, fmt.Errorf("%w: capabilities are required", ErrInvalidArgument)
	}
	if config.retention <= 0 {
		return nil, fmt.Errorf("%w: event retention must be positive", ErrInvalidArgument)
	}
	if config.providerID == "" {
		return nil, fmt.Errorf("%w: provider_id is required", ErrInvalidArgument)
	}
	if config.source == "" {
		return nil, fmt.Errorf("%w: provider source is required", ErrInvalidArgument)
	}
	config.capabilities.Source = config.source
	for _, device := range config.devices {
		if device == nil {
			continue
		}
		device.Capabilities = cloneCapabilities(config.capabilities)
	}
	provider := &MockResourceProvider{
		devices:      cloneDevices(config.devices),
		capabilities: cloneCapabilities(config.capabilities),
		sandboxes:    make(map[string]Sandbox, len(config.sandboxes)),
		providerID:   config.providerID,
		source:       config.source,
		faults:       cloneFaults(config.faults),
		now:          config.now,
		revision:     1,
		retention:    config.retention,
		actions:      make(map[string]actionLedgerEntry),
		plans:        make(map[string][]*tgsrlv1.ActionResult),
		planErrors:   make(map[string]error),
		planRequests: make(map[string]*tgsrlv1.PlacementPlan),
		planRecords:  make(map[string]*PlanRecord),
		events:       make(map[string]SandboxEvent),
		actionPlans:  make(map[string]string),
		resourceSubs: make(map[uint64]chan WatchedResourceEvent),
		sandboxSubs:  make(map[uint64]chan WatchedSandboxEvent),
	}
	for _, sandbox := range config.sandboxes {
		if sandbox.SandboxID == "" {
			return nil, fmt.Errorf("%w: sandbox_id is required", ErrInvalidArgument)
		}
		provider.sandboxes[sandbox.SandboxID] = cloneSandbox(sandbox)
	}
	for _, recovered := range config.recovered {
		if recovered == nil || recovered.Plan == nil || recovered.Plan.GetPlanId() == "" {
			return nil, fmt.Errorf("%w: recovered plan requires plan_id", ErrInvalidArgument)
		}
		record := clonePlanRecord(recovered)
		if record.Status == "" {
			record.Status = PlanStatusInFlight
		}
		provider.planRecords[record.Plan.GetPlanId()] = record
		provider.planRequests[record.Plan.GetPlanId()] = clonePlacementPlan(record.Plan)
	}
	provider.publishCapabilityRefreshLocked()
	provider.publishSnapshotLocked(tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED)
	for _, sandboxID := range provider.sortedSandboxIDsLocked() {
		provider.publishSandboxSnapshotLocked(provider.sandboxes[sandboxID], "")
	}
	return provider, nil
}

// DefaultMockCapabilities advertises the vendor-neutral mock action surface.
func DefaultMockCapabilities() *tgsrlv1.CapabilitySet {
	actions := make([]string, 0, len(actionNames))
	for _, name := range actionNames {
		actions = append(actions, name)
	}
	sort.Strings(actions)
	return &tgsrlv1.CapabilitySet{
		Names:            []string{"logical-cpu"},
		Algorithms:       []string{"grpo", "ppo"},
		RolloutModes:     []string{"fully_async", "partially_async", "sync"},
		Source:           "mock",
		Revision:         1,
		MeasuredAt:       timestamppb.Now(),
		SupportedActions: actions,
		Limits:           map[string]float64{"max_share": 1},
	}
}

// DefaultMockDevices returns one ready logical CPU device.
func DefaultMockDevices() []*tgsrlv1.Device {
	capabilities := DefaultMockCapabilities()
	resources := &tgsrlv1.ResourceVector{CpuMillis: 8000, MemoryBytes: 16 << 30, AcceleratorUnits: 1}
	return []*tgsrlv1.Device{{
		DeviceId:     "mock-cpu-0",
		Kind:         tgsrlv1.DeviceKind_DEVICE_KIND_CPU,
		Health:       tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
		Capacity:     proto.Clone(resources).(*tgsrlv1.ResourceVector),
		Allocatable:  resources,
		Capabilities: capabilities,
		Labels:       map[string]string{"provider": "mock"},
	}}
}
