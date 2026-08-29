package state

import (
	"context"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBootstrapProviderSnapshotDeepClonesComponentVersions(t *testing.T) {
	store, _ := newTestStore(t)
	observedAt := time.Date(2026, time.August, 29, 8, 30, 0, 0, time.UTC)
	capabilities := projectedDriverCapabilities(observedAt, 8)
	providerSnapshot := &tgsrlv1.ClusterSnapshot{
		Revision:   8,
		SnapshotId: "provider-8",
		Devices: []*tgsrlv1.Device{{
			DeviceId:     "gpu-0",
			Kind:         tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
			Capabilities: capabilities,
		}},
	}
	wantComponent := proto.Clone(capabilities.GetComponentVersions()[0]).(*tgsrlv1.ComponentVersion)

	projected, changed, err := store.BootstrapProviderSnapshot("nvidia", providerSnapshot)
	if err != nil || !changed {
		t.Fatalf("BootstrapProviderSnapshot() = changed:%v err:%v, want changed true nil error", changed, err)
	}
	assertProjectedComponentVersion(t, projected.GetDevices()[0].GetCapabilities(), wantComponent)

	providerSnapshot.Devices[0].Capabilities.ComponentVersions[0].Version = "mutated-bootstrap-input"
	projected.Devices[0].Capabilities.ComponentVersions[0].Attributes["query_field"] = "mutated-result"
	stored, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	assertProjectedComponentVersion(t, stored.GetDevices()[0].GetCapabilities(), wantComponent)
}

func TestApplyProviderCapabilityEventDeepClonesComponentVersions(t *testing.T) {
	store, _ := newTestStore(t)
	if _, changed, err := store.BootstrapProviderSnapshot("nvidia", &tgsrlv1.ClusterSnapshot{
		Revision:   8,
		SnapshotId: "provider-8",
		Devices: []*tgsrlv1.Device{
			{DeviceId: "gpu-0", Kind: tgsrlv1.DeviceKind_DEVICE_KIND_GPU},
			{DeviceId: "gpu-1", Kind: tgsrlv1.DeviceKind_DEVICE_KIND_GPU},
		},
	}); err != nil || !changed {
		t.Fatalf("BootstrapProviderSnapshot() = changed:%v err:%v, want changed true nil error", changed, err)
	}

	observedAt := time.Date(2026, time.August, 29, 8, 30, 0, 0, time.UTC)
	capabilities := projectedDriverCapabilities(observedAt, 9)
	wantComponent := proto.Clone(capabilities.GetComponentVersions()[0]).(*tgsrlv1.ComponentVersion)
	projected, changed, err := store.ApplyProviderResourceEvent(&tgsrlv1.ResourceEvent{
		EventId:          "capability-9",
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_CAPABILITY_REFRESHED,
		ProviderRevision: 9,
		Provider:         "nvidia",
		Capabilities:     capabilities,
	})
	if err != nil || !changed {
		t.Fatalf("ApplyProviderResourceEvent() = changed:%v err:%v, want changed true nil error", changed, err)
	}
	if len(projected.GetDevices()) != 2 {
		t.Fatalf("projected devices = %d, want 2", len(projected.GetDevices()))
	}
	assertProjectedComponentVersion(t, projected.GetDevices()[0].GetCapabilities(), wantComponent)
	assertProjectedComponentVersion(t, projected.GetDevices()[1].GetCapabilities(), wantComponent)

	capabilities.ComponentVersions[0].Version = "mutated-event-input"
	capabilities.ComponentVersions[0].Attributes["query_field"] = "mutated-event-input"
	projected.Devices[0].Capabilities.ComponentVersions[0].Version = "mutated-result"
	projected.Devices[0].Capabilities.ComponentVersions[0].Attributes["query_field"] = "mutated-result"
	assertProjectedComponentVersion(t, projected.GetDevices()[1].GetCapabilities(), wantComponent)

	stored, err := store.GetSnapshot(context.Background(), 0, true)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	for index, device := range stored.GetDevices() {
		assertProjectedComponentVersion(t, device.GetCapabilities(), wantComponent)
		if device.GetCapabilities() == capabilities {
			t.Fatalf("stored device %d aliases event capabilities", index)
		}
	}
}

func projectedDriverCapabilities(observedAt time.Time, revision uint64) *tgsrlv1.CapabilitySet {
	return &tgsrlv1.CapabilitySet{
		Source:     "nvidia",
		Revision:   revision,
		MeasuredAt: timestamppb.New(observedAt),
		ComponentVersions: []*tgsrlv1.ComponentVersion{{
			Kind:       tgsrlv1.ComponentKind_COMPONENT_KIND_CUDA_DRIVER,
			Name:       "nvidia-driver",
			Version:    "550.54.14",
			ObservedAt: timestamppb.New(observedAt),
			Source:     "nvidia-smi",
			Revision:   revision,
			Attributes: map[string]string{
				"query_field": "driver_version",
				"scope":       "nvidia-kernel-driver",
			},
		}},
	}
}

func assertProjectedComponentVersion(t *testing.T, capabilities *tgsrlv1.CapabilitySet, want *tgsrlv1.ComponentVersion) {
	t.Helper()
	if capabilities == nil || len(capabilities.GetComponentVersions()) != 1 {
		t.Fatalf("capabilities = %+v, want one component version", capabilities)
	}
	if got := capabilities.GetComponentVersions()[0]; !proto.Equal(got, want) {
		t.Fatalf("component version = %+v, want %+v", got, want)
	}
}
