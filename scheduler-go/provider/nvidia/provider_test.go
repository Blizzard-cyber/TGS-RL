package nvidia

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCommandBuilderBuildsArgvOnly(t *testing.T) {
	command, err := NewCommandBuilder("nvidia-smi").Arg("--query-gpu=index", "--format=csv,noheader").Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	want := []string{"nvidia-smi", "--query-gpu=index", "--format=csv,noheader"}
	if !reflect.DeepEqual(command.Argv, want) {
		t.Fatalf("command argv = %v, want %v", command.Argv, want)
	}
}

func TestUnavailableDriverReportsNoGPU(t *testing.T) {
	driver := NewUnavailableDriver("no GPU present")
	probe, err := driver.Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if probe.Available {
		t.Fatal("Probe().Available = true, want false")
	}
	if probe.Reason != "no GPU present" {
		t.Fatalf("Probe().Reason = %q, want no GPU present", probe.Reason)
	}
}

func TestProviderHealthReflectsDriverAvailability(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(NewUnavailableDriver("no GPU present")))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	health, err := p.Health(context.Background())
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if health.Healthy {
		t.Fatal("Health().Healthy = true, want false")
	}
	if health.Reason != "no GPU present" {
		t.Fatalf("Health().Reason = %q, want no GPU present", health.Reason)
	}
}

func TestProviderRejectsUnsupportedDriverAction(t *testing.T) {
	now := time.Date(2026, time.August, 27, 9, 0, 0, 0, time.UTC)
	p, err := New(WithNow(func() time.Time { return now }), WithDriver(NewFakeDriver([]*tgsrlv1.Device{{
		DeviceId:    "nvidia-0",
		Kind:        tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
		Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
		Capacity:    &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
		Allocatable: &tgsrlv1.ResourceVector{AcceleratorUnits: 1},
		Capabilities: &tgsrlv1.CapabilitySet{
			Names:            []string{CapabilityName},
			Source:           ProviderID,
			Revision:         1,
			SupportedActions: []string{"bind"},
		},
	}}, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	action := &tgsrlv1.Action{
		ActionId:                 "unknown",
		ActionType:               tgsrlv1.ActionType_ACTION_TYPE_UNKNOWN,
		PlanId:                   "plan",
		ExpectedSnapshotRevision: 1,
		Deadline:                 timestamppb.New(now.Add(time.Minute)),
		IdempotencyKey:           "key",
	}
	_, err = p.ExecuteAction(context.Background(), action)
	if !errors.Is(err, provider.ErrUnsupported) {
		t.Fatalf("ExecuteAction() error = %v, want ErrUnsupported", err)
	}
}
