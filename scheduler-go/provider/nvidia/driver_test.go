package nvidia

import (
	"context"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// fakeNvidiaSMIRunner validates the argv integration boundary with simulated
// command output. It does not establish compatibility with live NVIDIA
// hardware or a real nvidia-smi binary.
type fakeNvidiaSMIRunner struct {
	output   []byte
	err      error
	commands []Command
}

func (r *fakeNvidiaSMIRunner) Run(_ context.Context, command Command) ([]byte, error) {
	r.commands = append(r.commands, Command{Argv: append([]string(nil), command.Argv...)})
	return append([]byte(nil), r.output...), r.err
}

func TestLocalDriverProbePublishesObservedDriverVersionFromFakeCommand(t *testing.T) {
	observedAt := time.Date(2026, time.August, 29, 8, 30, 0, 0, time.UTC)
	runner := &fakeNvidiaSMIRunner{output: []byte(strings.Join([]string{
		"0, GPU-aaaa, 81920, NVIDIA H100, 550.54.14",
		"1, GPU-bbbb, 81920, NVIDIA H100, 550.54.14",
	}, "\n"))}
	driver := newLocalDriver(runner, func() time.Time { return observedAt })

	probe, err := driver.Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if !probe.Available || probe.Reason != "" {
		t.Fatalf("Probe() = available:%v reason:%q, want available without reason", probe.Available, probe.Reason)
	}
	wantCommand := []string{
		commandNvidiaSMI,
		"--query-gpu=index,uuid,memory.total,name,driver_version",
		"--format=csv,noheader,nounits",
	}
	if len(runner.commands) != 1 || !reflect.DeepEqual(runner.commands[0].Argv, wantCommand) {
		t.Fatalf("commands = %+v, want one argv-only query %v", runner.commands, wantCommand)
	}
	if len(probe.Devices) != 2 {
		t.Fatalf("devices = %d, want 2", len(probe.Devices))
	}
	if probe.Devices[0].GetAllocatable().GetCpuMillis() != ^uint64(0) || probe.Devices[0].GetAllocatable().GetEphemeralStorageBytes() != ^uint64(0) {
		t.Fatalf("GPU scheduling projection must not reject pod-level CPU/storage demand: %+v", probe.Devices[0].GetAllocatable())
	}
	assertObservedDriverVersion(t, probe.Capabilities, observedAt, "550.54.14")
	for index, device := range probe.Devices {
		assertObservedDriverVersion(t, device.GetCapabilities(), observedAt, "550.54.14")
		if device.GetCapabilities() == probe.Capabilities {
			t.Fatalf("device %d capabilities alias probe capabilities", index)
		}
	}

	probe.Devices[0].Capabilities.ComponentVersions[0].Version = "caller-mutation"
	if got := probe.Devices[1].GetCapabilities().GetComponentVersions()[0].GetVersion(); got != "550.54.14" {
		t.Fatalf("device capability clones alias each other: version = %q", got)
	}
	if got := probe.Capabilities.GetComponentVersions()[0].GetVersion(); got != "550.54.14" {
		t.Fatalf("device capabilities alias probe capabilities: version = %q", got)
	}
}

func TestLocalDriverProbeRejectsConflictingDriverVersionsFromFakeCommand(t *testing.T) {
	runner := &fakeNvidiaSMIRunner{output: []byte(strings.Join([]string{
		"0, GPU-aaaa, 81920, NVIDIA H100, 550.54.14",
		"1, GPU-bbbb, 81920, NVIDIA H100, 555.42.02",
	}, "\n"))}
	driver := newLocalDriver(runner, func() time.Time {
		return time.Date(2026, time.August, 29, 8, 30, 0, 0, time.UTC)
	})

	probe, err := driver.Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if probe.Available {
		t.Fatal("Probe().Available = true, want false for conflicting driver versions")
	}
	if !strings.Contains(probe.Reason, "conflicting nvidia driver versions") {
		t.Fatalf("Probe().Reason = %q, want explicit driver-version conflict", probe.Reason)
	}
	if len(probe.Devices) != 0 {
		t.Fatalf("Probe().Devices = %v, want no authoritative devices", probe.Devices)
	}
	if versions := probe.Capabilities.GetComponentVersions(); len(versions) != 0 {
		t.Fatalf("component versions = %v, want none for conflicting observations", versions)
	}
}

func TestLocalDriverProbeRejectsUnavailableDriverVersionFromFakeCommand(t *testing.T) {
	runner := &fakeNvidiaSMIRunner{output: []byte("0, GPU-aaaa, 81920, NVIDIA H100, N/A")}
	driver := newLocalDriver(runner, func() time.Time {
		return time.Date(2026, time.August, 29, 8, 30, 0, 0, time.UTC)
	})

	probe, err := driver.Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if probe.Available || !strings.Contains(probe.Reason, "unavailable driver_version") {
		t.Fatalf("Probe() = available:%v reason:%q, want unavailable driver version", probe.Available, probe.Reason)
	}
	if versions := probe.Capabilities.GetComponentVersions(); len(versions) != 0 {
		t.Fatalf("component versions = %v, want none without authoritative version evidence", versions)
	}
}

func TestLocalDriverProbeDoesNotInventVersionWhenFakeCommandIsUnavailable(t *testing.T) {
	runner := &fakeNvidiaSMIRunner{err: &exec.Error{Name: commandNvidiaSMI, Err: exec.ErrNotFound}}
	driver := newLocalDriver(runner, func() time.Time {
		return time.Date(2026, time.August, 29, 8, 30, 0, 0, time.UTC)
	})

	probe, err := driver.Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if probe.Available || probe.Reason != "nvidia-smi not found" {
		t.Fatalf("Probe() = available:%v reason:%q, want nvidia-smi unavailable", probe.Available, probe.Reason)
	}
	if versions := probe.Capabilities.GetComponentVersions(); len(versions) != 0 {
		t.Fatalf("component versions = %v, want none without command evidence", versions)
	}
}

func assertObservedDriverVersion(t *testing.T, capabilities *tgsrlv1.CapabilitySet, observedAt time.Time, version string) {
	t.Helper()
	if capabilities == nil {
		t.Fatal("capabilities = nil")
	}
	if !capabilities.GetMeasuredAt().AsTime().Equal(observedAt) {
		t.Fatalf("capability measured_at = %v, want %v", capabilities.GetMeasuredAt(), observedAt)
	}
	if capabilities.GetRevision() != capabilityObservationRevision {
		t.Fatalf("capability observation revision = %d, want %d", capabilities.GetRevision(), capabilityObservationRevision)
	}
	versions := capabilities.GetComponentVersions()
	if len(versions) != 1 {
		t.Fatalf("component versions = %d, want 1", len(versions))
	}
	got := versions[0]
	if got.GetKind() != tgsrlv1.ComponentKind_COMPONENT_KIND_CUDA_DRIVER || got.GetName() != "nvidia-driver" || got.GetVersion() != version {
		t.Fatalf("component version identity = %+v, want CUDA_DRIVER nvidia-driver %q", got, version)
	}
	if got.GetSource() != commandNvidiaSMI || got.GetRevision() != capabilities.GetRevision() {
		t.Fatalf("component version provenance = source:%q revision:%d, want %q revision %d", got.GetSource(), got.GetRevision(), commandNvidiaSMI, capabilities.GetRevision())
	}
	if !got.GetObservedAt().AsTime().Equal(observedAt) {
		t.Fatalf("component observed_at = %v, want %v", got.GetObservedAt(), observedAt)
	}
	if got.GetAttributes()["query_field"] != "driver_version" || got.GetAttributes()["scope"] != "nvidia-kernel-driver" {
		t.Fatalf("component attributes = %v, want driver query provenance", got.GetAttributes())
	}
	for _, component := range versions {
		if component.GetKind() == tgsrlv1.ComponentKind_COMPONENT_KIND_PROVIDER {
			t.Fatalf("provider component version was fabricated: %+v", component)
		}
	}
}
