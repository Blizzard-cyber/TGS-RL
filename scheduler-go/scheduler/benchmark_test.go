package scheduler

import (
	"fmt"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

func BenchmarkEvaluateSimulation(b *testing.B) {
	benchmarks := []struct {
		name    string
		devices int
		units   int
	}{
		{name: "8-devices-100-units", devices: 8, units: 100},
		{name: "1000-devices-1-unit", devices: 1000, units: 1},
		{name: "1000-devices-1000-units", devices: 1000, units: 1000},
		{name: "100-devices-100-units", devices: 100, units: 100},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			snapshot, intent := simulationFixture(benchmark.devices, benchmark.units)
			scheduler := testScheduler(b, FallbackNoOp)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				plan, record, err := scheduler.Evaluate(snapshot, intent)
				if err != nil || record.GetFallback() || len(plan.GetBindings()) != benchmark.units {
					b.Fatalf("Evaluate() = (%d bindings, fallback=%v, error=%v)", len(plan.GetBindings()), record.GetFallback(), err)
				}
			}
		})
	}
}

func TestEvaluateLargeScaleCorrectness(t *testing.T) {
	if testing.Short() {
		t.Skip("large-scale correctness gate skipped in short mode")
	}
	snapshot, intent := simulationFixture(1000, 1000)
	plan, decision, err := testScheduler(t, FallbackNoOp).Evaluate(snapshot, intent)
	if err != nil || decision.GetFallback() || len(plan.GetBindings()) != 1000 {
		t.Fatalf("large-scale Evaluate() = (%d bindings, fallback=%v, error=%v)", len(plan.GetBindings()), decision.GetFallback(), err)
	}
	seen := make(map[string]struct{}, len(plan.GetBindings()))
	for _, binding := range plan.GetBindings() {
		if _, duplicate := seen[binding.GetPendingUnitId()]; duplicate {
			t.Fatalf("pending unit %q was bound twice", binding.GetPendingUnitId())
		}
		seen[binding.GetPendingUnitId()] = struct{}{}
	}
}

func simulationFixture(deviceCount, unitCount int) (*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent) {
	snapshot, intent := validFixture()
	snapshot.Devices = make([]*tgsrlv1.Device, 0, deviceCount)
	templateDevice := validDeviceFromFixture()
	for index := 0; index < deviceCount; index++ {
		device := proto.Clone(templateDevice).(*tgsrlv1.Device)
		device.DeviceId = fmt.Sprintf("device-%04d", index)
		device.Capacity.CpuMillis = 1_000_000
		device.Allocatable.CpuMillis = 1_000_000
		device.Capacity.MemoryBytes = 1_000_000
		device.Allocatable.MemoryBytes = 1_000_000
		device.Capacity.AcceleratorUnits = 1
		device.Allocatable.AcceleratorUnits = 1
		device.Capacity.EphemeralStorageBytes = 1_000_000
		device.Allocatable.EphemeralStorageBytes = 1_000_000
		device.Capacity.NetworkBandwidthBps = 1_000_000
		device.Allocatable.NetworkBandwidthBps = 1_000_000
		snapshot.Devices = append(snapshot.Devices, device)
	}
	snapshot.PendingUnits = nil
	intent.UnitCount = uint32(unitCount)
	intent.ResourcesPerUnit.AcceleratorUnits = 0
	return snapshot, intent
}

func validDeviceFromFixture() *tgsrlv1.Device {
	snapshot, _ := validFixture()
	return snapshot.GetDevices()[0]
}
