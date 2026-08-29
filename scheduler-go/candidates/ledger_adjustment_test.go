package candidates

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

func TestEngineLedgerAdjustmentResizeMayReuseCurrentDevice(t *testing.T) {
	request := ledgerAdjustmentRequest()
	request.Snapshot.Devices[0].Allocatable.CpuMillis = 20
	request.Snapshot.Devices[0].Allocatable.AcceleratorUnits = 0.1
	request.Snapshot.Allocations = []*tgsrlv1.Allocation{ledgerAllocation("current", "device-a", 80, 0.4, tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE)}
	request.Intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 90, AcceleratorUnits: 0.5}
	request.LedgerAdjustment = LedgerAdjustment{
		ReclaimAllocationIDs: []string{"current"},
		IncludeDeviceIDs:     []string{"device-a"},
	}

	result, err := (Engine{}).Evaluate(request)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if len(result.Selected) != 1 || result.Selected[0].DeviceID != "device-a" {
		t.Fatalf("selected = %+v, want resized allocation on device-a", result.Selected)
	}
	if got := request.Snapshot.Devices[0].GetAllocatable(); got.GetCpuMillis() != 20 || got.GetAcceleratorUnits() != 0.1 {
		t.Fatalf("snapshot allocatable mutated to %+v", got)
	}
}

func TestEngineLedgerAdjustmentRebindExcludesCurrentDevice(t *testing.T) {
	request := ledgerAdjustmentRequest()
	request.Snapshot.Devices[0].Allocatable.CpuMillis = 0
	request.Snapshot.Allocations = []*tgsrlv1.Allocation{ledgerAllocation("current", "device-a", 100, 0, tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE)}
	request.Intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 100}
	request.LedgerAdjustment = LedgerAdjustment{
		ReclaimAllocationIDs: []string{"current"},
		ExcludeDeviceIDs:     []string{"device-a"},
	}

	result, err := (Engine{}).Evaluate(request)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if len(result.Selected) != 1 || result.Selected[0].DeviceID != "device-b" {
		t.Fatalf("selected = %+v, want rebind to device-b", result.Selected)
	}
	if result.TotalCandidateCount != 1 || result.TotalRejectionCount != 0 {
		t.Fatalf("evaluated totals = %d/%d, want only included surface 1/0", result.TotalCandidateCount, result.TotalRejectionCount)
	}
}

func TestEngineLedgerAdjustmentReclaimDoesNotDoubleReturnResources(t *testing.T) {
	request := ledgerAdjustmentRequest()
	request.Snapshot.Devices = request.Snapshot.Devices[:1]
	request.Snapshot.Devices[0].Allocatable.CpuMillis = 90
	request.Snapshot.Devices[0].Allocatable.AcceleratorUnits = 0.9
	request.Snapshot.Allocations = []*tgsrlv1.Allocation{ledgerAllocation("current", "device-a", 30, 0.3, tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING)}
	request.Intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 101, AcceleratorUnits: 1.01}
	request.LedgerAdjustment = LedgerAdjustment{ReclaimAllocationIDs: []string{"current", "current", "current"}}

	result, err := (Engine{}).Evaluate(request)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if len(result.Selected) != 0 || result.TotalCandidateCount != 0 || result.TotalRejectionCount != 1 {
		t.Fatalf("result = %+v, want request rejected after one capped return", result)
	}
}

func TestEngineLedgerAdjustmentInputPermutationIsDeterministic(t *testing.T) {
	base := ledgerAdjustmentRequest()
	base.Snapshot.Devices = append(base.Snapshot.Devices, ledgerDevice("device-c", 100, 1))
	base.Snapshot.Devices[0].Allocatable.CpuMillis = 40
	base.Snapshot.Devices[2].Allocatable.CpuMillis = 50
	base.Snapshot.Allocations = []*tgsrlv1.Allocation{
		ledgerAllocation("allocation-b", "device-c", 50, 0, tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING),
		ledgerAllocation("allocation-a", "device-a", 60, 0, tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE),
	}
	base.Intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 100}

	first := base
	first.LedgerAdjustment = LedgerAdjustment{
		ReclaimAllocationIDs: []string{"allocation-b", "allocation-a", "allocation-b"},
		IncludeDeviceIDs:     []string{"device-c", "device-a", "device-c"},
		ExcludeDeviceIDs:     []string{"device-b", "device-b"},
	}
	second := base
	second.LedgerAdjustment = LedgerAdjustment{
		ReclaimAllocationIDs: []string{"allocation-a", "allocation-b"},
		IncludeDeviceIDs:     []string{"device-a", "device-c"},
		ExcludeDeviceIDs:     []string{"device-b"},
	}

	firstResult, err := (Engine{}).Evaluate(first)
	if err != nil {
		t.Fatalf("first Evaluate() error = %v", err)
	}
	secondResult, err := (Engine{}).Evaluate(second)
	if err != nil {
		t.Fatalf("second Evaluate() error = %v", err)
	}
	if !reflect.DeepEqual(firstResult, secondResult) {
		t.Fatalf("permuted adjustment changed result:\nfirst:  %+v\nsecond: %+v", firstResult, secondResult)
	}
}

func TestEngineLedgerAdjustmentRejectsInvalidReferencesDeterministically(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*Request)
		wantDetail string
	}{
		{
			name: "missing reclaimed allocation chooses lexicographically first ID",
			configure: func(request *Request) {
				request.LedgerAdjustment.ReclaimAllocationIDs = []string{"missing-z", "missing-a"}
			},
			wantDetail: `reclaimed allocation "missing-a" does not exist`,
		},
		{
			name: "duplicate reclaimed allocation ID is ambiguous",
			configure: func(request *Request) {
				request.Snapshot.Allocations = []*tgsrlv1.Allocation{
					ledgerAllocation("current", "device-a", 1, 0, tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE),
					ledgerAllocation("current", "device-b", 1, 0, tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING),
				}
				request.LedgerAdjustment.ReclaimAllocationIDs = []string{"current"}
			},
			wantDetail: `reclaimed allocation ID "current" is not unique`,
		},
		{
			name: "released allocation cannot be reclaimed",
			configure: func(request *Request) {
				request.Snapshot.Allocations = []*tgsrlv1.Allocation{ledgerAllocation("released", "device-a", 1, 0, tgsrlv1.AllocationState_ALLOCATION_STATE_RELEASED)}
				request.LedgerAdjustment.ReclaimAllocationIDs = []string{"released"}
			},
			wantDetail: `reclaimed allocation "released" must be PENDING or ACTIVE`,
		},
		{
			name: "allocation cannot reference unknown device",
			configure: func(request *Request) {
				request.Snapshot.Allocations = []*tgsrlv1.Allocation{ledgerAllocation("current", "missing-device", 1, 0, tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE)}
				request.LedgerAdjustment.ReclaimAllocationIDs = []string{"current"}
			},
			wantDetail: `reclaimed allocation "current" references unknown device "missing-device"`,
		},
		{
			name: "multi-device reclaim is rejected",
			configure: func(request *Request) {
				allocation := ledgerAllocation("current", "device-a", 1, 0, tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE)
				allocation.DeviceIds = []string{"device-b", "device-a"}
				request.Snapshot.Allocations = []*tgsrlv1.Allocation{allocation}
				request.LedgerAdjustment.ReclaimAllocationIDs = []string{"current"}
			},
			wantDetail: `reclaimed allocation "current" must reference exactly one device, got 2`,
		},
		{
			name: "include and exclude overlap chooses first device",
			configure: func(request *Request) {
				request.LedgerAdjustment.IncludeDeviceIDs = []string{"device-b", "device-a"}
				request.LedgerAdjustment.ExcludeDeviceIDs = []string{"device-a", "device-b"}
			},
			wantDetail: `device "device-a" appears in both include and exclude filters`,
		},
		{
			name: "include device must exist",
			configure: func(request *Request) {
				request.LedgerAdjustment.IncludeDeviceIDs = []string{"missing-z", "missing-a"}
			},
			wantDetail: `included device "missing-a" does not exist`,
		},
		{
			name: "exclude device must exist",
			configure: func(request *Request) {
				request.LedgerAdjustment.ExcludeDeviceIDs = []string{"missing-z", "missing-a"}
			},
			wantDetail: `excluded device "missing-a" does not exist`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := ledgerAdjustmentRequest()
			test.configure(&request)
			_, err := (Engine{}).Evaluate(request)
			if !errors.Is(err, ErrInvalidLedgerAdjustment) || !strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("Evaluate() error = %v, want invalid adjustment containing %q", err, test.wantDetail)
			}
		})
	}
}

func TestEngineZeroLedgerAdjustmentPreservesResult(t *testing.T) {
	request := ledgerAdjustmentRequest()
	request.Snapshot.Allocations = []*tgsrlv1.Allocation{ledgerAllocation("current", "device-a", 10, 0.1, tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE)}
	request.Intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 10, AcceleratorUnits: 0.1}

	withoutField, err := (Engine{}).Evaluate(request)
	if err != nil {
		t.Fatalf("Evaluate() without adjustment error = %v", err)
	}
	request.LedgerAdjustment = LedgerAdjustment{}
	withZero, err := (Engine{}).Evaluate(request)
	if err != nil {
		t.Fatalf("Evaluate() with zero adjustment error = %v", err)
	}
	if !reflect.DeepEqual(withoutField, withZero) {
		t.Fatalf("zero adjustment changed result:\nwithout: %+v\nwith:    %+v", withoutField, withZero)
	}
}

func TestEngineLedgerAdjustmentDoesNotExposeProjectionToCallbacks(t *testing.T) {
	request := ledgerAdjustmentRequest()
	request.Snapshot.Devices[0].Allocatable.CpuMillis = 0
	request.Snapshot.Allocations = []*tgsrlv1.Allocation{ledgerAllocation("current", "device-a", 100, 0, tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE)}
	request.Intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 100}
	request.LedgerAdjustment = LedgerAdjustment{ReclaimAllocationIDs: []string{"current"}, IncludeDeviceIDs: []string{"device-a"}}
	request.BuildCandidate = func(input CandidateInput) *tgsrlv1.PlacementCandidate {
		if input.Snapshot.GetDevices()[0].GetAllocatable().GetCpuMillis() != 0 {
			t.Fatalf("factory snapshot contains projected allocatable resources")
		}
		if !reflect.DeepEqual(input.Snapshot.GetAllocations(), request.Snapshot.GetAllocations()) {
			t.Fatalf("factory snapshot allocation ledger was projected")
		}
		return defaultCandidateFactory(input)
	}
	request.Select = func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent, _ string, candidates []*tgsrlv1.PlacementCandidate, _ int) (*tgsrlv1.PlacementCandidate, string) {
		if len(snapshot.GetDevices()) != 2 || snapshot.GetDevices()[0].GetAllocatable().GetCpuMillis() != 0 {
			t.Fatalf("selector snapshot contains filtered or projected devices: %+v", snapshot.GetDevices())
		}
		return candidates[0], ""
	}

	result, err := (Engine{}).Evaluate(request)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if len(result.Selected) != 1 || result.Selected[0].DeviceID != "device-a" {
		t.Fatalf("selected = %+v, want projected device-a", result.Selected)
	}
}

func ledgerAdjustmentRequest() Request {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	return Request{
		Snapshot: &tgsrlv1.ClusterSnapshot{
			SnapshotId: "snapshot",
			Revision:   1,
			Devices: []*tgsrlv1.Device{
				ledgerDevice("device-a", 100, 1),
				ledgerDevice("device-b", 100, 1),
			},
		},
		Intent: &tgsrlv1.SchedulingIntent{
			ExecutionId:      "execution",
			StageId:          "stage",
			Version:          1,
			ResourcesPerUnit: &tgsrlv1.ResourceVector{CpuMillis: 10},
		},
		Units: []Unit{{ID: "unit"}},
		CandidateID: func(unitID, deviceID string) string {
			return unitID + "/" + deviceID
		},
		Config:   Config{TopK: 1, EvidenceBudget: 16},
		Decision: DecisionMetadata{DecisionID: now.Format(time.RFC3339)},
	}
}

func ledgerDevice(id string, cpu uint64, accelerator float64) *tgsrlv1.Device {
	resources := &tgsrlv1.ResourceVector{CpuMillis: cpu, AcceleratorUnits: accelerator}
	return &tgsrlv1.Device{
		DeviceId:    id,
		Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
		Capacity:    proto.Clone(resources).(*tgsrlv1.ResourceVector),
		Allocatable: proto.Clone(resources).(*tgsrlv1.ResourceVector),
		Capabilities: &tgsrlv1.CapabilitySet{
			Source:           "provider",
			Revision:         1,
			MeasuredAt:       engineRequirementsAvailableCapabilities().GetMeasuredAt(),
			SupportedActions: []string{"bind", "resize", "rebind"},
		},
	}
}

func ledgerAllocation(id, deviceID string, cpu uint64, accelerator float64, state tgsrlv1.AllocationState) *tgsrlv1.Allocation {
	return &tgsrlv1.Allocation{
		AllocationId: id,
		DeviceIds:    []string{deviceID},
		Resources:    &tgsrlv1.ResourceVector{CpuMillis: cpu, AcceleratorUnits: accelerator},
		State:        state,
	}
}
