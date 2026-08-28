package candidates

import (
	"fmt"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scoring"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEngineUsesStableUnitOrderAndConsumesAfterSelection(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	snapshot, intent := engineFixture(now)
	snapshot.Devices = []*tgsrlv1.Device{
		engineDevice("device-b", now, &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 100, AcceleratorUnits: 1, EphemeralStorageBytes: 100, NetworkBandwidthBps: 100}),
		engineDevice("device-a", now, &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 100, AcceleratorUnits: 1, EphemeralStorageBytes: 100, NetworkBandwidthBps: 100}),
	}
	request := &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 10, EphemeralStorageBytes: 10, NetworkBandwidthBps: 10}
	intent.ResourcesPerUnit = request
	units := []Unit{
		{ID: "unit-later", Pending: &tgsrlv1.PendingUnit{PendingUnitId: "unit-later", Priority: 1, QueuedAt: timestamppb.New(now.Add(time.Minute)), RequestedResources: request, RequiredCapabilities: intent.RequiredCapabilities}},
		{ID: "unit-first", Pending: &tgsrlv1.PendingUnit{PendingUnitId: "unit-first", Priority: 2, QueuedAt: timestamppb.New(now), RequestedResources: request, RequiredCapabilities: intent.RequiredCapabilities}},
	}

	result, err := (Engine{}).Evaluate(Request{
		Snapshot:       snapshot,
		Intent:         intent,
		Units:          units,
		BuildCandidate: bindingCandidate,
		Config:         Config{TopK: 1},
	})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if result.Fallback != "" || len(result.Selected) != 2 {
		t.Fatalf("result = %+v, want two selections", result)
	}
	if got := []string{result.Selected[0].UnitID, result.Selected[1].UnitID}; got[0] != "unit-first" || got[1] != "unit-later" {
		t.Fatalf("selected unit order = %v", got)
	}
	if result.Selected[0].DeviceID != "device-a" || result.Selected[1].DeviceID != "device-b" {
		t.Fatalf("selected devices = %q, %q; want device-a then device-b after consume", result.Selected[0].DeviceID, result.Selected[1].DeviceID)
	}
	for _, selected := range result.Selected {
		components := selected.Candidate.GetComponentScores()
		if len(components) != 4 {
			t.Fatalf("components = %#v, want exactly four", components)
		}
		wantScore := roundScore(components[scoring.CapacityHeadroomComponent] + components[scoring.ShareHeadroomComponent] + components[scoring.ReadyComponent] + components[scoring.TraceAffinityComponent])
		if selected.Candidate.GetScore() != wantScore {
			t.Fatalf("score = %v, want component sum %v", selected.Candidate.GetScore(), wantScore)
		}
	}
}

func TestEnginePassesStableScoreFirstTopKShortlistToPolicy(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	snapshot, intent := engineFixture(now)
	snapshot.Devices = []*tgsrlv1.Device{
		engineDevice("device-a-tight", now, &tgsrlv1.ResourceVector{CpuMillis: 110, MemoryBytes: 110, AcceleratorUnits: 1}),
		engineDevice("device-b-middle", now, &tgsrlv1.ResourceVector{CpuMillis: 300, MemoryBytes: 300, AcceleratorUnits: 1}),
		engineDevice("device-c-roomy", now, &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1000, AcceleratorUnits: 1}),
	}
	intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 100}

	for _, test := range []struct {
		name           string
		topK           int
		evidenceBudget int
		wantDevices    []string
		selectIndex    int
	}{
		{name: "K1-independent-of-small-evidence-budget", topK: 1, evidenceBudget: 1, wantDevices: []string{"device-c-roomy"}},
		{name: "K2-independent-of-large-evidence-budget", topK: 2, evidenceBudget: 64, wantDevices: []string{"device-c-roomy", "device-b-middle"}, selectIndex: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var policyInput []*tgsrlv1.PlacementCandidate
			result, err := (Engine{}).Evaluate(Request{
				Snapshot: snapshot, Intent: intent, Units: []Unit{{ID: "unit-1"}},
				BuildCandidate: bindingCandidate,
				Config:         Config{TopK: test.topK, EvidenceBudget: test.evidenceBudget},
				Select: func(_ *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent, unitID string, input []*tgsrlv1.PlacementCandidate, topK int) (*tgsrlv1.PlacementCandidate, string) {
					if unitID != "unit-1" || topK != test.topK {
						t.Fatalf("selector input unit=%q topK=%d", unitID, topK)
					}
					policyInput = input
					return input[test.selectIndex], ""
				},
			})
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if len(policyInput) != len(test.wantDevices) {
				t.Fatalf("policy shortlist length = %d, want %d", len(policyInput), len(test.wantDevices))
			}
			for index, candidate := range policyInput {
				deviceID := candidate.GetPlan().GetBindings()[0].GetDeviceIds()[0]
				if deviceID != test.wantDevices[index] {
					t.Fatalf("policy shortlist[%d] device = %q, want %q", index, deviceID, test.wantDevices[index])
				}
			}
			if len(result.Selected) != 1 || result.Selected[0].Candidate != policyInput[test.selectIndex] || result.Selected[0].DeviceID != test.wantDevices[test.selectIndex] {
				t.Fatalf("selected = %+v, want exact shortlist item %d on %q", result.Selected, test.selectIndex, test.wantDevices[test.selectIndex])
			}
			if result.TotalCandidateCount != uint64(len(snapshot.GetDevices())) || result.TotalRejectionCount != 0 {
				t.Fatalf("pair totals = %d/%d, want %d/0", result.TotalCandidateCount, result.TotalRejectionCount, len(snapshot.GetDevices()))
			}
		})
	}
}

func TestEngineRejectsSelectionOutsidePolicyInput(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	snapshot, intent := engineFixture(now)
	snapshot.Devices = []*tgsrlv1.Device{engineDevice("device-a", now, &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1000, AcceleratorUnits: 1})}
	intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 100}
	result, err := (Engine{}).Evaluate(Request{
		Snapshot: snapshot, Intent: intent, Units: []Unit{{ID: "unit-1"}}, BuildCandidate: bindingCandidate,
		Select: func(_ *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent, _ string, input []*tgsrlv1.PlacementCandidate, _ int) (*tgsrlv1.PlacementCandidate, string) {
			return &tgsrlv1.PlacementCandidate{CandidateId: input[0].GetCandidateId()}, ""
		},
	})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if result.Fallback != FallbackInvalidSelection || result.FailureUnit != "unit-1" || len(result.Selected) != 0 {
		t.Fatalf("result = %+v, want invalid-selection fallback", result)
	}
}

func TestEngineIsolatesCandidateFactoryInputMutations(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	snapshot, intent := engineFixture(now)
	snapshot.Devices = []*tgsrlv1.Device{
		engineDevice("device-a", now, &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 100, AcceleratorUnits: 1}),
		engineDevice("device-b", now, &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 100, AcceleratorUnits: 1}),
	}
	intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 1}
	units := []Unit{
		{ID: "unit-a", Pending: &tgsrlv1.PendingUnit{PendingUnitId: "unit-a", RequestedResources: intent.ResourcesPerUnit, RequiredCapabilities: intent.RequiredCapabilities}},
		{ID: "unit-b", Pending: &tgsrlv1.PendingUnit{PendingUnitId: "unit-b", RequestedResources: intent.ResourcesPerUnit, RequiredCapabilities: intent.RequiredCapabilities}},
	}
	factoryCalls := 0
	result, err := (Engine{}).Evaluate(Request{
		Snapshot: snapshot, Intent: intent, Units: units, Config: Config{TopK: 1},
		BuildCandidate: func(input CandidateInput) *tgsrlv1.PlacementCandidate {
			factoryCalls++
			candidate := defaultCandidateFactory(input)
			input.Snapshot.Devices[0].Allocatable.CpuMillis = 0
			input.Intent.ResourcesPerUnit.CpuMillis = 0
			input.Unit.Pending.RequestedResources.CpuMillis = 0
			input.Device.Allocatable.CpuMillis = 0
			input.RequestedResources.CpuMillis = 0
			input.RequiredCapabilities.Names[0] = "mutated"
			input.Components[scoring.ReadyComponent] = -100
			return candidate
		},
	})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if factoryCalls < 2 || len(result.Selected) != 2 {
		t.Fatalf("factoryCalls=%d selected=%d, want at least 2/2", factoryCalls, len(result.Selected))
	}
	if result.Selected[0].DeviceID != "device-a" || result.Selected[1].DeviceID != "device-b" {
		t.Fatalf("selected devices = %q, %q; callback mutation corrupted ledger", result.Selected[0].DeviceID, result.Selected[1].DeviceID)
	}
	if intent.GetResourcesPerUnit().GetCpuMillis() != 100 || snapshot.GetDevices()[0].GetAllocatable().GetCpuMillis() != 100 {
		t.Fatal("factory mutation escaped into caller inputs")
	}
}

func TestEngineClonesReusedCandidateFactoryOutput(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	snapshot, intent := engineFixture(now)
	snapshot.Devices = []*tgsrlv1.Device{
		engineDevice("device-a", now, &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1000, AcceleratorUnits: 1}),
		engineDevice("device-b", now, &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1000, AcceleratorUnits: 1}),
	}
	intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 1, MemoryBytes: 1}
	scratch := &tgsrlv1.PlacementCandidate{}
	var policySurface []*tgsrlv1.PlacementCandidate
	result, err := (Engine{}).Evaluate(Request{
		Snapshot: snapshot, Intent: intent, Units: []Unit{{ID: "unit-1"}}, Config: Config{TopK: 2},
		BuildCandidate: func(input CandidateInput) *tgsrlv1.PlacementCandidate {
			scratch.Plan = defaultCandidateFactory(input).Plan
			return scratch
		},
		Select: func(_ *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent, _ string, input []*tgsrlv1.PlacementCandidate, _ int) (*tgsrlv1.PlacementCandidate, string) {
			policySurface = input
			return input[1], ""
		},
	})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if len(policySurface) != 2 || policySurface[0] == policySurface[1] {
		t.Fatalf("policy surface pointers = %p/%p, want distinct clones", policySurface[0], policySurface[1])
	}
	if len(result.Selected) != 1 || result.Selected[0].Candidate != policySurface[1] || result.Selected[0].DeviceID != "device-b" {
		t.Fatalf("selected = %+v, want exact second policy object on device-b", result.Selected)
	}
	for _, candidate := range result.Candidates {
		binding := candidate.GetPlan().GetBindings()[0]
		if binding.GetDeviceIds()[0] == "" {
			t.Fatalf("candidate %q lost device identity", candidate.GetCandidateId())
		}
	}
}

func TestEngineEvidenceBudgetRetainsEverySelectedCandidate(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	snapshot, intent := engineFixture(now)
	for index := 0; index < 3; index++ {
		snapshot.Devices = append(snapshot.Devices, engineDevice(fmt.Sprintf("device-%d", index), now, &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1000, AcceleratorUnits: 1}))
	}
	intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 1, MemoryBytes: 1}
	units := make([]Unit, 6)
	for index := range units {
		units[index].ID = fmt.Sprintf("unit-%d", index)
	}
	result, err := (Engine{}).Evaluate(Request{
		Snapshot: snapshot, Intent: intent, Units: units, BuildCandidate: bindingCandidate,
		Config: Config{TopK: 1, EvidenceBudget: 2},
	})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if len(result.Selected) != 6 || len(result.Candidates) != 6 {
		t.Fatalf("selected/evidence = %d/%d, want 6/6 despite budget 2", len(result.Selected), len(result.Candidates))
	}
	if !result.EvidenceTruncated || result.TotalCandidateCount != 18 || result.TotalRejectionCount != 0 {
		t.Fatalf("evidence metadata = candidates:%d rejections:%d truncated:%v", result.TotalCandidateCount, result.TotalRejectionCount, result.EvidenceTruncated)
	}
	recorded := make(map[*tgsrlv1.PlacementCandidate]bool, len(result.Candidates))
	for _, candidate := range result.Candidates {
		recorded[candidate] = true
	}
	for _, selected := range result.Selected {
		if !recorded[selected.Candidate] {
			t.Fatalf("selected %q was not retained by identity", selected.Candidate.GetCandidateId())
		}
	}
}

func TestEngineLargeFrontierAvoidsCartesianCandidateMaterialization(t *testing.T) {
	if testing.Short() {
		t.Skip("large candidate-engine correctness test skipped in short mode")
	}
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	snapshot, intent := engineFixture(now)
	const size = 1000
	capacity := &tgsrlv1.ResourceVector{CpuMillis: 1_000_000, MemoryBytes: 1_000_000, AcceleratorUnits: 1, EphemeralStorageBytes: 1_000_000, NetworkBandwidthBps: 1_000_000}
	snapshot.Devices = make([]*tgsrlv1.Device, size)
	units := make([]Unit, size)
	for index := 0; index < size; index++ {
		snapshot.Devices[index] = engineDevice(fmt.Sprintf("device-%04d", size-index-1), now, capacity)
		units[index] = Unit{ID: fmt.Sprintf("unit-%04d", size-index-1)}
	}
	intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{CpuMillis: 1, MemoryBytes: 1, EphemeralStorageBytes: 1, NetworkBandwidthBps: 1}
	factoryCalls := 0
	result, err := (Engine{}).Evaluate(Request{
		Snapshot: snapshot, Intent: intent, Units: units,
		BuildCandidate: func(input CandidateInput) *tgsrlv1.PlacementCandidate {
			factoryCalls++
			return bindingCandidate(input)
		},
		Config: Config{TopK: 1},
	})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if len(result.Selected) != size || factoryCalls > DefaultEvidenceBudget+size {
		t.Fatalf("selected=%d factoryCalls=%d, want %d selections and at most evidence budget + selections", len(result.Selected), factoryCalls, size)
	}
	if result.TotalCandidateCount != size*size || !result.EvidenceTruncated {
		t.Fatalf("total=%d truncated=%v, want %d/true", result.TotalCandidateCount, result.EvidenceTruncated, size*size)
	}
}

func bindingCandidate(input CandidateInput) *tgsrlv1.PlacementCandidate {
	return &tgsrlv1.PlacementCandidate{
		Plan: &tgsrlv1.PlacementPlan{Bindings: []*tgsrlv1.Binding{{
			PendingUnitId: input.Unit.ID,
			DeviceIds:     []string{input.Device.GetDeviceId()},
			Resources:     cloneResources(input.RequestedResources),
		}}},
	}
}

func engineFixture(now time.Time) (*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent) {
	required := &tgsrlv1.CapabilitySet{
		Names:            []string{"logical-cpu"},
		Attributes:       map[string]string{"vendor": "acme"},
		Algorithms:       []string{"ppo"},
		RolloutModes:     []string{"sync"},
		Source:           "provider",
		Revision:         2,
		MeasuredAt:       timestamppb.New(now.Add(-time.Minute)),
		SupportedActions: []string{"bind"},
		Limits:           map[string]float64{"partitions": 2},
	}
	return &tgsrlv1.ClusterSnapshot{SnapshotId: "snapshot", Revision: 7}, &tgsrlv1.SchedulingIntent{
		ExecutionId: "execution", StageId: "stage", Version: 3, JobId: "job", TraceId: "trace",
		RequiredCapabilities: required,
	}
}

func engineDevice(id string, now time.Time, resources *tgsrlv1.ResourceVector) *tgsrlv1.Device {
	return &tgsrlv1.Device{
		DeviceId: id, Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
		Capacity: cloneResources(resources), Allocatable: cloneResources(resources),
		Capabilities: &tgsrlv1.CapabilitySet{
			Names:            []string{"logical-cpu", "extra"},
			Attributes:       map[string]string{"vendor": "acme"},
			Algorithms:       []string{"ppo", "grpo"},
			RolloutModes:     []string{"sync", "async"},
			Source:           "provider",
			Revision:         3,
			MeasuredAt:       timestamppb.New(now),
			SupportedActions: []string{"bind", "resize"},
			Limits:           map[string]float64{"partitions": 4},
		},
		Labels: map[string]string{"trace_id": "trace"},
	}
}
