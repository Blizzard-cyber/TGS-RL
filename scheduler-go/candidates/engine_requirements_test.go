package candidates

import (
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var engineRequirementsMeasuredAt = time.Date(2026, 8, 28, 8, 0, 0, 0, time.UTC)

func TestEngineAggregatesPendingAndActiveAllocationsAcrossResourceVector(t *testing.T) {
	request := &tgsrlv1.ResourceVector{
		CpuMillis:             250,
		MemoryBytes:           550,
		AcceleratorUnits:      0.625,
		EphemeralStorageBytes: 350,
		NetworkBandwidthBps:   150,
	}
	capacity := &tgsrlv1.ResourceVector{
		CpuMillis:             500,
		MemoryBytes:           1000,
		AcceleratorUnits:      1,
		EphemeralStorageBytes: 1000,
		NetworkBandwidthBps:   1000,
	}
	allocations := []*tgsrlv1.Allocation{
		{
			AllocationId: "pending",
			DeviceIds:    []string{"device-1"},
			State:        tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING,
			Resources: &tgsrlv1.ResourceVector{
				CpuMillis:             100,
				MemoryBytes:           200,
				AcceleratorUnits:      0.125,
				EphemeralStorageBytes: 300,
				NetworkBandwidthBps:   400,
			},
		},
		{
			AllocationId: "active",
			DeviceIds:    []string{"device-1"},
			State:        tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE,
			Resources: &tgsrlv1.ResourceVector{
				CpuMillis:             150,
				MemoryBytes:           250,
				AcceleratorUnits:      0.25,
				EphemeralStorageBytes: 350,
				NetworkBandwidthBps:   450,
			},
		},
		{
			AllocationId: "released-is-not-usage",
			DeviceIds:    []string{"device-1"},
			State:        tgsrlv1.AllocationState_ALLOCATION_STATE_RELEASED,
			Resources:    proto.Clone(capacity).(*tgsrlv1.ResourceVector),
		},
	}

	tests := []struct {
		name       string
		mutate     func(*tgsrlv1.Device, *tgsrlv1.ResourceVector)
		wantReason tgsrlv1.CandidateRejectionReason
		wantDetail string
	}{
		{name: "READY device accepts exact five-dimensional and share boundary"},
		{
			name: "non-READY device is rejected",
			mutate: func(device *tgsrlv1.Device, _ *tgsrlv1.ResourceVector) {
				device.Health = tgsrlv1.DeviceHealth_DEVICE_HEALTH_DRAINING
			},
			wantReason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VALIDITY_RULE,
			wantDetail: "device is not READY",
		},
		{
			name: "pending plus active CPU exceeds capacity",
			mutate: func(device *tgsrlv1.Device, _ *tgsrlv1.ResourceVector) {
				device.Capacity.CpuMillis--
			},
			wantReason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES,
			wantDetail: "pending and active allocations plus request exceed device capacity",
		},
		{
			name: "pending plus active memory exceeds capacity",
			mutate: func(device *tgsrlv1.Device, _ *tgsrlv1.ResourceVector) {
				device.Capacity.MemoryBytes--
			},
			wantReason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES,
			wantDetail: "pending and active allocations plus request exceed device capacity",
		},
		{
			name: "pending plus active accelerator exceeds physical capacity",
			mutate: func(device *tgsrlv1.Device, _ *tgsrlv1.ResourceVector) {
				device.Capacity.AcceleratorUnits = 0.875
			},
			wantReason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES,
			wantDetail: "pending and active allocations plus request exceed device capacity",
		},
		{
			name: "pending plus active ephemeral storage exceeds capacity",
			mutate: func(device *tgsrlv1.Device, _ *tgsrlv1.ResourceVector) {
				device.Capacity.EphemeralStorageBytes--
			},
			wantReason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES,
			wantDetail: "pending and active allocations plus request exceed device capacity",
		},
		{
			name: "pending plus active network bandwidth exceeds capacity",
			mutate: func(device *tgsrlv1.Device, _ *tgsrlv1.ResourceVector) {
				device.Capacity.NetworkBandwidthBps--
			},
			wantReason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES,
			wantDetail: "pending and active allocations plus request exceed device capacity",
		},
		{
			name: "aggregate accelerator share exceeds one despite physical capacity",
			mutate: func(device *tgsrlv1.Device, request *tgsrlv1.ResourceVector) {
				device.Capacity.AcceleratorUnits = 2
				device.Allocatable.AcceleratorUnits = 2
				request.AcceleratorUnits = 0.75
			},
			wantReason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES,
			wantDetail: "aggregate pending and active accelerator share plus request exceeds 1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			caseRequest := proto.Clone(request).(*tgsrlv1.ResourceVector)
			device := engineRequirementsDevice(proto.Clone(capacity).(*tgsrlv1.ResourceVector), engineRequirementsAvailableCapabilities())
			if test.mutate != nil {
				test.mutate(device, caseRequest)
			}

			result := engineRequirementsEvaluate(t, engineRequirementsRequest(device, allocations, caseRequest, nil, DecisionMetadata{}))
			if test.wantDetail == "" {
				engineRequirementsAssertSelected(t, result)
				return
			}
			engineRequirementsAssertRejected(t, result, test.wantReason, test.wantDetail)
		})
	}
}

func TestEngineCapabilityFullSurfaceReachesCandidateFactory(t *testing.T) {
	required := engineRequirementsRequiredCapabilities()
	available := engineRequirementsAvailableCapabilities()
	available.Evidence = []*tgsrlv1.CapabilityEvidence{engineRequirementsCapabilityEvidence(available)}
	device := engineRequirementsDevice(engineRequirementsCapacity(), available)
	request := engineRequirementsRequest(device, nil, engineRequirementsSmallRequest(), required, DecisionMetadata{})

	var captured CandidateInput
	request.BuildCandidate = func(input CandidateInput) *tgsrlv1.PlacementCandidate {
		captured = input
		return defaultCandidateFactory(input)
	}

	result := engineRequirementsEvaluate(t, request)
	engineRequirementsAssertSelected(t, result)
	if !proto.Equal(captured.RequiredCapabilities, required) {
		t.Fatalf("factory RequiredCapabilities = %v, want complete required surface %v", captured.RequiredCapabilities, required)
	}
	if !proto.Equal(captured.Device.GetCapabilities(), available) {
		t.Fatalf("factory device capabilities = %v, want complete available surface %v", captured.Device.GetCapabilities(), available)
	}
}

func TestEngineCapabilityRequirementSurface(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*tgsrlv1.CapabilitySet)
		wantDetail string
	}{
		{name: "complete superset is accepted"},
		{
			name: "source is required on available set",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.Source = " "
			},
			wantDetail: "device CapabilitySet lacks source, revision, or measured_at evidence",
		},
		{
			name: "revision is required on available set",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.Revision = 0
			},
			wantDetail: "device CapabilitySet lacks source, revision, or measured_at evidence",
		},
		{
			name: "measured_at is required on available set",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.MeasuredAt = nil
			},
			wantDetail: "device CapabilitySet lacks source, revision, or measured_at evidence",
		},
		{
			name: "measured_at must be valid",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.MeasuredAt = &timestamppb.Timestamp{Seconds: 253402300800}
			},
			wantDetail: "device CapabilitySet measured_at is invalid",
		},
		{
			name: "source must match",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.Source = "provider/other"
			},
			wantDetail: "capability source mismatch",
		},
		{
			name: "revision must be new enough",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.Revision = 6
			},
			wantDetail: "capability revision is older than required",
		},
		{
			name: "measurement must be new enough",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.MeasuredAt = timestamppb.New(engineRequirementsMeasuredAt.Add(-time.Nanosecond))
			},
			wantDetail: "capability measurement is older than required",
		},
		{
			name: "bind action is mandatory",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.SupportedActions = []string{"release", "set-share"}
			},
			wantDetail: "device does not advertise the bind action",
		},
		{
			name: "names must contain requirements",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.Names = []string{"compute"}
			},
			wantDetail: "missing capability names: generation-fencing",
		},
		{
			name: "algorithms must contain requirements",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.Algorithms = []string{"grpo"}
			},
			wantDetail: "missing declared algorithm capabilities: ppo",
		},
		{
			name: "rollout modes must contain requirements",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.RolloutModes = []string{"fully_async"}
			},
			wantDetail: "missing declared rollout capabilities: partially_async",
		},
		{
			name: "supported actions must contain requirements",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.SupportedActions = []string{" BIND ", "release"}
			},
			wantDetail: "missing supported actions: set_share",
		},
		{
			name: "attributes must match",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				delete(available.Attributes, "accelerator_vendor")
			},
			wantDetail: "capability attribute mismatch: accelerator_vendor",
		},
		{
			name: "limits must meet minimum",
			mutate: func(available *tgsrlv1.CapabilitySet) {
				available.Limits["partitions"] = 1
			},
			wantDetail: "capability limit below requirement: partitions",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			available := engineRequirementsAvailableCapabilities()
			if test.mutate != nil {
				test.mutate(available)
			}
			request := engineRequirementsRequest(
				engineRequirementsDevice(engineRequirementsCapacity(), available),
				nil,
				engineRequirementsSmallRequest(),
				engineRequirementsRequiredCapabilities(),
				DecisionMetadata{},
			)
			result := engineRequirementsEvaluate(t, request)
			if test.wantDetail == "" {
				engineRequirementsAssertSelected(t, result)
				return
			}
			engineRequirementsAssertRejected(
				t,
				result,
				tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH,
				test.wantDetail,
			)
		})
	}
}

func TestEngineCapabilityEvidenceMayBeEmptyButMustBeCurrentAndComplete(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*tgsrlv1.CapabilitySet)
		wantDetail string
	}{
		{
			name: "nil evidence is allowed",
			configure: func(available *tgsrlv1.CapabilitySet) {
				available.Evidence = nil
			},
		},
		{
			name: "empty evidence is allowed",
			configure: func(available *tgsrlv1.CapabilitySet) {
				available.Evidence = []*tgsrlv1.CapabilityEvidence{}
			},
		},
		{
			name: "evidence may equal top-level revision and measurement",
			configure: func(available *tgsrlv1.CapabilitySet) {
				available.Evidence = []*tgsrlv1.CapabilityEvidence{engineRequirementsCapabilityEvidence(available)}
			},
		},
		{
			name: "evidence may be newer and omit optional detail and attributes",
			configure: func(available *tgsrlv1.CapabilitySet) {
				evidence := engineRequirementsCapabilityEvidence(available)
				evidence.Revision++
				evidence.ObservedAt = timestamppb.New(available.GetMeasuredAt().AsTime().Add(time.Second))
				evidence.Detail = ""
				evidence.Attributes = nil
				available.Evidence = []*tgsrlv1.CapabilityEvidence{evidence}
			},
		},
		{
			name: "every evidence entry must be non-nil",
			configure: func(available *tgsrlv1.CapabilitySet) {
				available.Evidence = []*tgsrlv1.CapabilityEvidence{engineRequirementsCapabilityEvidence(available), nil}
			},
			wantDetail: "capability evidence 1 is nil",
		},
		{
			name: "evidence_id is required",
			configure: func(available *tgsrlv1.CapabilitySet) {
				evidence := engineRequirementsCapabilityEvidence(available)
				evidence.EvidenceId = " "
				available.Evidence = []*tgsrlv1.CapabilityEvidence{evidence}
			},
			wantDetail: "capability evidence 0 lacks evidence_id, source, revision, observed_at, or collector",
		},
		{
			name: "evidence source is required",
			configure: func(available *tgsrlv1.CapabilitySet) {
				evidence := engineRequirementsCapabilityEvidence(available)
				evidence.Source = ""
				available.Evidence = []*tgsrlv1.CapabilityEvidence{evidence}
			},
			wantDetail: "capability evidence 0 lacks evidence_id, source, revision, observed_at, or collector",
		},
		{
			name: "evidence revision is required",
			configure: func(available *tgsrlv1.CapabilitySet) {
				evidence := engineRequirementsCapabilityEvidence(available)
				evidence.Revision = 0
				available.Evidence = []*tgsrlv1.CapabilityEvidence{evidence}
			},
			wantDetail: "capability evidence 0 lacks evidence_id, source, revision, observed_at, or collector",
		},
		{
			name: "evidence observed_at is required",
			configure: func(available *tgsrlv1.CapabilitySet) {
				evidence := engineRequirementsCapabilityEvidence(available)
				evidence.ObservedAt = nil
				available.Evidence = []*tgsrlv1.CapabilityEvidence{evidence}
			},
			wantDetail: "capability evidence 0 lacks evidence_id, source, revision, observed_at, or collector",
		},
		{
			name: "evidence collector is required",
			configure: func(available *tgsrlv1.CapabilitySet) {
				evidence := engineRequirementsCapabilityEvidence(available)
				evidence.Collector = " "
				available.Evidence = []*tgsrlv1.CapabilityEvidence{evidence}
			},
			wantDetail: "capability evidence 0 lacks evidence_id, source, revision, observed_at, or collector",
		},
		{
			name: "evidence observed_at must be valid",
			configure: func(available *tgsrlv1.CapabilitySet) {
				evidence := engineRequirementsCapabilityEvidence(available)
				evidence.ObservedAt = &timestamppb.Timestamp{Seconds: 253402300800}
				available.Evidence = []*tgsrlv1.CapabilityEvidence{evidence}
			},
			wantDetail: "capability evidence 0 observed_at is invalid",
		},
		{
			name: "evidence source must match top-level source",
			configure: func(available *tgsrlv1.CapabilitySet) {
				evidence := engineRequirementsCapabilityEvidence(available)
				evidence.Source = "provider/other"
				available.Evidence = []*tgsrlv1.CapabilityEvidence{evidence}
			},
			wantDetail: "capability evidence 0 source does not match CapabilitySet source",
		},
		{
			name: "evidence revision must not predate top-level revision",
			configure: func(available *tgsrlv1.CapabilitySet) {
				evidence := engineRequirementsCapabilityEvidence(available)
				evidence.Revision--
				available.Evidence = []*tgsrlv1.CapabilityEvidence{evidence}
			},
			wantDetail: "capability evidence 0 revision is older than CapabilitySet revision",
		},
		{
			name: "evidence observation must not predate top-level measurement",
			configure: func(available *tgsrlv1.CapabilitySet) {
				evidence := engineRequirementsCapabilityEvidence(available)
				evidence.ObservedAt = timestamppb.New(available.GetMeasuredAt().AsTime().Add(-time.Nanosecond))
				available.Evidence = []*tgsrlv1.CapabilityEvidence{evidence}
			},
			wantDetail: "capability evidence 0 observed_at is older than CapabilitySet measured_at",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			available := engineRequirementsAvailableCapabilities()
			test.configure(available)
			request := engineRequirementsRequest(
				engineRequirementsDevice(engineRequirementsCapacity(), available),
				nil,
				engineRequirementsSmallRequest(),
				engineRequirementsRequiredCapabilities(),
				DecisionMetadata{},
			)
			result := engineRequirementsEvaluate(t, request)
			if test.wantDetail == "" {
				engineRequirementsAssertSelected(t, result)
				return
			}
			engineRequirementsAssertRejected(
				t,
				result,
				tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH,
				test.wantDetail,
			)
		})
	}
}

func TestEngineRequiresSafePointOnlyWhenRequested(t *testing.T) {
	tests := []struct {
		name              string
		requiresSafePoint bool
		safePoint         bool
		wantRejected      bool
	}{
		{name: "not required and absent"},
		{name: "not required and present", safePoint: true},
		{name: "required and present", requiresSafePoint: true, safePoint: true},
		{name: "required and absent", requiresSafePoint: true, wantRejected: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := engineRequirementsRequest(
				engineRequirementsDevice(engineRequirementsCapacity(), engineRequirementsAvailableCapabilities()),
				nil,
				engineRequirementsSmallRequest(),
				engineRequirementsRequiredCapabilities(),
				DecisionMetadata{RequiresSafePoint: test.requiresSafePoint, SafePoint: test.safePoint},
			)
			result := engineRequirementsEvaluate(t, request)
			if !test.wantRejected {
				engineRequirementsAssertSelected(t, result)
				return
			}
			engineRequirementsAssertRejected(
				t,
				result,
				tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VALIDITY_RULE,
				"execution contract requires a safe point not present in the evaluated context",
			)
		})
	}
}

func engineRequirementsRequest(device *tgsrlv1.Device, allocations []*tgsrlv1.Allocation, resources *tgsrlv1.ResourceVector, required *tgsrlv1.CapabilitySet, decision DecisionMetadata) Request {
	intent := &tgsrlv1.SchedulingIntent{
		ExecutionId:      "execution-1",
		StageId:          "stage-1",
		Version:          1,
		ResourcesPerUnit: proto.Clone(resources).(*tgsrlv1.ResourceVector),
	}
	if required != nil {
		intent.RequiredCapabilities = proto.Clone(required).(*tgsrlv1.CapabilitySet)
	}
	return Request{
		Snapshot: &tgsrlv1.ClusterSnapshot{
			SnapshotId:  "snapshot-1",
			Revision:    1,
			Devices:     []*tgsrlv1.Device{device},
			Allocations: allocations,
		},
		Intent:   intent,
		Decision: decision,
		Units:    []Unit{{ID: "unit-1"}},
		CandidateID: func(unitID, deviceID string) string {
			return unitID + "/" + deviceID
		},
		Config: Config{TopK: 1, EvidenceBudget: 8},
	}
}

func engineRequirementsEvaluate(t *testing.T, request Request) Result {
	t.Helper()
	result, err := (Engine{}).Evaluate(request)
	if err != nil {
		t.Fatalf("Engine.Evaluate() error = %v", err)
	}
	return result
}

func engineRequirementsAssertSelected(t *testing.T, result Result) {
	t.Helper()
	if len(result.Selected) != 1 || len(result.Candidates) != 1 || len(result.Rejections) != 0 {
		t.Fatalf("Engine.Evaluate() surface = selected:%d candidates:%d rejections:%d, want 1/1/0; result=%+v", len(result.Selected), len(result.Candidates), len(result.Rejections), result)
	}
	if result.FailureUnit != "" || result.Fallback != "" {
		t.Fatalf("Engine.Evaluate() failure = (%q, %q), want none", result.FailureUnit, result.Fallback)
	}
	if result.TotalCandidateCount != 1 || result.TotalRejectionCount != 0 || result.EvidenceTruncated {
		t.Fatalf("Engine.Evaluate() totals = candidates:%d rejections:%d truncated:%t, want 1/0/false", result.TotalCandidateCount, result.TotalRejectionCount, result.EvidenceTruncated)
	}
	if result.Selected[0].UnitID != "unit-1" || result.Selected[0].DeviceID != "device-1" {
		t.Fatalf("selected option = (%q, %q), want (unit-1, device-1)", result.Selected[0].UnitID, result.Selected[0].DeviceID)
	}
	if got := result.Selected[0].Candidate.GetCandidateId(); got != "unit-1/device-1" {
		t.Fatalf("selected candidate ID = %q, want unit-1/device-1", got)
	}
}

func engineRequirementsAssertRejected(t *testing.T, result Result, reason tgsrlv1.CandidateRejectionReason, detail string) {
	t.Helper()
	if len(result.Selected) != 0 || len(result.Candidates) != 0 || len(result.Rejections) != 1 {
		t.Fatalf("Engine.Evaluate() surface = selected:%d candidates:%d rejections:%d, want 0/0/1; result=%+v", len(result.Selected), len(result.Candidates), len(result.Rejections), result)
	}
	if result.FailureUnit != "unit-1" || result.Fallback != FallbackNoCandidate {
		t.Fatalf("Engine.Evaluate() failure = (%q, %q), want (unit-1, %s)", result.FailureUnit, result.Fallback, FallbackNoCandidate)
	}
	if result.TotalCandidateCount != 0 || result.TotalRejectionCount != 1 || result.EvidenceTruncated {
		t.Fatalf("Engine.Evaluate() totals = candidates:%d rejections:%d truncated:%t, want 0/1/false", result.TotalCandidateCount, result.TotalRejectionCount, result.EvidenceTruncated)
	}
	rejection := result.Rejections[0]
	if rejection.GetCandidateId() != "unit-1/device-1" || rejection.GetReason() != reason || rejection.GetDetail() != detail {
		t.Fatalf("rejection = (%q, %s, %q), want (%q, %s, %q)", rejection.GetCandidateId(), rejection.GetReason(), rejection.GetDetail(), "unit-1/device-1", reason, detail)
	}
}

func engineRequirementsDevice(capacity *tgsrlv1.ResourceVector, capabilities *tgsrlv1.CapabilitySet) *tgsrlv1.Device {
	return &tgsrlv1.Device{
		DeviceId:     "device-1",
		Health:       tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
		Capacity:     capacity,
		Allocatable:  proto.Clone(capacity).(*tgsrlv1.ResourceVector),
		Capabilities: capabilities,
	}
}

func engineRequirementsCapacity() *tgsrlv1.ResourceVector {
	return &tgsrlv1.ResourceVector{
		CpuMillis:             1000,
		MemoryBytes:           2000,
		AcceleratorUnits:      1,
		EphemeralStorageBytes: 3000,
		NetworkBandwidthBps:   4000,
	}
}

func engineRequirementsSmallRequest() *tgsrlv1.ResourceVector {
	return &tgsrlv1.ResourceVector{
		CpuMillis:             100,
		MemoryBytes:           200,
		AcceleratorUnits:      0.25,
		EphemeralStorageBytes: 300,
		NetworkBandwidthBps:   400,
	}
}

func engineRequirementsRequiredCapabilities() *tgsrlv1.CapabilitySet {
	return &tgsrlv1.CapabilitySet{
		Names:            []string{"compute", "generation-fencing"},
		Attributes:       map[string]string{"accelerator_vendor": "nvidia", "isolation": "sandbox"},
		Algorithms:       []string{"grpo", "ppo"},
		RolloutModes:     []string{"fully_async", "partially_async"},
		Source:           "provider/nvidia",
		Revision:         7,
		MeasuredAt:       timestamppb.New(engineRequirementsMeasuredAt),
		SupportedActions: []string{"bind", "release", "set_share"},
		Limits:           map[string]float64{"max_share": 0.5, "partitions": 2},
	}
}

func engineRequirementsAvailableCapabilities() *tgsrlv1.CapabilitySet {
	return &tgsrlv1.CapabilitySet{
		Names:            []string{"compute", "generation-fencing", "live-metrics"},
		Attributes:       map[string]string{"accelerator_vendor": "nvidia", "driver": "cuda", "isolation": "sandbox"},
		Algorithms:       []string{"dpo", "grpo", "ppo"},
		RolloutModes:     []string{"fully_async", "partially_async", "sync"},
		Source:           "provider/nvidia",
		Revision:         8,
		MeasuredAt:       timestamppb.New(engineRequirementsMeasuredAt.Add(time.Minute)),
		SupportedActions: []string{" BIND ", "rebind", "release", "set-share"},
		Limits:           map[string]float64{"max_share": 0.75, "partitions": 4},
	}
}

func engineRequirementsCapabilityEvidence(capabilities *tgsrlv1.CapabilitySet) *tgsrlv1.CapabilityEvidence {
	return &tgsrlv1.CapabilityEvidence{
		EvidenceId: "provider/nvidia/capabilities/8",
		Source:     capabilities.GetSource(),
		Revision:   capabilities.GetRevision(),
		ObservedAt: timestamppb.New(capabilities.GetMeasuredAt().AsTime()),
		Collector:  "probe-agent",
		Detail:     "live capability probe",
		Attributes: map[string]string{"probe": "live"},
	}
}
