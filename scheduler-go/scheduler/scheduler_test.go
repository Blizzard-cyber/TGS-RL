package scheduler

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var fixtureTime = time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)

func TestEvaluateBuildsCompleteDeterministicDecision(t *testing.T) {
	snapshot, intent := validFixture()
	scheduler := testScheduler(t, FallbackNoOp)
	snapshotBefore := proto.Clone(snapshot)
	intentBefore := proto.Clone(intent)

	plan, record, err := scheduler.Evaluate(snapshot, intent)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if record.GetFallback() {
		t.Fatalf("Evaluate() unexpectedly fell back: %s", record.GetFallbackReason())
	}
	if got, want := len(plan.GetBindings()), 2; got != want {
		t.Fatalf("bindings = %d, want %d", got, want)
	}
	if got, want := len(plan.GetActions()), 2; got != want {
		t.Fatalf("actions = %d, want %d", got, want)
	}
	if got, want := plan.GetBindings()[0].GetPendingUnitId(), "unit-a"; got != want {
		t.Errorf("first pending unit = %q, want %q", got, want)
	}
	if got, want := plan.GetBindings()[0].GetDeviceIds()[0], "device-a"; got != want {
		t.Errorf("first device = %q, want %q", got, want)
	}
	if got, want := plan.GetBindings()[1].GetPendingUnitId(), "unit-b"; got != want {
		t.Errorf("second pending unit = %q, want %q", got, want)
	}
	if got, want := plan.GetBindings()[1].GetDeviceIds()[0], "device-b"; got != want {
		t.Errorf("second device = %q, want %q", got, want)
	}
	if plan.GetPlanId() == "" || plan.GetDecisionId() == "" || record.GetDecisionId() != plan.GetDecisionId() {
		t.Errorf("plan/decision identifiers are incomplete: plan=%q decision=%q record=%q", plan.GetPlanId(), plan.GetDecisionId(), record.GetDecisionId())
	}
	if record.GetSequence() != 1 || record.GetSnapshotRevision() != snapshot.GetRevision() || record.GetIntentVersion() != intent.GetVersion() {
		t.Errorf("record version surface is incomplete: %+v", record)
	}
	if record.GetDataKind() != tgsrlv1.DataKind_DATA_KIND_SYNTHETIC || record.GetCodeRevision() != "test-code" || record.GetConfigRevision() != "test-config" {
		t.Errorf("record audit metadata is incomplete: %+v", record)
	}
	if !record.GetDecidedAt().AsTime().Equal(fixtureTime) || !plan.GetCreatedAt().AsTime().Equal(fixtureTime) {
		t.Errorf("injected clock was not used")
	}
	if !proto.Equal(plan, record.GetSelectedPlan()) {
		t.Errorf("selected plan differs from returned plan")
	}
	if got, want := len(record.GetCandidates()), 4; got != want {
		t.Errorf("candidates = %d, want %d", got, want)
	}
	for index, candidate := range record.GetCandidates() {
		if candidate.GetCandidateId() == "" || candidate.GetPlan() == nil {
			t.Errorf("candidate %d is incomplete: %+v", index, candidate)
		}
		components := candidate.GetComponentScores()
		if len(components) != 4 {
			t.Errorf("candidate %d component count = %d, want 4", index, len(components))
		}
		sum := components["capacity_headroom"] + components["share_headroom"] + components["ready"] + components["trace_affinity"]
		if math.Abs(candidate.GetScore()-roundScore(sum)) > 1e-12 {
			t.Errorf("candidate %d score %f != component sum %f", index, candidate.GetScore(), sum)
		}
	}
	for index, action := range plan.GetActions() {
		if action.GetActionType() != tgsrlv1.ActionType_ACTION_TYPE_BIND || action.GetLevel() != tgsrlv1.ActionLevel_ACTION_LEVEL_L1 {
			t.Errorf("action %d has unexpected type/level: %+v", index, action)
		}
		if action.GetOrder() != uint32(index+1) || action.GetPlanId() != plan.GetPlanId() || action.GetExpectedSnapshotRevision() != snapshot.GetRevision() {
			t.Errorf("action %d lacks ordering/fencing: %+v", index, action)
		}
		if action.GetRequiresSafePoint() || action.GetRollback() == nil || action.GetIdempotencyKey() == "" {
			t.Errorf("action %d lacks precondition/rollback/idempotency: %+v", index, action)
		}
		if action.GetTickKind() != tgsrlv1.TickKind_TICK_KIND_FAST {
			t.Errorf("action %d tick_kind = %s, want FAST", index, action.GetTickKind())
		}
	}
	if record.GetTickKind() != tgsrlv1.TickKind_TICK_KIND_FAST {
		t.Errorf("record tick_kind = %s, want FAST", record.GetTickKind())
	}
	if record.GetEvaluationContext() == nil || !record.GetEvaluationContext().GetCompatibilityDefaultsApplied() {
		t.Errorf("evaluation context = %+v, want compatibility defaults applied", record.GetEvaluationContext())
	}
	if !proto.Equal(snapshot, snapshotBefore) || !proto.Equal(intent, intentBefore) {
		t.Fatal("Evaluate mutated an input")
	}

	returnedDevice := plan.GetBindings()[0].GetDeviceIds()[0]
	plan.Bindings[0].DeviceIds[0] = "mutated-output"
	if record.GetSelectedPlan().GetBindings()[0].GetDeviceIds()[0] != returnedDevice {
		t.Fatal("returned plan aliases DecisionRecord.selected_plan")
	}
}

func TestEvaluateIsStableAcrossInputOrder(t *testing.T) {
	baseSnapshot, intent := validFixture()
	permuted := proto.Clone(baseSnapshot).(*tgsrlv1.ClusterSnapshot)
	permuted.Devices[0], permuted.Devices[1] = permuted.Devices[1], permuted.Devices[0]
	permuted.PendingUnits[0], permuted.PendingUnits[1] = permuted.PendingUnits[1], permuted.PendingUnits[0]

	basePlan, baseRecord, err := testScheduler(t, FallbackNoOp).Evaluate(baseSnapshot, intent)
	if err != nil {
		t.Fatalf("base Evaluate() error = %v", err)
	}
	permutedPlan, permutedRecord, err := testScheduler(t, FallbackNoOp).Evaluate(permuted, intent)
	if err != nil {
		t.Fatalf("permuted Evaluate() error = %v", err)
	}
	if !proto.Equal(basePlan, permutedPlan) || !proto.Equal(baseRecord, permutedRecord) {
		t.Fatalf("input permutation changed decision\nbase plan: %v\npermuted plan: %v\nbase record: %v\npermuted record: %v", basePlan, permutedPlan, baseRecord, permutedRecord)
	}
}

func TestEvaluateLargeDecisionThresholdKeepsChoiceAcross4096Boundary(t *testing.T) {
	atLimitSnapshot, atLimitIntent := largeSingleUnitFixture(t, 4096)
	aboveLimitSnapshot, aboveLimitIntent := largeSingleUnitFixture(t, 4097)

	atLimitPlan, atLimitRecord, err := testScheduler(t, FallbackNoOp).Evaluate(atLimitSnapshot, atLimitIntent)
	if err != nil {
		t.Fatalf("Evaluate(at limit) error = %v", err)
	}
	aboveLimitPlan, aboveLimitRecord, err := testScheduler(t, FallbackNoOp).Evaluate(aboveLimitSnapshot, aboveLimitIntent)
	if err != nil {
		t.Fatalf("Evaluate(above limit) error = %v", err)
	}

	if atLimitRecord.GetFallback() || aboveLimitRecord.GetFallback() {
		t.Fatalf("unexpected fallback at limit=%v above=%v", atLimitRecord.GetFallbackReason(), aboveLimitRecord.GetFallbackReason())
	}
	if !proto.Equal(atLimitPlan, aboveLimitPlan) {
		t.Fatalf("threshold crossing changed selected plan\nat limit: %v\nabove: %v", atLimitPlan, aboveLimitPlan)
	}
	if atLimitRecord.GetScore() != aboveLimitRecord.GetScore() {
		t.Fatalf("threshold crossing changed score: at limit=%f above=%f", atLimitRecord.GetScore(), aboveLimitRecord.GetScore())
	}
	if got, want := len(atLimitRecord.GetCandidates()), 4096; got != want {
		t.Fatalf("at-limit candidate evidence = %d, want %d", got, want)
	}
	if got := len(aboveLimitRecord.GetCandidates()); got == 0 {
		t.Fatal("above-limit decision did not retain selected candidate evidence")
	}
	if got, want := aboveLimitRecord.GetTotalCandidateCount(), uint64(4097); got != want {
		t.Fatalf("above-limit total candidate count = %d, want %d", got, want)
	}
	if !aboveLimitRecord.GetEvidenceTruncated() {
		t.Fatal("above-limit decision did not mark bounded evidence as truncated")
	}

	atLimitSelected := selectedCandidateForBinding(t, atLimitRecord, atLimitPlan.GetBindings()[0])
	_ = selectedCandidateForBinding(t, aboveLimitRecord, aboveLimitPlan.GetBindings()[0])
	if candidateDevice, candidateUnit := candidateSortKeys(atLimitSelected); candidateDevice != firstDeviceID(aboveLimitPlan) || candidateUnit != aboveLimitPlan.GetBindings()[0].GetPendingUnitId() {
		t.Fatalf("threshold crossing changed selected binding surface: at limit=(%q,%q) above=(%q,%q)", candidateUnit, candidateDevice, aboveLimitPlan.GetBindings()[0].GetPendingUnitId(), firstDeviceID(aboveLimitPlan))
	}
}

func TestEvaluateLargeDecisionIsStableAcrossInputOrder(t *testing.T) {
	baseSnapshot, intent := largeSingleUnitFixture(t, 4097)
	permuted := proto.Clone(baseSnapshot).(*tgsrlv1.ClusterSnapshot)
	random := rand.New(rand.NewSource(20260828))
	random.Shuffle(len(permuted.Devices), func(i, j int) {
		permuted.Devices[i], permuted.Devices[j] = permuted.Devices[j], permuted.Devices[i]
	})

	basePlan, baseRecord, err := testScheduler(t, FallbackNoOp).Evaluate(baseSnapshot, intent)
	if err != nil {
		t.Fatalf("base Evaluate() error = %v", err)
	}
	permutedPlan, permutedRecord, err := testScheduler(t, FallbackNoOp).Evaluate(permuted, intent)
	if err != nil {
		t.Fatalf("permuted Evaluate() error = %v", err)
	}
	if !proto.Equal(basePlan, permutedPlan) || !proto.Equal(baseRecord, permutedRecord) {
		t.Fatalf("large-decision device permutation changed decision\nbase plan: %v\npermuted plan: %v\nbase record: %v\npermuted record: %v", basePlan, permutedPlan, baseRecord, permutedRecord)
	}
}

func TestEvaluateSelectedBindingsMapToRecordedCandidates(t *testing.T) {
	snapshot, intent := validFixture()
	plan, record, err := testScheduler(t, FallbackNoOp).Evaluate(snapshot, intent)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if record.GetFallback() {
		t.Fatalf("unexpected fallback: %s", record.GetFallbackReason())
	}
	for _, binding := range plan.GetBindings() {
		candidate := selectedCandidateForBinding(t, record, binding)
		candidateBinding := candidate.GetPlan().GetBindings()[0]
		if !proto.Equal(candidateBinding, binding) {
			t.Fatalf("binding %+v does not match recorded candidate binding %+v", binding, candidateBinding)
		}
	}
}

func TestEvaluateEvidenceTotalsAreExactForMixedSurface(t *testing.T) {
	snapshot, intent := orderedDecisionSurfaceFixture(t)

	_, record, err := testScheduler(t, FallbackNoOp).Evaluate(snapshot, intent)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if got, want := record.GetTotalCandidateCount(), uint64(2); got != want {
		t.Fatalf("total_candidate_count = %d, want %d", got, want)
	}
	if got, want := record.GetTotalRejectedCandidateCount(), uint64(2); got != want {
		t.Fatalf("total_rejected_candidate_count = %d, want %d", got, want)
	}
	if record.GetEvidenceTruncated() {
		t.Fatal("small complete evidence surface unexpectedly marked truncated")
	}
}

func TestEvaluateLargeDecisionConsumesResourcesAfterEachSelection(t *testing.T) {
	snapshot, intent := largeMultiUnitConsumptionFixture(t)

	plan, record, err := testScheduler(t, FallbackNoOp).Evaluate(snapshot, intent)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if record.GetFallback() {
		t.Fatalf("unexpected fallback: %s", record.GetFallbackReason())
	}
	if got, want := len(plan.GetBindings()), 2; got != want {
		t.Fatalf("bindings = %d, want %d", got, want)
	}
	if got, want := plan.GetBindings()[0].GetDeviceIds()[0], "device-a"; got != want {
		t.Fatalf("first binding device = %q, want %q", got, want)
	}
	if got, want := plan.GetBindings()[1].GetDeviceIds()[0], "device-b"; got != want {
		t.Fatalf("second binding device = %q, want %q", got, want)
	}
	if plan.GetBindings()[0].GetDeviceIds()[0] == plan.GetBindings()[1].GetDeviceIds()[0] {
		t.Fatal("multi-unit selection reused a fully consumed device")
	}
}

func TestEvaluateDecisionSurfaceHasTotalOrder(t *testing.T) {
	snapshot, intent := orderedDecisionSurfaceFixture(t)

	_, record, err := testScheduler(t, FallbackNoOp).Evaluate(snapshot, intent)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if record.GetFallback() {
		t.Fatalf("unexpected fallback: %s", record.GetFallbackReason())
	}
	if len(record.GetCandidates()) < 2 || len(record.GetRejectedCandidates()) < 2 {
		t.Fatalf("need multiple candidates and rejections, got %d/%d", len(record.GetCandidates()), len(record.GetRejectedCandidates()))
	}
	for index := 1; index < len(record.GetCandidates()); index++ {
		if !candidateOrdered(record.GetCandidates()[index-1], record.GetCandidates()[index]) {
			t.Fatalf("candidates out of order at %d: prev=%+v next=%+v", index, record.GetCandidates()[index-1], record.GetCandidates()[index])
		}
	}
	for index := 1; index < len(record.GetRejectedCandidates()); index++ {
		if !rejectionOrdered(record.GetRejectedCandidates()[index-1], record.GetRejectedCandidates()[index]) {
			t.Fatalf("rejections out of order at %d: prev=%+v next=%+v", index, record.GetRejectedCandidates()[index-1], record.GetRejectedCandidates()[index])
		}
	}
}

func TestEvaluateSemanticValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent)
		field  string
	}{
		{name: "zero snapshot revision", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) { snapshot.Revision = 0 }, field: "snapshot.revision"},
		{name: "revision annotation mismatch", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			snapshot.Annotations["snapshot_revision"] = "8"
		}, field: "snapshot.annotations"},
		{name: "duplicate device id", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			snapshot.Devices[1].DeviceId = snapshot.Devices[0].DeviceId
		}, field: "device_id"},
		{name: "allocatable exceeds capacity", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			snapshot.Devices[0].Allocatable.MemoryBytes = snapshot.Devices[0].Capacity.MemoryBytes + 1
		}, field: "allocatable"},
		{name: "invalid accelerator value", mutate: func(_ *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) {
			intent.ResourcesPerUnit.AcceleratorUnits = math.NaN()
		}, field: "accelerator_units"},
		{name: "pending version zero", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			snapshot.PendingUnits[0].IntentVersion = 0
		}, field: "intent_version"},
		{name: "ttl mismatch", mutate: func(_ *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) {
			intent.Ttl = durationpb.New(30 * time.Minute)
		}, field: "valid_until"},
		{name: "physical selector", mutate: func(_ *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) {
			intent.Labels["device_id"] = "forbidden"
		}, field: "intent.labels"},
		{name: "duplicate capability", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			snapshot.Devices[0].Capabilities.Names = []string{"compute", "compute"}
		}, field: "capabilities.names"},
		{name: "invalid limit", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			snapshot.Devices[0].Capabilities.Limits["partitions"] = math.Inf(1)
		}, field: "capabilities.limits"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot, intent := validFixture()
			test.mutate(snapshot, intent)
			plan, record, err := testScheduler(t, FallbackNoOp).Evaluate(snapshot, intent)
			if err == nil || !IsValidationError(err) {
				t.Fatalf("Evaluate() = (%v, %v, %v), want validation error", plan, record, err)
			}
			if !strings.Contains(err.Error(), test.field) {
				t.Errorf("error %q does not mention %q", err, test.field)
			}
		})
	}
}

func TestExecutionContractValidationMatchesWireSemantics(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*tgsrlv1.ExecutionContract)
		field  string
	}{
		{name: "unknown phase kind", mutate: func(contract *tgsrlv1.ExecutionContract) {
			contract.PhaseGraph.Phases[0].Kind = tgsrlv1.PhaseKind(99)
		}, field: "phase_graph"},
		{name: "unknown validity failure mode", mutate: func(contract *tgsrlv1.ExecutionContract) {
			contract.ValidityRules[0].FailureMode = tgsrlv1.ValidityFailureMode(99)
		}, field: "validity_rules"},
		{name: "unknown version operator", mutate: func(contract *tgsrlv1.ExecutionContract) {
			contract.VersionConstraints[0].Operator = tgsrlv1.VersionOperator(99)
		}, field: "version_constraints"},
		{name: "unknown commit mode", mutate: func(contract *tgsrlv1.ExecutionContract) {
			contract.CommitPolicy.Mode = tgsrlv1.CommitMode(99)
		}, field: "commit_policy"},
		{name: "unknown backpressure mode", mutate: func(contract *tgsrlv1.ExecutionContract) {
			contract.BackpressurePolicy.Mode = tgsrlv1.BackpressureMode(99)
		}, field: "backpressure_policy"},
		{name: "unknown safe point trigger", mutate: func(contract *tgsrlv1.ExecutionContract) {
			contract.SafePointPolicy.Trigger = tgsrlv1.SafePointTrigger(99)
		}, field: "safe_point_policy"},
		{name: "disabled safe point carries state", mutate: func(contract *tgsrlv1.ExecutionContract) {
			contract.SafePointPolicy.Enabled = false
		}, field: "safe_point_policy"},
		{name: "non-interval trigger carries interval", mutate: func(contract *tgsrlv1.ExecutionContract) {
			contract.SafePointPolicy.Interval = durationpb.New(time.Second)
		}, field: "safe_point_policy.interval"},
		{name: "duplicate required phase", mutate: func(contract *tgsrlv1.ExecutionContract) {
			contract.SafePointPolicy.RequiredPhaseIds = []string{"stage", "stage"}
		}, field: "required_phase_ids"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot, intent := validFixture()
			test.mutate(intent.ExecutionContract)
			_, _, err := testScheduler(t, FallbackNoOp).Evaluate(snapshot, intent)
			if err == nil || !IsValidationError(err) || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("Evaluate() error = %v, want validation error containing %q", err, test.field)
			}
		})
	}
}

func TestExecutionContractAllowsVersionOnePrerelease(t *testing.T) {
	snapshot, intent := validFixture()
	intent.ExecutionContract.Version = "1.2.3-rc.1"
	intent.ExecutionContract.ContractId, _ = CanonicalContractID(intent.ExecutionContract)
	if _, record, err := testScheduler(t, FallbackNoOp).Evaluate(snapshot, intent); err != nil || record.GetFallback() {
		t.Fatalf("Evaluate(prerelease contract) record=%v error=%v", record, err)
	}
}

func TestProtocolVersionNormalizationAndCompatibility(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: "0.3", want: "0.3.0"},
		{input: "v0.3", want: "0.3.0"},
		{input: "V0.3", want: "0.3.0"},
		{input: "0.3.1-rc.2", want: "0.3.1-rc.2"},
	}
	for _, test := range tests {
		if got, err := NormalizeProtocolVersion(test.input); err != nil || got != test.want {
			t.Errorf("NormalizeProtocolVersion(%q) = (%q, %v), want (%q, nil)", test.input, got, err, test.want)
		}
	}
	for _, invalidVersion := range []string{"", " 0.3", "0.3 ", "1", "1.x", "1.2.3.4", "1.2-"} {
		if _, err := NormalizeProtocolVersion(invalidVersion); err == nil {
			t.Errorf("NormalizeProtocolVersion(%q) unexpectedly succeeded", invalidVersion)
		}
	}
	if !protocolCompatible(ProtocolVersion, "0.3.99-rc.2") {
		t.Fatal("0.3 prerelease should remain compatible with protocol 0.3")
	}
	for _, version := range []string{"0.3.999-rc.2", "v0.3.0-alpha.beta.7"} {
		normalized, err := NormalizeProtocolVersion(version)
		if err != nil {
			t.Fatalf("NormalizeProtocolVersion(%q) error = %v", version, err)
		}
		if !protocolCompatible(ProtocolVersion, normalized) {
			t.Fatalf("dotted prerelease %q normalized to %q should be protocol-compatible", version, normalized)
		}
	}
}

func TestEvaluateHardConstraints(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent)
		reason     tgsrlv1.CandidateRejectionReason
		detailPart string
	}{
		{name: "ready health", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			setAllDevices(snapshot, func(device *tgsrlv1.Device) { device.Health = tgsrlv1.DeviceHealth_DEVICE_HEALTH_DRAINING })
		}, reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VALIDITY_RULE, detailPart: "not READY"},
		{name: "cpu capacity", mutate: insufficient(func(vector *tgsrlv1.ResourceVector) { vector.CpuMillis = 99 }), reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, detailPart: "allocatable"},
		{name: "memory capacity", mutate: insufficient(func(vector *tgsrlv1.ResourceVector) { vector.MemoryBytes = 99 }), reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, detailPart: "allocatable"},
		{name: "accelerator capacity", mutate: insufficient(func(vector *tgsrlv1.ResourceVector) { vector.AcceleratorUnits = 0.24 }), reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, detailPart: "allocatable"},
		{name: "storage capacity", mutate: insufficient(func(vector *tgsrlv1.ResourceVector) { vector.EphemeralStorageBytes = 9 }), reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, detailPart: "allocatable"},
		{name: "network capacity", mutate: insufficient(func(vector *tgsrlv1.ResourceVector) { vector.NetworkBandwidthBps = 9 }), reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, detailPart: "allocatable"},
		{name: "unsigned overflow safe", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) {
			intent.ResourcesPerUnit.CpuMillis = 1
			for _, unit := range snapshot.PendingUnits {
				unit.RequestedResources.CpuMillis = 1
			}
			setAllDevices(snapshot, func(device *tgsrlv1.Device) {
				device.Capacity.CpuMillis = math.MaxUint64
				device.Allocatable.CpuMillis = 1
			})
			snapshot.Allocations = []*tgsrlv1.Allocation{activeAllocation("full", "other", "other", 1, "device-a", &tgsrlv1.ResourceVector{CpuMillis: math.MaxUint64})}
			snapshot.Allocations = append(snapshot.Allocations, activeAllocation("full-b", "other", "other", 1, "device-b", &tgsrlv1.ResourceVector{CpuMillis: math.MaxUint64}))
		}, reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, detailPart: "capacity"},
		{name: "aggregate share", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			snapshot.Allocations = []*tgsrlv1.Allocation{
				activeAllocation("share-a", "other", "other", 1, "device-a", &tgsrlv1.ResourceVector{AcceleratorUnits: .8}),
				activeAllocation("share-b", "other", "other", 1, "device-b", &tgsrlv1.ResourceVector{AcceleratorUnits: .8}),
			}
		}, reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_INSUFFICIENT_RESOURCES, detailPart: "share"},
		{name: "capability name", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			setAllDevices(snapshot, func(device *tgsrlv1.Device) { device.Capabilities.Names = []string{"other"} })
		}, reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH, detailPart: "names"},
		{name: "capability attribute", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			setAllDevices(snapshot, func(device *tgsrlv1.Device) { device.Capabilities.Attributes["isolation"] = "process" })
		}, reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH, detailPart: "attribute"},
		{name: "capability action", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			setAllDevices(snapshot, func(device *tgsrlv1.Device) { device.Capabilities.SupportedActions = []string{"release"} })
		}, reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH, detailPart: "bind"},
		{name: "capability limit", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			setAllDevices(snapshot, func(device *tgsrlv1.Device) { device.Capabilities.Limits["partitions"] = 0 })
		}, reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH, detailPart: "limit"},
		{name: "capability source", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			setAllDevices(snapshot, func(device *tgsrlv1.Device) { device.Capabilities.Source = "other" })
		}, reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH, detailPart: "source"},
		{name: "capability unmeasured", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			setAllDevices(snapshot, func(device *tgsrlv1.Device) { device.Capabilities.MeasuredAt = nil })
		}, reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_CAPABILITY_MISMATCH, detailPart: "measured_at"},
		{name: "safe point", mutate: func(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) {
			intent.ExecutionContract.CommitPolicy.RequireSafePoint = true
			snapshot.Annotations[SafePointAnnotation] = "false"
		}, reason: tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VALIDITY_RULE, detailPart: "safe point"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot, intent := validFixture()
			test.mutate(snapshot, intent)
			if intent.GetExecutionContract() != nil {
				intent.ExecutionContract.ContractId, _ = CanonicalContractID(intent.GetExecutionContract())
			}
			plan, record, err := testScheduler(t, FallbackNoOp).Evaluate(snapshot, intent)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if !record.GetFallback() || len(plan.GetActions()) != 0 {
				t.Fatalf("Evaluate() did not safely fall back: plan=%v record=%v", plan, record)
			}
			if len(record.GetRejectedCandidates()) == 0 {
				t.Fatal("decision has no rejected candidate evidence")
			}
			for _, rejection := range record.GetRejectedCandidates() {
				if rejection.GetReason() != test.reason || !strings.Contains(rejection.GetDetail(), test.detailPart) {
					t.Errorf("rejection = (%s, %q), want (%s, containing %q)", rejection.GetReason(), rejection.GetDetail(), test.reason, test.detailPart)
				}
			}
		})
	}
}

func TestEvaluateFallbackReasonsAndModes(t *testing.T) {
	tests := []struct {
		name        string
		mode        FallbackMode
		mutate      func(*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent)
		wantReason  string
		wantBinding int
	}{
		{name: "expired noop", mode: FallbackNoOp, mutate: func(_ *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) {
			intent.ValidUntil = timestamppb.New(fixtureTime)
			intent.Ttl = durationpb.New(time.Minute)
			intent.SubmittedAt = timestamppb.New(fixtureTime.Add(-time.Minute))
		}, wantReason: FallbackReasonIntentExpired},
		{name: "no candidate noop", mode: FallbackNoOp, mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			setAllDevices(snapshot, func(device *tgsrlv1.Device) { device.Health = tgsrlv1.DeviceHealth_DEVICE_HEALTH_UNAVAILABLE })
		}, wantReason: FallbackReasonNoCandidate},
		{name: "stale intent", mode: FallbackNoOp, mutate: func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
			snapshot.PendingUnits[0].IntentVersion++
		}, wantReason: FallbackReasonStaleIntent},
		{name: "older pending mismatch", mode: FallbackNoOp, mutate: func(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) {
			intent.Version++
			intent.IdempotencyKey = IntentIdempotencyKey(intent.GetExecutionId(), intent.GetStageId(), intent.GetVersion())
			for _, unit := range snapshot.PendingUnits {
				unit.IntentVersion = intent.Version - 1
			}
		}, wantReason: FallbackReasonRevision},
		{name: "safe point missing", mode: FallbackNoOp, mutate: func(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) {
			intent.ExecutionContract.CommitPolicy.RequireSafePoint = true
			delete(snapshot.Annotations, SafePointAnnotation)
		}, wantReason: FallbackReasonSafePoint},
		{name: "expired static holds active allocation", mode: FallbackStatic, mutate: func(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) {
			intent.ValidUntil = timestamppb.New(fixtureTime)
			intent.Ttl = durationpb.New(time.Minute)
			intent.SubmittedAt = timestamppb.New(fixtureTime.Add(-time.Minute))
			snapshot.Allocations = []*tgsrlv1.Allocation{activeAllocation("held", intent.GetExecutionId(), intent.GetStageId(), intent.GetVersion(), "device-a", intent.GetResourcesPerUnit())}
		}, wantReason: FallbackReasonIntentExpired, wantBinding: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot, intent := validFixture()
			test.mutate(snapshot, intent)
			if intent.GetExecutionContract() != nil {
				intent.ExecutionContract.ContractId, _ = CanonicalContractID(intent.GetExecutionContract())
			}
			plan, record, err := testScheduler(t, test.mode).Evaluate(snapshot, intent)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if !record.GetFallback() || record.GetFallbackReason() != test.wantReason {
				t.Fatalf("fallback = (%v, %q), want (true, %q)", record.GetFallback(), record.GetFallbackReason(), test.wantReason)
			}
			if got := len(plan.GetBindings()); got != test.wantBinding {
				t.Errorf("bindings = %d, want %d", got, test.wantBinding)
			}
			if len(plan.GetActions()) != 0 || len(record.GetSelectedPlan().GetActions()) != 0 {
				t.Error("fallback plan must never authorize a mutation")
			}
			if record.GetSelectedPlan() == plan {
				t.Error("fallback returned aliased plan object")
			}
		})
	}
}

func TestResourceConstraintProperty(t *testing.T) {
	random := rand.New(rand.NewSource(20260827))
	for trial := 0; trial < 500; trial++ {
		snapshot, intent := validFixture()
		snapshot.PendingUnits = nil
		intent.UnitCount = 1
		request := &tgsrlv1.ResourceVector{
			CpuMillis:             uint64(random.Intn(10_000)),
			MemoryBytes:           uint64(random.Intn(10_000)),
			AcceleratorUnits:      random.Float64(),
			EphemeralStorageBytes: uint64(random.Intn(10_000)),
			NetworkBandwidthBps:   uint64(random.Intn(10_000)),
		}
		if resourceIsZero(request) {
			request.CpuMillis = 1
		}
		intent.ResourcesPerUnit = proto.Clone(request).(*tgsrlv1.ResourceVector)
		capacity := &tgsrlv1.ResourceVector{
			CpuMillis:             uint64(random.Intn(10_000)),
			MemoryBytes:           uint64(random.Intn(10_000)),
			AcceleratorUnits:      random.Float64(),
			EphemeralStorageBytes: uint64(random.Intn(10_000)),
			NetworkBandwidthBps:   uint64(random.Intn(10_000)),
		}
		snapshot.Devices = snapshot.Devices[:1]
		snapshot.Devices[0].Capacity = proto.Clone(capacity).(*tgsrlv1.ResourceVector)
		snapshot.Devices[0].Allocatable = proto.Clone(capacity).(*tgsrlv1.ResourceVector)

		plan, record, err := testScheduler(t, FallbackNoOp).Evaluate(snapshot, intent)
		if err != nil {
			t.Fatalf("trial %d Evaluate() error = %v", trial, err)
		}
		fits := resourceLessOrEqual(request, capacity) && request.GetAcceleratorUnits() <= 1+1e-12
		if fits == record.GetFallback() {
			t.Fatalf("trial %d fits=%v fallback=%v request=%v capacity=%v plan=%v", trial, fits, record.GetFallback(), request, capacity, plan)
		}
		if !record.GetFallback() && !resourceLessOrEqual(plan.GetBindings()[0].GetResources(), capacity) {
			t.Fatalf("trial %d selected an over-capacity binding", trial)
		}
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	if _, err := New(Config{Fallback: FallbackMode("unsafe")}); err == nil || !IsValidationError(err) {
		t.Fatalf("New() error = %v, want ValidationError", err)
	}
	if _, err := New(Config{DataKind: tgsrlv1.DataKind(99)}); err == nil || !IsValidationError(err) {
		t.Fatalf("New() error = %v, want ValidationError", err)
	}
}

func validFixture() (*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent) {
	resources := &tgsrlv1.ResourceVector{
		CpuMillis:             100,
		MemoryBytes:           100,
		AcceleratorUnits:      .25,
		EphemeralStorageBytes: 10,
		NetworkBandwidthBps:   10,
	}
	required := &tgsrlv1.CapabilitySet{
		Names:            []string{"compute"},
		Attributes:       map[string]string{"isolation": "sandbox"},
		Source:           "mock",
		Revision:         1,
		MeasuredAt:       timestamppb.New(fixtureTime.Add(-2 * time.Minute)),
		SupportedActions: []string{"bind"},
		Limits:           map[string]float64{"partitions": 1},
	}
	intent := &tgsrlv1.SchedulingIntent{
		ExecutionId:          "execution",
		StageId:              "stage",
		Version:              7,
		ValidUntil:           timestamppb.New(fixtureTime.Add(10 * time.Minute)),
		Ttl:                  durationpb.New(11 * time.Minute),
		IdempotencyKey:       "intent-key",
		SubmittedAt:          timestamppb.New(fixtureTime.Add(-time.Minute)),
		JobId:                "job",
		ResourcesPerUnit:     proto.Clone(resources).(*tgsrlv1.ResourceVector),
		UnitCount:            2,
		RequiredCapabilities: proto.Clone(required).(*tgsrlv1.CapabilitySet),
		ExecutionContract: &tgsrlv1.ExecutionContract{
			Version: "1.0.0",
			PhaseGraph: &tgsrlv1.PhaseGraph{
				Phases:        []*tgsrlv1.Phase{{PhaseId: "stage", DisplayName: "Stage", Kind: tgsrlv1.PhaseKind_PHASE_KIND_DECODE, Parallelism: 1, MaxAttempts: 1}},
				EntryPhaseIds: []string{"stage"},
			},
			ValidityRules: []*tgsrlv1.ValidityRule{{RuleId: "valid", Description: "valid", Expression: "true", FailureMode: tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_REJECT}},
			VersionConstraints: []*tgsrlv1.VersionConstraint{{
				Component: "protocol", Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_COMPATIBLE, Version: "v0.3",
			}},
			CommitPolicy:       &tgsrlv1.CommitPolicy{Mode: tgsrlv1.CommitMode_COMMIT_MODE_ALL_OR_NOTHING, MinimumSuccessfulUnits: 1, CommitTimeout: durationpb.New(time.Minute)},
			BackpressurePolicy: &tgsrlv1.BackpressurePolicy{Mode: tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_BLOCK_PRODUCER, MaximumBufferLevel: 1, StallTimeout: durationpb.New(time.Minute)},
			SafePointPolicy: &tgsrlv1.SafePointPolicy{
				Enabled:     true,
				Trigger:     tgsrlv1.SafePointTrigger_SAFE_POINT_TRIGGER_EXPLICIT,
				MaximumWait: durationpb.New(time.Minute),
			},
			Capabilities: &tgsrlv1.Capabilities{
				DeterministicReplay:  true,
				TransactionalCommits: true,
			},
		},
		PolicyVersion:     "policy-1",
		Queue:             "default",
		RolloutMode:       tgsrlv1.RolloutMode_ROLLOUT_MODE_PARTIALLY_ASYNC,
		PhaseKind:         tgsrlv1.PhaseKind_PHASE_KIND_DECODE,
		DeterministicSeed: 42,
		Labels:            map[string]string{"data_kind": "synthetic"},
	}
	intent.IdempotencyKey = IntentIdempotencyKey(intent.GetExecutionId(), intent.GetStageId(), intent.GetVersion())
	intent.ExecutionContract.ContractId, _ = CanonicalContractID(intent.ExecutionContract)
	capabilities := &tgsrlv1.CapabilitySet{
		Names:            []string{"compute", "generation-fencing"},
		Attributes:       map[string]string{"isolation": "sandbox"},
		Source:           "mock",
		Revision:         2,
		MeasuredAt:       timestamppb.New(fixtureTime.Add(-time.Minute)),
		SupportedActions: []string{"bind", "release"},
		Limits:           map[string]float64{"partitions": 4},
	}
	device := func(id string) *tgsrlv1.Device {
		capacity := &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1000, AcceleratorUnits: 1, EphemeralStorageBytes: 100, NetworkBandwidthBps: 100}
		return &tgsrlv1.Device{
			DeviceId:     id,
			Kind:         tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
			Health:       tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:     proto.Clone(capacity).(*tgsrlv1.ResourceVector),
			Allocatable:  proto.Clone(capacity).(*tgsrlv1.ResourceVector),
			Capabilities: proto.Clone(capabilities).(*tgsrlv1.CapabilitySet),
		}
	}
	pending := func(id string) *tgsrlv1.PendingUnit {
		return &tgsrlv1.PendingUnit{
			PendingUnitId:        id,
			ExecutionId:          intent.GetExecutionId(),
			StageId:              intent.GetStageId(),
			IntentVersion:        intent.GetVersion(),
			JobId:                intent.GetJobId(),
			RequestedResources:   proto.Clone(resources).(*tgsrlv1.ResourceVector),
			RequiredCapabilities: proto.Clone(required).(*tgsrlv1.CapabilitySet),
		}
	}
	snapshot := &tgsrlv1.ClusterSnapshot{
		SnapshotId:   "snapshot",
		Revision:     9,
		ObservedAt:   timestamppb.New(fixtureTime.Add(-time.Second)),
		Devices:      []*tgsrlv1.Device{device("device-b"), device("device-a")},
		PendingUnits: []*tgsrlv1.PendingUnit{pending("unit-b"), pending("unit-a")},
		Annotations:  map[string]string{SafePointAnnotation: "true", "snapshot_revision": "9"},
	}
	return snapshot, intent
}

func testScheduler(t testing.TB, fallback FallbackMode) *Scheduler {
	t.Helper()
	var sequence uint64
	scheduler, err := New(Config{
		Fallback:       fallback,
		Clock:          ClockFunc(func() time.Time { return fixtureTime }),
		Sequence:       SequenceFunc(func() uint64 { sequence++; return sequence }),
		CodeRevision:   "test-code",
		ConfigRevision: "test-config",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return scheduler
}

func setAllDevices(snapshot *tgsrlv1.ClusterSnapshot, mutate func(*tgsrlv1.Device)) {
	for _, device := range snapshot.GetDevices() {
		mutate(device)
	}
}

func insufficient(mutate func(*tgsrlv1.ResourceVector)) func(*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent) {
	return func(snapshot *tgsrlv1.ClusterSnapshot, _ *tgsrlv1.SchedulingIntent) {
		setAllDevices(snapshot, func(device *tgsrlv1.Device) { mutate(device.Capacity); mutate(device.Allocatable) })
	}
}

func activeAllocation(id, executionID, stageID string, version uint64, deviceID string, resources *tgsrlv1.ResourceVector) *tgsrlv1.Allocation {
	return &tgsrlv1.Allocation{
		AllocationId:  id,
		ExecutionId:   executionID,
		StageId:       stageID,
		IntentVersion: version,
		PendingUnitId: "allocated-" + id,
		DeviceIds:     []string{deviceID},
		Resources:     proto.Clone(resources).(*tgsrlv1.ResourceVector),
		State:         tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE,
	}
}

func largeSingleUnitFixture(t testing.TB, deviceCount int) (*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent) {
	t.Helper()
	if deviceCount < 1 {
		t.Fatalf("deviceCount = %d, want >= 1", deviceCount)
	}
	snapshot, intent := validFixture()
	intent.UnitCount = 1
	snapshot.PendingUnits = []*tgsrlv1.PendingUnit{pendingUnitByID(t, snapshot, "unit-a")}
	prototype := proto.Clone(snapshot.GetDevices()[0]).(*tgsrlv1.Device)
	devices := make([]*tgsrlv1.Device, 0, deviceCount)
	devices = append(devices, deviceWithAllocatable(prototype, "device-best", 1000))
	for index := 1; index < deviceCount; index++ {
		devices = append(devices, deviceWithAllocatable(prototype, fmt.Sprintf("device-%04d", index), 400))
	}
	snapshot.Devices = devices
	return snapshot, intent
}

func largeMultiUnitConsumptionFixture(t testing.TB) (*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent) {
	t.Helper()
	snapshot, intent := validFixture()
	intent.ResourcesPerUnit = &tgsrlv1.ResourceVector{
		CpuMillis:             600,
		MemoryBytes:           100,
		AcceleratorUnits:      .25,
		EphemeralStorageBytes: 10,
		NetworkBandwidthBps:   10,
	}
	for _, unit := range snapshot.PendingUnits {
		unit.RequestedResources = proto.Clone(intent.GetResourcesPerUnit()).(*tgsrlv1.ResourceVector)
	}

	prototype := proto.Clone(snapshot.GetDevices()[0]).(*tgsrlv1.Device)
	devices := []*tgsrlv1.Device{
		deviceWithAllocatable(prototype, "device-a", 1000),
		deviceWithAllocatable(prototype, "device-b", 1000),
	}
	for index := 0; index < 2047; index++ {
		bad := deviceWithAllocatable(prototype, fmt.Sprintf("device-z%04d", index), 1000)
		bad.Health = tgsrlv1.DeviceHealth_DEVICE_HEALTH_DRAINING
		devices = append(devices, bad)
	}
	snapshot.Devices = devices
	return snapshot, intent
}

func orderedDecisionSurfaceFixture(t testing.TB) (*tgsrlv1.ClusterSnapshot, *tgsrlv1.SchedulingIntent) {
	t.Helper()
	snapshot, intent := validFixture()
	intent.UnitCount = 1
	snapshot.PendingUnits = []*tgsrlv1.PendingUnit{pendingUnitByID(t, snapshot, "unit-a")}

	prototype := proto.Clone(snapshot.GetDevices()[0]).(*tgsrlv1.Device)
	goodA := deviceWithAllocatable(prototype, "device-a", 900)
	goodB := deviceWithAllocatable(prototype, "device-b", 900)
	badCapability := deviceWithAllocatable(prototype, "device-y", 900)
	badCapability.Capabilities.Names = []string{"other"}
	badHealth := deviceWithAllocatable(prototype, "device-z", 900)
	badHealth.Health = tgsrlv1.DeviceHealth_DEVICE_HEALTH_DRAINING
	snapshot.Devices = []*tgsrlv1.Device{badCapability, goodB, badHealth, goodA}
	return snapshot, intent
}

func pendingUnitByID(t testing.TB, snapshot *tgsrlv1.ClusterSnapshot, id string) *tgsrlv1.PendingUnit {
	t.Helper()
	for _, unit := range snapshot.GetPendingUnits() {
		if unit.GetPendingUnitId() == id {
			return proto.Clone(unit).(*tgsrlv1.PendingUnit)
		}
	}
	t.Fatalf("pending unit %q not found", id)
	return nil
}

func deviceWithAllocatable(prototype *tgsrlv1.Device, id string, cpu uint64) *tgsrlv1.Device {
	device := proto.Clone(prototype).(*tgsrlv1.Device)
	device.DeviceId = id
	device.Capacity.CpuMillis = cpu
	device.Allocatable.CpuMillis = cpu
	return device
}

func selectedCandidateForBinding(t testing.TB, record *tgsrlv1.DecisionRecord, binding *tgsrlv1.Binding) *tgsrlv1.PlacementCandidate {
	t.Helper()
	candidate := findCandidateByUnitAndDevice(record, binding.GetPendingUnitId(), binding.GetDeviceIds()[0])
	if candidate == nil {
		t.Fatalf("no recorded candidate for pending=%q device=%q", binding.GetPendingUnitId(), binding.GetDeviceIds()[0])
	}
	return candidate
}

func findCandidateByUnitAndDevice(record *tgsrlv1.DecisionRecord, pendingUnitID, deviceID string) *tgsrlv1.PlacementCandidate {
	for _, candidate := range record.GetCandidates() {
		candidateDeviceID, candidatePendingUnitID := candidateSortKeys(candidate)
		if candidatePendingUnitID == pendingUnitID && candidateDeviceID == deviceID {
			return candidate
		}
	}
	return nil
}

func candidateOrdered(left, right *tgsrlv1.PlacementCandidate) bool {
	if left.GetScore() != right.GetScore() {
		return left.GetScore() > right.GetScore()
	}
	leftDevice, leftUnit := candidateSortKeys(left)
	rightDevice, rightUnit := candidateSortKeys(right)
	if leftDevice != rightDevice {
		return leftDevice < rightDevice
	}
	if leftUnit != rightUnit {
		return leftUnit < rightUnit
	}
	return left.GetCandidateId() <= right.GetCandidateId()
}

func rejectionOrdered(left, right *tgsrlv1.CandidateRejection) bool {
	if left.GetCandidateId() != right.GetCandidateId() {
		return left.GetCandidateId() < right.GetCandidateId()
	}
	if left.GetReason() != right.GetReason() {
		return left.GetReason() < right.GetReason()
	}
	return left.GetDetail() <= right.GetDetail()
}
