package semantics

import (
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCriticalFactObservationDispositions(t *testing.T) {
	tests := []struct {
		name        string
		missing     bool
		disposition tgsrlv1.ObservationDisposition
		wantStatus  tgsrlv1.ContractEvaluationStatus
		wantAction  tgsrlv1.ContractDecisionAction
		wantBlock   bool
	}{
		{name: "block-missing", missing: true, disposition: tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK, wantStatus: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED, wantAction: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT, wantBlock: true},
		{name: "hold-stale", disposition: tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD, wantStatus: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE, wantAction: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED, wantBlock: true},
		{name: "degrade-stale", disposition: tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_DEGRADE, wantStatus: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE, wantAction: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW},
		{name: "not-applicable-stale", disposition: tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_NOT_APPLICABLE, wantStatus: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_NOT_APPLICABLE, wantAction: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := pr2Input()
			input.Contract.CriticalFactPolicies = []*tgsrlv1.CriticalFactPolicy{{
				FactPath: "sample.policy_lag",
				ObservationPolicy: &tgsrlv1.ObservationPolicy{
					MaximumAge: durationpb.New(time.Second),
					Missing:    test.disposition,
					Stale:      test.disposition,
				},
			}}
			if !test.missing {
				input.Context.ContractObservation.FactObservations = []*tgsrlv1.ObservedFact{observedUint("sample.policy_lag", 2, time.Unix(18, 0))}
			}
			evaluations, aggregate, err := Evaluate(input)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			evaluation := evaluationForID(evaluations, "sample.policy_lag")
			if evaluation == nil {
				t.Fatal("missing critical fact evaluation")
			}
			if evaluation.GetStatus() != test.wantStatus || evaluation.GetRecommendedAction() != test.wantAction || evaluation.GetObservationDisposition() != test.disposition {
				t.Fatalf("evaluation = status %v action %v disposition %v", evaluation.GetStatus(), evaluation.GetRecommendedAction(), evaluation.GetObservationDisposition())
			}
			if aggregate.Blocking != test.wantBlock {
				t.Fatalf("aggregate.Blocking = %v, want %v", aggregate.Blocking, test.wantBlock)
			}
			if len(evaluation.GetEvidence()) == 0 {
				t.Fatal("freshness evaluation has no evidence")
			}
		})
	}
}

func TestAggregatePrioritizesBlockOverHold(t *testing.T) {
	hold := &tgsrlv1.ContractEvaluation{
		ClauseKind:             tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_POLICY_LAG,
		ClauseId:               "hold-fact",
		Status:                 tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE,
		ObservationDisposition: tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD,
		RecommendedAction:      tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED,
	}
	block := &tgsrlv1.ContractEvaluation{
		ClauseKind:             tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_SAMPLE_COVERAGE,
		ClauseId:               "block-fact",
		Status:                 tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED,
		ObservationDisposition: tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK,
		RecommendedAction:      tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT,
	}
	for _, evaluations := range [][]*tgsrlv1.ContractEvaluation{{block, hold}, {hold, block}} {
		got := Aggregate(evaluations)
		if !got.Blocking || got.Action != tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT || got.BlockingAction != "REJECT" {
			t.Fatalf("Aggregate() = %+v, want BLOCK provenance and REJECT", got)
		}
		if !strings.Contains(got.FallbackReason, "block-fact") {
			t.Fatalf("fallback reason = %q, want block-fact provenance", got.FallbackReason)
		}
	}
}

func TestMultipleCriticalPoliciesPrioritizeBlockOverHold(t *testing.T) {
	input := pr2Input()
	input.Contract.CriticalFactPolicies = []*tgsrlv1.CriticalFactPolicy{
		{
			FactPath: "sample.policy_lag",
			ObservationPolicy: &tgsrlv1.ObservationPolicy{
				Missing: tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD,
				Stale:   tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD,
			},
		},
		{
			FactPath: "batch.effective_sample_size",
			ObservationPolicy: &tgsrlv1.ObservationPolicy{
				Missing: tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK,
				Stale:   tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK,
			},
		},
	}
	evaluations, aggregate, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	hold := evaluationForID(evaluations, "sample.policy_lag")
	block := evaluationForID(evaluations, "batch.effective_sample_size")
	if hold == nil || hold.GetObservationDisposition() != tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD {
		t.Fatalf("hold evaluation = %+v", hold)
	}
	if block == nil || block.GetObservationDisposition() != tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK {
		t.Fatalf("block evaluation = %+v", block)
	}
	if !aggregate.Blocking || aggregate.Action != tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT || aggregate.BlockingAction != "REJECT" {
		t.Fatalf("aggregate = %+v, want BLOCK/REJECT", aggregate)
	}
	if !strings.Contains(aggregate.FallbackReason, "batch.effective_sample_size") {
		t.Fatalf("fallback reason = %q, want BLOCK policy provenance", aggregate.FallbackReason)
	}
}

func TestCriticalFactMaximumAgeZeroIsInvalid(t *testing.T) {
	input := pr2Input()
	input.Contract.CriticalFactPolicies = []*tgsrlv1.CriticalFactPolicy{{
		FactPath: "sample.policy_lag",
		ObservationPolicy: &tgsrlv1.ObservationPolicy{
			MaximumAge: durationpb.New(0),
			Missing:    tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK,
			Stale:      tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK,
		},
	}}
	if _, _, err := Evaluate(input); err == nil || !strings.Contains(err.Error(), "must be positive") {
		t.Fatalf("Evaluate() error = %v, want positive maximum_age error", err)
	}
}

func TestCriticalPolicyControlsConsumingCondition(t *testing.T) {
	for _, disposition := range []tgsrlv1.ObservationDisposition{
		tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_DEGRADE,
		tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_NOT_APPLICABLE,
	} {
		input := pr2Input()
		input.Contract.CriticalFactPolicies = []*tgsrlv1.CriticalFactPolicy{{
			FactPath: "sample.policy_lag",
			ObservationPolicy: &tgsrlv1.ObservationPolicy{
				Missing: disposition,
				Stale:   disposition,
			},
		}}
		input.Contract.Conditions = []*tgsrlv1.Condition{{
			ConditionId: "uses-critical-fact",
			Operator:    tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LE,
			FactPath:    "sample.policy_lag",
			Operands:    []*tgsrlv1.SemanticValue{semanticUint(3)},
		}}
		evaluations, aggregate, err := Evaluate(input)
		if err != nil {
			t.Fatalf("Evaluate(%v) error = %v", disposition, err)
		}
		condition := evaluationForID(evaluations, "uses-critical-fact")
		if condition == nil || condition.GetObservationDisposition() != disposition || condition.GetRecommendedAction() != tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW {
			t.Fatalf("condition did not inherit %v: %+v", disposition, condition)
		}
		if aggregate.Blocking {
			t.Fatalf("%v condition unexpectedly blocked", disposition)
		}
	}
}

func TestLegacyTypedFactsUseObservationObservedAt(t *testing.T) {
	input := pr2Input()
	input.Contract.CriticalFactPolicies = []*tgsrlv1.CriticalFactPolicy{{
		FactPath:          "sample.policy_lag",
		ObservationPolicy: &tgsrlv1.ObservationPolicy{MaximumAge: durationpb.New(time.Second), Missing: tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK, Stale: tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD},
	}}
	input.Context.ContractObservation.ObservedAt = timestamppb.New(time.Unix(18, 0))
	input.Context.ContractObservation.TypedFacts = []*tgsrlv1.SemanticField{{Key: "sample.policy_lag", Value: semanticUint(2)}}
	evaluations, aggregate, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	evaluation := evaluationForID(evaluations, "sample.policy_lag")
	if evaluation.GetObservationDisposition() != tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD || !aggregate.Blocking {
		t.Fatalf("legacy freshness = %+v aggregate=%+v", evaluation, aggregate)
	}
}

func TestObservedFactConflictsFailClosed(t *testing.T) {
	input := pr2Input()
	input.Context.ContractObservation.PolicyLag = proto.Uint64(2)
	input.Context.ContractObservation.FactObservations = []*tgsrlv1.ObservedFact{observedUint("sample.policy_lag", 3, time.Unix(20, 0))}
	if _, _, err := Evaluate(input); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("Evaluate() error = %v, want conflict", err)
	}
}

func TestNegativeLegacyFactCannotBeWashedByTypedFact(t *testing.T) {
	input := pr2Input()
	negative := -1.0
	input.Context.ContractObservation.EffectiveSampleSize = &negative
	input.Context.ContractObservation.TypedFacts = []*tgsrlv1.SemanticField{{Key: "batch.effective_sample_size", Value: semanticDouble(negative)}}
	if _, _, err := Evaluate(input); err == nil || (!strings.Contains(err.Error(), "invalid registry fact") && !strings.Contains(err.Error(), "non-negative double")) {
		t.Fatalf("Evaluate() error = %v, want invalid negative fact rejection", err)
	}
}

func TestVersionConstraintSemverMinimumAndCompatibleWindow(t *testing.T) {
	tests := []struct {
		name     string
		operator tgsrlv1.VersionOperator
		actual   string
		required string
		want     bool
	}{
		{name: "semver-greater", operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER, actual: "1.4.0", required: "1.2.3", want: true},
		{name: "semver-lower", operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER, actual: "1.2.2", required: "1.2.3"},
		{name: "compatible-greater", operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_COMPATIBLE, actual: "1.9.0", required: "1.2.3", want: true},
		{name: "compatible-wrong-major", operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_COMPATIBLE, actual: "2.0.0", required: "1.2.3"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := pr2Input()
			input.Contract.VersionConstraints = []*tgsrlv1.VersionConstraint{{Component: "trainer-main", ComponentKind: tgsrlv1.ComponentKind_COMPONENT_KIND_TRAINER, Operator: test.operator, Version: test.required}}
			input.Context.ContractObservation.ComponentVersions = []*tgsrlv1.ComponentVersion{observedComponent(tgsrlv1.ComponentKind_COMPONENT_KIND_TRAINER, "trainer-main", test.actual, time.Unix(20, 0))}
			evaluations, aggregate, err := Evaluate(input)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			evaluation := evaluationForID(evaluations, "trainer-main")
			if got := evaluation.GetStatus() == tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED; got != test.want {
				t.Fatalf("satisfied = %v, want %v: %s", got, test.want, evaluation.GetDetail())
			}
			if aggregate.Blocking == test.want {
				t.Fatalf("blocking = %v, want %v", aggregate.Blocking, !test.want)
			}
		})
	}
}

func TestVersionConflictFailsClosedAndCPUCUDAIsNotApplicable(t *testing.T) {
	conflict := pr2Input()
	conflict.Contract.VersionConstraints = []*tgsrlv1.VersionConstraint{{Component: "trainer-main", ComponentKind: tgsrlv1.ComponentKind_COMPONENT_KIND_TRAINER, Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER, Version: "1.0.0"}}
	conflict.Context.ContractObservation.ComponentVersions = []*tgsrlv1.ComponentVersion{
		observedComponent(tgsrlv1.ComponentKind_COMPONENT_KIND_TRAINER, "trainer-main", "1.2.0", time.Unix(20, 0)),
		observedComponent(tgsrlv1.ComponentKind_COMPONENT_KIND_TRAINER, "trainer-main", "1.3.0", time.Unix(20, 0)),
	}
	evaluations, aggregate, err := Evaluate(conflict)
	if err != nil {
		t.Fatalf("Evaluate(conflict) error = %v", err)
	}
	if evaluationForID(evaluations, "trainer-main").GetStatus() != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE || !aggregate.Blocking {
		t.Fatalf("version conflict did not fail closed: evaluations=%v aggregate=%+v", evaluations, aggregate)
	}

	cpu := pr2Input()
	cpu.Contract.VersionConstraints = []*tgsrlv1.VersionConstraint{{Component: "cuda-driver", Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER, Version: "12.0.0"}}
	cpu.Snapshot.Devices = []*tgsrlv1.Device{{DeviceId: "cpu-1", Kind: tgsrlv1.DeviceKind_DEVICE_KIND_CPU}}
	evaluations, aggregate, err = Evaluate(cpu)
	if err != nil {
		t.Fatalf("Evaluate(cpu) error = %v", err)
	}
	evaluation := evaluationForID(evaluations, "cuda-driver")
	if evaluation.GetStatus() != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_NOT_APPLICABLE || evaluation.GetObservationDisposition() != tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_NOT_APPLICABLE || aggregate.Blocking {
		t.Fatalf("CPU CUDA evaluation = %+v aggregate=%+v", evaluation, aggregate)
	}
	cpu.Snapshot.Devices = nil
	evaluations, aggregate, err = Evaluate(cpu)
	if err != nil {
		t.Fatalf("Evaluate(unknown devices) error = %v", err)
	}
	if evaluationForID(evaluations, "cuda-driver").GetStatus() == tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_NOT_APPLICABLE || !aggregate.Blocking {
		t.Fatalf("unknown device set incorrectly treated as N/A")
	}
}

func TestComponentVersionPriorityPrefersObservationOverSnapshot(t *testing.T) {
	input := pr2Input()
	input.Contract.VersionConstraints = []*tgsrlv1.VersionConstraint{{
		Component: "cuda-main", ComponentKind: tgsrlv1.ComponentKind_COMPONENT_KIND_CUDA_DRIVER,
		Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_EXACT, Version: "551",
	}}
	input.Context.ContractObservation.ComponentVersions = []*tgsrlv1.ComponentVersion{
		observedComponent(tgsrlv1.ComponentKind_COMPONENT_KIND_CUDA_DRIVER, "cuda-main", "551", time.Unix(20, 0)),
	}
	input.Snapshot.Devices = []*tgsrlv1.Device{{
		DeviceId: "gpu-1", Kind: tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
		Capabilities: &tgsrlv1.CapabilitySet{ComponentVersions: []*tgsrlv1.ComponentVersion{
			observedComponent(tgsrlv1.ComponentKind_COMPONENT_KIND_CUDA_DRIVER, "cuda-main", "550", time.Unix(19, 0)),
		}},
	}}
	evaluations, aggregate, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	evaluation := evaluationForID(evaluations, "cuda-main")
	if evaluation == nil || evaluation.GetStatus() != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED || aggregate.Blocking {
		t.Fatalf("observation did not override snapshot: evaluation=%+v aggregate=%+v", evaluation, aggregate)
	}
	if !hasSemanticString(evaluation.GetObservations(), "component.version.current", "551") {
		t.Fatalf("current version evidence = %v, want observation version 551", evaluation.GetObservations())
	}
}

func TestBuiltInProtocolIgnoresLowerPriorityObservation(t *testing.T) {
	input := pr2Input()
	input.Contract.VersionConstraints = []*tgsrlv1.VersionConstraint{{
		Component: "protocol", Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_EXACT, Version: ProtocolVersion,
	}}
	input.Context.ContractObservation.ComponentVersions = []*tgsrlv1.ComponentVersion{
		observedComponent(tgsrlv1.ComponentKind_COMPONENT_KIND_PROTOCOL, "protocol", "999.0.0", time.Unix(20, 0)),
	}
	evaluations, aggregate, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	evaluation := evaluationForID(evaluations, "protocol")
	if evaluation == nil || evaluation.GetStatus() != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED || aggregate.Blocking {
		t.Fatalf("lower-priority protocol observation affected built-in: evaluation=%+v aggregate=%+v", evaluation, aggregate)
	}
	if !hasSemanticString(evaluation.GetObservations(), "component.version.current", ProtocolVersion) {
		t.Fatalf("current protocol evidence = %v, want built-in %s", evaluation.GetObservations(), ProtocolVersion)
	}
}

func TestSamePriorityComponentVersionConflictBlocks(t *testing.T) {
	input := pr2Input()
	input.Contract.VersionConstraints = []*tgsrlv1.VersionConstraint{{
		Component: "runtime-main", ComponentKind: tgsrlv1.ComponentKind_COMPONENT_KIND_RUNTIME,
		Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER, Version: "1.0.0",
	}}
	input.Context.ContractObservation.ComponentVersions = []*tgsrlv1.ComponentVersion{
		observedComponent(tgsrlv1.ComponentKind_COMPONENT_KIND_RUNTIME, "runtime-main", "1.1.0", time.Unix(20, 0)),
		observedComponent(tgsrlv1.ComponentKind_COMPONENT_KIND_RUNTIME, "runtime-main", "1.2.0", time.Unix(20, 0)),
	}
	evaluations, aggregate, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	evaluation := evaluationForID(evaluations, "runtime-main")
	if evaluation == nil || evaluation.GetStatus() != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE || evaluation.GetRecommendedAction() != tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT || !aggregate.Blocking {
		t.Fatalf("same-priority conflict did not fail closed: evaluation=%+v aggregate=%+v", evaluation, aggregate)
	}
	if !strings.Contains(evaluation.GetDetail(), "same priority") {
		t.Fatalf("conflict detail = %q", evaluation.GetDetail())
	}
}

func TestPrereleaseRequiresExplicitAllowance(t *testing.T) {
	input := pr2Input()
	input.Contract.VersionConstraints = []*tgsrlv1.VersionConstraint{{Component: "runtime-main", ComponentKind: tgsrlv1.ComponentKind_COMPONENT_KIND_RUNTIME, Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER, Version: "1.2.0"}}
	input.Context.ContractObservation.ComponentVersions = []*tgsrlv1.ComponentVersion{observedComponent(tgsrlv1.ComponentKind_COMPONENT_KIND_RUNTIME, "runtime-main", "1.3.0-rc.1", time.Unix(20, 0))}
	evaluations, _, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if evaluationForID(evaluations, "runtime-main").GetStatus() != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED {
		t.Fatal("prerelease unexpectedly accepted")
	}
	input.Contract.VersionConstraints[0].AllowPrerelease = true
	evaluations, _, err = Evaluate(input)
	if err != nil || evaluationForID(evaluations, "runtime-main").GetStatus() != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED {
		t.Fatalf("allowed prerelease not accepted: err=%v evaluation=%+v", err, evaluationForID(evaluations, "runtime-main"))
	}
}

func TestSemanticVersionPrecedenceAndBuildMetadata(t *testing.T) {
	tests := []struct {
		actual, required string
		want             bool
	}{
		{actual: "1.0.0-alpha", required: "1.0.0-alpha.1", want: false},
		{actual: "1.0.0-rc.1", required: "1.0.0-beta.11", want: true},
		{actual: "1.2.3+build.9", required: "1.2.3+build.1", want: true},
	}
	for _, test := range tests {
		constraint := &tgsrlv1.VersionConstraint{Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER, Version: test.required, AllowPrerelease: true}
		got, _ := evaluateVersionMatch(test.actual, constraint)
		if got != test.want {
			t.Errorf("SEMVER %q >= %q = %v, want %v", test.actual, test.required, got, test.want)
		}
	}
}

func TestBackpressureDirectivesBlockAggregate(t *testing.T) {
	for _, mode := range []tgsrlv1.BackpressureMode{
		tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_BLOCK_PRODUCER,
		tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_SHED_OLDEST,
		tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_SHED_NEWEST,
		tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_REQUEST_SCALE_OUT,
	} {
		input := pr2Input()
		input.Contract.VersionConstraints = nil
		input.Contract.BackpressurePolicy.Mode = mode
		input.Context.ContractObservation.BufferLevel = proto.Uint64(8)
		_, aggregate, err := Evaluate(input)
		if err != nil {
			t.Fatalf("Evaluate(%v) error = %v", mode, err)
		}
		if !aggregate.Blocking {
			t.Fatalf("%v directive did not block", mode)
		}
	}
}

func TestEvaluationIDBindsCanonicalEvaluationContent(t *testing.T) {
	input := pr2Input()
	input.Contract.CriticalFactPolicies = []*tgsrlv1.CriticalFactPolicy{{
		FactPath: "sample.policy_lag",
		ObservationPolicy: &tgsrlv1.ObservationPolicy{
			MaximumAge: durationpb.New(5 * time.Second),
			Missing:    tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK,
			Stale:      tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_DEGRADE,
		},
	}}
	input.Context.ContractObservation.FactObservations = []*tgsrlv1.ObservedFact{observedUint("sample.policy_lag", 2, time.Unix(19, 0))}
	baseline, _, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate(baseline) error = %v", err)
	}
	baselineID := evaluationForID(baseline, "sample.policy_lag").GetEvaluationId()

	changedTime := proto.Clone(input.Context.ContractObservation).(*tgsrlv1.ContractObservation)
	changedTime.FactObservations[0].ObservedAt = timestamppb.New(time.Unix(10, 0))
	input.Context.ContractObservation = changedTime
	changed, _, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate(changed time) error = %v", err)
	}
	if got := evaluationForID(changed, "sample.policy_lag").GetEvaluationId(); got == baselineID {
		t.Fatal("evaluation ID did not change with observed_at/age/evidence")
	}

	input.Contract.CriticalFactPolicies[0].ObservationPolicy.Stale = tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD
	held, _, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate(changed disposition) error = %v", err)
	}
	if got := evaluationForID(held, "sample.policy_lag").GetEvaluationId(); got == evaluationForID(changed, "sample.policy_lag").GetEvaluationId() {
		t.Fatal("evaluation ID did not change with disposition/action")
	}
}

func TestVersionEvaluationIDChangesWithObservedVersion(t *testing.T) {
	input := pr2Input()
	input.Contract.VersionConstraints = []*tgsrlv1.VersionConstraint{{
		Component: "trainer-main", ComponentKind: tgsrlv1.ComponentKind_COMPONENT_KIND_TRAINER,
		Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER, Version: "1.0.0",
	}}
	input.Context.ContractObservation.ComponentVersions = []*tgsrlv1.ComponentVersion{observedComponent(tgsrlv1.ComponentKind_COMPONENT_KIND_TRAINER, "trainer-main", "1.1.0", time.Unix(20, 0))}
	first, _, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate(first) error = %v", err)
	}
	input.Context.ContractObservation.ComponentVersions[0].Version = "1.2.0"
	second, _, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate(second) error = %v", err)
	}
	if evaluationForID(first, "trainer-main").GetEvaluationId() == evaluationForID(second, "trainer-main").GetEvaluationId() {
		t.Fatal("evaluation ID did not change with observed component version")
	}
}

func TestEvaluationIDStableAcrossMapAndInputOrder(t *testing.T) {
	input := pr2Input()
	input.Contract.VersionConstraints = []*tgsrlv1.VersionConstraint{
		{Component: "trainer-main", ComponentKind: tgsrlv1.ComponentKind_COMPONENT_KIND_TRAINER, Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER, Version: "1.0.0"},
		{Component: "runtime-main", ComponentKind: tgsrlv1.ComponentKind_COMPONENT_KIND_RUNTIME, Operator: tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER, Version: "1.0.0"},
	}
	input.Context.ContractObservation.ComponentVersions = []*tgsrlv1.ComponentVersion{
		observedComponent(tgsrlv1.ComponentKind_COMPONENT_KIND_TRAINER, "trainer-main", "1.1.0", time.Unix(20, 0)),
		observedComponent(tgsrlv1.ComponentKind_COMPONENT_KIND_RUNTIME, "runtime-main", "1.1.0", time.Unix(20, 0)),
	}
	input.Context.ContractObservation.ComponentVersions[0].Attributes = map[string]string{"zone": "a", "region": "test"}
	first, _, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate(first) error = %v", err)
	}
	input.Contract.VersionConstraints[0], input.Contract.VersionConstraints[1] = input.Contract.VersionConstraints[1], input.Contract.VersionConstraints[0]
	input.Context.ContractObservation.ComponentVersions[0], input.Context.ContractObservation.ComponentVersions[1] = input.Context.ContractObservation.ComponentVersions[1], input.Context.ContractObservation.ComponentVersions[0]
	input.Context.ContractObservation.ComponentVersions[1].Attributes = map[string]string{"region": "test", "zone": "a"}
	second, _, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate(second) error = %v", err)
	}
	if len(first) != len(second) {
		t.Fatalf("evaluation length changed: %d != %d", len(first), len(second))
	}
	for index := range first {
		if first[index].GetClauseId() != second[index].GetClauseId() || first[index].GetEvaluationId() != second[index].GetEvaluationId() {
			t.Fatalf("evaluation[%d] changed after input/map reorder: first=%s/%s second=%s/%s", index, first[index].GetClauseId(), first[index].GetEvaluationId(), second[index].GetClauseId(), second[index].GetEvaluationId())
		}
	}
}

func pr2Input() Input {
	input := testInput()
	input.Contract.VersionConstraints = nil
	input.Context.ContractObservation = &tgsrlv1.ContractObservation{BufferLevel: proto.Uint64(0)}
	return input
}

func observedUint(path string, value uint64, observedAt time.Time) *tgsrlv1.ObservedFact {
	return &tgsrlv1.ObservedFact{
		Fact:       &tgsrlv1.SemanticField{Key: path, Value: semanticUint(value)},
		ObservedAt: timestamppb.New(observedAt),
		Source:     "unit-test",
		Revision:   1,
	}
}

func observedComponent(kind tgsrlv1.ComponentKind, name, version string, observedAt time.Time) *tgsrlv1.ComponentVersion {
	return &tgsrlv1.ComponentVersion{Kind: kind, Name: name, Version: version, ObservedAt: timestamppb.New(observedAt), Source: "unit-test", Revision: 1}
}

func evaluationForID(evaluations []*tgsrlv1.ContractEvaluation, id string) *tgsrlv1.ContractEvaluation {
	for _, evaluation := range evaluations {
		if evaluation.GetClauseId() == id {
			return evaluation
		}
	}
	return nil
}

func hasSemanticString(fields []*tgsrlv1.SemanticField, key, value string) bool {
	for _, field := range fields {
		if field.GetKey() == key && field.GetValue().GetStringValue() == value {
			return true
		}
	}
	return false
}
