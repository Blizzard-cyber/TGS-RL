package semantics

import (
	"math"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEvaluateUsesContextObservationPrecedence(t *testing.T) {
	input := testInput()
	intentLag := uint64(7)
	contextLag := uint64(2)
	intentSafe := false
	contextSafe := true
	input.Intent.ContractObservation = &tgsrlv1.ContractObservation{
		PolicyLag:   &intentLag,
		SafePoint:   &intentSafe,
		BufferLevel: ptrUint(0),
	}
	input.Context.ContractObservation = &tgsrlv1.ContractObservation{
		PolicyLag:   &contextLag,
		SafePoint:   &contextSafe,
		BufferLevel: ptrUint(0),
	}
	input.Contract.ValidityRules = []*tgsrlv1.ValidityRule{{
		RuleId:      "lag",
		FailureMode: tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_REJECT,
		Predicate: &tgsrlv1.Condition{
			Operator: tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LE,
			FactPath: "sample.policy_lag",
			Operands: []*tgsrlv1.SemanticValue{semanticUint(3)},
		},
	}}
	input.Contract.CommitPolicy.RequireSafePoint = true

	evals, aggregate, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if len(evals) != 4 {
		t.Fatalf("len(evals) = %d, want 4", len(evals))
	}
	if got := evals[0].GetClauseId(); got != "lag" {
		t.Fatalf("first clause id = %q, want lag", got)
	}
	if got := evals[0].GetStatus(); got != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED {
		t.Fatalf("lag status = %v, want SATISFIED", got)
	}
	if !aggregate.Blocking {
		t.Fatalf("aggregate.Blocking = false, want true from version constraint reject")
	}
}

func TestEvaluateRejectsTypedFactDuplicateConflict(t *testing.T) {
	input := testInput()
	input.Context.ContractObservation = &tgsrlv1.ContractObservation{
		PolicyLag: ptrUint(4),
		TypedFacts: []*tgsrlv1.SemanticField{{
			Key:   "sample.policy_lag",
			Value: semanticUint(5),
		}},
	}

	_, _, err := Evaluate(input)
	if err == nil {
		t.Fatal("Evaluate() unexpectedly succeeded")
	}
}

func TestEvaluateTypedPredicatesAndNestedLogic(t *testing.T) {
	input := testInput()
	policyLag := uint64(2)
	ratio := 0.9
	stale := false
	buffer := uint64(0)
	input.Context.ContractObservation = &tgsrlv1.ContractObservation{
		PolicyLag:                &policyLag,
		EffectiveSampleSizeRatio: &ratio,
		SampleStale:              &stale,
		BufferLevel:              &buffer,
		TypedFacts: []*tgsrlv1.SemanticField{{
			Key: "custom.labels",
			Value: &tgsrlv1.SemanticValue{
				Kind: &tgsrlv1.SemanticValue_ListValue{
					ListValue: &tgsrlv1.SemanticList{
						Values: []*tgsrlv1.SemanticValue{semanticString("a"), semanticString("b")},
					},
				},
			},
		}},
	}
	input.Contract.Conditions = []*tgsrlv1.Condition{
		{
			ConditionId: "nested",
			Operator:    tgsrlv1.ConditionOperator_CONDITION_OPERATOR_AND,
			Predicates: []*tgsrlv1.Condition{
				{
					Operator: tgsrlv1.ConditionOperator_CONDITION_OPERATOR_LE,
					FactPath: "sample.policy_lag",
					Operands: []*tgsrlv1.SemanticValue{semanticUint(3)},
				},
				{
					Operator: tgsrlv1.ConditionOperator_CONDITION_OPERATOR_NOT,
					Predicates: []*tgsrlv1.Condition{{
						Operator: tgsrlv1.ConditionOperator_CONDITION_OPERATOR_EQ,
						FactPath: "sample.stale",
						Operands: []*tgsrlv1.SemanticValue{semanticBool(true)},
					}},
				},
			},
		},
		{
			ConditionId: "membership",
			Operator:    tgsrlv1.ConditionOperator_CONDITION_OPERATOR_IN,
			FactPath:    "sample.policy_lag",
			Operands: []*tgsrlv1.SemanticValue{{
				Kind: &tgsrlv1.SemanticValue_ListValue{
					ListValue: &tgsrlv1.SemanticList{
						Values: []*tgsrlv1.SemanticValue{semanticUint(1), semanticUint(2), semanticUint(3)},
					},
				},
			}},
		},
	}

	evals, _, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	var nested, membership *tgsrlv1.ContractEvaluation
	for _, eval := range evals {
		switch eval.GetClauseId() {
		case "nested":
			nested = eval
		case "membership":
			membership = eval
		}
	}
	if nested == nil || membership == nil {
		t.Fatalf("missing condition evaluations")
	}
	if nested.GetStatus() != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED {
		t.Fatalf("nested status = %v, want SATISFIED", nested.GetStatus())
	}
	if membership.GetStatus() != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED {
		t.Fatalf("membership status = %v, want SATISFIED", membership.GetStatus())
	}
}

func TestEvaluateMarksMissingAndTypeMismatchIndeterminate(t *testing.T) {
	input := testInput()
	buffer := uint64(0)
	input.Context.ContractObservation = &tgsrlv1.ContractObservation{BufferLevel: &buffer}
	input.Contract.Conditions = []*tgsrlv1.Condition{
		{
			ConditionId: "missing",
			Operator:    tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GT,
			FactPath:    "sample.policy_lag",
			Operands:    []*tgsrlv1.SemanticValue{semanticUint(1)},
		},
		{
			ConditionId: "type-mismatch",
			Operator:    tgsrlv1.ConditionOperator_CONDITION_OPERATOR_EQ,
			FactPath:    "buffer.level.current",
			Operands:    []*tgsrlv1.SemanticValue{semanticString("0")},
		},
	}

	evals, _, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	for _, clauseID := range []string{"missing", "type-mismatch"} {
		var found *tgsrlv1.ContractEvaluation
		for _, eval := range evals {
			if eval.GetClauseId() == clauseID {
				found = eval
				break
			}
		}
		if found == nil {
			t.Fatalf("missing evaluation for %s", clauseID)
		}
		if found.GetStatus() != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE {
			t.Fatalf("%s status = %v, want INDETERMINATE", clauseID, found.GetStatus())
		}
	}
}

func TestEvaluateRejectsNaNAndInf(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1)} {
		input := testInput()
		buffer := uint64(0)
		input.Context.ContractObservation = &tgsrlv1.ContractObservation{
			BufferLevel:              &buffer,
			EffectiveSampleSizeRatio: &value,
		}
		if _, _, err := Evaluate(input); err == nil {
			t.Fatalf("Evaluate() unexpectedly succeeded for %v", value)
		}
	}
}

func TestEvaluateComputesAgeESSAndOrderingDeterministically(t *testing.T) {
	input := testInput()
	input.Context.EvaluationTime = timestamppb.New(time.Unix(10, 0))
	oldest := timestamppb.New(time.Unix(8, 500_000_000))
	buffer := uint64(0)
	ess := 12.5
	ratio := 0.8
	accepted := uint64(10)
	expected := uint64(12)
	input.Context.ContractObservation = &tgsrlv1.ContractObservation{
		OldestSampleAt:           oldest,
		BufferLevel:              &buffer,
		EffectiveSampleSize:      &ess,
		EffectiveSampleSizeRatio: &ratio,
		AcceptedSamples:          &accepted,
		ExpectedSamples:          &expected,
	}
	input.Contract.Conditions = []*tgsrlv1.Condition{
		{
			ConditionId: "age",
			Operator:    tgsrlv1.ConditionOperator_CONDITION_OPERATOR_EQ,
			FactPath:    "sample.age_ms",
			Operands:    []*tgsrlv1.SemanticValue{semanticInt(1500)},
		},
		{
			ConditionId: "ess",
			Operator:    tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GT,
			FactPath:    "batch.effective_sample_size",
			Operands:    []*tgsrlv1.SemanticValue{semanticDouble(10)},
		},
		{
			ConditionId: "ratio",
			Operator:    tgsrlv1.ConditionOperator_CONDITION_OPERATOR_GE,
			FactPath:    "batch.effective_sample_size_ratio",
			Operands:    []*tgsrlv1.SemanticValue{semanticDouble(0.75)},
		},
	}

	first, _, err := Evaluate(input)
	if err != nil {
		t.Fatalf("first Evaluate() error = %v", err)
	}
	second, _, err := Evaluate(input)
	if err != nil {
		t.Fatalf("second Evaluate() error = %v", err)
	}
	if len(first) != len(second) {
		t.Fatalf("evaluation length mismatch: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].GetEvaluationId() != second[i].GetEvaluationId() {
			t.Fatalf("evaluation_id[%d] mismatch: %q vs %q", i, first[i].GetEvaluationId(), second[i].GetEvaluationId())
		}
	}
}

func TestEvaluateBackpressureSemantics(t *testing.T) {
	tests := []struct {
		name    string
		level   *uint64
		mode    tgsrlv1.BackpressureMode
		want    tgsrlv1.ContractEvaluationStatus
		wantAct tgsrlv1.ContractDecisionAction
	}{
		{name: "allow-low", level: ptrUint(2), mode: tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_BLOCK_PRODUCER, want: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED, wantAct: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW},
		{name: "directive-high", level: ptrUint(8), mode: tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_REQUEST_SCALE_OUT, want: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED, wantAct: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_SCALE_OUT_REQUESTED},
		{name: "reject-max", level: ptrUint(12), mode: tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_SHED_OLDEST, want: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED, wantAct: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT},
		{name: "indeterminate-middle", level: ptrUint(5), mode: tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_SHED_OLDEST, want: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE, wantAct: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW},
		{name: "indeterminate-missing", level: nil, mode: tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_SHED_OLDEST, want: tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE, wantAct: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := testInput()
			input.Contract.BackpressurePolicy = &tgsrlv1.BackpressurePolicy{
				Mode:               test.mode,
				LowWatermark:       2,
				HighWatermark:      8,
				MaximumBufferLevel: 10,
				StallTimeout:       durationpb.New(time.Second),
			}
			if test.level != nil {
				input.Context.ContractObservation = &tgsrlv1.ContractObservation{BufferLevel: test.level}
			}
			evals, _, err := Evaluate(input)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			var got *tgsrlv1.ContractEvaluation
			for _, eval := range evals {
				if eval.GetClauseKind() == tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_BACKPRESSURE_POLICY {
					got = eval
				}
			}
			if got == nil {
				t.Fatal("missing backpressure evaluation")
			}
			if got.GetStatus() != test.want {
				t.Fatalf("status = %v, want %v", got.GetStatus(), test.want)
			}
			if got.GetRecommendedAction() != test.wantAct {
				t.Fatalf("action = %v, want %v", got.GetRecommendedAction(), test.wantAct)
			}
		})
	}
}

func TestEvaluateSafePointFullAsyncSemantics(t *testing.T) {
	input := testInput()
	buffer := uint64(0)
	falseValue := false
	input.Contract.CommitPolicy.RequireSafePoint = false
	input.Contract.SafePointPolicy.Enabled = true
	input.Context.ContractObservation = &tgsrlv1.ContractObservation{
		BufferLevel: &buffer,
		SafePoint:   &falseValue,
	}

	evals, aggregate, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	var safePoint *tgsrlv1.ContractEvaluation
	for _, eval := range evals {
		if eval.GetClauseKind() == tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_SAFE_POINT_POLICY {
			safePoint = eval
			break
		}
	}
	if safePoint == nil {
		t.Fatal("missing safe point evaluation")
	}
	if safePoint.GetStatus() != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_NOT_APPLICABLE {
		t.Fatalf("safe point status = %v, want NOT_APPLICABLE", safePoint.GetStatus())
	}
	if aggregate.Action != tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT {
		t.Fatalf("aggregate action = %v, want REJECT from version constraint", aggregate.Action)
	}
}

func TestAggregatePriorityAbortPauseReject(t *testing.T) {
	evals := []*tgsrlv1.ContractEvaluation{
		{ClauseId: "reject", RecommendedAction: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT},
		{ClauseId: "pause", RecommendedAction: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_PAUSE_REQUIRED},
		{ClauseId: "abort", RecommendedAction: tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ABORT_REQUIRED},
	}
	got := Aggregate(evals)
	if !got.Blocking {
		t.Fatal("Aggregate().Blocking = false, want true")
	}
	if got.Action != tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ABORT_REQUIRED {
		t.Fatalf("Aggregate().Action = %v, want ABORT_REQUIRED", got.Action)
	}
	if got.BlockingAction != "ABORT" {
		t.Fatalf("Aggregate().BlockingAction = %q, want ABORT", got.BlockingAction)
	}
}

func TestLegacyValidityRuleWithoutPredicateIsIndeterminateNonBlocking(t *testing.T) {
	input := testInput()
	buffer := uint64(0)
	input.Context.ContractObservation = &tgsrlv1.ContractObservation{BufferLevel: &buffer}
	input.Contract.ValidityRules = []*tgsrlv1.ValidityRule{{
		RuleId:      "legacy",
		Expression:  "sample.policy_lag <= 3",
		FailureMode: tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_ABORT,
	}}
	input.Contract.VersionConstraints = nil

	evals, aggregate, err := Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if len(evals) == 0 {
		t.Fatal("no evaluations returned")
	}
	if evals[0].GetStatus() != tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE {
		t.Fatalf("legacy status = %v, want INDETERMINATE", evals[0].GetStatus())
	}
	if aggregate.Blocking {
		t.Fatal("aggregate.Blocking = true, want false")
	}
}

func testInput() Input {
	evaluationTime := timestamppb.New(time.Unix(20, 0))
	return Input{
		Contract: &tgsrlv1.ExecutionContract{
			ContractId: "contract-1",
			Version:    "v1",
			VersionConstraints: []*tgsrlv1.VersionConstraint{{
				Component: "protocol",
				Operator:  tgsrlv1.VersionOperator_VERSION_OPERATOR_COMPATIBLE,
				Version:   "0.4",
			}},
			CommitPolicy: &tgsrlv1.CommitPolicy{
				RequireSafePoint: false,
			},
			BackpressurePolicy: &tgsrlv1.BackpressurePolicy{
				Mode:               tgsrlv1.BackpressureMode_BACKPRESSURE_MODE_BLOCK_PRODUCER,
				LowWatermark:       0,
				HighWatermark:      1,
				MaximumBufferLevel: 1,
				StallTimeout:       durationpb.New(time.Second),
			},
			SafePointPolicy: &tgsrlv1.SafePointPolicy{
				Enabled: true,
			},
		},
		Intent: &tgsrlv1.SchedulingIntent{
			Version: 11,
		},
		Snapshot: &tgsrlv1.ClusterSnapshot{
			Revision: 19,
		},
		Context: &tgsrlv1.EvaluationContext{
			TickKind:                     tgsrlv1.TickKind_TICK_KIND_FAST,
			EvaluationTime:               evaluationTime,
			DecisionSequence:             23,
			Cause:                        "unit-test",
			ObservedRevision:             19,
			CompatibilityDefaultsApplied: true,
		},
	}
}

func ptrUint(value uint64) *uint64 {
	return proto.Uint64(value)
}
