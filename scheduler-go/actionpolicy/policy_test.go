package actionpolicy

import (
	"errors"
	"fmt"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

func TestLevelForActionAndCeilingForTick(t *testing.T) {
	tests := []struct {
		name        string
		actionType  tgsrlv1.ActionType
		wantLevel   tgsrlv1.ActionLevel
		wantSupport bool
	}{
		{name: "bind", actionType: tgsrlv1.ActionType_ACTION_TYPE_BIND, wantLevel: tgsrlv1.ActionLevel_ACTION_LEVEL_L1, wantSupport: true},
		{name: "pause", actionType: tgsrlv1.ActionType_ACTION_TYPE_PAUSE, wantLevel: tgsrlv1.ActionLevel_ACTION_LEVEL_L2, wantSupport: true},
		{name: "sleep", actionType: tgsrlv1.ActionType_ACTION_TYPE_SLEEP, wantLevel: tgsrlv1.ActionLevel_ACTION_LEVEL_L3, wantSupport: true},
		{name: "recreate", actionType: tgsrlv1.ActionType_ACTION_TYPE_RECREATE, wantLevel: tgsrlv1.ActionLevel_ACTION_LEVEL_L4, wantSupport: true},
		{name: "unknown", actionType: tgsrlv1.ActionType_ACTION_TYPE_UNKNOWN, wantLevel: tgsrlv1.ActionLevel_ACTION_LEVEL_UNKNOWN, wantSupport: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := LevelForAction(test.actionType)
			if got != test.wantLevel || ok != test.wantSupport {
				t.Fatalf("LevelForAction(%s) = %s, %v, want %s, %v", test.actionType, got, ok, test.wantLevel, test.wantSupport)
			}
		})
	}

	tickTests := []struct {
		tick        tgsrlv1.TickKind
		wantCeiling tgsrlv1.ActionLevel
		wantSupport bool
	}{
		{tick: tgsrlv1.TickKind_TICK_KIND_FAST, wantCeiling: tgsrlv1.ActionLevel_ACTION_LEVEL_L1, wantSupport: true},
		{tick: tgsrlv1.TickKind_TICK_KIND_MEDIUM, wantCeiling: tgsrlv1.ActionLevel_ACTION_LEVEL_L3, wantSupport: true},
		{tick: tgsrlv1.TickKind_TICK_KIND_SLOW, wantCeiling: tgsrlv1.ActionLevel_ACTION_LEVEL_L4, wantSupport: true},
		{tick: tgsrlv1.TickKind_TICK_KIND_UNKNOWN, wantCeiling: tgsrlv1.ActionLevel_ACTION_LEVEL_UNKNOWN, wantSupport: false},
	}
	for _, test := range tickTests {
		got, ok := CeilingForTick(test.tick)
		if got != test.wantCeiling || ok != test.wantSupport {
			t.Fatalf("CeilingForTick(%s) = %s, %v, want %s, %v", test.tick, got, ok, test.wantCeiling, test.wantSupport)
		}
	}
}

func TestValidateAction(t *testing.T) {
	tests := []struct {
		name         string
		action       *tgsrlv1.Action
		expectedTick tgsrlv1.TickKind
		options      ValidationOptions
		wantKind     ViolationKind
	}{
		{
			name:         "legacy unknown L1 allowed",
			action:       testAction(tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, tgsrlv1.TickKind_TICK_KIND_UNKNOWN),
			expectedTick: tgsrlv1.TickKind_TICK_KIND_UNKNOWN,
			options:      ValidationOptions{AllowLegacyUnknownTickL1: true},
		},
		{
			name:         "unknown tick rejects L2",
			action:       testAction(tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, tgsrlv1.TickKind_TICK_KIND_UNKNOWN),
			expectedTick: tgsrlv1.TickKind_TICK_KIND_UNKNOWN,
			options:      ValidationOptions{AllowLegacyUnknownTickL1: true},
			wantKind:     ViolationUnknownTick,
		},
		{
			name:         "fast rejects L2",
			action:       testAction(tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, tgsrlv1.TickKind_TICK_KIND_FAST),
			expectedTick: tgsrlv1.TickKind_TICK_KIND_FAST,
			wantKind:     ViolationTickExceedsCeiling,
		},
		{
			name:         "medium allows L3",
			action:       testAction(tgsrlv1.ActionType_ACTION_TYPE_SLEEP, tgsrlv1.ActionLevel_ACTION_LEVEL_L3, tgsrlv1.TickKind_TICK_KIND_MEDIUM),
			expectedTick: tgsrlv1.TickKind_TICK_KIND_MEDIUM,
		},
		{
			name:         "medium rejects L4",
			action:       testAction(tgsrlv1.ActionType_ACTION_TYPE_RECREATE, tgsrlv1.ActionLevel_ACTION_LEVEL_L4, tgsrlv1.TickKind_TICK_KIND_MEDIUM),
			expectedTick: tgsrlv1.TickKind_TICK_KIND_MEDIUM,
			wantKind:     ViolationTickExceedsCeiling,
		},
		{
			name:         "expected tick mismatch",
			action:       testAction(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, tgsrlv1.TickKind_TICK_KIND_FAST),
			expectedTick: tgsrlv1.TickKind_TICK_KIND_SLOW,
			wantKind:     ViolationTickMismatch,
		},
		{
			name:         "declared level mismatch",
			action:       testAction(tgsrlv1.ActionType_ACTION_TYPE_SLEEP, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, tgsrlv1.TickKind_TICK_KIND_MEDIUM),
			expectedTick: tgsrlv1.TickKind_TICK_KIND_MEDIUM,
			wantKind:     ViolationLevelMismatch,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateAction(test.action, test.expectedTick, test.options)
			if test.wantKind == "" {
				if err != nil {
					t.Fatalf("ValidateAction() error = %v", err)
				}
				return
			}
			var validationErr *ValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("ValidateAction() error = %v, want ValidationError", err)
			}
			if validationErr.Kind != test.wantKind {
				t.Fatalf("ValidateAction() kind = %s, want %s", validationErr.Kind, test.wantKind)
			}
		})
	}
}

func TestValidatePlan(t *testing.T) {
	plan := &tgsrlv1.PlacementPlan{
		PlanId:         "plan",
		Purpose:        tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		RollbackPolicy: tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION,
		CapabilityRequirements: []*tgsrlv1.CapabilityRequirement{
			NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION),
			NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK),
		},
		Actions: []*tgsrlv1.Action{
			testAction(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, tgsrlv1.TickKind_TICK_KIND_MEDIUM),
			testAction(tgsrlv1.ActionType_ACTION_TYPE_SLEEP, tgsrlv1.ActionLevel_ACTION_LEVEL_L3, tgsrlv1.TickKind_TICK_KIND_MEDIUM),
		},
	}
	for index, action := range plan.Actions {
		definition, _ := DefinitionForAction(action.GetActionType())
		action.Order = uint32(index + 1)
		action.ExpectedSnapshotRevision = 1
		action.ExpectedGeneration = 1
		action.Preconditions = []tgsrlv1.ActionPrecondition{
			tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_SNAPSHOT_REVISION_MATCH,
			tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_TARGET_GENERATION_MATCH,
		}
		action.ExpectedImpacts = append([]tgsrlv1.ExpectedImpact(nil), definition.ExpectedImpacts...)
		action.RequiredCapabilities = &tgsrlv1.CapabilitySet{SupportedActions: []string{definition.CapabilityName}}
		action.TargetId = "sandbox-a"
		action.SandboxId = "sandbox-a"
		action.Rollback = &tgsrlv1.Rollback{ActionType: definition.CompensationType, TargetId: "sandbox-a"}
	}
	plan.SnapshotRevision = 1
	if err := ValidatePlan(plan, tgsrlv1.TickKind_TICK_KIND_MEDIUM, ValidationOptions{}); err != nil {
		t.Fatalf("ValidatePlan(valid) error = %v", err)
	}

	plan.Actions[1].TickKind = tgsrlv1.TickKind_TICK_KIND_FAST
	err := ValidatePlan(plan, tgsrlv1.TickKind_TICK_KIND_MEDIUM, ValidationOptions{})
	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("ValidatePlan() error = %v, want ValidationError", err)
	}
	if validationErr.Kind != ViolationTickMismatch {
		t.Fatalf("ValidatePlan() kind = %s, want %s", validationErr.Kind, ViolationTickMismatch)
	}
}

func TestValidatePlanPurposeContracts(t *testing.T) {
	strictAction := func(actionType tgsrlv1.ActionType, level tgsrlv1.ActionLevel, tick tgsrlv1.TickKind) *tgsrlv1.Action {
		action := testAction(actionType, level, tick)
		action.Order = 1
		action.ExpectedSnapshotRevision = 7
		action.ExpectedGeneration = 2
		definition, ok := DefinitionForAction(actionType)
		if !ok {
			t.Fatalf("DefinitionForAction(%s) missing", actionType)
		}
		action.RequiredCapabilities = &tgsrlv1.CapabilitySet{SupportedActions: []string{definition.CapabilityName}}
		action.TargetId = "sandbox-a"
		action.SandboxId = "sandbox-a"
		action.Rollback = &tgsrlv1.Rollback{ActionType: definition.CompensationType, TargetId: "sandbox-a"}
		if actionType == tgsrlv1.ActionType_ACTION_TYPE_BIND {
			action.Binding = &tgsrlv1.Binding{BindingId: "binding-a", SandboxId: "sandbox-a"}
			action.Rollback.TargetId = "binding-a"
		} else if definition.CompensationNeedsBinding {
			action.Rollback.RestoreBinding = &tgsrlv1.Binding{BindingId: "binding-a", SandboxId: "sandbox-a"}
		}
		action.Preconditions = []tgsrlv1.ActionPrecondition{
			tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_SNAPSHOT_REVISION_MATCH,
			tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_TARGET_GENERATION_MATCH,
		}
		if actionType == tgsrlv1.ActionType_ACTION_TYPE_BIND {
			action.Preconditions = action.Preconditions[:1]
		}
		action.ExpectedImpacts = append([]tgsrlv1.ExpectedImpact(nil), definition.ExpectedImpacts...)
		return action
	}
	strictPlan := func(purpose tgsrlv1.PlanPurpose, action *tgsrlv1.Action) *tgsrlv1.PlacementPlan {
		return &tgsrlv1.PlacementPlan{
			PlanId:           "plan",
			SnapshotRevision: 7,
			Purpose:          purpose,
			RollbackPolicy:   tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION,
			CapabilityRequirements: []*tgsrlv1.CapabilityRequirement{
				NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK),
			},
			Actions: []*tgsrlv1.Action{action},
		}
	}

	tests := []struct {
		name     string
		plan     *tgsrlv1.PlacementPlan
		wantKind ViolationKind
	}{
		{
			name: "fast admission bind",
			plan: strictPlan(tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION, strictAction(
				tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, tgsrlv1.TickKind_TICK_KIND_FAST,
			)),
		},
		{
			name: "fast rebalance cannot disguise migration as bind",
			plan: func() *tgsrlv1.PlacementPlan {
				plan := strictPlan(tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE, strictAction(tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, tgsrlv1.TickKind_TICK_KIND_FAST))
				plan.AffectedAllocationIds = []string{"allocation-1"}
				return plan
			}(),
			wantKind: ViolationPurposeMismatch,
		},
		{
			name: "medium lifecycle boundary",
			plan: strictPlan(tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION, strictAction(
				tgsrlv1.ActionType_ACTION_TYPE_SLEEP, tgsrlv1.ActionLevel_ACTION_LEVEL_L3, tgsrlv1.TickKind_TICK_KIND_MEDIUM,
			)),
		},
		{
			name: "slow rebind boundary",
			plan: strictPlan(tgsrlv1.PlanPurpose_PLAN_PURPOSE_REBALANCE, strictAction(
				tgsrlv1.ActionType_ACTION_TYPE_REBIND, tgsrlv1.ActionLevel_ACTION_LEVEL_L4, tgsrlv1.TickKind_TICK_KIND_SLOW,
			)),
		},
	}
	tests[3].plan.AffectedAllocationIds = []string{"allocation-1"}
	tests[3].plan.Actions[0].TargetId = "allocation-1"
	tests[3].plan.Actions[0].Preconditions = append(tests[3].plan.Actions[0].Preconditions, tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_AFFECTED_ALLOCATIONS_ACTIVE)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidatePlan(test.plan, tgsrlv1.TickKind_TICK_KIND_UNKNOWN, ValidationOptions{})
			if test.wantKind == "" {
				if err != nil {
					t.Fatalf("ValidatePlan() error = %v", err)
				}
				return
			}
			var validationErr *ValidationError
			if !errors.As(err, &validationErr) || validationErr.Kind != test.wantKind {
				t.Fatalf("ValidatePlan() error = %v, want kind %s", err, test.wantKind)
			}
		})
	}
}

func TestValidatePlanLegacyAndPreemptionFailClosed(t *testing.T) {
	legacy := &tgsrlv1.PlacementPlan{
		PlanId: "legacy",
		Actions: []*tgsrlv1.Action{{
			ActionId:   "bind",
			ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND,
			Level:      tgsrlv1.ActionLevel_ACTION_LEVEL_L1,
			Rollback:   &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE},
		}},
	}
	purpose, compatibility, err := EffectivePurpose(legacy)
	if err != nil || !compatibility || purpose != tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION {
		t.Fatalf("EffectivePurpose(legacy) = (%s, %v, %v)", purpose, compatibility, err)
	}
	if err := ValidatePlan(legacy, tgsrlv1.TickKind_TICK_KIND_UNKNOWN, ValidationOptions{AllowLegacyUnknownTickL1: true}); err != nil {
		t.Fatalf("ValidatePlan(legacy) error = %v", err)
	}
	legacy.Actions[0].TickKind = tgsrlv1.TickKind_TICK_KIND_FAST
	if err := ValidatePlan(legacy, tgsrlv1.TickKind_TICK_KIND_FAST, ValidationOptions{AllowLegacyUnknownTickL1: true}); err != nil {
		t.Fatalf("ValidatePlan(legacy plan with explicit tick) error = %v", err)
	}

	unknownMutation := &tgsrlv1.PlacementPlan{PlanId: "unknown", Actions: []*tgsrlv1.Action{testAction(tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, tgsrlv1.TickKind_TICK_KIND_MEDIUM)}}
	if _, _, err := EffectivePurpose(unknownMutation); err == nil {
		t.Fatal("EffectivePurpose(unknown mutation) error = nil")
	}

	preemption := &tgsrlv1.PlacementPlan{
		PlanId:                "preempt",
		SnapshotRevision:      7,
		Purpose:               tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION,
		RollbackPolicy:        tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION,
		AffectedAllocationIds: []string{"allocation-1"},
		CapabilityRequirements: []*tgsrlv1.CapabilityRequirement{
			NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION),
			NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK),
			NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_TRANSACTIONAL_PLAN_EXECUTION),
			NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ATOMIC_REPLACEMENT),
		},
		Actions: []*tgsrlv1.Action{
			strictPolicyAction(tgsrlv1.ActionType_ACTION_TYPE_RELEASE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 1),
			strictPolicyAction(tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, 2),
		},
	}
	preemption.Actions[0].TargetId = "allocation-1"
	preemption.Actions[0].Preconditions = append(preemption.Actions[0].Preconditions, tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_AFFECTED_ALLOCATIONS_ACTIVE)
	err = ValidatePlan(preemption, tgsrlv1.TickKind_TICK_KIND_SLOW, ValidationOptions{})
	var validationErr *ValidationError
	if !errors.As(err, &validationErr) || validationErr.Kind != ViolationPreemptionUnavailable {
		t.Fatalf("ValidatePlan(preemption) error = %v, want fail closed", err)
	}
}

func TestValidatePlanRejectsMalformedTypedContract(t *testing.T) {
	base := &tgsrlv1.PlacementPlan{
		PlanId:           "plan",
		SnapshotRevision: 7,
		Purpose:          tgsrlv1.PlanPurpose_PLAN_PURPOSE_RECONCILIATION,
		RollbackPolicy:   tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION,
		CapabilityRequirements: []*tgsrlv1.CapabilityRequirement{
			NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK),
		},
		Actions: []*tgsrlv1.Action{strictPolicyAction(tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionLevel_ACTION_LEVEL_L2, 1)},
	}
	tests := []struct {
		name     string
		mutate   func(*tgsrlv1.PlacementPlan)
		wantKind ViolationKind
	}{
		{name: "duplicate capability", mutate: func(plan *tgsrlv1.PlacementPlan) {
			plan.CapabilityRequirements = []*tgsrlv1.CapabilityRequirement{
				NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION),
				NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION),
			}
		}, wantKind: ViolationCapabilityRequirement},
		{name: "unknown capability", mutate: func(plan *tgsrlv1.PlacementPlan) {
			plan.CapabilityRequirements = []*tgsrlv1.CapabilityRequirement{{Kind: tgsrlv1.CapabilityRequirementKind(99), Required: true}}
		}, wantKind: ViolationCapabilityRequirement},
		{name: "zero order", mutate: func(plan *tgsrlv1.PlacementPlan) { plan.Actions[0].Order = 0 }, wantKind: ViolationInvalidArgument},
		{name: "missing generation fence", mutate: func(plan *tgsrlv1.PlacementPlan) { plan.Actions[0].ExpectedGeneration = 0 }, wantKind: ViolationPrecondition},
		{name: "safe point mismatch", mutate: func(plan *tgsrlv1.PlacementPlan) { plan.Actions[0].RequiresSafePoint = true }, wantKind: ViolationPrecondition},
		{name: "missing action capability", mutate: func(plan *tgsrlv1.PlacementPlan) { plan.Actions[0].RequiredCapabilities = nil }, wantKind: ViolationCapabilityRequirement},
		{name: "incorrect impact", mutate: func(plan *tgsrlv1.PlacementPlan) {
			plan.Actions[0].ExpectedImpacts = []tgsrlv1.ExpectedImpact{tgsrlv1.ExpectedImpact_EXPECTED_IMPACT_ALLOCATION_CREATED}
		}, wantKind: ViolationExpectedImpact},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := proto.Clone(base).(*tgsrlv1.PlacementPlan)
			test.mutate(plan)
			err := ValidatePlan(plan, tgsrlv1.TickKind_TICK_KIND_SLOW, ValidationOptions{})
			var validationErr *ValidationError
			if !errors.As(err, &validationErr) || validationErr.Kind != test.wantKind {
				t.Fatalf("ValidatePlan() error = %v, want kind %s", err, test.wantKind)
			}
		})
	}
}

func strictPolicyAction(actionType tgsrlv1.ActionType, level tgsrlv1.ActionLevel, order uint32) *tgsrlv1.Action {
	definition, _ := DefinitionForAction(actionType)
	action := &tgsrlv1.Action{
		ActionId:                 fmt.Sprintf("action-%d", order),
		ActionType:               actionType,
		Level:                    level,
		Order:                    order,
		ExpectedSnapshotRevision: 7,
		ExpectedGeneration:       2,
		TickKind:                 tgsrlv1.TickKind_TICK_KIND_SLOW,
		RequiredCapabilities:     &tgsrlv1.CapabilitySet{SupportedActions: []string{definition.CapabilityName}},
		TargetId:                 "sandbox-a",
		SandboxId:                "sandbox-a",
		Rollback:                 &tgsrlv1.Rollback{ActionType: definition.CompensationType, TargetId: "sandbox-a"},
		Preconditions: []tgsrlv1.ActionPrecondition{
			tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_SNAPSHOT_REVISION_MATCH,
			tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_TARGET_GENERATION_MATCH,
		},
		ExpectedImpacts: append([]tgsrlv1.ExpectedImpact(nil), definition.ExpectedImpacts...),
	}
	if actionType == tgsrlv1.ActionType_ACTION_TYPE_BIND {
		action.Preconditions = action.Preconditions[:1]
		action.Binding = &tgsrlv1.Binding{BindingId: "binding-a", SandboxId: "sandbox-a"}
		action.Rollback.TargetId = "binding-a"
	} else if definition.CompensationNeedsBinding {
		action.Rollback.RestoreBinding = &tgsrlv1.Binding{BindingId: "binding-a", SandboxId: "sandbox-a"}
	}
	return action
}

func testAction(actionType tgsrlv1.ActionType, level tgsrlv1.ActionLevel, tick tgsrlv1.TickKind) *tgsrlv1.Action {
	return &tgsrlv1.Action{
		ActionId:   "action-1",
		ActionType: actionType,
		Level:      level,
		TickKind:   tick,
	}
}
