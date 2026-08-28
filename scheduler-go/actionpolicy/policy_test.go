package actionpolicy

import (
	"errors"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
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
		PlanId: "plan",
		Actions: []*tgsrlv1.Action{
			testAction(tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionLevel_ACTION_LEVEL_L1, tgsrlv1.TickKind_TICK_KIND_MEDIUM),
			testAction(tgsrlv1.ActionType_ACTION_TYPE_SLEEP, tgsrlv1.ActionLevel_ACTION_LEVEL_L3, tgsrlv1.TickKind_TICK_KIND_MEDIUM),
		},
	}
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

func testAction(actionType tgsrlv1.ActionType, level tgsrlv1.ActionLevel, tick tgsrlv1.TickKind) *tgsrlv1.Action {
	return &tgsrlv1.Action{
		ActionId:   "action-1",
		ActionType: actionType,
		Level:      level,
		TickKind:   tick,
	}
}
