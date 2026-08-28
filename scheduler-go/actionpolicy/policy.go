package actionpolicy

import (
	"fmt"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// ValidationOptions controls compatibility exceptions during policy rollout.
type ValidationOptions struct {
	AllowLegacyUnknownTickL1 bool
}

// ViolationKind classifies a policy validation failure.
type ViolationKind string

const (
	ViolationInvalidArgument    ViolationKind = "invalid_argument"
	ViolationUnknownAction      ViolationKind = "unknown_action"
	ViolationLevelMismatch      ViolationKind = "level_mismatch"
	ViolationUnknownTick        ViolationKind = "unknown_tick"
	ViolationTickMismatch       ViolationKind = "tick_mismatch"
	ViolationTickExceedsCeiling ViolationKind = "tick_exceeds_ceiling"
)

// ValidationError describes why an action or plan violates the centralized
// action authorization policy.
type ValidationError struct {
	Kind          ViolationKind
	Message       string
	ActionID      string
	ActionType    tgsrlv1.ActionType
	DeclaredLevel tgsrlv1.ActionLevel
	ExpectedLevel tgsrlv1.ActionLevel
	TickKind      tgsrlv1.TickKind
	ExpectedTick  tgsrlv1.TickKind
	Ceiling       tgsrlv1.ActionLevel
}

func (e *ValidationError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Message
}

// LevelForAction is the single authority that maps an action type to its
// declared authorization level.
func LevelForAction(actionType tgsrlv1.ActionType) (tgsrlv1.ActionLevel, bool) {
	switch actionType {
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE,
		tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY,
		tgsrlv1.ActionType_ACTION_TYPE_RESIZE,
		tgsrlv1.ActionType_ACTION_TYPE_BIND,
		tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_L1, true
	case tgsrlv1.ActionType_ACTION_TYPE_PAUSE,
		tgsrlv1.ActionType_ACTION_TYPE_RESUME:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_L2, true
	case tgsrlv1.ActionType_ACTION_TYPE_SLEEP,
		tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_L3, true
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND,
		tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_L4, true
	default:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_UNKNOWN, false
	}
}

// CeilingForTick is the single authority that maps a scheduler tick to the
// strongest action level that tick may execute.
func CeilingForTick(tick tgsrlv1.TickKind) (tgsrlv1.ActionLevel, bool) {
	switch tick {
	case tgsrlv1.TickKind_TICK_KIND_FAST:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_L1, true
	case tgsrlv1.TickKind_TICK_KIND_MEDIUM:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_L3, true
	case tgsrlv1.TickKind_TICK_KIND_SLOW:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_L4, true
	default:
		return tgsrlv1.ActionLevel_ACTION_LEVEL_UNKNOWN, false
	}
}

// ValidateAction ensures an action declares the authoritative level for its
// type, uses a permitted tick kind, matches the expected tick when one is
// provided, and does not exceed that tick's action ceiling.
func ValidateAction(action *tgsrlv1.Action, expectedTick tgsrlv1.TickKind, options ValidationOptions) error {
	if action == nil {
		return &ValidationError{Kind: ViolationInvalidArgument, Message: "action is required"}
	}

	expectedLevel, supported := LevelForAction(action.GetActionType())
	if !supported {
		return &ValidationError{
			Kind:       ViolationUnknownAction,
			Message:    fmt.Sprintf("action %q has unsupported action_type %s", action.GetActionId(), action.GetActionType()),
			ActionID:   action.GetActionId(),
			ActionType: action.GetActionType(),
		}
	}
	if action.GetLevel() != expectedLevel {
		return &ValidationError{
			Kind:          ViolationLevelMismatch,
			Message:       fmt.Sprintf("action %q declares level %s, want %s for %s", action.GetActionId(), action.GetLevel(), expectedLevel, action.GetActionType()),
			ActionID:      action.GetActionId(),
			ActionType:    action.GetActionType(),
			DeclaredLevel: action.GetLevel(),
			ExpectedLevel: expectedLevel,
		}
	}

	actionTick := action.GetTickKind()
	if actionTick == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		if options.AllowLegacyUnknownTickL1 && expectedLevel == tgsrlv1.ActionLevel_ACTION_LEVEL_L1 {
			return nil
		}
		return &ValidationError{
			Kind:          ViolationUnknownTick,
			Message:       fmt.Sprintf("action %q uses unknown tick_kind for level %s", action.GetActionId(), action.GetLevel()),
			ActionID:      action.GetActionId(),
			ActionType:    action.GetActionType(),
			DeclaredLevel: action.GetLevel(),
			ExpectedLevel: expectedLevel,
			TickKind:      actionTick,
			ExpectedTick:  expectedTick,
		}
	}

	if expectedTick != tgsrlv1.TickKind_TICK_KIND_UNKNOWN && actionTick != expectedTick {
		return &ValidationError{
			Kind:          ViolationTickMismatch,
			Message:       fmt.Sprintf("action %q tick_kind %s does not match expected tick %s", action.GetActionId(), actionTick, expectedTick),
			ActionID:      action.GetActionId(),
			ActionType:    action.GetActionType(),
			DeclaredLevel: action.GetLevel(),
			ExpectedLevel: expectedLevel,
			TickKind:      actionTick,
			ExpectedTick:  expectedTick,
		}
	}

	ceiling, ok := CeilingForTick(actionTick)
	if !ok {
		return &ValidationError{
			Kind:          ViolationUnknownTick,
			Message:       fmt.Sprintf("action %q uses unsupported tick_kind %s", action.GetActionId(), actionTick),
			ActionID:      action.GetActionId(),
			ActionType:    action.GetActionType(),
			DeclaredLevel: action.GetLevel(),
			ExpectedLevel: expectedLevel,
			TickKind:      actionTick,
			ExpectedTick:  expectedTick,
		}
	}
	if action.GetLevel() > ceiling {
		return &ValidationError{
			Kind:          ViolationTickExceedsCeiling,
			Message:       fmt.Sprintf("action %q level %s exceeds tick %s ceiling %s", action.GetActionId(), action.GetLevel(), actionTick, ceiling),
			ActionID:      action.GetActionId(),
			ActionType:    action.GetActionType(),
			DeclaredLevel: action.GetLevel(),
			ExpectedLevel: expectedLevel,
			TickKind:      actionTick,
			ExpectedTick:  expectedTick,
			Ceiling:       ceiling,
		}
	}
	return nil
}

// ValidatePlan applies the centralized policy to every action in a plan. When
// expectedTick is unknown, the validator derives a plan-wide tick from the
// first non-unknown action tick and then enforces consistency against it.
func ValidatePlan(plan *tgsrlv1.PlacementPlan, expectedTick tgsrlv1.TickKind, options ValidationOptions) error {
	if plan == nil {
		return &ValidationError{Kind: ViolationInvalidArgument, Message: "plan is required"}
	}
	resolvedTick := expectedTick
	if resolvedTick == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		resolvedTick = derivePlanTick(plan)
	}
	for index, action := range plan.GetActions() {
		if action == nil {
			return &ValidationError{
				Kind:    ViolationInvalidArgument,
				Message: fmt.Sprintf("plan %q contains nil action at index %d", plan.GetPlanId(), index),
			}
		}
		if err := ValidateAction(action, resolvedTick, options); err != nil {
			return err
		}
	}
	return nil
}

func derivePlanTick(plan *tgsrlv1.PlacementPlan) tgsrlv1.TickKind {
	for _, action := range plan.GetActions() {
		if action != nil && action.GetTickKind() != tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
			return action.GetTickKind()
		}
	}
	return tgsrlv1.TickKind_TICK_KIND_UNKNOWN
}
