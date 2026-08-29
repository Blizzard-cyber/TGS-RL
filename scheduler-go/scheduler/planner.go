package scheduler

import (
	"sort"
	"strconv"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func makeBinding(decisionID, pendingUnitID, runtimeUnitID, deviceID string, resources *tgsrlv1.ResourceVector) *tgsrlv1.Binding {
	sandboxID := stableID("sandbox", decisionID, pendingUnitID)
	return &tgsrlv1.Binding{
		BindingId:     stableID("binding", decisionID, pendingUnitID, deviceID),
		PendingUnitId: pendingUnitID,
		RuntimeUnitId: runtimeUnitID,
		DeviceIds:     []string{deviceID},
		Resources:     cloneResources(resources),
		SandboxId:     sandboxID,
		Generation:    1,
	}
}

func populateBindingMetadata(binding *tgsrlv1.Binding, decisionID string, intent *tgsrlv1.SchedulingIntent) {
	if binding == nil {
		return
	}
	binding.BindingId = stableID("binding", decisionID, binding.GetPendingUnitId(), binding.GetDeviceIds()[0])
	binding.RuntimeUnitId = intent.GetLabels()["runtime_unit_id"]
	binding.SandboxId = stableID("sandbox", decisionID, binding.GetPendingUnitId())
	binding.Generation = 1
}

func populateCandidatePlanMetadata(plan *tgsrlv1.PlacementPlan, candidateID, decisionID string, snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, now time.Time, requiresSafePoint bool, tick tgsrlv1.TickKind) {
	if plan == nil || len(plan.GetBindings()) != 1 {
		return
	}
	complete := makePlan(decisionID, stableID("candidate-plan", candidateID), snapshot, intent, now, plan.GetBindings(), requiresSafePoint, tick)
	proto.Reset(plan)
	proto.Merge(plan, complete)
}

func makePlan(decisionID, planID string, snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, now time.Time, bindings []*tgsrlv1.Binding, requiresSafePoint bool, tick tgsrlv1.TickKind) *tgsrlv1.PlacementPlan {
	bindings = stableBindings(bindings)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:           planID,
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		CreatedAt:        timestamppb.New(now),
		ExpiresAt:        proto.Clone(intent.GetValidUntil()).(*timestamppb.Timestamp),
		DecisionId:       decisionID,
		JobId:            intent.GetJobId(),
		RunId:            intent.GetRunId(),
		TraceId:          intent.GetTraceId(),
		DataKind:         intent.GetDataKind(),
		SemanticContext:  cloneSemanticEnvelope(intent.GetSemanticContext()),
		Generation:       intent.GetGeneration(),
		Cursor:           intent.GetCursor(),
		Purpose:          tgsrlv1.PlanPurpose_PLAN_PURPOSE_ADMISSION,
		RollbackPolicy:   tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_NOT_REQUIRED,
	}
	for index, binding := range bindings {
		clonedBinding := proto.Clone(binding).(*tgsrlv1.Binding)
		plan.Bindings = append(plan.Bindings, clonedBinding)
		order := uint32(index + 1)
		actionID := stableID("action", planID, binding.GetBindingId(), strconv.FormatUint(uint64(order), 10))
		definition, _ := actionpolicy.DefinitionForAction(tgsrlv1.ActionType_ACTION_TYPE_BIND)
		preconditions := []tgsrlv1.ActionPrecondition{
			tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_SNAPSHOT_REVISION_MATCH,
		}
		if requiresSafePoint {
			preconditions = append(preconditions, tgsrlv1.ActionPrecondition_ACTION_PRECONDITION_SAFE_POINT_REACHED)
		}
		plan.Actions = append(plan.Actions, &tgsrlv1.Action{
			ActionId:                 actionID,
			ActionType:               tgsrlv1.ActionType_ACTION_TYPE_BIND,
			Level:                    tgsrlv1.ActionLevel_ACTION_LEVEL_L1,
			TargetId:                 binding.GetPendingUnitId(),
			Binding:                  proto.Clone(binding).(*tgsrlv1.Binding),
			Rollback:                 &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, TargetId: binding.GetBindingId(), Reason: "compensate bind if plan application fails"},
			Order:                    order,
			PlanId:                   planID,
			SandboxId:                binding.GetSandboxId(),
			ExpectedGeneration:       binding.GetGeneration(),
			ExpectedSnapshotRevision: snapshot.GetRevision(),
			RequiredCapabilities:     actionCapabilities(intent.GetRequiredCapabilities(), "bind"),
			RequiresSafePoint:        requiresSafePoint,
			Deadline:                 proto.Clone(intent.GetValidUntil()).(*timestamppb.Timestamp),
			IdempotencyKey:           stableID("action-idempotency", intent.GetIdempotencyKey(), binding.GetPendingUnitId(), strconv.FormatUint(uint64(order), 10)),
			TickKind:                 tick,
			Preconditions:            preconditions,
			ExpectedImpacts:          append([]tgsrlv1.ExpectedImpact(nil), definition.ExpectedImpacts...),
		})
	}
	if len(plan.GetActions()) > 0 {
		plan.RollbackPolicy = tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_NOT_REQUIRED
	}
	if len(plan.GetActions()) > 1 {
		plan.RollbackPolicy = tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION
		plan.CapabilityRequirements = actionpolicy.StableCapabilityRequirements(
			actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION),
			actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK),
		)
	}
	return plan
}

func stableBindings(bindings []*tgsrlv1.Binding) []*tgsrlv1.Binding {
	ordered := append([]*tgsrlv1.Binding(nil), bindings...)
	sort.SliceStable(ordered, func(i, j int) bool {
		left, right := ordered[i], ordered[j]
		if left.GetPendingUnitId() != right.GetPendingUnitId() {
			return left.GetPendingUnitId() < right.GetPendingUnitId()
		}
		leftDevice, rightDevice := "", ""
		if len(left.GetDeviceIds()) > 0 {
			leftDevice = left.GetDeviceIds()[0]
		}
		if len(right.GetDeviceIds()) > 0 {
			rightDevice = right.GetDeviceIds()[0]
		}
		if leftDevice != rightDevice {
			return leftDevice < rightDevice
		}
		return left.GetBindingId() < right.GetBindingId()
	})
	return ordered
}

func actionCapabilities(capabilities *tgsrlv1.CapabilitySet, action string) *tgsrlv1.CapabilitySet {
	required := cloneCapabilities(capabilities)
	if !containsNormalized(required.GetSupportedActions(), action) {
		required.SupportedActions = append(required.SupportedActions, action)
	}
	sort.Strings(required.SupportedActions)
	return required
}

func cloneCapabilities(capabilities *tgsrlv1.CapabilitySet) *tgsrlv1.CapabilitySet {
	if capabilities == nil {
		return &tgsrlv1.CapabilitySet{}
	}
	return proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)
}

func cloneSemanticEnvelope(envelope *tgsrlv1.SemanticEnvelope) *tgsrlv1.SemanticEnvelope {
	if envelope == nil {
		return nil
	}
	return proto.Clone(envelope).(*tgsrlv1.SemanticEnvelope)
}
