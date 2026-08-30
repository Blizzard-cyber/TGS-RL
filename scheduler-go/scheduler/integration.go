package scheduler

import (
	"sort"
	"strconv"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/actionpolicy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/candidates"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/preemption"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type schedulerProtectionClock struct{ clock Clock }

func (c schedulerProtectionClock) Now() time.Time { return c.clock.Now() }

func firstDeviceID(plan *tgsrlv1.PlacementPlan) string {
	if plan == nil || len(plan.GetBindings()) == 0 || len(plan.GetBindings()[0].GetDeviceIds()) == 0 {
		return ""
	}
	return plan.GetBindings()[0].GetDeviceIds()[0]
}

func (s *Scheduler) preemptionPlan(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, now time.Time, record *tgsrlv1.DecisionRecord, unit workUnit, safePoint bool) (*tgsrlv1.PlacementPlan, string) {
	if !s.policyBundle.AllowPreemption {
		return nil, ""
	}
	if s.preemption == nil || s.preemption.Name() == "noop" {
		return nil, preemption.FallbackDisabled
	}
	pending := findPendingUnit(snapshot, unit.id, intent)
	victims := s.preemption.Pick(snapshot, pending)
	if len(victims) == 0 {
		return nil, preemption.FallbackInsufficient
	}
	if s.policyBundle.RequireSafePoint && !safePoint {
		return nil, preemption.FallbackUnsafe
	}
	selectedByDevice := make(map[string][]preemption.Victim)
	seenVictims := make(map[string]struct{}, len(victims))
	hadInexpressibleVictim := false
	for _, victim := range victims {
		reason, ok := preemption.CanRelease(snapshot, victim, safePoint)
		if !ok {
			if reason == preemption.FallbackUnsafe {
				return nil, reason
			}
			hadInexpressibleVictim = true
			continue
		}
		allocationID := victim.Allocation.GetAllocationId()
		if _, duplicate := seenVictims[allocationID]; duplicate {
			continue
		}
		seenVictims[allocationID] = struct{}{}
		deviceID := victim.Allocation.GetDeviceIds()[0]
		selectedByDevice[deviceID] = append(selectedByDevice[deviceID], victim)
		if preemptionTargetFits(snapshot, intent, record, pending, deviceID, selectedByDevice[deviceID]) {
			plan := buildPreemptionPlan(snapshot, intent, now, record, pending, selectedByDevice[deviceID], deviceID, safePoint)
			if plan == nil {
				return nil, preemption.FallbackNotExpressible
			}
			return plan, ""
		}
	}
	if hadInexpressibleVictim {
		return nil, preemption.FallbackNotExpressible
	}
	return nil, preemption.FallbackInsufficient
}

func preemptionTargetFits(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, record *tgsrlv1.DecisionRecord, pending *tgsrlv1.PendingUnit, deviceID string, victims []preemption.Victim) bool {
	reclaimIDs := make([]string, 0, len(victims))
	for _, victim := range victims {
		reclaimIDs = append(reclaimIDs, victim.Allocation.GetAllocationId())
	}
	result, err := (candidates.Engine{}).Evaluate(candidates.Request{
		Snapshot: snapshot,
		Intent:   intent,
		Decision: candidates.DecisionMetadata{
			DecisionID: record.GetDecisionId(),
		},
		LedgerAdjustment: candidates.LedgerAdjustment{
			ReclaimAllocationIDs: reclaimIDs,
			IncludeDeviceIDs:     []string{deviceID},
		},
		Units:  []candidates.Unit{{ID: pending.GetPendingUnitId(), Pending: pending}},
		Config: candidates.Config{TopK: 1, EvidenceBudget: 1},
	})
	return err == nil && len(result.Selected) == 1 && result.Selected[0].DeviceID == deviceID
}

func buildPreemptionPlan(
	snapshot *tgsrlv1.ClusterSnapshot,
	intent *tgsrlv1.SchedulingIntent,
	now time.Time,
	record *tgsrlv1.DecisionRecord,
	pending *tgsrlv1.PendingUnit,
	selected []preemption.Victim,
	replacementDeviceID string,
	safePoint bool,
) *tgsrlv1.PlacementPlan {
	decisionID := record.GetDecisionId()
	planID := stableID("preemption-plan", decisionID, snapshot.GetSnapshotId(), replacementDeviceID, joinedVictimIDs(selected))
	replacementBinding := makeBinding(decisionID, pending.GetPendingUnitId(), pending.GetRuntimeUnitId(), replacementDeviceID, pending.GetRequestedResources())
	populateBindingMetadata(replacementBinding, decisionID, intent)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:           planID,
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: snapshot.GetRevision(),
		Bindings:         []*tgsrlv1.Binding{proto.Clone(replacementBinding).(*tgsrlv1.Binding)},
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
		Purpose:          tgsrlv1.PlanPurpose_PLAN_PURPOSE_PREEMPTION,
		RollbackPolicy:   tgsrlv1.RollbackPolicy_ROLLBACK_POLICY_REQUIRED_COMPENSATION,
	}
	for _, victim := range selected {
		allocation := victim.Allocation
		restoreBinding, ok := restoredVictimBinding(allocation)
		if !ok {
			return nil
		}
		definition, _ := actionpolicy.DefinitionForAction(tgsrlv1.ActionType_ACTION_TYPE_RELEASE)
		order := uint32(len(plan.Actions) + 1)
		actionID := stableID("preemption-action", planID, "release", allocation.GetAllocationId(), strconv.FormatUint(uint64(order), 10))
		plan.Actions = append(plan.Actions, &tgsrlv1.Action{
			ActionId:                 actionID,
			ActionType:               tgsrlv1.ActionType_ACTION_TYPE_RELEASE,
			Level:                    definition.Level,
			TargetId:                 allocation.GetAllocationId(),
			Binding:                  proto.Clone(restoreBinding).(*tgsrlv1.Binding),
			Share:                    allocation.GetResources().GetAcceleratorUnits(),
			Priority:                 allocation.GetPriority(),
			Rollback:                 &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_BIND, TargetId: restoreBinding.GetSandboxId(), RestoreBinding: proto.Clone(restoreBinding).(*tgsrlv1.Binding), Reason: "restore released victim if replacement cannot commit"},
			Order:                    order,
			PlanId:                   planID,
			SandboxId:                restoreBinding.GetSandboxId(),
			ExpectedGeneration:       allocation.GetGeneration(),
			ExpectedSnapshotRevision: snapshot.GetRevision(),
			RequiredCapabilities:     actionCapabilities(intent.GetRequiredCapabilities(), "release"),
			RequiresSafePoint:        safePoint,
			Deadline:                 proto.Clone(intent.GetValidUntil()).(*timestamppb.Timestamp),
			IdempotencyKey:           stableID("preemption-idempotency", intent.GetIdempotencyKey(), allocation.GetAllocationId(), "release", strconv.FormatUint(uint64(order), 10)),
			TickKind:                 record.GetTickKind(),
			Preconditions: actionpolicy.RequiredPreconditions(
				tgsrlv1.ActionType_ACTION_TYPE_RELEASE,
				safePoint,
				true,
			),
			ExpectedImpacts: append([]tgsrlv1.ExpectedImpact(nil), definition.ExpectedImpacts...),
		})
		plan.AffectedAllocationIds = append(plan.AffectedAllocationIds, allocation.GetAllocationId())
	}
	definition, _ := actionpolicy.DefinitionForAction(tgsrlv1.ActionType_ACTION_TYPE_BIND)
	order := uint32(len(plan.Actions) + 1)
	bindActionID := stableID("preemption-action", planID, "bind", replacementBinding.GetBindingId(), strconv.FormatUint(uint64(order), 10))
	plan.Actions = append(plan.Actions, &tgsrlv1.Action{
		ActionId:                 bindActionID,
		ActionType:               tgsrlv1.ActionType_ACTION_TYPE_BIND,
		Level:                    definition.Level,
		TargetId:                 replacementBinding.GetPendingUnitId(),
		Binding:                  proto.Clone(replacementBinding).(*tgsrlv1.Binding),
		Share:                    replacementBinding.GetResources().GetAcceleratorUnits(),
		Priority:                 intent.GetPriority(),
		Rollback:                 &tgsrlv1.Rollback{ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE, TargetId: replacementBinding.GetBindingId(), Reason: "compensate replacement bind if preemption sequence fails"},
		Order:                    order,
		PlanId:                   planID,
		SandboxId:                replacementBinding.GetSandboxId(),
		ExpectedGeneration:       replacementBinding.GetGeneration(),
		ExpectedSnapshotRevision: snapshot.GetRevision(),
		RequiredCapabilities:     actionCapabilities(intent.GetRequiredCapabilities(), "bind"),
		RequiresSafePoint:        safePoint,
		Deadline:                 proto.Clone(intent.GetValidUntil()).(*timestamppb.Timestamp),
		IdempotencyKey:           stableID("preemption-idempotency", intent.GetIdempotencyKey(), replacementBinding.GetPendingUnitId(), "bind", strconv.FormatUint(uint64(order), 10)),
		TickKind:                 record.GetTickKind(),
		Preconditions:            actionpolicy.RequiredPreconditions(tgsrlv1.ActionType_ACTION_TYPE_BIND, safePoint, false),
		ExpectedImpacts:          append([]tgsrlv1.ExpectedImpact(nil), definition.ExpectedImpacts...),
	})
	sort.Strings(plan.AffectedAllocationIds)
	plan.CapabilityRequirements = actionpolicy.StableCapabilityRequirements(
		actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION),
		actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK),
		actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_TRANSACTIONAL_PLAN_EXECUTION),
		actionpolicy.NewCapabilityRequirement(tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ATOMIC_REPLACEMENT),
	)
	return plan
}

func restoredVictimBinding(allocation *tgsrlv1.Allocation) (*tgsrlv1.Binding, bool) {
	sandboxID, ok := derivedVictimSandboxID(allocation)
	if !ok {
		return nil, false
	}
	deviceIDs := sortedDeviceIDs(allocation.GetDeviceIds())
	return &tgsrlv1.Binding{
		BindingId:     allocation.GetBindingId(),
		PendingUnitId: allocation.GetPendingUnitId(),
		RuntimeUnitId: allocation.GetRuntimeUnitId(),
		DeviceIds:     deviceIDs,
		Resources:     cloneResources(allocation.GetResources()),
		SandboxId:     sandboxID,
		Generation:    allocation.GetGeneration(),
	}, true
}

func derivedVictimSandboxID(allocation *tgsrlv1.Allocation) (string, bool) {
	if allocation == nil ||
		allocation.GetSandboxId() == "" ||
		allocation.GetBindingId() == "" ||
		allocation.GetPendingUnitId() == "" ||
		allocation.GetRuntimeUnitId() == "" {
		return "", false
	}
	return allocation.GetSandboxId(), true
}

func sortedDeviceIDs(deviceIDs []string) []string {
	ordered := append([]string(nil), deviceIDs...)
	sort.Strings(ordered)
	return ordered
}

func joinedVictimIDs(victims []preemption.Victim) string {
	ids := make([]string, 0, len(victims))
	for _, victim := range victims {
		if victim.Allocation != nil {
			ids = append(ids, victim.Allocation.GetAllocationId())
		}
	}
	sort.Strings(ids)
	return strconv.QuoteToASCII(joinStrings(ids))
}

func joinStrings(values []string) string {
	switch len(values) {
	case 0:
		return ""
	case 1:
		return values[0]
	}
	result := values[0]
	for _, value := range values[1:] {
		result += "|" + value
	}
	return result
}

func findPendingUnit(snapshot *tgsrlv1.ClusterSnapshot, unitID string, intent *tgsrlv1.SchedulingIntent) *tgsrlv1.PendingUnit {
	for _, unit := range snapshot.GetPendingUnits() {
		if unit.GetPendingUnitId() == unitID {
			return proto.Clone(unit).(*tgsrlv1.PendingUnit)
		}
	}
	return &tgsrlv1.PendingUnit{PendingUnitId: unitID, ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(), IntentVersion: intent.GetVersion(), JobId: intent.GetJobId(), RequestedResources: cloneResources(intent.GetResourcesPerUnit()), RequiredCapabilities: cloneCapabilities(intent.GetRequiredCapabilities()), Priority: intent.GetPriority(), RunId: intent.GetRunId(), TraceId: intent.GetTraceId(), DataKind: intent.GetDataKind(), RuntimeUnitId: intent.GetLabels()["runtime_unit_id"]}
}
