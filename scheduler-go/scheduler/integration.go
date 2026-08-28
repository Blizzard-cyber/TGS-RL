package scheduler

import (
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/preemption"
	"google.golang.org/protobuf/proto"
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
	request := intent.GetResourcesPerUnit()
	freed := &tgsrlv1.ResourceVector{}
	selected := make([]preemption.Victim, 0, len(victims))
	for _, victim := range victims {
		reason, ok := preemption.CanRelease(snapshot, victim, safePoint)
		if !ok {
			return nil, reason
		}
		selected = append(selected, victim)
		addResources(freed, victim.Allocation.GetResources())
		if resourceLessOrEqual(request, freed) {
			break
		}
	}
	if !resourceLessOrEqual(request, freed) {
		return nil, preemption.FallbackInsufficient
	}
	_ = selected
	_ = now
	_ = record
	// Action/Rollback can describe each release, but the current authoritative
	// Store cannot atomically reserve a replacement binding while releasing
	// victims. Returning a release-only plan would bypass ReservePlan and expose
	// capacity between decisions, so fail closed until that transaction exists.
	return nil, preemption.FallbackNotExpressible
}

func findPendingUnit(snapshot *tgsrlv1.ClusterSnapshot, unitID string, intent *tgsrlv1.SchedulingIntent) *tgsrlv1.PendingUnit {
	for _, unit := range snapshot.GetPendingUnits() {
		if unit.GetPendingUnitId() == unitID {
			return proto.Clone(unit).(*tgsrlv1.PendingUnit)
		}
	}
	return &tgsrlv1.PendingUnit{PendingUnitId: unitID, ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(), IntentVersion: intent.GetVersion(), JobId: intent.GetJobId(), RequestedResources: cloneResources(intent.GetResourcesPerUnit()), RequiredCapabilities: cloneCapabilities(intent.GetRequiredCapabilities()), Priority: intent.GetPriority()}
}
