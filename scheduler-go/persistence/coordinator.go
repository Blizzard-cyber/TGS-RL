package persistence

import (
	"context"
	"errors"
	"fmt"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	"google.golang.org/protobuf/proto"
)

type durableStateExporter interface {
	ExportDurableState() state.DurableState
}

// CheckpointRepository is the fail-closed persistence boundary used by the
// scheduler service and startup wiring.
type CheckpointRepository interface {
	SaveCheckpoint(SchedulerState) error
	Recover() (*SchedulerState, error)
}

// IsEmpty reports whether recovery found no durable scheduler state.
func IsEmpty(recovered *SchedulerState) bool {
	return recovered == nil || (recovered.Snapshot == nil && len(recovered.Intents) == 0 && len(recovered.Decisions) == 0 && len(recovered.Reservations) == 0)
}

// Capture combines Store authority and service-owned decision state into one
// atomically persisted checkpoint.
func Capture(store durableStateExporter, decisions []*tgsrlv1.DecisionRecord, cursor uint64) (SchedulerState, error) {
	if store == nil {
		return SchedulerState{}, errors.New("scheduler persistence: store is required")
	}
	durable := store.ExportDurableState()
	checkpoint := SchedulerState{Snapshot: durable.Snapshot, Intents: make(map[string]*tgsrlv1.SchedulingIntent, len(durable.Intents)), Cursor: cursor}
	for _, intent := range durable.Intents {
		checkpoint.Intents[intentKey(intent.GetExecutionId(), intent.GetStageId())] = cloneIntent(intent)
	}
	for _, decision := range decisions {
		checkpoint.Decisions = append(checkpoint.Decisions, cloneDecision(decision))
		for _, result := range decision.GetActionResults() {
			checkpoint.ActionResults = append(checkpoint.ActionResults, cloneActionResult(result))
		}
	}
	for _, reservation := range durable.Reservations {
		record := ReservationRecord{Plan: clonePlan(reservation.Plan), AllocationIDs: append([]string(nil), reservation.AllocationIDs...), Finalized: reservation.Finalized, Succeeded: reservation.Succeeded, RetainedAllocationIDs: append([]string(nil), reservation.RetainedAllocationIDs...)}
		for _, unit := range reservation.PendingUnits {
			record.PendingUnits = append(record.PendingUnits, clonePendingUnit(unit))
		}
		checkpoint.Reservations = append(checkpoint.Reservations, record)
	}
	return checkpoint, nil
}

// RestoreStore applies recovered Store authority. Callers should restore
// decisions/cursor into the service separately.
func RestoreStore(store *state.Store, recovered *SchedulerState) error {
	if store == nil || recovered == nil || recovered.Snapshot == nil {
		return errors.New("scheduler persistence: recovered store state is incomplete")
	}
	durable := state.DurableState{Snapshot: cloneSnapshot(recovered.Snapshot)}
	for _, intent := range recovered.Intents {
		durable.Intents = append(durable.Intents, cloneIntent(intent))
	}
	for _, reservation := range recovered.Reservations {
		record := state.ReservationRecord{Plan: clonePlan(reservation.Plan), AllocationIDs: append([]string(nil), reservation.AllocationIDs...), Finalized: reservation.Finalized, Succeeded: reservation.Succeeded, RetainedAllocationIDs: append([]string(nil), reservation.RetainedAllocationIDs...)}
		for _, unit := range reservation.PendingUnits {
			record.PendingUnits = append(record.PendingUnits, clonePendingUnit(unit))
		}
		durable.Reservations = append(durable.Reservations, record)
	}
	if err := store.Restore(durable); err != nil {
		return fmt.Errorf("scheduler persistence: restore store: %w", err)
	}
	return nil
}

// OpenAndRecover opens the repository and returns an empty state when no
// checkpoint or journal records have ever been written. Corruption and I/O
// failures are returned and must fail startup closed.
func OpenAndRecover(_ context.Context, root string) (*Repository, *SchedulerState, error) {
	repository, err := OpenRepository(root)
	if err != nil {
		return nil, nil, err
	}
	recovered, err := repository.Recover()
	if err != nil {
		return nil, nil, err
	}
	return repository, recovered, nil
}

func cloneDecision(decision *tgsrlv1.DecisionRecord) *tgsrlv1.DecisionRecord {
	if decision == nil {
		return nil
	}
	return proto.Clone(decision).(*tgsrlv1.DecisionRecord)
}

func cloneActionResult(result *tgsrlv1.ActionResult) *tgsrlv1.ActionResult {
	if result == nil {
		return nil
	}
	return proto.Clone(result).(*tgsrlv1.ActionResult)
}

func clonePlan(plan *tgsrlv1.PlacementPlan) *tgsrlv1.PlacementPlan {
	if plan == nil {
		return nil
	}
	return proto.Clone(plan).(*tgsrlv1.PlacementPlan)
}

func clonePendingUnit(unit *tgsrlv1.PendingUnit) *tgsrlv1.PendingUnit {
	if unit == nil {
		return nil
	}
	return proto.Clone(unit).(*tgsrlv1.PendingUnit)
}
