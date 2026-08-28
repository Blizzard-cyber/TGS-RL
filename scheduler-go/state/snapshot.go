package state

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ResourceMutation changes a private working snapshot. Returning an error
// aborts the commit. The Store overwrites revision, snapshot_id, and observed_at
// when it commits, regardless of values assigned by the mutation.
type ResourceMutation func(*tgsrlv1.ClusterSnapshot) error

// MutateResources atomically applies mutate when expectedRevision equals the
// current revision. A successful commit advances the revision exactly once.
func (s *Store) MutateResources(expectedRevision uint64, mutate ResourceMutation) (*tgsrlv1.ClusterSnapshot, error) {
	if mutate == nil {
		return nil, errors.New("state: nil resource mutation")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	currentRevision := s.snapshot.GetRevision()
	if currentRevision != expectedRevision {
		return nil, fmt.Errorf("%w: expected %d, current %d", ErrRevisionConflict, expectedRevision, currentRevision)
	}
	if currentRevision == math.MaxUint64 {
		return nil, ErrRevisionExhausted
	}

	working := cloneSnapshot(s.snapshot)
	if err := mutate(working); err != nil {
		return nil, err
	}

	now := s.clock.Now()
	working.Revision = currentRevision + 1
	working.SnapshotId = snapshotID(working.Revision)
	working.ObservedAt = timestamppb.New(now)

	// Clone after invoking foreign code so a mutation that retained working
	// cannot alter committed state after this method returns.
	s.snapshot = cloneSnapshot(working)
	s.signalRevisionLocked()
	return cloneSnapshot(s.snapshot), nil
}

// GetSnapshot returns an immutable-by-ownership snapshot clone. When
// minimumRevision is ahead of the current state, it waits for a commit or for
// ctx cancellation. A zero minimumRevision is an immediate read.
func (s *Store) GetSnapshot(ctx context.Context, minimumRevision uint64, includePendingUnits bool) (*tgsrlv1.ClusterSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		s.mu.RLock()
		if s.snapshot.GetRevision() >= minimumRevision {
			snapshot := cloneSnapshot(s.snapshot)
			s.mu.RUnlock()
			if !includePendingUnits {
				snapshot.PendingUnits = nil
			}
			return snapshot, nil
		}
		revisionChanged := s.revisionChanged
		s.mu.RUnlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-revisionChanged:
		}
	}
}

// Revision returns the currently committed snapshot revision.
func (s *Store) Revision() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot.GetRevision()
}

func (s *Store) exportDurableStateLocked() storeDurableState {
	result := storeDurableState{
		snapshot:           cloneSnapshot(s.snapshot),
		intents:            make(map[intentKey]*tgsrlv1.SchedulingIntent, len(s.intents)),
		providerProjection: s.providerProjection.clone(),
		idempotencyKeys:    make(map[string]intentIdentity, len(s.idempotencyKeys)),
		reservations:       make(map[string]*planReservation, len(s.reservations)),
	}
	for key, intent := range s.intents {
		result.intents[key] = cloneIntent(intent)
	}
	for key, identity := range s.idempotencyKeys {
		result.idempotencyKeys[key] = intentIdentity{
			key:     identity.key,
			version: identity.version,
			intent:  cloneIntent(identity.intent),
		}
	}
	for planID, reservation := range s.reservations {
		cloned := &planReservation{
			plan:                  clonePlan(reservation.plan),
			allocationIDs:         append([]string(nil), reservation.allocationIDs...),
			finalized:             reservation.finalized,
			succeeded:             reservation.succeeded,
			retainedAllocationIDs: append([]string(nil), reservation.retainedAllocationIDs...),
		}
		for _, unit := range reservation.pendingUnits {
			cloned.pendingUnits = append(cloned.pendingUnits, clonePendingUnit(unit))
		}
		result.reservations[planID] = cloned
	}
	return result
}

func (s *Store) restoreDurableStateLocked(durable storeDurableState) {
	s.snapshot = cloneSnapshot(durable.snapshot)
	s.intents = make(map[intentKey]*tgsrlv1.SchedulingIntent, len(durable.intents))
	for key, intent := range durable.intents {
		s.intents[key] = cloneIntent(intent)
	}
	s.providerProjection = durable.providerProjection.clone()
	s.idempotencyKeys = make(map[string]intentIdentity, len(durable.idempotencyKeys))
	for key, identity := range durable.idempotencyKeys {
		s.idempotencyKeys[key] = intentIdentity{
			key:     identity.key,
			version: identity.version,
			intent:  cloneIntent(identity.intent),
		}
	}
	s.reservations = make(map[string]*planReservation, len(durable.reservations))
	for planID, reservation := range durable.reservations {
		cloned := &planReservation{
			plan:                  clonePlan(reservation.plan),
			allocationIDs:         append([]string(nil), reservation.allocationIDs...),
			finalized:             reservation.finalized,
			succeeded:             reservation.succeeded,
			retainedAllocationIDs: append([]string(nil), reservation.retainedAllocationIDs...),
		}
		for _, unit := range reservation.pendingUnits {
			cloned.pendingUnits = append(cloned.pendingUnits, clonePendingUnit(unit))
		}
		s.reservations[planID] = cloned
	}
}

// ExportDurableState returns a deep copy of all Store-owned durable state.
func (s *Store) ExportDurableState() DurableState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := DurableState{Snapshot: cloneSnapshot(s.snapshot)}
	keys := make([]intentKey, 0, len(s.intents))
	for key := range s.intents {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].executionID != keys[j].executionID {
			return keys[i].executionID < keys[j].executionID
		}
		return keys[i].stageID < keys[j].stageID
	})
	for _, key := range keys {
		result.Intents = append(result.Intents, cloneIntent(s.intents[key]))
	}

	planIDs := make([]string, 0, len(s.reservations))
	for planID := range s.reservations {
		planIDs = append(planIDs, planID)
	}
	sort.Strings(planIDs)
	for _, planID := range planIDs {
		reservation := s.reservations[planID]
		record := ReservationRecord{
			Plan:                  clonePlan(reservation.plan),
			AllocationIDs:         append([]string(nil), reservation.allocationIDs...),
			Finalized:             reservation.finalized,
			Succeeded:             reservation.succeeded,
			RetainedAllocationIDs: append([]string(nil), reservation.retainedAllocationIDs...),
		}
		for _, unit := range reservation.pendingUnits {
			record.PendingUnits = append(record.PendingUnits, clonePendingUnit(unit))
		}
		result.Reservations = append(result.Reservations, record)
	}
	return result
}

// Restore replaces Store authority from a previously validated durable state.
func (s *Store) Restore(durable DurableState) error {
	if durable.Snapshot == nil {
		return errors.New("state: restored snapshot is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshot = cloneSnapshot(durable.Snapshot)
	s.intents = make(map[intentKey]*tgsrlv1.SchedulingIntent, len(durable.Intents))
	s.providerProjection = newProviderProjectionState()
	s.idempotencyKeys = make(map[string]intentIdentity, len(durable.Intents))
	for _, intent := range durable.Intents {
		if err := validateIntentStructure(intent); err != nil {
			return fmt.Errorf("state: restore intent: %w", err)
		}
		key := intentKey{executionID: intent.GetExecutionId(), stageID: intent.GetStageId()}
		cloned := cloneIntent(intent)
		s.intents[key] = cloned
		s.idempotencyKeys[intent.GetIdempotencyKey()] = intentIdentity{
			key:     key,
			version: intent.GetVersion(),
			intent:  cloneIntent(intent),
		}
	}

	s.reservations = make(map[string]*planReservation, len(durable.Reservations))
	for _, record := range durable.Reservations {
		if record.Plan == nil || record.Plan.GetPlanId() == "" {
			return errors.New("state: restored reservation requires plan_id")
		}
		reservation := &planReservation{
			plan:                  clonePlan(record.Plan),
			allocationIDs:         append([]string(nil), record.AllocationIDs...),
			finalized:             record.Finalized,
			succeeded:             record.Succeeded,
			retainedAllocationIDs: append([]string(nil), record.RetainedAllocationIDs...),
		}
		for _, unit := range record.PendingUnits {
			reservation.pendingUnits = append(reservation.pendingUnits, clonePendingUnit(unit))
		}
		s.reservations[record.Plan.GetPlanId()] = reservation
	}
	s.signalRevisionLocked()
	return nil
}

func (s *Store) signalRevisionLocked() {
	close(s.revisionChanged)
	s.revisionChanged = make(chan struct{})
}

func (s *Store) commitSnapshotLocked(working *tgsrlv1.ClusterSnapshot) {
	working.Revision = s.snapshot.GetRevision() + 1
	working.SnapshotId = snapshotID(working.GetRevision())
	working.ObservedAt = timestamppb.New(s.clock.Now())
	s.snapshot = cloneSnapshot(working)
	s.signalRevisionLocked()
}
