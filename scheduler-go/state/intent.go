package state

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PreparedIntentPublication struct {
	store    *Store
	response *tgsrlv1.PublishIntentResponse
	staged   storeDurableState
}

func (p *PreparedIntentPublication) Response() *tgsrlv1.PublishIntentResponse {
	if p == nil || p.response == nil {
		return nil
	}
	return proto.Clone(p.response).(*tgsrlv1.PublishIntentResponse)
}

func (p *PreparedIntentPublication) Commit() {
	if p == nil || p.store == nil {
		return
	}
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	p.store.restoreDurableStateLocked(p.staged)
	p.store.signalRevisionLocked()
	p.store = nil
}

func (p *PreparedIntentPublication) ExportDurableState() DurableState {
	if p == nil {
		return DurableState{}
	}
	result := DurableState{Snapshot: cloneSnapshot(p.staged.snapshot)}
	keys := make([]intentKey, 0, len(p.staged.intents))
	for key := range p.staged.intents {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].executionID != keys[j].executionID {
			return keys[i].executionID < keys[j].executionID
		}
		return keys[i].stageID < keys[j].stageID
	})
	for _, key := range keys {
		result.Intents = append(result.Intents, cloneIntent(p.staged.intents[key]))
	}
	exportProviderProjection(&result, p.staged.providerProjection)
	planIDs := make([]string, 0, len(p.staged.reservations))
	for planID := range p.staged.reservations {
		planIDs = append(planIDs, planID)
	}
	sort.Strings(planIDs)
	for _, planID := range planIDs {
		reservation := p.staged.reservations[planID]
		record := ReservationRecord{
			Plan:                  clonePlan(reservation.plan),
			BeforeSnapshot:        cloneSnapshot(reservation.beforeSnapshot),
			DeviceAllocatable:     cloneResourceVectorMap(reservation.deviceAllocatable),
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
	transactionIDs := make([]string, 0, len(p.staged.transactions))
	for transactionID := range p.staged.transactions {
		transactionIDs = append(transactionIDs, transactionID)
	}
	sort.Strings(transactionIDs)
	for _, transactionID := range transactionIDs {
		result.Transactions = append(result.Transactions, *cloneTransactionRecord(p.staged.transactions[transactionID]))
	}
	return result
}

// PublishIntent validates and atomically publishes a SchedulingIntent. An
// accepted intent advances the snapshot revision and replaces pending units for
// its execution/stage. An identical retry of the current version is deduplicated
// without advancing state. All rejected publishes also leave state unchanged.
func (s *Store) PublishIntent(intent *tgsrlv1.SchedulingIntent) (*tgsrlv1.PublishIntentResponse, error) {
	prepared, response, err := s.PreparePublishIntent(intent)
	if prepared != nil {
		prepared.Commit()
		return response, nil
	}
	return response, err
}

// PreparePublishIntent validates and stages a SchedulingIntent publication
// without exposing the new state or notifying revision waiters. Callers must
// commit only after the matching durable checkpoint succeeds.
func (s *Store) PreparePublishIntent(intent *tgsrlv1.SchedulingIntent) (*PreparedIntentPublication, *tgsrlv1.PublishIntentResponse, error) {
	incoming := cloneIntent(intent)
	if err := validateIntentStructure(incoming); err != nil {
		response := rejectedIntentResponse(incoming, err)
		return nil, response, err
	}

	key := intentKey{executionID: incoming.GetExecutionId(), stageID: incoming.GetStageId()}
	identity := intentIdentity{key: key, version: incoming.GetVersion(), intent: cloneIntent(incoming)}

	s.mu.Lock()
	defer s.mu.Unlock()

	if current, ok := s.intents[key]; ok {
		switch {
		case incoming.GetVersion() < current.GetVersion():
			if previous, exists := s.idempotencyKeys[incoming.GetIdempotencyKey()]; exists && !proto.Equal(incoming, previous.intent) {
				err := fmt.Errorf("%w: %q was reused with different content", ErrIdempotencyConflict, incoming.GetIdempotencyKey())
				response := rejectedIntentResponse(incoming, err)
				return nil, response, err
			}
			err := fmt.Errorf("%w: version %d is older than current version %d", ErrStaleIntent, incoming.GetVersion(), current.GetVersion())
			response := rejectedIntentResponse(incoming, err)
			return nil, response, err
		case incoming.GetVersion() == current.GetVersion():
			if proto.Equal(incoming, current) {
				response := intentResponse(incoming, tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_DEDUPLICATED, "identical intent already accepted")
				return nil, response, nil
			}
			if _, exists := s.idempotencyKeys[incoming.GetIdempotencyKey()]; exists {
				err := fmt.Errorf("%w: %q was reused with different content", ErrIdempotencyConflict, incoming.GetIdempotencyKey())
				response := rejectedIntentResponse(incoming, err)
				return nil, response, err
			}
			err := fmt.Errorf("%w: version %d is immutable", ErrIntentConflict, incoming.GetVersion())
			response := rejectedIntentResponse(incoming, err)
			return nil, response, err
		}
	}
	if previous, ok := s.idempotencyKeys[incoming.GetIdempotencyKey()]; ok {
		err := fmt.Errorf("%w: %q belongs to %s/%s version %d with different content", ErrIdempotencyConflict, incoming.GetIdempotencyKey(), previous.key.executionID, previous.key.stageID, previous.version)
		response := rejectedIntentResponse(incoming, err)
		return nil, response, err
	}

	now := s.clock.Now()
	if !incoming.GetValidUntil().AsTime().After(now) {
		err := fmt.Errorf("%w: valid_until %s is not in the future", ErrIntentExpired, incoming.GetValidUntil().AsTime().Format(time.RFC3339Nano))
		response := rejectedIntentResponse(incoming, err)
		return nil, response, err
	}
	if s.snapshot.GetRevision() == math.MaxUint64 {
		response := rejectedIntentResponse(incoming, ErrRevisionExhausted)
		return nil, response, ErrRevisionExhausted
	}

	working := cloneSnapshot(s.snapshot)
	for _, allocation := range working.GetAllocations() {
		if allocation.GetExecutionId() != incoming.GetExecutionId() ||
			allocation.GetStageId() != incoming.GetStageId() {
			continue
		}
		if allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING &&
			allocation.GetState() != tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE {
			continue
		}
		// Desired-state intent publication carries forward existing live
		// allocations even when resources, capabilities, or priority drift.
		// We only advance the observed intent version here; explicit mutation
		// plans remain responsible for materializing semantic changes.
		allocation.IntentVersion = incoming.GetVersion()
	}

	liveCount := liveAllocationCount(working.GetAllocations(), incoming)
	pendingCount := uint32(0)
	if liveCount < incoming.GetUnitCount() {
		pendingCount = incoming.GetUnitCount() - liveCount
	}
	working.PendingUnits = replacePendingUnits(working.GetPendingUnits(), incoming, pendingCount, now)
	working.Revision++
	working.SnapshotId = snapshotID(working.Revision)
	working.ObservedAt = timestamppb.New(now)

	stored := cloneIntent(incoming)
	staged := s.exportDurableStateLocked()
	staged.intents[key] = stored
	staged.idempotencyKeys[incoming.GetIdempotencyKey()] = identity
	staged.snapshot = cloneSnapshot(working)

	response := intentResponse(incoming, tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_ACCEPTED, "intent accepted")
	return &PreparedIntentPublication{
		store:    s,
		response: response,
		staged:   staged,
	}, response, nil
}

// LatestValidIntent returns a clone of the latest accepted intent for an
// execution/stage only while that version remains valid. Superseded versions
// never become current again after a newer version expires.
func (s *Store) LatestValidIntent(executionID, stageID string) (*tgsrlv1.SchedulingIntent, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	intent, ok := s.intents[intentKey{executionID: executionID, stageID: stageID}]
	now := s.clock.Now()
	if !ok || !intent.GetValidUntil().AsTime().After(now) {
		return nil, false
	}
	return cloneIntent(intent), true
}

// LatestIntent returns the latest accepted intent even if it has expired.
// Callers that may execute actions must prefer LatestValidIntent.
func (s *Store) LatestIntent(executionID, stageID string) (*tgsrlv1.SchedulingIntent, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	intent, ok := s.intents[intentKey{executionID: executionID, stageID: stageID}]
	if !ok {
		return nil, false
	}
	return cloneIntent(intent), true
}

func validateIntentStructure(intent *tgsrlv1.SchedulingIntent) error {
	if intent == nil {
		return fmt.Errorf("%w: intent is nil", ErrInvalidIntent)
	}
	if intent.GetExecutionId() == "" {
		return fmt.Errorf("%w: execution_id is required", ErrInvalidIntent)
	}
	if intent.GetStageId() == "" {
		return fmt.Errorf("%w: stage_id is required", ErrInvalidIntent)
	}
	if intent.GetVersion() == 0 {
		return fmt.Errorf("%w: version must be greater than zero", ErrInvalidIntent)
	}
	if intent.GetIdempotencyKey() == "" {
		return fmt.Errorf("%w: idempotency_key is required", ErrInvalidIntent)
	}
	if intent.GetJobId() == "" {
		return fmt.Errorf("%w: job_id is required", ErrInvalidIntent)
	}
	if intent.GetPolicyVersion() == "" {
		return fmt.Errorf("%w: policy_version is required", ErrInvalidIntent)
	}
	if intent.GetUnitCount() == 0 {
		return fmt.Errorf("%w: unit_count must be greater than zero", ErrInvalidIntent)
	}
	if intent.GetResourcesPerUnit() == nil {
		return fmt.Errorf("%w: resources_per_unit is required", ErrInvalidIntent)
	}
	if intent.GetResourcesPerUnit().GetCpuMillis() == 0 &&
		intent.GetResourcesPerUnit().GetMemoryBytes() == 0 &&
		intent.GetResourcesPerUnit().GetAcceleratorUnits() == 0 &&
		intent.GetResourcesPerUnit().GetEphemeralStorageBytes() == 0 &&
		intent.GetResourcesPerUnit().GetNetworkBandwidthBps() == 0 {
		return fmt.Errorf("%w: resources_per_unit must contain positive demand", ErrInvalidIntent)
	}
	if math.IsNaN(intent.GetResourcesPerUnit().GetAcceleratorUnits()) ||
		math.IsInf(intent.GetResourcesPerUnit().GetAcceleratorUnits(), 0) ||
		intent.GetResourcesPerUnit().GetAcceleratorUnits() < 0 {
		return fmt.Errorf("%w: accelerator_units must be finite and non-negative", ErrInvalidIntent)
	}
	if intent.GetSubmittedAt() == nil {
		return fmt.Errorf("%w: submitted_at is required", ErrInvalidIntent)
	}
	if err := intent.GetSubmittedAt().CheckValid(); err != nil {
		return fmt.Errorf("%w: invalid submitted_at: %v", ErrInvalidIntent, err)
	}
	if intent.GetTtl() == nil {
		return fmt.Errorf("%w: ttl is required", ErrInvalidIntent)
	}
	if err := intent.GetTtl().CheckValid(); err != nil {
		return fmt.Errorf("%w: invalid ttl: %v", ErrInvalidIntent, err)
	}
	if intent.GetTtl().AsDuration() <= 0 {
		return fmt.Errorf("%w: ttl must be positive", ErrInvalidIntent)
	}
	if intent.GetValidUntil() == nil {
		return fmt.Errorf("%w: valid_until is required", ErrInvalidIntent)
	}
	if err := intent.GetValidUntil().CheckValid(); err != nil {
		return fmt.Errorf("%w: invalid valid_until: %v", ErrInvalidIntent, err)
	}

	expectedValidUntil := addTimestampAndDuration(intent.GetSubmittedAt(), intent.GetTtl().GetSeconds(), intent.GetTtl().GetNanos())
	if err := expectedValidUntil.CheckValid(); err != nil {
		return fmt.Errorf("%w: submitted_at + ttl is invalid: %v", ErrInvalidIntent, err)
	}
	if !proto.Equal(intent.GetValidUntil(), expectedValidUntil) {
		return fmt.Errorf("%w: valid_until must equal submitted_at + ttl", ErrInvalidIntent)
	}
	return nil
}

func addTimestampAndDuration(timestamp *timestamppb.Timestamp, seconds int64, nanos int32) *timestamppb.Timestamp {
	resultSeconds := timestamp.GetSeconds() + seconds
	resultNanos := int64(timestamp.GetNanos()) + int64(nanos)
	if resultNanos >= int64(time.Second) {
		resultSeconds++
		resultNanos -= int64(time.Second)
	}
	return &timestamppb.Timestamp{Seconds: resultSeconds, Nanos: int32(resultNanos)}
}

func replacePendingUnits(existing []*tgsrlv1.PendingUnit, intent *tgsrlv1.SchedulingIntent, count uint32, queuedAt time.Time) []*tgsrlv1.PendingUnit {
	pending := make([]*tgsrlv1.PendingUnit, 0, len(existing)+int(count))
	for _, unit := range existing {
		if unit.GetExecutionId() == intent.GetExecutionId() && unit.GetStageId() == intent.GetStageId() {
			continue
		}
		pending = append(pending, clonePendingUnit(unit))
	}
	for index := uint32(0); index < count; index++ {
		pending = append(pending, &tgsrlv1.PendingUnit{
			PendingUnitId:        pendingUnitID(intent, index),
			ExecutionId:          intent.GetExecutionId(),
			StageId:              intent.GetStageId(),
			IntentVersion:        intent.GetVersion(),
			JobId:                intent.GetJobId(),
			RequestedResources:   cloneResourceVector(intent.GetResourcesPerUnit()),
			RequiredCapabilities: cloneCapabilitySet(intent.GetRequiredCapabilities()),
			Priority:             intent.GetPriority(),
			QueuedAt:             timestamppb.New(queuedAt),
			RunId:                intent.GetRunId(),
			TraceId:              intent.GetTraceId(),
			DataKind:             intent.GetDataKind(),
			RuntimeUnitId:        intent.GetLabels()["runtime_unit_id"],
		})
	}
	sortPendingUnits(pending)
	return pending
}

func sortPendingUnits(pending []*tgsrlv1.PendingUnit) {
	sort.SliceStable(pending, func(left, right int) bool {
		if pending[left].GetPriority() != pending[right].GetPriority() {
			return pending[left].GetPriority() > pending[right].GetPriority()
		}
		leftQueued := pending[left].GetQueuedAt().AsTime()
		rightQueued := pending[right].GetQueuedAt().AsTime()
		if !leftQueued.Equal(rightQueued) {
			return leftQueued.Before(rightQueued)
		}
		return pending[left].GetPendingUnitId() < pending[right].GetPendingUnitId()
	})
}

func liveAllocationCount(allocations []*tgsrlv1.Allocation, intent *tgsrlv1.SchedulingIntent) uint32 {
	var count uint32
	for _, allocation := range allocations {
		if allocation.GetExecutionId() == intent.GetExecutionId() &&
			allocation.GetStageId() == intent.GetStageId() &&
			(allocation.GetState() == tgsrlv1.AllocationState_ALLOCATION_STATE_PENDING ||
				allocation.GetState() == tgsrlv1.AllocationState_ALLOCATION_STATE_ACTIVE) {
			count++
		}
	}
	return count
}

func pendingUnitID(intent *tgsrlv1.SchedulingIntent, index uint32) string {
	return stableID(
		"pending",
		intent.GetExecutionId(),
		intent.GetStageId(),
		strconv.FormatUint(intent.GetVersion(), 10),
		strconv.FormatUint(uint64(index), 10),
	)
}

func intentResponse(intent *tgsrlv1.SchedulingIntent, status tgsrlv1.IntentPublishStatus, detail string) *tgsrlv1.PublishIntentResponse {
	response := &tgsrlv1.PublishIntentResponse{Status: status, Detail: detail}
	if intent != nil {
		response.ExecutionId = intent.GetExecutionId()
		response.StageId = intent.GetStageId()
		response.Version = intent.GetVersion()
	}
	return response
}

func rejectedIntentResponse(intent *tgsrlv1.SchedulingIntent, err error) *tgsrlv1.PublishIntentResponse {
	detail := "intent rejected"
	if err != nil {
		detail = err.Error()
	}
	return intentResponse(intent, tgsrlv1.IntentPublishStatus_INTENT_PUBLISH_STATUS_REJECTED, detail)
}
