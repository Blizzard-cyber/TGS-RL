package state

import (
	"errors"
	"fmt"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TransactionState string

const (
	TransactionStateUnknown       TransactionState = "unknown"
	TransactionStateProposed      TransactionState = "proposed"
	TransactionStateReserved      TransactionState = "reserved"
	TransactionStatePrepared      TransactionState = "prepared"
	TransactionStateApplying      TransactionState = "applying"
	TransactionStateCommitted     TransactionState = "committed"
	TransactionStatePrepareFailed TransactionState = "prepare_failed"
	TransactionStateApplyFailed   TransactionState = "apply_failed"
	TransactionStateCompensating  TransactionState = "compensating"
	TransactionStateAborted       TransactionState = "aborted"
	TransactionStateDegraded      TransactionState = "degraded"
)

type EffectStatus string

const (
	EffectStatusUnknown    EffectStatus = "unknown"
	EffectStatusPending    EffectStatus = "pending"
	EffectStatusSucceeded  EffectStatus = "succeeded"
	EffectStatusFailed     EffectStatus = "failed"
	EffectStatusRolledBack EffectStatus = "rolled_back"
	EffectStatusDegraded   EffectStatus = "degraded"
)

type FailureClass string

const (
	FailureClassUnknown        FailureClass = "unknown"
	FailureClassProvider       FailureClass = "provider"
	FailureClassCompensation   FailureClass = "compensation"
	FailureClassInfrastructure FailureClass = "infrastructure"
)

type TransactionStep struct {
	StepIndex             int
	ActionID              string
	IdempotencyKey        string
	Status                EffectStatus
	Revision              uint64
	ErrorCode             string
	ErrorMessage          string
	CompensationAttempted bool
	Compensated           bool
}

type Receipt struct {
	Phase            string
	ObservedRevision uint64
	Steps            []TransactionStep
	ErrorCode        string
	ErrorMessage     string
	PreparedAt       *timestamppb.Timestamp
	UpdatedAt        *timestamppb.Timestamp
}

type TransactionRecord struct {
	TransactionID string
	PlanID        string
	Plan          *tgsrlv1.PlacementPlan
	// Generation is the Store CAS revision. ProviderGeneration is a stable
	// execution epoch shared with the provider for this transaction lifetime.
	Generation            uint64
	ProviderGeneration    uint64
	State                 TransactionState
	ReservationHeld       bool
	Terminal              bool
	FailureClass          FailureClass
	FailureReason         string
	AffectedAllocationIDs []string
	ProviderReceipt       *Receipt
	CreatedAt             *timestamppb.Timestamp
	UpdatedAt             *timestamppb.Timestamp
	FinalizedAt           *timestamppb.Timestamp
	FinalizedSucceeded    bool
	RetainedAllocationIDs []string
}

func (r *TransactionRecord) terminalLocked() bool {
	if r == nil {
		return false
	}
	return r.Terminal || isTerminalTransactionState(r.State)
}

func isTerminalTransactionState(state TransactionState) bool {
	switch state {
	case TransactionStateCommitted,
		TransactionStateAborted,
		TransactionStateDegraded,
		TransactionStatePrepareFailed:
		return true
	default:
		return false
	}
}

func cloneTransactionStep(step TransactionStep) TransactionStep {
	return step
}

func cloneTransactionSteps(steps []TransactionStep) []TransactionStep {
	if len(steps) == 0 {
		return nil
	}
	out := make([]TransactionStep, 0, len(steps))
	for _, step := range steps {
		out = append(out, cloneTransactionStep(step))
	}
	return out
}

func cloneReceipt(receipt *Receipt) *Receipt {
	if receipt == nil {
		return nil
	}
	return &Receipt{
		Phase:            receipt.Phase,
		ObservedRevision: receipt.ObservedRevision,
		Steps:            cloneTransactionSteps(receipt.Steps),
		ErrorCode:        receipt.ErrorCode,
		ErrorMessage:     receipt.ErrorMessage,
		PreparedAt:       cloneTimestamp(receipt.PreparedAt),
		UpdatedAt:        cloneTimestamp(receipt.UpdatedAt),
	}
}

func cloneTransactionRecord(record *TransactionRecord) *TransactionRecord {
	if record == nil {
		return nil
	}
	return &TransactionRecord{
		TransactionID:         record.TransactionID,
		PlanID:                record.PlanID,
		Plan:                  clonePlan(record.Plan),
		Generation:            record.Generation,
		ProviderGeneration:    record.ProviderGeneration,
		State:                 record.State,
		ReservationHeld:       record.ReservationHeld,
		Terminal:              record.Terminal,
		FailureClass:          record.FailureClass,
		FailureReason:         record.FailureReason,
		AffectedAllocationIDs: append([]string(nil), record.AffectedAllocationIDs...),
		ProviderReceipt:       cloneReceipt(record.ProviderReceipt),
		CreatedAt:             cloneTimestamp(record.CreatedAt),
		UpdatedAt:             cloneTimestamp(record.UpdatedAt),
		FinalizedAt:           cloneTimestamp(record.FinalizedAt),
		FinalizedSucceeded:    record.FinalizedSucceeded,
		RetainedAllocationIDs: append([]string(nil), record.RetainedAllocationIDs...),
	}
}

// IsTerminal reports whether no further provider transition is allowed.
func (r *TransactionRecord) IsTerminal() bool {
	return r != nil && r.terminalLocked()
}

func validateTransactionRecord(record *TransactionRecord) error {
	if record == nil {
		return errors.New("state: transaction record is required")
	}
	if record.TransactionID == "" {
		return errors.New("state: transaction_id is required")
	}
	if record.Plan == nil || record.Plan.GetPlanId() == "" {
		return errors.New("state: transaction plan is required")
	}
	if record.PlanID == "" {
		return errors.New("state: transaction plan_id is required")
	}
	if record.PlanID != record.Plan.GetPlanId() {
		return errors.New("state: transaction plan_id must match plan.plan_id")
	}
	if record.Generation == 0 {
		return errors.New("state: transaction generation must be greater than zero")
	}
	if record.ProviderGeneration == 0 {
		return errors.New("state: transaction provider_generation must be greater than zero")
	}
	if record.State == TransactionStateUnknown {
		return errors.New("state: transaction state is required")
	}
	for _, allocationID := range record.AffectedAllocationIDs {
		if allocationID == "" {
			return errors.New("state: affected allocation IDs must be non-empty")
		}
	}
	return nil
}

func normalizeAllocationIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		set[id] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func affectedAllocationIDsForPlan(plan *tgsrlv1.PlacementPlan) []string {
	if plan == nil {
		return nil
	}
	affected := append([]string(nil), plan.GetAffectedAllocationIds()...)
	for _, binding := range plan.GetBindings() {
		if binding == nil || binding.GetBindingId() == "" {
			continue
		}
		affected = append(affected, allocationID(binding.GetBindingId()))
	}
	return normalizeAllocationIDs(affected)
}

func validateTransactionGeneration(actual, expected uint64) error {
	if actual != expected {
		return fmt.Errorf("%w: expected generation %d, current generation %d", ErrTransactionConflict, expected, actual)
	}
	return nil
}

func transactionStateAllowed(from, to TransactionState) bool {
	if from == to {
		return true
	}
	switch from {
	case TransactionStateProposed:
		return to == TransactionStateReserved || to == TransactionStatePrepared || to == TransactionStatePrepareFailed
	case TransactionStateReserved:
		return to == TransactionStatePrepared || to == TransactionStatePrepareFailed
	case TransactionStatePrepared:
		return to == TransactionStateApplying || to == TransactionStateCompensating
	case TransactionStateApplying:
		return to == TransactionStateCommitted || to == TransactionStateApplyFailed || to == TransactionStateCompensating || to == TransactionStateDegraded
	case TransactionStateApplyFailed:
		return to == TransactionStateCompensating || to == TransactionStateDegraded
	case TransactionStateCompensating:
		return to == TransactionStateAborted || to == TransactionStateDegraded
	default:
		return false
	}
}

func (s *Store) transactionOverlapLocked(transactionID string, allocationIDs []string) (string, bool) {
	for id, record := range s.transactions {
		if id == transactionID || record == nil || !record.ReservationHeld || record.terminalLocked() {
			continue
		}
		if overlapsAllocationIDs(record.AffectedAllocationIDs, allocationIDs) {
			return id, true
		}
	}
	return "", false
}

func overlapsAllocationIDs(left, right []string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(left))
	for _, id := range left {
		set[id] = struct{}{}
	}
	for _, id := range right {
		if _, ok := set[id]; ok {
			return true
		}
	}
	return false
}

func newTransactionRecord(plan *tgsrlv1.PlacementPlan, generation uint64, now *timestamppb.Timestamp) *TransactionRecord {
	return &TransactionRecord{
		TransactionID:         plan.GetPlanId(),
		PlanID:                plan.GetPlanId(),
		Plan:                  clonePlan(plan),
		Generation:            generation,
		ProviderGeneration:    1,
		State:                 TransactionStateProposed,
		AffectedAllocationIDs: affectedAllocationIDsForPlan(plan),
		CreatedAt:             cloneTimestamp(now),
		UpdatedAt:             cloneTimestamp(now),
	}
}

func (s *Store) CreateTransaction(plan *tgsrlv1.PlacementPlan, generation uint64) (*TransactionRecord, error) {
	if plan == nil || plan.GetPlanId() == "" {
		return nil, fmt.Errorf("%w: transaction plan_id is required", ErrInvalidIntent)
	}
	now := timestamppb.New(s.clock.Now())
	record := newTransactionRecord(plan, generation, now)
	if err := validateTransactionRecord(record); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.transactions[record.TransactionID]; ok {
		if protoPlanEqual(existing.Plan, record.Plan) && existing.Generation == generation {
			return cloneTransactionRecord(existing), nil
		}
		return nil, fmt.Errorf("%w: transaction %q already exists", ErrTransactionConflict, record.TransactionID)
	}
	s.transactions[record.TransactionID] = cloneTransactionRecord(record)
	return cloneTransactionRecord(record), nil
}

// BeginTransaction atomically creates a transaction and reserves its logical
// resources. A failed reservation leaves neither a transaction record nor a
// partial resource mutation behind.
func (s *Store) BeginTransaction(plan *tgsrlv1.PlacementPlan, generation uint64) (*TransactionRecord, *tgsrlv1.ClusterSnapshot, error) {
	if plan == nil || plan.GetPlanId() == "" {
		return nil, nil, fmt.Errorf("%w: transaction plan_id is required", ErrInvalidIntent)
	}
	record := newTransactionRecord(plan, generation, timestamppb.New(s.clock.Now()))
	if err := validateTransactionRecord(record); err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.transactions[record.TransactionID]; ok {
		if !protoPlanEqual(existing.Plan, record.Plan) {
			return nil, nil, fmt.Errorf("%w: transaction %q already exists", ErrTransactionConflict, record.TransactionID)
		}
		return cloneTransactionRecord(existing), cloneSnapshot(s.snapshot), nil
	}
	snapshot, err := s.reservePlanLocked(record.Plan)
	if err != nil && !errors.Is(err, ErrPlanAlreadyReserved) {
		return nil, nil, err
	}
	record.ReservationHeld = true
	record.State = TransactionStateReserved
	record.Generation++
	record.UpdatedAt = timestamppb.New(s.clock.Now())
	s.transactions[record.TransactionID] = cloneTransactionRecord(record)
	return cloneTransactionRecord(record), cloneSnapshot(snapshot), nil
}

func (s *Store) ReserveTransaction(transactionID string, expectedGeneration uint64) (*TransactionRecord, *tgsrlv1.ClusterSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.transactions[transactionID]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrTransactionNotFound, transactionID)
	}
	if err := validateTransactionGeneration(record.Generation, expectedGeneration); err != nil {
		return nil, nil, err
	}
	if record.terminalLocked() {
		return nil, nil, fmt.Errorf("%w: %s", ErrTransactionImmutable, transactionID)
	}
	if record.ReservationHeld {
		return cloneTransactionRecord(record), cloneSnapshot(s.snapshot), nil
	}
	if record.State != TransactionStateProposed {
		return nil, nil, fmt.Errorf("%w: transaction %q cannot reserve from %s", ErrTransactionConflict, transactionID, record.State)
	}
	if conflictedBy, ok := s.transactionOverlapLocked(transactionID, record.AffectedAllocationIDs); ok {
		return nil, nil, fmt.Errorf("%w: transaction %q overlaps %q", ErrTransactionLocked, transactionID, conflictedBy)
	}
	snapshot, err := s.reservePlanLocked(record.Plan)
	if err != nil && !errors.Is(err, ErrPlanAlreadyReserved) {
		return nil, nil, err
	}
	record.ReservationHeld = true
	record.State = TransactionStateReserved
	record.Generation++
	record.UpdatedAt = timestamppb.New(s.clock.Now())
	return cloneTransactionRecord(record), cloneSnapshot(snapshot), nil
}

type TransactionAdvanceRequest struct {
	TransactionID      string
	ExpectedGeneration uint64
	NextState          TransactionState
	FailureClass       FailureClass
	FailureReason      string
	ProviderReceipt    *Receipt
}

func (s *Store) AdvanceTransaction(request TransactionAdvanceRequest) (*TransactionRecord, error) {
	if request.TransactionID == "" {
		return nil, errors.New("state: transaction_id is required")
	}
	if request.NextState == TransactionStateUnknown {
		return nil, errors.New("state: next transaction state is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.transactions[request.TransactionID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransactionNotFound, request.TransactionID)
	}
	if err := validateTransactionGeneration(record.Generation, request.ExpectedGeneration); err != nil {
		return nil, err
	}
	if record.terminalLocked() {
		if record.State == request.NextState &&
			record.FailureClass == request.FailureClass &&
			record.FailureReason == request.FailureReason {
			return cloneTransactionRecord(record), nil
		}
		return nil, fmt.Errorf("%w: %s", ErrTransactionImmutable, request.TransactionID)
	}
	if !transactionStateAllowed(record.State, request.NextState) {
		return nil, fmt.Errorf("%w: cannot advance %s from %s to %s", ErrTransactionConflict, request.TransactionID, record.State, request.NextState)
	}
	if record.State == request.NextState &&
		record.FailureClass == request.FailureClass &&
		record.FailureReason == request.FailureReason &&
		request.ProviderReceipt == nil {
		return cloneTransactionRecord(record), nil
	}
	if record.ReservationHeld {
		if conflictedBy, ok := s.transactionOverlapLocked(request.TransactionID, record.AffectedAllocationIDs); ok {
			return nil, fmt.Errorf("%w: transaction %q overlaps %q", ErrTransactionLocked, request.TransactionID, conflictedBy)
		}
	}
	record.State = request.NextState
	record.FailureClass = request.FailureClass
	record.FailureReason = request.FailureReason
	if request.ProviderReceipt != nil {
		record.ProviderReceipt = cloneReceipt(request.ProviderReceipt)
	}
	record.Terminal = isTerminalTransactionState(record.State)
	record.Generation++
	record.UpdatedAt = timestamppb.New(s.clock.Now())
	return cloneTransactionRecord(record), nil
}

type TransactionFinalizeRequest struct {
	TransactionID      string
	ExpectedGeneration uint64
	Succeeded          bool
	Results            []*tgsrlv1.ActionResult
	FinalState         TransactionState
	FailureClass       FailureClass
	FailureReason      string
	ProviderReceipt    *Receipt
}

func (s *Store) FinalizeTransaction(request TransactionFinalizeRequest) (*TransactionRecord, *tgsrlv1.ClusterSnapshot, error) {
	if request.TransactionID == "" {
		return nil, nil, errors.New("state: transaction_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.transactions[request.TransactionID]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrTransactionNotFound, request.TransactionID)
	}
	if err := validateTransactionGeneration(record.Generation, request.ExpectedGeneration); err != nil {
		return nil, nil, err
	}
	if record.terminalLocked() {
		if record.FinalizedSucceeded == request.Succeeded && record.State == request.FinalState {
			return cloneTransactionRecord(record), cloneSnapshot(s.snapshot), nil
		}
		return nil, nil, fmt.Errorf("%w: %s", ErrTransactionImmutable, request.TransactionID)
	}
	if !record.ReservationHeld {
		return nil, nil, fmt.Errorf("%w: transaction %q has no reservation", ErrTransactionConflict, request.TransactionID)
	}
	if conflictedBy, ok := s.transactionOverlapLocked(request.TransactionID, record.AffectedAllocationIDs); ok {
		return nil, nil, fmt.Errorf("%w: transaction %q overlaps %q", ErrTransactionLocked, request.TransactionID, conflictedBy)
	}
	snapshot, err := s.finalizePlanResultsLocked(record.Plan, request.Succeeded, request.Results)
	if err != nil {
		return nil, nil, err
	}
	reservation := s.reservations[record.PlanID]
	record.ReservationHeld = false
	record.FinalizedSucceeded = request.Succeeded
	record.RetainedAllocationIDs = nil
	if reservation != nil {
		record.RetainedAllocationIDs = append(record.RetainedAllocationIDs, reservation.retainedAllocationIDs...)
	}
	record.State = request.FinalState
	if record.State == TransactionStateUnknown {
		if request.Succeeded {
			record.State = TransactionStateCommitted
		} else {
			record.State = TransactionStateAborted
			if len(record.RetainedAllocationIDs) > 0 {
				record.State = TransactionStateDegraded
			}
		}
	}
	record.Terminal = isTerminalTransactionState(record.State)
	record.FailureClass = request.FailureClass
	record.FailureReason = request.FailureReason
	if request.ProviderReceipt != nil {
		record.ProviderReceipt = cloneReceipt(request.ProviderReceipt)
	}
	now := timestamppb.New(s.clock.Now())
	record.Generation++
	record.UpdatedAt = cloneTimestamp(now)
	record.FinalizedAt = cloneTimestamp(now)
	return cloneTransactionRecord(record), cloneSnapshot(snapshot), nil
}

func (s *Store) GetTransaction(transactionID string) (*TransactionRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.transactions[transactionID]
	if !ok {
		return nil, false
	}
	return cloneTransactionRecord(record), true
}

func (s *Store) ListInFlightTransactions() []*TransactionRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.transactions))
	for key, record := range s.transactions {
		if record == nil || record.terminalLocked() {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]*TransactionRecord, 0, len(keys))
	for _, key := range keys {
		out = append(out, cloneTransactionRecord(s.transactions[key]))
	}
	return out
}

func protoPlanEqual(left, right *tgsrlv1.PlacementPlan) bool {
	switch {
	case left == nil && right == nil:
		return true
	case left == nil || right == nil:
		return false
	default:
		return proto.Equal(left, right)
	}
}
