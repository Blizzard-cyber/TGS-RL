package persistence

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/protection"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	rootstorage "github.com/Blizzard-cyber/TGS-RL/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrCorruptPayload = errors.New("scheduler persistence: corrupt protobuf payload")

// SchedulerState is the complete durable scheduler checkpoint. Provider
// projection fields are optional so checkpoints written before their addition
// continue to decode as empty projections and cursors.
type SchedulerState struct {
	Snapshot           *tgsrlv1.ClusterSnapshot
	Intents            map[string]*tgsrlv1.SchedulingIntent
	Decisions          []*tgsrlv1.DecisionRecord
	ActionResults      []*tgsrlv1.ActionResult
	Cursor             uint64
	Reservations       []ReservationRecord
	ProjectedSandboxes []*tgsrlv1.Sandbox
	ResourceCursors    map[string]state.ProviderResourceCursor
	SandboxCursors     map[string]state.ProviderSandboxCursor
	Protection         protection.State
}

// ReservationRecord is the durable representation of an in-flight or
// finalized Store reservation. Protobuf DTOs remain the wire-visible domain
// model; the extra booleans are persistence metadata only.
type ReservationRecord struct {
	Plan                  *tgsrlv1.PlacementPlan
	BeforeSnapshot        *tgsrlv1.ClusterSnapshot
	DeviceAllocatable     map[string]*tgsrlv1.ResourceVector
	PendingUnits          []*tgsrlv1.PendingUnit
	AllocationIDs         []string
	Finalized             bool
	Succeeded             bool
	RetainedAllocationIDs []string
}

const checkpointFormatVersion = 2

type checkpointEnvelope struct {
	FormatVersion      int                               `json:"format_version"`
	Snapshot           string                            `json:"snapshot,omitempty"`
	Intents            map[string]string                 `json:"intents,omitempty"`
	Decisions          []string                          `json:"decisions,omitempty"`
	ActionResults      []string                          `json:"action_results,omitempty"`
	Cursor             uint64                            `json:"cursor,omitempty"`
	Reservations       []reservationEnvelope             `json:"reservations,omitempty"`
	ProjectedSandboxes []string                          `json:"projected_sandboxes,omitempty"`
	ResourceCursors    map[string]resourceCursorEnvelope `json:"resource_cursors,omitempty"`
	SandboxCursors     map[string]sandboxCursorEnvelope  `json:"sandbox_cursors,omitempty"`
	Protection         protectionEnvelope                `json:"protection,omitempty"`
}

type resourceCursorEnvelope struct {
	Revision uint64 `json:"revision,omitempty"`
	EventID  string `json:"event_id,omitempty"`
}

type sandboxCursorEnvelope struct {
	Generation          uint64   `json:"generation,omitempty"`
	ProviderRevision    uint64   `json:"provider_revision,omitempty"`
	EventID             string   `json:"event_id,omitempty"`
	IdempotencyKey      string   `json:"idempotency_key,omitempty"`
	OccurredAt          string   `json:"occurred_at,omitempty"`
	SeenEventIDs        []string `json:"seen_event_ids,omitempty"`
	SeenIdempotencyKeys []string `json:"seen_idempotency_keys,omitempty"`
}

type reservationEnvelope struct {
	Plan                  string            `json:"plan"`
	BeforeSnapshot        string            `json:"before_snapshot,omitempty"`
	PendingUnits          []string          `json:"pending_units,omitempty"`
	AllocationIDs         []string          `json:"allocation_ids,omitempty"`
	DeviceAllocatable     map[string]string `json:"device_allocatable,omitempty"`
	Finalized             bool              `json:"finalized,omitempty"`
	Succeeded             bool              `json:"succeeded,omitempty"`
	RetainedAllocationIDs []string          `json:"retained_allocation_ids,omitempty"`
}

type protectionEnvelope struct {
	Entries map[string]protectionStateEnvelope `json:"entries,omitempty"`
}

type protectionStateEnvelope struct {
	LastAction    string  `json:"last_action,omitempty"`
	LastScore     float64 `json:"last_score,omitempty"`
	WindowStart   string  `json:"window_start,omitempty"`
	WindowCount   int     `json:"window_count,omitempty"`
	BreakerCount  int     `json:"breaker_count,omitempty"`
	BreakerOpened string  `json:"breaker_opened,omitempty"`
}

// SchedulerRepository is the integration surface for scheduler-facing durable
// state. The scheduler checkpoints a consistent view and rebuilds in-memory
// state on restart.
type SchedulerRepository interface {
	SaveCheckpoint(state SchedulerState) error
	Recover() (*SchedulerState, error)
}

type Repository struct {
	mu             sync.Mutex
	journal        *rootstorage.Journal
	checkpoint     *rootstorage.CheckpointStore
	replaceJournal func([]rootstorage.JournalRecord) error
}

func OpenRepository(root string) (*Repository, error) {
	base := filepath.Join(root, "scheduler")
	journal, err := rootstorage.OpenJournal(base, "state")
	if err != nil {
		return nil, err
	}
	checkpoint, err := rootstorage.OpenCheckpointStore(base, "state")
	if err != nil {
		return nil, err
	}
	return &Repository{
		journal:        journal,
		checkpoint:     checkpoint,
		replaceJournal: journal.Replace,
	}, nil
}

func (r *Repository) SaveCheckpoint(state SchedulerState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	records, err := checkpointJournalRecords(state)
	if err != nil {
		return err
	}
	if err := rootstorage.ValidateJournalReplacement(records); err != nil {
		return err
	}
	envelope, err := encodeCheckpoint(state)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("scheduler persistence: marshal checkpoint envelope: %w", err)
	}
	if err := r.checkpoint.Save(payload); err != nil {
		return err
	}
	// Decisions, action results, cursors, and reservations are committed in the
	// atomic checkpoint envelope. The journal remains a snapshot/intent replay
	// aid, avoiding a torn multi-record critical write.
	//
	// Journal refresh is auxiliary after the checkpoint is durable. Returning an
	// error here would violate the caller's rollback semantics because restart
	// recovery prefers the checkpoint over the journal.
	if err := r.replaceJournal(records); err != nil {
		return nil
	}
	return nil
}

func (r *Repository) Recover() (*SchedulerState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := &SchedulerState{Intents: make(map[string]*tgsrlv1.SchedulingIntent)}
	payload, err := r.checkpoint.Load()
	completeCheckpoint := false
	switch {
	case err == nil:
		if decodeErr := decodeCheckpoint(payload, state); decodeErr != nil {
			// Read the original v1 protobuf checkpoint format for compatibility.
			response := &tgsrlv1.GetSnapshotResponse{}
			if protoErr := proto.Unmarshal(payload, response); protoErr != nil || response.GetSnapshot() == nil {
				return nil, fmt.Errorf("%w: checkpoint: %v", ErrCorruptPayload, decodeErr)
			}
			state.Snapshot = cloneSnapshot(response.GetSnapshot())
		} else {
			completeCheckpoint = true
		}
	case errors.Is(err, rootstorage.ErrCheckpointNotFound):
	default:
		return nil, err
	}
	if completeCheckpoint {
		return state, nil
	}
	if err := r.journal.Replay(func(record rootstorage.JournalRecord) error {
		switch record.Kind {
		case "snapshot":
			snapshot := &tgsrlv1.ClusterSnapshot{}
			if err := proto.Unmarshal(record.Payload, snapshot); err != nil {
				return fmt.Errorf("%w: snapshot %s: %v", ErrCorruptPayload, record.Key, err)
			}
			state.Snapshot = cloneSnapshot(snapshot)
		case "intent":
			intent := &tgsrlv1.SchedulingIntent{}
			if err := proto.Unmarshal(record.Payload, intent); err != nil {
				return fmt.Errorf("%w: intent %s: %v", ErrCorruptPayload, record.Key, err)
			}
			state.Intents[intentKey(intent.GetExecutionId(), intent.GetStageId())] = cloneIntent(intent)
		case "decision":
			decision := &tgsrlv1.DecisionRecord{}
			if err := proto.Unmarshal(record.Payload, decision); err != nil {
				return fmt.Errorf("%w: decision %s: %v", ErrCorruptPayload, record.Key, err)
			}
			state.Decisions = upsertDecision(state.Decisions, decision)
		case "action_result":
			result := &tgsrlv1.ActionResult{}
			if err := proto.Unmarshal(record.Payload, result); err != nil {
				return fmt.Errorf("%w: action result %s: %v", ErrCorruptPayload, record.Key, err)
			}
			state.ActionResults = upsertActionResult(state.ActionResults, result)
		case "cursor":
			metadata := struct {
				Cursor uint64 `json:"cursor"`
			}{}
			if err := json.Unmarshal(record.Payload, &metadata); err != nil {
				return fmt.Errorf("%w: cursor: %v", ErrCorruptPayload, err)
			}
			state.Cursor = metadata.Cursor
		default:
			return fmt.Errorf("scheduler persistence: unknown journal record kind %q", record.Kind)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return state, nil
}

func (r *Repository) JournalPath() string {
	return r.journal.Path()
}

func checkpointJournalRecords(state SchedulerState) ([]rootstorage.JournalRecord, error) {
	records := make([]rootstorage.JournalRecord, 0, len(state.Intents)+1)
	if state.Snapshot != nil {
		wire, err := marshalProto(state.Snapshot)
		if err != nil {
			return nil, err
		}
		records = append(records, rootstorage.JournalRecord{
			Kind:    "snapshot",
			Key:     state.Snapshot.GetSnapshotId(),
			Payload: wire,
		})
	}
	keys := make([]string, 0, len(state.Intents))
	for key := range state.Intents {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		wire, err := marshalProto(state.Intents[key])
		if err != nil {
			return nil, err
		}
		records = append(records, rootstorage.JournalRecord{
			Kind:    "intent",
			Key:     key,
			Payload: wire,
		})
	}
	return records, nil
}

func encodeCheckpoint(state SchedulerState) (checkpointEnvelope, error) {
	envelope := checkpointEnvelope{
		FormatVersion:   checkpointFormatVersion,
		Intents:         make(map[string]string),
		Cursor:          state.Cursor,
		ResourceCursors: make(map[string]resourceCursorEnvelope, len(state.ResourceCursors)),
		SandboxCursors:  make(map[string]sandboxCursorEnvelope, len(state.SandboxCursors)),
	}
	var err error
	if state.Snapshot != nil {
		envelope.Snapshot, err = encodeProto(state.Snapshot)
		if err != nil {
			return checkpointEnvelope{}, err
		}
	}
	for key, intent := range state.Intents {
		envelope.Intents[key], err = encodeProto(intent)
		if err != nil {
			return checkpointEnvelope{}, err
		}
	}
	for _, decision := range state.Decisions {
		encoded, encodeErr := encodeProto(decision)
		if encodeErr != nil {
			return checkpointEnvelope{}, encodeErr
		}
		envelope.Decisions = append(envelope.Decisions, encoded)
	}
	for _, result := range state.ActionResults {
		encoded, encodeErr := encodeProto(result)
		if encodeErr != nil {
			return checkpointEnvelope{}, encodeErr
		}
		envelope.ActionResults = append(envelope.ActionResults, encoded)
	}
	for _, sandbox := range state.ProjectedSandboxes {
		encoded, encodeErr := encodeProto(sandbox)
		if encodeErr != nil {
			return checkpointEnvelope{}, encodeErr
		}
		envelope.ProjectedSandboxes = append(envelope.ProjectedSandboxes, encoded)
	}
	for providerID, cursor := range state.ResourceCursors {
		envelope.ResourceCursors[providerID] = resourceCursorEnvelope{
			Revision: cursor.Revision,
			EventID:  cursor.EventID,
		}
	}
	for sandboxID, cursor := range state.SandboxCursors {
		encodedCursor := sandboxCursorEnvelope{
			Generation:          cursor.Generation,
			ProviderRevision:    cursor.ProviderRevision,
			EventID:             cursor.EventID,
			IdempotencyKey:      cursor.IdempotencyKey,
			SeenEventIDs:        append([]string(nil), cursor.SeenEventIDs...),
			SeenIdempotencyKeys: append([]string(nil), cursor.SeenIdempotencyKeys...),
		}
		if cursor.OccurredAt != nil {
			encodedCursor.OccurredAt, err = encodeProto(cursor.OccurredAt)
			if err != nil {
				return checkpointEnvelope{}, err
			}
		}
		envelope.SandboxCursors[sandboxID] = encodedCursor
	}
	if len(state.Protection.Entries) > 0 {
		envelope.Protection.Entries = make(map[string]protectionStateEnvelope, len(state.Protection.Entries))
		for key, entry := range state.Protection.Entries {
			encoded := protectionStateEnvelope{
				LastScore:    entry.LastScore,
				WindowCount:  entry.WindowCount,
				BreakerCount: entry.BreakerCount,
			}
			if !entry.LastAction.IsZero() {
				encoded.LastAction = entry.LastAction.UTC().Format(time.RFC3339Nano)
			}
			if !entry.WindowStart.IsZero() {
				encoded.WindowStart = entry.WindowStart.UTC().Format(time.RFC3339Nano)
			}
			if !entry.BreakerOpened.IsZero() {
				encoded.BreakerOpened = entry.BreakerOpened.UTC().Format(time.RFC3339Nano)
			}
			envelope.Protection.Entries[key] = encoded
		}
	}
	for _, reservation := range state.Reservations {
		encodedPlan, encodeErr := encodeProto(reservation.Plan)
		if encodeErr != nil {
			return checkpointEnvelope{}, encodeErr
		}
		encoded := reservationEnvelope{
			Plan:                  encodedPlan,
			AllocationIDs:         append([]string(nil), reservation.AllocationIDs...),
			DeviceAllocatable:     make(map[string]string, len(reservation.DeviceAllocatable)),
			Finalized:             reservation.Finalized,
			Succeeded:             reservation.Succeeded,
			RetainedAllocationIDs: append([]string(nil), reservation.RetainedAllocationIDs...),
		}
		if reservation.BeforeSnapshot != nil {
			encoded.BeforeSnapshot, encodeErr = encodeProto(reservation.BeforeSnapshot)
			if encodeErr != nil {
				return checkpointEnvelope{}, encodeErr
			}
		}
		for deviceID, vector := range reservation.DeviceAllocatable {
			encodedVector, vectorErr := encodeProto(vector)
			if vectorErr != nil {
				return checkpointEnvelope{}, vectorErr
			}
			encoded.DeviceAllocatable[deviceID] = encodedVector
		}
		for _, unit := range reservation.PendingUnits {
			encodedUnit, unitErr := encodeProto(unit)
			if unitErr != nil {
				return checkpointEnvelope{}, unitErr
			}
			encoded.PendingUnits = append(encoded.PendingUnits, encodedUnit)
		}
		envelope.Reservations = append(envelope.Reservations, encoded)
	}
	return envelope, nil
}

func decodeCheckpoint(payload []byte, recovered *SchedulerState) error {
	envelope := checkpointEnvelope{}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return err
	}
	if envelope.FormatVersion != checkpointFormatVersion {
		return fmt.Errorf("unsupported format version %d", envelope.FormatVersion)
	}
	if envelope.Snapshot != "" {
		recovered.Snapshot = &tgsrlv1.ClusterSnapshot{}
		if err := decodeProto(envelope.Snapshot, recovered.Snapshot); err != nil {
			return err
		}
	}
	recovered.Intents = make(map[string]*tgsrlv1.SchedulingIntent, len(envelope.Intents))
	for key, encoded := range envelope.Intents {
		intent := &tgsrlv1.SchedulingIntent{}
		if err := decodeProto(encoded, intent); err != nil {
			return err
		}
		recovered.Intents[key] = intent
	}
	for _, encoded := range envelope.Decisions {
		decision := &tgsrlv1.DecisionRecord{}
		if err := decodeProto(encoded, decision); err != nil {
			return err
		}
		recovered.Decisions = append(recovered.Decisions, decision)
	}
	for _, encoded := range envelope.ActionResults {
		result := &tgsrlv1.ActionResult{}
		if err := decodeProto(encoded, result); err != nil {
			return err
		}
		recovered.ActionResults = append(recovered.ActionResults, result)
	}
	for _, encoded := range envelope.ProjectedSandboxes {
		sandbox := &tgsrlv1.Sandbox{}
		if err := decodeProto(encoded, sandbox); err != nil {
			return err
		}
		recovered.ProjectedSandboxes = append(recovered.ProjectedSandboxes, sandbox)
	}
	recovered.ResourceCursors = make(map[string]state.ProviderResourceCursor, len(envelope.ResourceCursors))
	for providerID, cursor := range envelope.ResourceCursors {
		recovered.ResourceCursors[providerID] = state.ProviderResourceCursor{
			Revision: cursor.Revision,
			EventID:  cursor.EventID,
		}
	}
	recovered.SandboxCursors = make(map[string]state.ProviderSandboxCursor, len(envelope.SandboxCursors))
	for sandboxID, cursor := range envelope.SandboxCursors {
		decodedCursor := state.ProviderSandboxCursor{
			Generation:          cursor.Generation,
			ProviderRevision:    cursor.ProviderRevision,
			EventID:             cursor.EventID,
			IdempotencyKey:      cursor.IdempotencyKey,
			SeenEventIDs:        append([]string(nil), cursor.SeenEventIDs...),
			SeenIdempotencyKeys: append([]string(nil), cursor.SeenIdempotencyKeys...),
		}
		if cursor.OccurredAt != "" {
			decodedCursor.OccurredAt = &timestamppb.Timestamp{}
			if err := decodeProto(cursor.OccurredAt, decodedCursor.OccurredAt); err != nil {
				return err
			}
		}
		recovered.SandboxCursors[sandboxID] = decodedCursor
	}
	if len(envelope.Protection.Entries) > 0 {
		recovered.Protection.Entries = make(map[string]protection.StateEntry, len(envelope.Protection.Entries))
		for key, encoded := range envelope.Protection.Entries {
			entry := protection.StateEntry{
				LastScore:    encoded.LastScore,
				WindowCount:  encoded.WindowCount,
				BreakerCount: encoded.BreakerCount,
			}
			if encoded.LastAction != "" {
				parsed, err := time.Parse(time.RFC3339Nano, encoded.LastAction)
				if err != nil {
					return fmt.Errorf("%w: protection last_action: %v", ErrCorruptPayload, err)
				}
				entry.LastAction = parsed
			}
			if encoded.WindowStart != "" {
				parsed, err := time.Parse(time.RFC3339Nano, encoded.WindowStart)
				if err != nil {
					return fmt.Errorf("%w: protection window_start: %v", ErrCorruptPayload, err)
				}
				entry.WindowStart = parsed
			}
			if encoded.BreakerOpened != "" {
				parsed, err := time.Parse(time.RFC3339Nano, encoded.BreakerOpened)
				if err != nil {
					return fmt.Errorf("%w: protection breaker_opened: %v", ErrCorruptPayload, err)
				}
				entry.BreakerOpened = parsed
			}
			recovered.Protection.Entries[key] = entry
		}
	}
	recovered.Cursor = envelope.Cursor
	for _, encoded := range envelope.Reservations {
		record := ReservationRecord{
			AllocationIDs:         append([]string(nil), encoded.AllocationIDs...),
			DeviceAllocatable:     make(map[string]*tgsrlv1.ResourceVector, len(encoded.DeviceAllocatable)),
			Finalized:             encoded.Finalized,
			Succeeded:             encoded.Succeeded,
			RetainedAllocationIDs: append([]string(nil), encoded.RetainedAllocationIDs...),
		}
		record.Plan = &tgsrlv1.PlacementPlan{}
		if err := decodeProto(encoded.Plan, record.Plan); err != nil {
			return err
		}
		if encoded.BeforeSnapshot != "" {
			record.BeforeSnapshot = &tgsrlv1.ClusterSnapshot{}
			if err := decodeProto(encoded.BeforeSnapshot, record.BeforeSnapshot); err != nil {
				return err
			}
		}
		for deviceID, encodedVector := range encoded.DeviceAllocatable {
			vector := &tgsrlv1.ResourceVector{}
			if err := decodeProto(encodedVector, vector); err != nil {
				return err
			}
			record.DeviceAllocatable[deviceID] = vector
		}
		for _, encodedUnit := range encoded.PendingUnits {
			unit := &tgsrlv1.PendingUnit{}
			if err := decodeProto(encodedUnit, unit); err != nil {
				return err
			}
			record.PendingUnits = append(record.PendingUnits, unit)
		}
		recovered.Reservations = append(recovered.Reservations, record)
	}
	return nil
}

func encodeProto(message proto.Message) (string, error) {
	wire, err := marshalProto(message)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(wire), nil
}

func decodeProto(encoded string, message proto.Message) error {
	wire, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("%w: base64: %v", ErrCorruptPayload, err)
	}
	if err := proto.Unmarshal(wire, message); err != nil {
		return fmt.Errorf("%w: protobuf: %v", ErrCorruptPayload, err)
	}
	return nil
}

func upsertDecision(decisions []*tgsrlv1.DecisionRecord, incoming *tgsrlv1.DecisionRecord) []*tgsrlv1.DecisionRecord {
	for index, decision := range decisions {
		if decision.GetDecisionId() == incoming.GetDecisionId() {
			decisions[index] = proto.Clone(incoming).(*tgsrlv1.DecisionRecord)
			return decisions
		}
	}
	return append(decisions, proto.Clone(incoming).(*tgsrlv1.DecisionRecord))
}

func upsertActionResult(results []*tgsrlv1.ActionResult, incoming *tgsrlv1.ActionResult) []*tgsrlv1.ActionResult {
	for index, result := range results {
		if result.GetPlanId() == incoming.GetPlanId() && result.GetActionId() == incoming.GetActionId() {
			results[index] = proto.Clone(incoming).(*tgsrlv1.ActionResult)
			return results
		}
	}
	return append(results, proto.Clone(incoming).(*tgsrlv1.ActionResult))
}

func marshalProto(message proto.Message) ([]byte, error) {
	if message == nil {
		return nil, errors.New("scheduler persistence: nil protobuf message")
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("scheduler persistence: marshal protobuf: %w", err)
	}
	return wire, nil
}

func intentKey(executionID, stageID string) string {
	return executionID + "\x00" + stageID
}

func cloneSnapshot(snapshot *tgsrlv1.ClusterSnapshot) *tgsrlv1.ClusterSnapshot {
	if snapshot == nil {
		return nil
	}
	return proto.Clone(snapshot).(*tgsrlv1.ClusterSnapshot)
}

func cloneIntent(intent *tgsrlv1.SchedulingIntent) *tgsrlv1.SchedulingIntent {
	if intent == nil {
		return nil
	}
	return proto.Clone(intent).(*tgsrlv1.SchedulingIntent)
}

func cloneResourceVector(vector *tgsrlv1.ResourceVector) *tgsrlv1.ResourceVector {
	if vector == nil {
		return nil
	}
	return proto.Clone(vector).(*tgsrlv1.ResourceVector)
}

func cloneResourceVectorMap(in map[string]*tgsrlv1.ResourceVector) map[string]*tgsrlv1.ResourceVector {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*tgsrlv1.ResourceVector, len(in))
	for key, value := range in {
		out[key] = cloneResourceVector(value)
	}
	return out
}

func IsCorruption(err error) bool {
	return errors.Is(err, rootstorage.ErrCorruptJournal) ||
		errors.Is(err, rootstorage.ErrCorruptRecord) ||
		errors.Is(err, ErrCorruptPayload)
}

func TruncateTail(path string, bytesToKeep int64) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Truncate(bytesToKeep); err != nil {
		return err
	}
	return file.Sync()
}

func SplitIntentKey(key string) (string, string, bool) {
	before, after, ok := strings.Cut(key, "\x00")
	return before, after, ok
}
