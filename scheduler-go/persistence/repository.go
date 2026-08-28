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

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	rootstorage "github.com/Blizzard-cyber/TGS-RL/storage"
	"google.golang.org/protobuf/proto"
)

var ErrCorruptPayload = errors.New("scheduler persistence: corrupt protobuf payload")

type SchedulerState struct {
	Snapshot      *tgsrlv1.ClusterSnapshot
	Intents       map[string]*tgsrlv1.SchedulingIntent
	Decisions     []*tgsrlv1.DecisionRecord
	ActionResults []*tgsrlv1.ActionResult
	Cursor        uint64
	Reservations  []ReservationRecord
}

// ReservationRecord is the durable representation of an in-flight or
// finalized Store reservation. Protobuf DTOs remain the wire-visible domain
// model; the extra booleans are persistence metadata only.
type ReservationRecord struct {
	Plan                  *tgsrlv1.PlacementPlan
	PendingUnits          []*tgsrlv1.PendingUnit
	AllocationIDs         []string
	Finalized             bool
	Succeeded             bool
	RetainedAllocationIDs []string
}

const checkpointFormatVersion = 2

type checkpointEnvelope struct {
	FormatVersion int                   `json:"format_version"`
	Snapshot      string                `json:"snapshot,omitempty"`
	Intents       map[string]string     `json:"intents,omitempty"`
	Decisions     []string              `json:"decisions,omitempty"`
	ActionResults []string              `json:"action_results,omitempty"`
	Cursor        uint64                `json:"cursor,omitempty"`
	Reservations  []reservationEnvelope `json:"reservations,omitempty"`
}

type reservationEnvelope struct {
	Plan                  string   `json:"plan"`
	PendingUnits          []string `json:"pending_units,omitempty"`
	AllocationIDs         []string `json:"allocation_ids,omitempty"`
	Finalized             bool     `json:"finalized,omitempty"`
	Succeeded             bool     `json:"succeeded,omitempty"`
	RetainedAllocationIDs []string `json:"retained_allocation_ids,omitempty"`
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
	envelope := checkpointEnvelope{FormatVersion: checkpointFormatVersion, Intents: make(map[string]string), Cursor: state.Cursor}
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
	for _, reservation := range state.Reservations {
		encodedPlan, encodeErr := encodeProto(reservation.Plan)
		if encodeErr != nil {
			return checkpointEnvelope{}, encodeErr
		}
		encoded := reservationEnvelope{Plan: encodedPlan, AllocationIDs: append([]string(nil), reservation.AllocationIDs...), Finalized: reservation.Finalized, Succeeded: reservation.Succeeded, RetainedAllocationIDs: append([]string(nil), reservation.RetainedAllocationIDs...)}
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

func decodeCheckpoint(payload []byte, state *SchedulerState) error {
	envelope := checkpointEnvelope{}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return err
	}
	if envelope.FormatVersion != checkpointFormatVersion {
		return fmt.Errorf("unsupported format version %d", envelope.FormatVersion)
	}
	if envelope.Snapshot != "" {
		state.Snapshot = &tgsrlv1.ClusterSnapshot{}
		if err := decodeProto(envelope.Snapshot, state.Snapshot); err != nil {
			return err
		}
	}
	state.Intents = make(map[string]*tgsrlv1.SchedulingIntent, len(envelope.Intents))
	for key, encoded := range envelope.Intents {
		intent := &tgsrlv1.SchedulingIntent{}
		if err := decodeProto(encoded, intent); err != nil {
			return err
		}
		state.Intents[key] = intent
	}
	for _, encoded := range envelope.Decisions {
		decision := &tgsrlv1.DecisionRecord{}
		if err := decodeProto(encoded, decision); err != nil {
			return err
		}
		state.Decisions = append(state.Decisions, decision)
	}
	for _, encoded := range envelope.ActionResults {
		result := &tgsrlv1.ActionResult{}
		if err := decodeProto(encoded, result); err != nil {
			return err
		}
		state.ActionResults = append(state.ActionResults, result)
	}
	state.Cursor = envelope.Cursor
	for _, encoded := range envelope.Reservations {
		record := ReservationRecord{AllocationIDs: append([]string(nil), encoded.AllocationIDs...), Finalized: encoded.Finalized, Succeeded: encoded.Succeeded, RetainedAllocationIDs: append([]string(nil), encoded.RetainedAllocationIDs...)}
		record.Plan = &tgsrlv1.PlacementPlan{}
		if err := decodeProto(encoded.Plan, record.Plan); err != nil {
			return err
		}
		for _, encodedUnit := range encoded.PendingUnits {
			unit := &tgsrlv1.PendingUnit{}
			if err := decodeProto(encodedUnit, unit); err != nil {
				return err
			}
			record.PendingUnits = append(record.PendingUnits, unit)
		}
		state.Reservations = append(state.Reservations, record)
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
