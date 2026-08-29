package persistence

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/protection"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
	rootstorage "github.com/Blizzard-cyber/TGS-RL/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testSnapshot() *tgsrlv1.ClusterSnapshot {
	return &tgsrlv1.ClusterSnapshot{
		SnapshotId: "snapshot-7",
		Revision:   7,
		Annotations: map[string]string{
			"owner": "persistence-test",
		},
		Devices: []*tgsrlv1.Device{
			{DeviceId: "device-1"},
		},
	}
}

func testIntent(version uint64) *tgsrlv1.SchedulingIntent {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	ttl := 5 * time.Minute
	return &tgsrlv1.SchedulingIntent{
		ExecutionId:    "execution-1",
		StageId:        "stage-1",
		Version:        version,
		ValidUntil:     timestamppb.New(now.Add(ttl)),
		Ttl:            durationpb.New(ttl),
		IdempotencyKey: "intent-key",
		SubmittedAt:    timestamppb.New(now),
		JobId:          "job-1",
		ResourcesPerUnit: &tgsrlv1.ResourceVector{
			CpuMillis:   500,
			MemoryBytes: 1024,
		},
		UnitCount:         1,
		Priority:          12,
		Queue:             "default",
		RolloutMode:       tgsrlv1.RolloutMode_ROLLOUT_MODE_SYNC,
		PhaseKind:         tgsrlv1.PhaseKind_PHASE_KIND_DECODE,
		PolicyVersion:     "policy-1",
		DataKind:          tgsrlv1.DataKind_DATA_KIND_SYNTHETIC,
		RunId:             "run-1",
		TraceId:           "trace-1",
		DeterministicSeed: 7,
		ExecutionContract: &tgsrlv1.ExecutionContract{
			ContractId: "contract-1",
			Version:    "1.0.0",
		},
	}
}

func checkpointIntentMap(intents ...*tgsrlv1.SchedulingIntent) map[string]*tgsrlv1.SchedulingIntent {
	items := make(map[string]*tgsrlv1.SchedulingIntent, len(intents))
	for _, intent := range intents {
		items[intentKey(intent.GetExecutionId(), intent.GetStageId())] = intent
	}
	return items
}

func appendLegacyJournalRecord(
	t *testing.T,
	root string,
	record rootstorage.JournalRecord,
) {
	t.Helper()
	journal, err := rootstorage.OpenJournal(filepath.Join(root, "scheduler"), "state")
	if err != nil {
		t.Fatalf("OpenJournal() error = %v", err)
	}
	if err := journal.Append(record); err != nil {
		t.Fatalf("journal.Append(%s) error = %v", record.Kind, err)
	}
}

func appendLegacyProtoRecord(
	t *testing.T,
	root string,
	kind string,
	key string,
	message proto.Message,
) {
	t.Helper()
	payload, err := marshalProto(message)
	if err != nil {
		t.Fatalf("marshalProto() error = %v", err)
	}
	appendLegacyJournalRecord(t, root, rootstorage.JournalRecord{
		Kind:    kind,
		Key:     key,
		Payload: payload,
	})
}

func TestRepositoryRecoversLegacySnapshotIntentJournal(t *testing.T) {
	root := t.TempDir()
	snapshot := testSnapshot()
	intent := testIntent(1)
	appendLegacyProtoRecord(t, root, "snapshot", snapshot.GetSnapshotId(), snapshot)
	appendLegacyProtoRecord(
		t,
		root,
		"intent",
		intentKey(intent.GetExecutionId(), intent.GetStageId()),
		intent,
	)

	restarted, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(restart) error = %v", err)
	}
	state, err := restarted.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if !proto.Equal(state.Snapshot, snapshot) {
		t.Fatalf("recovered snapshot = %+v, want %+v", state.Snapshot, snapshot)
	}
	got := state.Intents[intentKey(intent.GetExecutionId(), intent.GetStageId())]
	if !proto.Equal(got, intent) {
		t.Fatalf("recovered intent = %+v, want %+v", got, intent)
	}
}

func TestRepositoryRecoversLegacyDuplicateIntentJournalAsLatestValue(t *testing.T) {
	root := t.TempDir()
	intent := testIntent(1)
	appendLegacyProtoRecord(
		t,
		root,
		"intent",
		intentKey(intent.GetExecutionId(), intent.GetStageId()),
		intent,
	)
	appendLegacyProtoRecord(
		t,
		root,
		"intent",
		intentKey(intent.GetExecutionId(), intent.GetStageId()),
		proto.Clone(intent).(*tgsrlv1.SchedulingIntent),
	)

	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository() error = %v", err)
	}
	state, err := repo.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if len(state.Intents) != 1 {
		t.Fatalf("len(intents) = %d, want 1", len(state.Intents))
	}
	got := state.Intents[intentKey(intent.GetExecutionId(), intent.GetStageId())]
	if !proto.Equal(got, intent) {
		t.Fatalf("recovered duplicate intent = %+v, want %+v", got, intent)
	}
}

func TestRepositoryCheckpointCompactsAndRecovers(t *testing.T) {
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository() error = %v", err)
	}
	snapshot := testSnapshot()
	intent := testIntent(2)
	state := SchedulerState{
		Snapshot: snapshot,
		Intents: map[string]*tgsrlv1.SchedulingIntent{
			intentKey(intent.GetExecutionId(), intent.GetStageId()): intent,
		},
	}
	if err := repo.SaveCheckpoint(state); err != nil {
		t.Fatalf("SaveCheckpoint() error = %v", err)
	}

	restarted, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(restart) error = %v", err)
	}
	recovered, err := restarted.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if !proto.Equal(recovered.Snapshot, snapshot) {
		t.Fatalf("recovered snapshot = %+v, want %+v", recovered.Snapshot, snapshot)
	}
	if got := recovered.Intents[intentKey(intent.GetExecutionId(), intent.GetStageId())]; !proto.Equal(got, intent) {
		t.Fatalf("recovered intent = %+v, want %+v", got, intent)
	}
}

func TestRepositoryCheckpointRecoversCompleteSchedulerState(t *testing.T) {
	repo, err := OpenRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	intent := testIntent(3)
	plan := &tgsrlv1.PlacementPlan{PlanId: "plan-1", ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(), IntentVersion: intent.GetVersion(), SnapshotRevision: 7}
	result := &tgsrlv1.ActionResult{ActionId: "action-1", PlanId: plan.GetPlanId(), Status: tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED}
	decision := &tgsrlv1.DecisionRecord{DecisionId: "decision-1", Sequence: 11, ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(), IntentVersion: intent.GetVersion(), SelectedPlan: plan, ActionResults: []*tgsrlv1.ActionResult{result}}
	state := SchedulerState{
		Snapshot: testSnapshot(), Intents: map[string]*tgsrlv1.SchedulingIntent{intentKey(intent.GetExecutionId(), intent.GetStageId()): intent}, Decisions: []*tgsrlv1.DecisionRecord{decision}, ActionResults: []*tgsrlv1.ActionResult{result}, Cursor: 11,
		Reservations: []ReservationRecord{{Plan: plan, PendingUnits: []*tgsrlv1.PendingUnit{{PendingUnitId: "unit-1"}}, AllocationIDs: []string{"allocation-1"}}},
	}
	if err := repo.SaveCheckpoint(state); err != nil {
		t.Fatalf("SaveCheckpoint() error = %v", err)
	}
	recovered, err := repo.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if recovered.Cursor != 11 || len(recovered.Decisions) != 1 || !proto.Equal(recovered.Decisions[0], decision) || len(recovered.ActionResults) != 1 || !proto.Equal(recovered.ActionResults[0], result) {
		t.Fatalf("recovered audit state = %+v", recovered)
	}
	if len(recovered.Reservations) != 1 || !proto.Equal(recovered.Reservations[0].Plan, plan) || len(recovered.Reservations[0].PendingUnits) != 1 {
		t.Fatalf("recovered reservations = %+v", recovered.Reservations)
	}
}

func TestRepositoryCheckpointPreservesReservationBeforeSnapshot(t *testing.T) {
	repo, err := OpenRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	intent := testIntent(3)
	plan := &tgsrlv1.PlacementPlan{PlanId: "plan-before-snapshot", ExecutionId: intent.GetExecutionId(), StageId: intent.GetStageId(), IntentVersion: intent.GetVersion(), SnapshotRevision: 7}
	before := testSnapshot()
	before.PendingUnits = []*tgsrlv1.PendingUnit{{PendingUnitId: "before-unit"}}
	state := SchedulerState{
		Snapshot: testSnapshot(),
		Intents:  map[string]*tgsrlv1.SchedulingIntent{intentKey(intent.GetExecutionId(), intent.GetStageId()): intent},
		Reservations: []ReservationRecord{{
			Plan:              plan,
			BeforeSnapshot:    before,
			DeviceAllocatable: map[string]*tgsrlv1.ResourceVector{"device-1": {CpuMillis: 900, MemoryBytes: 1024}},
			PendingUnits:      []*tgsrlv1.PendingUnit{{PendingUnitId: "unit-1"}},
			AllocationIDs:     []string{"allocation-1"},
		}},
	}
	if err := repo.SaveCheckpoint(state); err != nil {
		t.Fatalf("SaveCheckpoint() error = %v", err)
	}
	recovered, err := repo.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if len(recovered.Reservations) != 1 {
		t.Fatalf("recovered reservations = %+v, want 1", recovered.Reservations)
	}
	record := recovered.Reservations[0]
	if !proto.Equal(record.BeforeSnapshot, before) {
		t.Fatalf("recovered before snapshot = %+v, want %+v", record.BeforeSnapshot, before)
	}
	if got := record.DeviceAllocatable["device-1"]; got == nil || got.GetCpuMillis() != 900 {
		t.Fatalf("recovered device allocatable = %+v, want preserved device-1 snapshot", record.DeviceAllocatable)
	}
}

func TestRepositoryCheckpointRecoversProviderProjectionAndCursors(t *testing.T) {
	repo, err := OpenRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	occurredAt := timestamppb.New(time.Date(2026, time.August, 29, 10, 30, 0, 0, time.UTC))
	sandbox := &tgsrlv1.Sandbox{
		SandboxId: "sandbox-1", Generation: 4, State: tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED,
		ObservedAt: occurredAt, Share: 0.5, Priority: 7, Offloaded: true,
	}
	checkpoint := SchedulerState{
		Snapshot:           testSnapshot(),
		Intents:            map[string]*tgsrlv1.SchedulingIntent{},
		ProjectedSandboxes: []*tgsrlv1.Sandbox{sandbox},
		ResourceCursors: map[string]state.ProviderResourceCursor{
			"mock": {Revision: 12, EventID: "resource-12"},
		},
		SandboxCursors: map[string]state.ProviderSandboxCursor{
			"sandbox-1": {
				Generation: 4, ProviderRevision: 12, EventID: "sandbox-12", IdempotencyKey: "sandbox-key-12", OccurredAt: occurredAt,
				SeenEventIDs: []string{"sandbox-11", "sandbox-12"}, SeenIdempotencyKeys: []string{"sandbox-key-11", "sandbox-key-12"},
			},
		},
	}
	if err := repo.SaveCheckpoint(checkpoint); err != nil {
		t.Fatalf("SaveCheckpoint() error = %v", err)
	}
	recovered, err := repo.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if len(recovered.ProjectedSandboxes) != 1 || !proto.Equal(recovered.ProjectedSandboxes[0], sandbox) {
		t.Fatalf("recovered projected sandboxes = %+v, want %+v", recovered.ProjectedSandboxes, sandbox)
	}
	if got := recovered.ResourceCursors["mock"]; got.Revision != 12 || got.EventID != "resource-12" {
		t.Fatalf("recovered resource cursor = %+v", got)
	}
	if got := recovered.SandboxCursors["sandbox-1"]; got.Generation != 4 || got.ProviderRevision != 12 || got.EventID != "sandbox-12" || got.IdempotencyKey != "sandbox-key-12" || !proto.Equal(got.OccurredAt, occurredAt) || len(got.SeenEventIDs) != 2 || len(got.SeenIdempotencyKeys) != 2 {
		t.Fatalf("recovered sandbox cursor = %+v", got)
	}

	// Checkpoint DTOs must not alias caller-owned projection state.
	sandbox.State = tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED
	occurredAt.Seconds++
	if recovered.ProjectedSandboxes[0].GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED || recovered.SandboxCursors["sandbox-1"].OccurredAt.AsTime().Equal(occurredAt.AsTime()) {
		t.Fatalf("recovered provider projection aliases checkpoint input: %+v", recovered)
	}
}

func TestRepositoryCheckpointRecoversProtectionState(t *testing.T) {
	repo, err := OpenRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lastAction := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	windowStart := lastAction.Add(-5 * time.Second)
	breakerOpened := lastAction.Add(-2 * time.Second)
	checkpoint := SchedulerState{
		Snapshot: testSnapshot(),
		Intents:  map[string]*tgsrlv1.SchedulingIntent{},
		Protection: protection.State{
			Entries: map[string]protection.StateEntry{
				"execution-1/stage-1": {
					LastAction:    lastAction,
					LastScore:     1.25,
					WindowStart:   windowStart,
					WindowCount:   2,
					BreakerCount:  1,
					BreakerOpened: breakerOpened,
				},
			},
		},
	}
	if err := repo.SaveCheckpoint(checkpoint); err != nil {
		t.Fatalf("SaveCheckpoint() error = %v", err)
	}
	recovered, err := repo.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	got, ok := recovered.Protection.Entries["execution-1/stage-1"]
	if !ok {
		t.Fatalf("recovered protection state missing entry: %+v", recovered.Protection)
	}
	if !got.LastAction.Equal(lastAction) || got.LastScore != 1.25 || !got.WindowStart.Equal(windowStart) || got.WindowCount != 2 || got.BreakerCount != 1 || !got.BreakerOpened.Equal(breakerOpened) {
		t.Fatalf("recovered protection entry = %+v", got)
	}
}

func TestDecodeCheckpointWithoutProviderProjectionRemainsCompatible(t *testing.T) {
	snapshot, err := encodeProto(testSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{
		"format_version": checkpointFormatVersion,
		"snapshot":       snapshot,
		"intents":        map[string]string{},
		"cursor":         uint64(9),
	})
	if err != nil {
		t.Fatal(err)
	}
	recovered := &SchedulerState{}
	if err := decodeCheckpoint(payload, recovered); err != nil {
		t.Fatalf("decodeCheckpoint(old payload) error = %v", err)
	}
	if !proto.Equal(recovered.Snapshot, testSnapshot()) || recovered.Cursor != 9 {
		t.Fatalf("recovered old payload = %+v", recovered)
	}
	if len(recovered.ProjectedSandboxes) != 0 || len(recovered.ResourceCursors) != 0 || len(recovered.SandboxCursors) != 0 {
		t.Fatalf("old payload provider state = sandboxes:%+v resources:%+v sandbox cursors:%+v, want empty", recovered.ProjectedSandboxes, recovered.ResourceCursors, recovered.SandboxCursors)
	}
}

func TestCheckpointRestartRestoresProjectionFences(t *testing.T) {
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, time.August, 29, 11, 0, 0, 0, time.UTC)
	source, err := state.NewStore(testSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	resourceEvent := &tgsrlv1.ResourceEvent{
		EventId: "resource-restart-15", EventType: tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED,
		Provider: "mock", ProviderRevision: 15, Device: &tgsrlv1.Device{DeviceId: "restart-device"},
	}
	if _, changed, err := source.ApplyProviderResourceEvent(resourceEvent); err != nil || !changed {
		t.Fatalf("ApplyProviderResourceEvent() = changed:%v err:%v", changed, err)
	}
	sandboxEvent := &tgsrlv1.SandboxEvent{
		EventId: "sandbox-restart-15", IdempotencyKey: "sandbox-key-restart-15", SandboxId: "sandbox-restart",
		Generation: 6, ProviderRevision: 15, State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, OccurredAt: timestamppb.New(observedAt),
	}
	if _, changed, err := source.ApplyProviderSandboxEvent(sandboxEvent); err != nil || !changed {
		t.Fatalf("ApplyProviderSandboxEvent() = changed:%v err:%v", changed, err)
	}
	checkpoint, err := Capture(source, nil, 0, protection.State{})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if err := repo.SaveCheckpoint(checkpoint); err != nil {
		t.Fatalf("SaveCheckpoint() error = %v", err)
	}

	restartedRepo, err := OpenRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := restartedRepo.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	restartedStore, err := state.NewStore(testSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	if err := RestoreStore(restartedStore, recovered); err != nil {
		t.Fatalf("RestoreStore() error = %v", err)
	}
	if _, changed, err := restartedStore.ApplyProviderResourceEvent(resourceEvent); err != nil || changed {
		t.Fatalf("ApplyProviderResourceEvent(replayed after restart) = changed:%v err:%v, want false nil", changed, err)
	}
	projected, changed, err := restartedStore.ApplyProviderSandboxEvent(sandboxEvent)
	if err != nil || changed {
		t.Fatalf("ApplyProviderSandboxEvent(replayed after restart) = changed:%v err:%v, want false nil", changed, err)
	}
	if projected.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING || projected.GetGeneration() != 6 {
		t.Fatalf("restarted sandbox projection = %+v", projected)
	}
}

func TestRepositoryDetectsCorruptedJournalTail(t *testing.T) {
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository() error = %v", err)
	}
	appendLegacyProtoRecord(
		t,
		root,
		"intent",
		intentKey("execution-1", "stage-1"),
		testIntent(1),
	)
	info, err := os.Stat(repo.JournalPath())
	if err != nil {
		t.Fatalf("Stat(journal) error = %v", err)
	}
	if err := TruncateTail(repo.JournalPath(), info.Size()-3); err != nil {
		t.Fatalf("TruncateTail() error = %v", err)
	}

	_, err = repo.Recover()
	if !IsCorruption(err) {
		t.Fatalf("Recover() error = %v, want corruption", err)
	}
}

func TestSplitIntentKey(t *testing.T) {
	key := intentKey("execution-1", "stage-1")
	executionID, stageID, ok := SplitIntentKey(key)
	if !ok || executionID != "execution-1" || stageID != "stage-1" {
		t.Fatalf("SplitIntentKey() = (%q, %q, %v)", executionID, stageID, ok)
	}
}

func TestRepositoryCreatesOwnedFiles(t *testing.T) {
	root := t.TempDir()
	repo, err := OpenRepository(filepath.Join(root, "nested"))
	if err != nil {
		t.Fatalf("OpenRepository() error = %v", err)
	}
	if err := repo.SaveCheckpoint(SchedulerState{Snapshot: testSnapshot(), Intents: map[string]*tgsrlv1.SchedulingIntent{}}); err != nil {
		t.Fatalf("SaveCheckpoint() error = %v", err)
	}
	if _, err := os.Stat(repo.JournalPath()); err != nil {
		t.Fatalf("Stat(journal) error = %v", err)
	}
	checkpointPath := filepath.Join(root, "nested", "scheduler", "state.checkpoint")
	err = os.WriteFile(checkpointPath, []byte{}, 0o644)
	if err != nil {
		t.Fatalf("WriteFile(empty checkpoint) error = %v", err)
	}
	_, err = repo.Recover()
	if !errors.Is(err, ErrCorruptPayload) && !IsCorruption(err) {
		t.Fatalf("Recover(empty checkpoint) error = %v", err)
	}
}

func TestRepositoryRecoversLegacyDecisionJournal(t *testing.T) {
	root := t.TempDir()
	intent := testIntent(3)
	plan := &tgsrlv1.PlacementPlan{
		PlanId:           "plan-1",
		ExecutionId:      intent.GetExecutionId(),
		StageId:          intent.GetStageId(),
		IntentVersion:    intent.GetVersion(),
		SnapshotRevision: 7,
	}
	result := &tgsrlv1.ActionResult{
		ActionId: "action-1",
		PlanId:   plan.GetPlanId(),
		Status:   tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED,
	}
	decision := &tgsrlv1.DecisionRecord{
		DecisionId:    "decision-1",
		Sequence:      11,
		ExecutionId:   intent.GetExecutionId(),
		StageId:       intent.GetStageId(),
		IntentVersion: intent.GetVersion(),
		SelectedPlan:  plan,
		ActionResults: []*tgsrlv1.ActionResult{result},
	}
	appendLegacyProtoRecord(t, root, "decision", decision.GetDecisionId(), decision)
	appendLegacyProtoRecord(
		t,
		root,
		"action_result",
		result.GetPlanId()+"/"+result.GetActionId(),
		result,
	)
	cursorPayload, err := json.Marshal(struct {
		Cursor uint64 `json:"cursor"`
	}{Cursor: decision.GetSequence()})
	if err != nil {
		t.Fatalf("json.Marshal(cursor) error = %v", err)
	}
	appendLegacyJournalRecord(t, root, rootstorage.JournalRecord{
		Kind:    "cursor",
		Key:     "decision",
		Payload: cursorPayload,
	})

	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository() error = %v", err)
	}
	recovered, err := repo.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if recovered.Cursor != 11 {
		t.Fatalf("recovered cursor = %d, want 11", recovered.Cursor)
	}
	if len(recovered.Decisions) != 1 || !proto.Equal(recovered.Decisions[0], decision) {
		t.Fatalf("recovered decisions = %+v", recovered.Decisions)
	}
	if len(recovered.ActionResults) != 1 || !proto.Equal(recovered.ActionResults[0], result) {
		t.Fatalf("recovered action_results = %+v", recovered.ActionResults)
	}
}

func TestRepositoryRejectsOversizedJournalReplacementBeforeCheckpointCommit(t *testing.T) {
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository() error = %v", err)
	}
	baseline := SchedulerState{
		Snapshot: testSnapshot(),
		Intents: checkpointIntentMap(
			testIntent(1),
		),
	}
	if err := repo.SaveCheckpoint(baseline); err != nil {
		t.Fatalf("SaveCheckpoint(baseline) error = %v", err)
	}

	oversizedIntents := make([]*tgsrlv1.SchedulingIntent, 0, 68)
	largeQueue := strings.Repeat("q", 1<<20)
	for index := range 68 {
		intent := testIntent(uint64(index + 2))
		intent.StageId = "stage-" + strings.Repeat("x", 8) + string(rune('A'+(index%26))) + string(rune('a'+((index/26)%26)))
		intent.Queue = largeQueue
		oversizedIntents = append(oversizedIntents, intent)
	}
	err = repo.SaveCheckpoint(SchedulerState{
		Snapshot: testSnapshot(),
		Intents:  checkpointIntentMap(oversizedIntents...),
	})
	if !errors.Is(err, rootstorage.ErrJournalTooLarge) {
		t.Fatalf("SaveCheckpoint(oversized) error = %v, want ErrJournalTooLarge", err)
	}

	restarted, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(restart) error = %v", err)
	}
	recovered, err := restarted.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if !proto.Equal(recovered.Snapshot, baseline.Snapshot) {
		t.Fatalf("recovered snapshot after oversized failure = %+v, want %+v", recovered.Snapshot, baseline.Snapshot)
	}
	if len(recovered.Intents) != 1 {
		t.Fatalf("len(recovered.Intents) = %d, want 1", len(recovered.Intents))
	}
	want := baseline.Intents[intentKey("execution-1", "stage-1")]
	got := recovered.Intents[intentKey("execution-1", "stage-1")]
	if !proto.Equal(got, want) {
		t.Fatalf("recovered baseline intent = %+v, want %+v", got, want)
	}
}

func TestRepositoryCheckpointCommitIgnoresAuxiliaryJournalRefreshFailure(t *testing.T) {
	root := t.TempDir()
	repo, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository() error = %v", err)
	}
	baseline := SchedulerState{
		Snapshot: testSnapshot(),
		Intents: checkpointIntentMap(
			testIntent(1),
		),
	}
	if err := repo.SaveCheckpoint(baseline); err != nil {
		t.Fatalf("SaveCheckpoint(baseline) error = %v", err)
	}

	repo.replaceJournal = func([]rootstorage.JournalRecord) error {
		return errors.New("injected journal replace failure")
	}
	updatedIntent := testIntent(9)
	updatedIntent.StageId = "stage-aux"
	updated := SchedulerState{
		Snapshot: testSnapshot(),
		Intents:  checkpointIntentMap(updatedIntent),
		Cursor:   9,
	}
	if err := repo.SaveCheckpoint(updated); err != nil {
		t.Fatalf("SaveCheckpoint(updated) error = %v, want nil", err)
	}

	restarted, err := OpenRepository(root)
	if err != nil {
		t.Fatalf("OpenRepository(restart) error = %v", err)
	}
	recovered, err := restarted.Recover()
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if recovered.Cursor != 9 {
		t.Fatalf("recovered cursor = %d, want 9", recovered.Cursor)
	}
	if len(recovered.Intents) != 1 {
		t.Fatalf("len(recovered.Intents) = %d, want 1", len(recovered.Intents))
	}
	got := recovered.Intents[intentKey(updatedIntent.GetExecutionId(), updatedIntent.GetStageId())]
	if !proto.Equal(got, updatedIntent) {
		t.Fatalf("recovered intent after auxiliary journal failure = %+v, want %+v", got, updatedIntent)
	}
}
