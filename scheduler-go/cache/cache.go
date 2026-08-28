package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Clock supplies stable time for revisioned cache updates.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

// Now implements Clock.
func (f ClockFunc) Now() time.Time { return f() }

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// Cursor identifies the last accepted position for an event stream.
type Cursor struct {
	Stream     string
	Revision   uint64
	EventID    string
	Generation uint64
}

// Snapshot is the immutable scheduler view materialized by the event loop.
type Snapshot struct {
	Revision  uint64
	Snapshot  *tgsrlv1.ClusterSnapshot
	Intents   map[string]*tgsrlv1.SchedulingIntent
	Sandboxes map[string]*tgsrlv1.Sandbox
	Cursors   map[string]Cursor
}

// State owns immutable snapshots. Writers replace the full Snapshot pointer,
// readers receive a cloned copy they may freely mutate.
type State struct {
	mu      sync.RWMutex
	clock   Clock
	current *Snapshot
}

// NewState constructs a State from an optional initial cluster snapshot.
func NewState(initial *tgsrlv1.ClusterSnapshot, clock Clock) *State {
	if clock == nil {
		clock = wallClock{}
	}
	snapshot := cloneClusterSnapshot(initial)
	if snapshot == nil {
		snapshot = &tgsrlv1.ClusterSnapshot{
			SnapshotId: "snapshot-0",
			ObservedAt: timestamppb.New(clock.Now().UTC()),
		}
	}
	return &State{
		clock: clock,
		current: &Snapshot{
			Revision:  snapshot.GetRevision(),
			Snapshot:  snapshot,
			Intents:   map[string]*tgsrlv1.SchedulingIntent{},
			Sandboxes: map[string]*tgsrlv1.Sandbox{},
			Cursors:   map[string]Cursor{},
		},
	}
}

// View returns a fully cloned immutable snapshot view.
func (s *State) View() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneSnapshotState(s.current)
}

// Update replaces the current immutable snapshot by applying mutate to a deep
// clone of the current state. Returning false keeps the current state.
func (s *State) Update(mutate func(*Snapshot) bool) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := cloneSnapshotPtr(s.current)
	if !mutate(next) {
		return cloneSnapshotState(s.current)
	}
	next.Revision++
	next.Snapshot = cloneClusterSnapshot(next.Snapshot)
	next.Snapshot.Revision = next.Revision
	next.Snapshot.SnapshotId = SnapshotID(next.Revision)
	next.Snapshot.ObservedAt = timestamppb.New(s.clock.Now().UTC())
	s.current = next
	return cloneSnapshotState(s.current)
}

// AcceptCursor records a stream cursor if it advances the last accepted event.
func (s *Snapshot) AcceptCursor(cursor Cursor) bool {
	current, ok := s.Cursors[cursor.Stream]
	if ok {
		switch {
		case cursor.Revision < current.Revision:
			return false
		case cursor.Revision == current.Revision && cursor.EventID != "" && cursor.EventID == current.EventID:
			return false
		case cursor.Generation > 0 && cursor.Generation < current.Generation:
			return false
		}
	}
	s.Cursors[cursor.Stream] = cursor
	return true
}

// UpsertIntent stores one immutable intent keyed by execution/stage.
func (s *Snapshot) UpsertIntent(intent *tgsrlv1.SchedulingIntent) bool {
	if intent == nil {
		return false
	}
	key := intentKey(intent.GetExecutionId(), intent.GetStageId())
	current := s.Intents[key]
	if current != nil && current.GetVersion() > intent.GetVersion() {
		return false
	}
	if current != nil && current.GetVersion() == intent.GetVersion() && proto.Equal(current, intent) {
		return false
	}
	s.Intents[key] = cloneIntent(intent)
	return true
}

// ApplyResourceEvent applies a provider resource event to the working snapshot.
func (s *Snapshot) ApplyResourceEvent(event *tgsrlv1.ResourceEvent) bool {
	if event == nil {
		return false
	}
	if !s.AcceptCursor(Cursor{
		Stream:   resourceStream(event.GetProvider()),
		Revision: event.GetProviderRevision(),
		EventID:  event.GetEventId(),
	}) {
		return false
	}
	switch event.GetEventType() {
	case tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED:
		if event.GetSnapshot() == nil {
			return false
		}
		s.Snapshot = cloneClusterSnapshot(event.GetSnapshot())
		return true
	case tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED:
		if event.GetDevice() == nil {
			return false
		}
		upsertDevice(s.Snapshot, event.GetDevice())
		return true
	case tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_ALLOCATION_CHANGED:
		if event.GetAllocation() == nil {
			return false
		}
		upsertAllocation(s.Snapshot, event.GetAllocation())
		return true
	case tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_PENDING_UNIT_CHANGED:
		if event.GetPendingUnit() == nil {
			return false
		}
		upsertPendingUnit(s.Snapshot, event.GetPendingUnit())
		return true
	case tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_CAPABILITY_REFRESHED:
		if event.GetCapabilities() == nil {
			return false
		}
		for _, device := range s.Snapshot.GetDevices() {
			device.Capabilities = cloneCapabilities(event.GetCapabilities())
		}
		return true
	default:
		return false
	}
}

// ApplySandboxEvent applies a generation-fenced runtime event.
func (s *Snapshot) ApplySandboxEvent(event *tgsrlv1.SandboxEvent) bool {
	if event == nil || event.GetSandboxId() == "" {
		return false
	}
	stream := sandboxStream(event.GetSandboxId())
	if !s.AcceptCursor(Cursor{
		Stream:     stream,
		Revision:   0,
		EventID:    event.GetEventId(),
		Generation: event.GetGeneration(),
	}) {
		return false
	}
	current := s.Sandboxes[event.GetSandboxId()]
	if current != nil {
		if event.GetGeneration() < current.GetGeneration() {
			return false
		}
		if event.GetGeneration() == current.GetGeneration() && event.GetEventId() != "" && event.GetEventId() == s.Cursors[stream].EventID {
			return false
		}
	}
	sandbox := &tgsrlv1.Sandbox{
		SandboxId:  event.GetSandboxId(),
		RunId:      event.GetRunId(),
		JobId:      event.GetJobId(),
		TraceId:    event.GetTraceId(),
		State:      event.GetState(),
		Generation: event.GetGeneration(),
		Binding:    cloneBinding(event.GetBinding()),
		SafePoint:  event.GetSafePoint(),
		ObservedAt: cloneTimestamp(event.GetOccurredAt()),
		DataKind:   event.GetDataKind(),
	}
	if current != nil {
		merged := cloneSandbox(current)
		merged.RunId = firstNonEmpty(sandbox.GetRunId(), merged.GetRunId())
		merged.JobId = firstNonEmpty(sandbox.GetJobId(), merged.GetJobId())
		merged.TraceId = firstNonEmpty(sandbox.GetTraceId(), merged.GetTraceId())
		merged.State = sandbox.GetState()
		merged.Generation = sandbox.GetGeneration()
		if sandbox.GetBinding() != nil {
			merged.Binding = cloneBinding(sandbox.GetBinding())
		}
		merged.SafePoint = sandbox.GetSafePoint()
		merged.ObservedAt = cloneTimestamp(sandbox.GetObservedAt())
		merged.DataKind = sandbox.GetDataKind()
		sandbox = merged
	}
	s.Sandboxes[event.GetSandboxId()] = sandbox
	return true
}

// RebuildPendingUnits rebuilds pending units from the latest immutable intents.
func (s *Snapshot) RebuildPendingUnits(now time.Time) {
	units := make([]*tgsrlv1.PendingUnit, 0)
	keys := make([]string, 0, len(s.Intents))
	for key := range s.Intents {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		intent := s.Intents[key]
		if intent == nil || !intent.GetValidUntil().AsTime().After(now) {
			continue
		}
		for index := uint32(0); index < intent.GetUnitCount(); index++ {
			units = append(units, &tgsrlv1.PendingUnit{
				PendingUnitId:        PendingUnitID(intent, index),
				ExecutionId:          intent.GetExecutionId(),
				StageId:              intent.GetStageId(),
				IntentVersion:        intent.GetVersion(),
				JobId:                intent.GetJobId(),
				RequestedResources:   cloneResources(intent.GetResourcesPerUnit()),
				RequiredCapabilities: cloneCapabilities(intent.GetRequiredCapabilities()),
				Priority:             intent.GetPriority(),
				QueuedAt:             cloneTimestamp(intent.GetSubmittedAt()),
				Attempt:              0,
			})
		}
	}
	s.Snapshot.PendingUnits = units
}

// SnapshotID returns the canonical snapshot identifier.
func SnapshotID(revision uint64) string {
	return fmt.Sprintf("snapshot-%d", revision)
}

// PendingUnitID returns the stable pending-unit identifier for an intent copy.
func PendingUnitID(intent *tgsrlv1.SchedulingIntent, index uint32) string {
	return StableID(
		"pending",
		intent.GetExecutionId(),
		intent.GetStageId(),
		strconv.FormatUint(intent.GetVersion(), 10),
		strconv.FormatUint(uint64(index), 10),
	)
}

// StableID creates a deterministic short hash identifier.
func StableID(prefix string, parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(strconv.Itoa(len(part))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write([]byte(part))
	}
	return prefix + "-" + hex.EncodeToString(hash.Sum(nil)[:12])
}

func resourceStream(provider string) string {
	return "resource:" + provider
}

func sandboxStream(sandboxID string) string {
	return "sandbox:" + sandboxID
}

func intentKey(executionID, stageID string) string {
	return executionID + "/" + stageID
}

func firstNonEmpty(left, right string) string {
	if left != "" {
		return left
	}
	return right
}

func upsertDevice(snapshot *tgsrlv1.ClusterSnapshot, device *tgsrlv1.Device) {
	for index, existing := range snapshot.GetDevices() {
		if existing.GetDeviceId() == device.GetDeviceId() {
			snapshot.Devices[index] = cloneDevice(device)
			return
		}
	}
	snapshot.Devices = append(snapshot.Devices, cloneDevice(device))
}

func upsertAllocation(snapshot *tgsrlv1.ClusterSnapshot, allocation *tgsrlv1.Allocation) {
	for index, existing := range snapshot.GetAllocations() {
		if existing.GetAllocationId() == allocation.GetAllocationId() {
			snapshot.Allocations[index] = cloneAllocation(allocation)
			return
		}
	}
	snapshot.Allocations = append(snapshot.Allocations, cloneAllocation(allocation))
}

func upsertPendingUnit(snapshot *tgsrlv1.ClusterSnapshot, unit *tgsrlv1.PendingUnit) {
	for index, existing := range snapshot.GetPendingUnits() {
		if existing.GetPendingUnitId() == unit.GetPendingUnitId() {
			snapshot.PendingUnits[index] = clonePendingUnit(unit)
			return
		}
	}
	snapshot.PendingUnits = append(snapshot.PendingUnits, clonePendingUnit(unit))
}

func cloneSnapshotState(snapshot *Snapshot) Snapshot {
	return *cloneSnapshotPtr(snapshot)
}

func cloneSnapshotPtr(snapshot *Snapshot) *Snapshot {
	if snapshot == nil {
		return &Snapshot{
			Snapshot:  &tgsrlv1.ClusterSnapshot{},
			Intents:   map[string]*tgsrlv1.SchedulingIntent{},
			Sandboxes: map[string]*tgsrlv1.Sandbox{},
			Cursors:   map[string]Cursor{},
		}
	}
	out := &Snapshot{
		Revision:  snapshot.Revision,
		Snapshot:  cloneClusterSnapshot(snapshot.Snapshot),
		Intents:   make(map[string]*tgsrlv1.SchedulingIntent, len(snapshot.Intents)),
		Sandboxes: make(map[string]*tgsrlv1.Sandbox, len(snapshot.Sandboxes)),
		Cursors:   make(map[string]Cursor, len(snapshot.Cursors)),
	}
	for key, intent := range snapshot.Intents {
		out.Intents[key] = cloneIntent(intent)
	}
	for key, sandbox := range snapshot.Sandboxes {
		out.Sandboxes[key] = cloneSandbox(sandbox)
	}
	for key, cursor := range snapshot.Cursors {
		out.Cursors[key] = cursor
	}
	return out
}

func cloneClusterSnapshot(snapshot *tgsrlv1.ClusterSnapshot) *tgsrlv1.ClusterSnapshot {
	if snapshot == nil {
		return &tgsrlv1.ClusterSnapshot{}
	}
	return proto.Clone(snapshot).(*tgsrlv1.ClusterSnapshot)
}

func cloneIntent(intent *tgsrlv1.SchedulingIntent) *tgsrlv1.SchedulingIntent {
	if intent == nil {
		return nil
	}
	return proto.Clone(intent).(*tgsrlv1.SchedulingIntent)
}

func cloneSandbox(sandbox *tgsrlv1.Sandbox) *tgsrlv1.Sandbox {
	if sandbox == nil {
		return nil
	}
	return proto.Clone(sandbox).(*tgsrlv1.Sandbox)
}

func cloneBinding(binding *tgsrlv1.Binding) *tgsrlv1.Binding {
	if binding == nil {
		return nil
	}
	return proto.Clone(binding).(*tgsrlv1.Binding)
}

func cloneTimestamp(ts *timestamppb.Timestamp) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return proto.Clone(ts).(*timestamppb.Timestamp)
}

func cloneDevice(device *tgsrlv1.Device) *tgsrlv1.Device {
	if device == nil {
		return nil
	}
	return proto.Clone(device).(*tgsrlv1.Device)
}

func cloneAllocation(allocation *tgsrlv1.Allocation) *tgsrlv1.Allocation {
	if allocation == nil {
		return nil
	}
	return proto.Clone(allocation).(*tgsrlv1.Allocation)
}

func clonePendingUnit(unit *tgsrlv1.PendingUnit) *tgsrlv1.PendingUnit {
	if unit == nil {
		return nil
	}
	return proto.Clone(unit).(*tgsrlv1.PendingUnit)
}

func cloneResources(resources *tgsrlv1.ResourceVector) *tgsrlv1.ResourceVector {
	if resources == nil {
		return &tgsrlv1.ResourceVector{}
	}
	return proto.Clone(resources).(*tgsrlv1.ResourceVector)
}

func cloneCapabilities(capabilities *tgsrlv1.CapabilitySet) *tgsrlv1.CapabilitySet {
	if capabilities == nil {
		return &tgsrlv1.CapabilitySet{}
	}
	return proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)
}
