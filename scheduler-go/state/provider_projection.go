package state

import (
	"errors"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

type providerResourceCursor struct {
	revision uint64
	eventID  string
}

type providerSandboxCursor struct {
	generation uint64
	eventID    string
}

type providerProjectionState struct {
	resourceCursors map[string]providerResourceCursor
	sandboxes       map[string]*tgsrlv1.Sandbox
	sandboxCursors  map[string]providerSandboxCursor
}

func (p providerProjectionState) clone() providerProjectionState {
	cloned := providerProjectionState{
		resourceCursors: make(map[string]providerResourceCursor, len(p.resourceCursors)),
		sandboxes:       make(map[string]*tgsrlv1.Sandbox, len(p.sandboxes)),
		sandboxCursors:  make(map[string]providerSandboxCursor, len(p.sandboxCursors)),
	}
	for providerID, cursor := range p.resourceCursors {
		cloned.resourceCursors[providerID] = cursor
	}
	for sandboxID, sandbox := range p.sandboxes {
		cloned.sandboxes[sandboxID] = cloneProjectedSandbox(sandbox)
	}
	for sandboxID, cursor := range p.sandboxCursors {
		cloned.sandboxCursors[sandboxID] = cursor
	}
	return cloned
}

func newProviderProjectionState() providerProjectionState {
	return providerProjectionState{
		resourceCursors: make(map[string]providerResourceCursor),
		sandboxes:       make(map[string]*tgsrlv1.Sandbox),
		sandboxCursors:  make(map[string]providerSandboxCursor),
	}
}

// BootstrapProviderSnapshot projects a live provider snapshot into Store
// authority while preserving Store-owned allocations and pending units.
func (s *Store) BootstrapProviderSnapshot(provider string, snapshot *tgsrlv1.ClusterSnapshot) (*tgsrlv1.ClusterSnapshot, bool, error) {
	if snapshot == nil {
		return nil, false, errors.New("state: provider snapshot is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if !acceptResourceCursorLocked(&s.providerProjection, provider, snapshot.GetRevision(), "bootstrap-snapshot") {
		return cloneSnapshot(s.snapshot), false, nil
	}

	working := cloneSnapshot(snapshot)
	working.Allocations = cloneAllocations(s.snapshot.GetAllocations())
	working.PendingUnits = clonePendingUnits(s.snapshot.GetPendingUnits())
	if proto.Equal(working, s.snapshot) {
		return cloneSnapshot(s.snapshot), false, nil
	}
	s.commitSnapshotLocked(working)
	return cloneSnapshot(s.snapshot), true, nil
}

// ApplyProviderResourceEvent projects one provider resource event into Store
// authority with provider revision fencing.
func (s *Store) ApplyProviderResourceEvent(event *tgsrlv1.ResourceEvent) (*tgsrlv1.ClusterSnapshot, bool, error) {
	if event == nil {
		return nil, false, errors.New("state: resource event is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if !acceptResourceCursorLocked(&s.providerProjection, event.GetProvider(), event.GetProviderRevision(), event.GetEventId()) {
		return cloneSnapshot(s.snapshot), false, nil
	}

	working := cloneSnapshot(s.snapshot)
	switch event.GetEventType() {
	case tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED:
		if event.GetSnapshot() == nil {
			return nil, false, errors.New("state: snapshot event missing snapshot")
		}
		providerSnapshot := cloneSnapshot(event.GetSnapshot())
		providerSnapshot.Allocations = cloneAllocations(working.GetAllocations())
		providerSnapshot.PendingUnits = clonePendingUnits(working.GetPendingUnits())
		working = providerSnapshot
	case tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED:
		if event.GetDevice() == nil {
			return nil, false, errors.New("state: device event missing device")
		}
		upsertProjectedDevice(working, event.GetDevice())
	case tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_ALLOCATION_CHANGED:
		if event.GetAllocation() == nil {
			return nil, false, errors.New("state: allocation event missing allocation")
		}
		upsertProjectedAllocation(working, event.GetAllocation())
	case tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_PENDING_UNIT_CHANGED:
		if event.GetPendingUnit() == nil {
			return nil, false, errors.New("state: pending unit event missing pending unit")
		}
		upsertProjectedPendingUnit(working, event.GetPendingUnit())
	case tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_CAPABILITY_REFRESHED:
		if event.GetCapabilities() == nil {
			return nil, false, errors.New("state: capability event missing capabilities")
		}
		for _, device := range working.GetDevices() {
			device.Capabilities = cloneCapabilitySet(event.GetCapabilities())
		}
	default:
		return cloneSnapshot(s.snapshot), false, nil
	}

	if proto.Equal(working, s.snapshot) {
		return cloneSnapshot(s.snapshot), false, nil
	}
	s.commitSnapshotLocked(working)
	return cloneSnapshot(s.snapshot), true, nil
}

// ReplaceProjectedSandboxes replaces the Store-owned provider sandbox index with
// the supplied authoritative list.
func (s *Store) ReplaceProjectedSandboxes(sandboxes []*tgsrlv1.Sandbox) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nextSandboxes := make(map[string]*tgsrlv1.Sandbox, len(sandboxes))
	nextCursors := make(map[string]providerSandboxCursor, len(sandboxes))
	for _, sandbox := range sandboxes {
		if sandbox == nil || sandbox.GetSandboxId() == "" {
			return false, errors.New("state: sandbox bootstrap requires sandbox_id")
		}
		nextSandboxes[sandbox.GetSandboxId()] = cloneProjectedSandbox(sandbox)
		nextCursors[sandbox.GetSandboxId()] = providerSandboxCursor{
			generation: sandbox.GetGeneration(),
			eventID:    "bootstrap-sandbox",
		}
	}
	if equalProjectedSandboxes(s.providerProjection.sandboxes, nextSandboxes) {
		s.providerProjection.sandboxCursors = nextCursors
		return false, nil
	}
	s.providerProjection.sandboxes = nextSandboxes
	s.providerProjection.sandboxCursors = nextCursors
	return true, nil
}

// ApplyProviderSandboxEvent projects one provider sandbox event into Store
// authority with generation and event fencing.
func (s *Store) ApplyProviderSandboxEvent(event *tgsrlv1.SandboxEvent) (*tgsrlv1.Sandbox, bool, error) {
	if event == nil {
		return nil, false, errors.New("state: sandbox event is nil")
	}
	if event.GetSandboxId() == "" {
		return nil, false, errors.New("state: sandbox event missing sandbox_id")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !acceptSandboxCursorLocked(&s.providerProjection, event) {
		current := s.providerProjection.sandboxes[event.GetSandboxId()]
		return cloneProjectedSandbox(current), false, nil
	}

	next := &tgsrlv1.Sandbox{
		SandboxId:  event.GetSandboxId(),
		RunId:      event.GetRunId(),
		JobId:      event.GetJobId(),
		TraceId:    event.GetTraceId(),
		State:      event.GetState(),
		Generation: event.GetGeneration(),
		Binding:    cloneProjectedBinding(event.GetBinding()),
		SafePoint:  event.GetSafePoint(),
		ObservedAt: cloneTimestamp(event.GetOccurredAt()),
		DataKind:   event.GetDataKind(),
	}
	if current := s.providerProjection.sandboxes[event.GetSandboxId()]; current != nil {
		merged := cloneProjectedSandbox(current)
		if next.GetRunId() != "" {
			merged.RunId = next.GetRunId()
		}
		if next.GetJobId() != "" {
			merged.JobId = next.GetJobId()
		}
		if next.GetTraceId() != "" {
			merged.TraceId = next.GetTraceId()
		}
		merged.State = next.GetState()
		merged.Generation = next.GetGeneration()
		if next.GetBinding() != nil {
			merged.Binding = cloneProjectedBinding(next.GetBinding())
		}
		merged.SafePoint = next.GetSafePoint()
		merged.ObservedAt = cloneTimestamp(next.GetObservedAt())
		merged.DataKind = next.GetDataKind()
		next = merged
	}

	current := s.providerProjection.sandboxes[event.GetSandboxId()]
	if proto.Equal(current, next) {
		return cloneProjectedSandbox(current), false, nil
	}
	s.providerProjection.sandboxes[event.GetSandboxId()] = cloneProjectedSandbox(next)
	return cloneProjectedSandbox(next), true, nil
}

// ListProjectedSandboxes returns a stable-ID-ordered clone of projected
// provider sandbox authority.
func (s *Store) ListProjectedSandboxes() []*tgsrlv1.Sandbox {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]string, 0, len(s.providerProjection.sandboxes))
	for sandboxID := range s.providerProjection.sandboxes {
		ids = append(ids, sandboxID)
	}
	sort.Strings(ids)
	out := make([]*tgsrlv1.Sandbox, 0, len(ids))
	for _, sandboxID := range ids {
		out = append(out, cloneProjectedSandbox(s.providerProjection.sandboxes[sandboxID]))
	}
	return out
}

func acceptResourceCursorLocked(projected *providerProjectionState, provider string, revision uint64, eventID string) bool {
	current, ok := projected.resourceCursors[provider]
	if ok {
		switch {
		case revision < current.revision:
			return false
		case revision == current.revision && eventID != "" && eventID == current.eventID:
			return false
		}
	}
	projected.resourceCursors[provider] = providerResourceCursor{revision: revision, eventID: eventID}
	return true
}

func acceptSandboxCursorLocked(projected *providerProjectionState, event *tgsrlv1.SandboxEvent) bool {
	current, ok := projected.sandboxCursors[event.GetSandboxId()]
	if ok {
		switch {
		case event.GetGeneration() < current.generation:
			return false
		case event.GetGeneration() == current.generation && event.GetEventId() != "" && event.GetEventId() == current.eventID:
			return false
		}
	}
	projected.sandboxCursors[event.GetSandboxId()] = providerSandboxCursor{
		generation: event.GetGeneration(),
		eventID:    event.GetEventId(),
	}
	return true
}

func upsertProjectedDevice(snapshot *tgsrlv1.ClusterSnapshot, device *tgsrlv1.Device) {
	for index, existing := range snapshot.GetDevices() {
		if existing.GetDeviceId() == device.GetDeviceId() {
			snapshot.Devices[index] = proto.Clone(device).(*tgsrlv1.Device)
			return
		}
	}
	snapshot.Devices = append(snapshot.Devices, proto.Clone(device).(*tgsrlv1.Device))
}

func upsertProjectedAllocation(snapshot *tgsrlv1.ClusterSnapshot, allocation *tgsrlv1.Allocation) {
	for index, existing := range snapshot.GetAllocations() {
		if existing.GetAllocationId() == allocation.GetAllocationId() {
			snapshot.Allocations[index] = proto.Clone(allocation).(*tgsrlv1.Allocation)
			return
		}
	}
	snapshot.Allocations = append(snapshot.Allocations, proto.Clone(allocation).(*tgsrlv1.Allocation))
}

func upsertProjectedPendingUnit(snapshot *tgsrlv1.ClusterSnapshot, unit *tgsrlv1.PendingUnit) {
	for index, existing := range snapshot.GetPendingUnits() {
		if existing.GetPendingUnitId() == unit.GetPendingUnitId() {
			snapshot.PendingUnits[index] = proto.Clone(unit).(*tgsrlv1.PendingUnit)
			return
		}
	}
	snapshot.PendingUnits = append(snapshot.PendingUnits, proto.Clone(unit).(*tgsrlv1.PendingUnit))
}

func cloneAllocations(in []*tgsrlv1.Allocation) []*tgsrlv1.Allocation {
	out := make([]*tgsrlv1.Allocation, 0, len(in))
	for _, allocation := range in {
		if allocation != nil {
			out = append(out, proto.Clone(allocation).(*tgsrlv1.Allocation))
		}
	}
	return out
}

func clonePendingUnits(in []*tgsrlv1.PendingUnit) []*tgsrlv1.PendingUnit {
	out := make([]*tgsrlv1.PendingUnit, 0, len(in))
	for _, unit := range in {
		if unit != nil {
			out = append(out, proto.Clone(unit).(*tgsrlv1.PendingUnit))
		}
	}
	return out
}

func cloneProjectedSandbox(sandbox *tgsrlv1.Sandbox) *tgsrlv1.Sandbox {
	if sandbox == nil {
		return nil
	}
	return proto.Clone(sandbox).(*tgsrlv1.Sandbox)
}

func cloneProjectedBinding(binding *tgsrlv1.Binding) *tgsrlv1.Binding {
	if binding == nil {
		return nil
	}
	return proto.Clone(binding).(*tgsrlv1.Binding)
}

func equalProjectedSandboxes(left, right map[string]*tgsrlv1.Sandbox) bool {
	if len(left) != len(right) {
		return false
	}
	for sandboxID, sandbox := range left {
		if !proto.Equal(sandbox, right[sandboxID]) {
			return false
		}
	}
	return true
}
