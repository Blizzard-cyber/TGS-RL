package state

import (
	"context"
	"errors"
	"sort"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/internal/protocolmeta"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type providerResourceCursor struct {
	revision uint64
	eventID  string
}

type providerSandboxCursor struct {
	generation       uint64
	providerRevision uint64
	eventID          string
	idempotencyKey   string
	occurredAt       time.Time
	seenEventIDs     map[string]struct{}
	seenIdempotency  map[string]struct{}
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
		cloned.sandboxCursors[sandboxID] = cloneProviderSandboxCursor(cursor)
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
	// Retain cursors for sandboxes absent from the latest list as tombstones.
	// Otherwise a delayed event could resurrect a sandbox immediately after an
	// authoritative bootstrap removed it.
	nextCursors := make(map[string]providerSandboxCursor, len(s.providerProjection.sandboxCursors)+len(sandboxes))
	for sandboxID, cursor := range s.providerProjection.sandboxCursors {
		nextCursors[sandboxID] = cursor
	}
	var confirmedAt time.Time
	for _, sandbox := range sandboxes {
		if sandbox == nil || sandbox.GetSandboxId() == "" {
			return false, errors.New("state: sandbox bootstrap requires sandbox_id")
		}
		if sandbox.GetObservedAt() != nil && sandbox.GetObservedAt().CheckValid() != nil {
			return false, errors.New("state: sandbox bootstrap has invalid observed_at")
		}
		sandboxID := sandbox.GetSandboxId()
		current := s.providerProjection.sandboxes[sandboxID]
		cursor, hasCursor := s.providerProjection.sandboxCursors[sandboxID]
		if !hasCursor && current != nil {
			cursor = providerSandboxCursor{
				generation: current.GetGeneration(),
				occurredAt: timestampTime(current.GetObservedAt()),
			}
			hasCursor = true
		}
		if hasCursor && !acceptSandboxBootstrap(cursor, current, sandbox) {
			if current != nil {
				nextSandboxes[sandboxID] = cloneProjectedSandbox(current)
			}
			continue
		}
		next := mergeDurableSandboxDisposition(current, sandbox)
		if confirmedAt.IsZero() {
			confirmedAt = s.clock.Now()
		}
		stampSandboxConfirmation(current, next, confirmedAt)
		nextSandboxes[sandboxID] = next
		if !hasCursor || sandbox.GetGeneration() > cursor.generation {
			cursor = providerSandboxCursor{
				generation: sandbox.GetGeneration(),
				eventID:    "bootstrap-sandbox",
			}
		}
		cursor.occurredAt = timestampTime(sandbox.GetObservedAt())
		rememberSandboxCursorIdentity(&cursor)
		nextCursors[sandboxID] = cursor
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
	if event.GetOccurredAt() != nil && event.GetOccurredAt().CheckValid() != nil {
		return nil, false, errors.New("state: sandbox event has invalid occurred_at")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.providerProjection.sandboxes[event.GetSandboxId()]
	if !acceptSandboxCursorLocked(&s.providerProjection, current, event) {
		current := s.providerProjection.sandboxes[event.GetSandboxId()]
		return cloneProjectedSandbox(current), false, nil
	}

	next := &tgsrlv1.Sandbox{
		SandboxId:       event.GetSandboxId(),
		RunId:           event.GetRunId(),
		JobId:           event.GetJobId(),
		TraceId:         event.GetTraceId(),
		State:           event.GetState(),
		Generation:      event.GetGeneration(),
		Binding:         cloneProjectedBinding(event.GetBinding()),
		SafePoint:       event.GetSafePoint(),
		ObservedAt:      cloneTimestamp(event.GetOccurredAt()),
		DataKind:        event.GetDataKind(),
		SemanticContext: cloneSemanticEnvelope(event.GetSemanticContext()),
	}
	if current != nil {
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
		if event.SafePoint != nil {
			merged.SafePoint = event.GetSafePoint()
		}
		if event.Share != nil {
			merged.Share = event.GetShare()
		}
		if event.Priority != nil {
			merged.Priority = event.GetPriority()
		}
		if event.Offloaded != nil {
			merged.Offloaded = event.GetOffloaded()
		}
		merged.ObservedAt = cloneTimestamp(next.GetObservedAt())
		merged.DataKind = next.GetDataKind()
		if next.GetSemanticContext() != nil {
			merged.SemanticContext = cloneSemanticEnvelope(next.GetSemanticContext())
		} else if next.GetGeneration() != current.GetGeneration() {
			merged.SemanticContext = nil
		}
		next = merged
	}
	if current == nil {
		if event.Share != nil {
			next.Share = event.GetShare()
		}
		if event.Priority != nil {
			next.Priority = event.GetPriority()
		}
		if event.Offloaded != nil {
			next.Offloaded = event.GetOffloaded()
		}
	}
	stampSandboxConfirmation(current, next, s.clock.Now())

	working := cloneSnapshot(s.snapshot)
	retired := retireRunFromTerminalEventLocked(s.intents, s.providerProjection.sandboxes, working, event)
	if proto.Equal(current, next) && !retired {
		return cloneProjectedSandbox(current), false, nil
	}
	s.providerProjection.sandboxes[event.GetSandboxId()] = cloneProjectedSandbox(next)
	s.commitSnapshotLocked(working)
	return cloneProjectedSandbox(next), true, nil
}

func retireRunFromTerminalEventLocked(intents map[intentKey]*tgsrlv1.SchedulingIntent, observed map[string]*tgsrlv1.Sandbox, working *tgsrlv1.ClusterSnapshot, event *tgsrlv1.SandboxEvent) bool {
	if working == nil || event == nil || !terminalRuntimeState(event.GetState()) ||
		event.GetRunId() == "" || event.GetSandboxId() == "" || event.GetGeneration() == 0 {
		return false
	}
	matched := false
	for _, allocation := range working.GetAllocations() {
		if allocation.GetRunId() == event.GetRunId() && allocationMatchesTerminalEvent(allocation, event) {
			matched = true
			break
		}
	}
	if !matched {
		return false
	}
	if protocolmeta.HasRunRetirementDisposition(event.GetSemanticContext()) {
		if !protocolmeta.RetiresRun(event) || !allRunAllocationsTerminal(working.GetAllocations(), observed, event) {
			return false
		}
	} else if !allRunAllocationsTerminal(working.GetAllocations(), observed, event) {
		return false
	}
	kept := working.Allocations[:0]
	changed := false
	for _, allocation := range working.GetAllocations() {
		if allocation.GetRunId() != event.GetRunId() {
			kept = append(kept, allocation)
			continue
		}
		for _, deviceID := range allocation.GetDeviceIds() {
			if device := findDevice(working.GetDevices(), deviceID); device != nil {
				addResourcesCapped(device.Allocatable, allocation.GetResources(), device.GetCapacity())
			}
		}
		changed = true
	}
	working.Allocations = kept
	for key, intent := range intents {
		if intent.GetRunId() == event.GetRunId() {
			delete(intents, key)
			changed = true
		}
	}
	keptPending := working.PendingUnits[:0]
	for _, unit := range working.GetPendingUnits() {
		if unit.GetRunId() == event.GetRunId() {
			changed = true
			continue
		}
		keptPending = append(keptPending, unit)
	}
	working.PendingUnits = keptPending
	if !changed {
		return false
	}
	return true
}

func allRunAllocationsTerminal(allocations []*tgsrlv1.Allocation, observed map[string]*tgsrlv1.Sandbox, current *tgsrlv1.SandboxEvent) bool {
	matched := 0
	for _, allocation := range allocations {
		if allocation.GetRunId() != current.GetRunId() {
			continue
		}
		matched++
		if allocationMatchesTerminalEvent(allocation, current) {
			continue
		}
		sandbox := observed[allocation.GetSandboxId()]
		if !sandboxMatchesAllocation(allocation, sandbox) || !terminalRuntimeState(sandbox.GetState()) {
			return false
		}
	}
	return matched > 0
}

// ReconcileProviderSandboxes repairs the durable allocation ledger against a
// successfully-read authoritative provider inventory. Terminal sandboxes and
// expired allocations missing from that inventory no longer hold provider
// resources. Active intents are requeued; explicitly retired Runs lose their
// intent and pending work as well.
func (s *Store) ReconcileProviderSandboxes(sandboxes []*tgsrlv1.Sandbox) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	retiredRuns := make(map[string]struct{})
	retirementRequestedRuns := make(map[string]struct{})
	nonRetiredRuns := make(map[string]struct{})
	live := make(map[string]*tgsrlv1.Sandbox, len(sandboxes))
	observed := make(map[string]*tgsrlv1.Sandbox, len(s.providerProjection.sandboxes)+len(sandboxes))
	for sandboxID, sandbox := range s.providerProjection.sandboxes {
		observed[sandboxID] = sandbox
	}
	for _, sandbox := range sandboxes {
		if sandbox == nil || sandbox.GetSandboxId() == "" {
			continue
		}
		live[sandbox.GetSandboxId()] = sandbox
		observed[sandbox.GetSandboxId()] = mergeDurableSandboxDisposition(observed[sandbox.GetSandboxId()], sandbox)
	}
	// Classify only the final merged view so a newer live observation for a
	// sandbox overrides its durable pre-restart projection.
	for _, sandbox := range observed {
		if terminalRuntimeState(sandbox.GetState()) && sandbox.GetRunId() != "" && protocolmeta.EnvelopeRetiresRun(sandbox.GetSemanticContext()) {
			retirementRequestedRuns[sandbox.GetRunId()] = struct{}{}
		} else if terminalRuntimeState(sandbox.GetState()) && sandbox.GetRunId() != "" && protocolmeta.HasRunRetirementDisposition(sandbox.GetSemanticContext()) {
			nonRetiredRuns[sandbox.GetRunId()] = struct{}{}
		}
	}
	for runID := range runAllocationCounts(s.snapshot.GetAllocations()) {
		_, retirementRequested := retirementRequestedRuns[runID]
		_, explicitlyNotRetired := nonRetiredRuns[runID]
		allTerminal := allRunAllocationsTerminal(s.snapshot.GetAllocations(), observed, &tgsrlv1.SandboxEvent{RunId: runID})
		if allTerminal && (retirementRequested || !explicitlyNotRetired) {
			retiredRuns[runID] = struct{}{}
		}
	}
	now := s.clock.Now()
	working := cloneSnapshot(s.snapshot)
	kept := working.Allocations[:0]
	retryCandidates := make([]*tgsrlv1.Allocation, 0)
	changed := false
	for _, allocation := range working.GetAllocations() {
		sandbox := live[allocation.GetSandboxId()]
		_, retireRun := retiredRuns[allocation.GetRunId()]
		_, retirementRequested := retirementRequestedRuns[allocation.GetRunId()]
		if retirementRequested && !retireRun {
			// A whole-Run lifecycle stop is still converging. Retain every
			// allocation until all siblings are terminal so the scheduler cannot
			// replace a unit or reuse capacity while the execution layer drains.
			kept = append(kept, allocation)
			continue
		}
		providerReleased := sandboxMatchesAllocation(allocation, sandbox) && terminalRuntimeState(sandbox.GetState()) || sandbox == nil && allocationExpired(allocation, now)
		if !retireRun && !providerReleased {
			kept = append(kept, allocation)
			continue
		}
		for _, deviceID := range allocation.GetDeviceIds() {
			if device := findDevice(working.GetDevices(), deviceID); device != nil {
				addResourcesCapped(device.Allocatable, allocation.GetResources(), device.GetCapacity())
			}
		}
		if !retireRun && !allocationHasExplicitReleaseDisposition(allocation, observed) {
			retryCandidates = append(retryCandidates, allocation)
		}
		changed = true
	}
	working.Allocations = kept
	for _, allocation := range retryCandidates {
		requeueAllocation(working, s.intents, allocation, now)
	}
	for key, intent := range s.intents {
		if _, retire := retiredRuns[intent.GetRunId()]; retire {
			delete(s.intents, key)
			changed = true
		}
	}
	keptPending := working.PendingUnits[:0]
	for _, unit := range working.GetPendingUnits() {
		if _, retire := retiredRuns[unit.GetRunId()]; retire {
			changed = true
			continue
		}
		keptPending = append(keptPending, unit)
	}
	working.PendingUnits = keptPending
	if changed {
		s.commitSnapshotLocked(working)
	}
	return changed
}

func runAllocationCounts(allocations []*tgsrlv1.Allocation) map[string]int {
	counts := make(map[string]int)
	for _, allocation := range allocations {
		if allocation.GetRunId() != "" {
			counts[allocation.GetRunId()]++
		}
	}
	return counts
}

func terminalRuntimeState(value tgsrlv1.RuntimeState) bool {
	return value == tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED || value == tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED
}

func requeueAllocation(snapshot *tgsrlv1.ClusterSnapshot, intents map[intentKey]*tgsrlv1.SchedulingIntent, allocation *tgsrlv1.Allocation, now time.Time) {
	if snapshot == nil || allocation == nil {
		return
	}
	intent := intents[intentKey{executionID: allocation.GetExecutionId(), stageID: allocation.GetStageId()}]
	if intent == nil || intent.GetVersion() != allocation.GetIntentVersion() || !intent.GetValidUntil().AsTime().After(now) {
		return
	}
	current := liveAllocationCount(snapshot.GetAllocations(), intent)
	for _, unit := range snapshot.GetPendingUnits() {
		if unit.GetExecutionId() == intent.GetExecutionId() && unit.GetStageId() == intent.GetStageId() && unit.GetIntentVersion() == intent.GetVersion() {
			current++
		}
		if unit.GetPendingUnitId() == allocation.GetPendingUnitId() {
			return
		}
	}
	if current >= intent.GetUnitCount() {
		return
	}
	priority := intent.GetPriority()
	if allocation.Priority != nil {
		priority = allocation.GetPriority()
	}
	snapshot.PendingUnits = append(snapshot.PendingUnits, &tgsrlv1.PendingUnit{
		PendingUnitId: allocation.GetPendingUnitId(), ExecutionId: allocation.GetExecutionId(), StageId: allocation.GetStageId(),
		IntentVersion: allocation.GetIntentVersion(), JobId: allocation.GetJobId(), RequestedResources: cloneResourceVector(allocation.GetResources()),
		RequiredCapabilities: cloneCapabilitySet(intent.GetRequiredCapabilities()), Priority: priority, QueuedAt: timestamppb.New(now),
		RunId: allocation.GetRunId(), TraceId: allocation.GetTraceId(), DataKind: allocation.GetDataKind(), RuntimeUnitId: allocation.GetRuntimeUnitId(),
	})
	sortPendingUnits(snapshot.PendingUnits)
}

func sandboxMatchesAllocation(allocation *tgsrlv1.Allocation, sandbox *tgsrlv1.Sandbox) bool {
	if allocation == nil || sandbox == nil || allocation.GetGeneration() != sandbox.GetGeneration() {
		return false
	}
	if allocation.GetSandboxId() != "" {
		return allocation.GetSandboxId() == sandbox.GetSandboxId()
	}
	binding := sandbox.GetBinding()
	return binding != nil && allocation.GetBindingId() != "" && allocation.GetBindingId() == binding.GetBindingId()
}

func allocationMatchesTerminalEvent(allocation *tgsrlv1.Allocation, event *tgsrlv1.SandboxEvent) bool {
	if allocation == nil || event == nil || allocation.GetGeneration() != event.GetGeneration() {
		return false
	}
	if allocation.GetSandboxId() != "" || event.GetSandboxId() != "" {
		return allocation.GetSandboxId() != "" && allocation.GetSandboxId() == event.GetSandboxId()
	}
	binding := event.GetBinding()
	return binding != nil && allocation.GetBindingId() != "" && allocation.GetBindingId() == binding.GetBindingId()
}

func allocationHasExplicitReleaseDisposition(allocation *tgsrlv1.Allocation, observed map[string]*tgsrlv1.Sandbox) bool {
	sandbox := observed[allocation.GetSandboxId()]
	return sandboxMatchesAllocation(allocation, sandbox) && terminalRuntimeState(sandbox.GetState()) &&
		protocolmeta.HasRunRetirementDisposition(sandbox.GetSemanticContext()) &&
		!protocolmeta.EnvelopeRetiresRun(sandbox.GetSemanticContext())
}

func allocationExpired(allocation *tgsrlv1.Allocation, now time.Time) bool {
	expiresAt := allocation.GetExpiresAt()
	return expiresAt != nil && expiresAt.CheckValid() == nil && !now.Before(expiresAt.AsTime())
}

// stampSandboxConfirmation records scheduler receipt time independently from
// observed_at, which remains the provider's source-event time. A confirmation
// of the same generation and state preserves the effective transition time;
// for legacy snapshots that means materializing their observed_at fallback
// before the new observation replaces it.
func stampSandboxConfirmation(current, next *tgsrlv1.Sandbox, confirmedAt time.Time) {
	if next == nil {
		return
	}
	next.LastConfirmedAt = timestamppb.New(confirmedAt)
	if current == nil || current.GetGeneration() != next.GetGeneration() || current.GetState() != next.GetState() {
		next.StateChangedAt = timestamppb.New(confirmedAt)
		return
	}
	next.StateChangedAt = cloneTimestamp(effectiveSandboxStateChangedAt(current))
}

func effectiveSandboxStateChangedAt(sandbox *tgsrlv1.Sandbox) *timestamppb.Timestamp {
	if sandbox == nil {
		return nil
	}
	if sandbox.GetStateChangedAt() != nil {
		return sandbox.GetStateChangedAt()
	}
	return sandbox.GetObservedAt()
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

// ListSandboxes is the context-shaped compatibility read used by service tests
// and restart reconciliation paths. Store authority is fully local, so ctx is
// accepted for interface symmetry but does not block.
func (s *Store) ListSandboxes(_ context.Context) ([]*tgsrlv1.Sandbox, error) {
	return s.ListProjectedSandboxes(), nil
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

func acceptSandboxCursorLocked(projected *providerProjectionState, sandbox *tgsrlv1.Sandbox, event *tgsrlv1.SandboxEvent) bool {
	current, ok := projected.sandboxCursors[event.GetSandboxId()]
	if ok {
		switch {
		case event.GetGeneration() < current.generation:
			return false
		case duplicateSandboxEvent(current, event):
			return false
		case event.GetGeneration() == current.generation && !acceptSameGenerationSandboxEvent(current, sandbox, event):
			return false
		}
	}
	nextRevision := event.GetProviderRevision()
	if ok && event.GetGeneration() == current.generation && nextRevision == 0 {
		nextRevision = current.providerRevision
	}
	projected.sandboxCursors[event.GetSandboxId()] = providerSandboxCursor{
		generation:       event.GetGeneration(),
		providerRevision: nextRevision,
		eventID:          event.GetEventId(),
		idempotencyKey:   event.GetIdempotencyKey(),
		occurredAt:       timestampTime(event.GetOccurredAt()),
	}
	next := projected.sandboxCursors[event.GetSandboxId()]
	if ok && event.GetGeneration() == current.generation {
		next.seenEventIDs = cloneStringSet(current.seenEventIDs)
		next.seenIdempotency = cloneStringSet(current.seenIdempotency)
	}
	rememberSandboxCursorIdentity(&next)
	projected.sandboxCursors[event.GetSandboxId()] = next
	return true
}

func duplicateSandboxEvent(current providerSandboxCursor, event *tgsrlv1.SandboxEvent) bool {
	if event.GetEventId() != "" {
		if event.GetEventId() == current.eventID {
			return true
		}
		_, duplicate := current.seenEventIDs[event.GetEventId()]
		return duplicate
	}
	if event.GetIdempotencyKey() == "" {
		return false
	}
	if event.GetIdempotencyKey() == current.idempotencyKey {
		return true
	}
	_, duplicate := current.seenIdempotency[event.GetIdempotencyKey()]
	return duplicate
}

func rememberSandboxCursorIdentity(cursor *providerSandboxCursor) {
	if cursor.eventID != "" {
		if cursor.seenEventIDs == nil {
			cursor.seenEventIDs = make(map[string]struct{})
		}
		cursor.seenEventIDs[cursor.eventID] = struct{}{}
	}
	if cursor.idempotencyKey != "" {
		if cursor.seenIdempotency == nil {
			cursor.seenIdempotency = make(map[string]struct{})
		}
		cursor.seenIdempotency[cursor.idempotencyKey] = struct{}{}
	}
}

func cloneProviderSandboxCursor(cursor providerSandboxCursor) providerSandboxCursor {
	cursor.seenEventIDs = cloneStringSet(cursor.seenEventIDs)
	cursor.seenIdempotency = cloneStringSet(cursor.seenIdempotency)
	return cursor
}

func cloneStringSet(in map[string]struct{}) map[string]struct{} {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(in))
	for value := range in {
		out[value] = struct{}{}
	}
	return out
}

func acceptSameGenerationSandboxEvent(current providerSandboxCursor, sandbox *tgsrlv1.Sandbox, event *tgsrlv1.SandboxEvent) bool {
	incomingRevision := event.GetProviderRevision()
	if incomingRevision > 0 {
		switch {
		case current.providerRevision == 0:
			return true
		case incomingRevision < current.providerRevision:
			return false
		case incomingRevision > current.providerRevision:
			return true
		}
	}

	// Equal-revision events use occurrence time to order distinct transitions.
	// Revision-zero events use the same fallback because they predate provider
	// revision fencing. In either case the timestamp must advance and the event
	// must not regress the projected lifecycle state.
	incomingOccurredAt := timestampTime(event.GetOccurredAt())
	if incomingOccurredAt.IsZero() || !incomingOccurredAt.After(current.occurredAt) {
		return false
	}
	return sandbox == nil || !runtimeStateRegresses(sandbox.GetState(), event.GetState())
}

func acceptSandboxBootstrap(current providerSandboxCursor, sandbox, incoming *tgsrlv1.Sandbox) bool {
	switch {
	case incoming.GetGeneration() < current.generation:
		return false
	case incoming.GetGeneration() > current.generation:
		return true
	}
	incomingObservedAt := timestampTime(incoming.GetObservedAt())
	if incomingObservedAt.IsZero() || !incomingObservedAt.After(current.occurredAt) {
		return false
	}
	return sandbox == nil || !runtimeStateRegresses(sandbox.GetState(), incoming.GetState())
}

func runtimeStateRegresses(current, next tgsrlv1.RuntimeState) bool {
	if current == next {
		return false
	}
	if (current == tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED || current == tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING) && next == tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		return false
	}
	rank := map[tgsrlv1.RuntimeState]int{
		tgsrlv1.RuntimeState_RUNTIME_STATE_REQUESTED:  1,
		tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND:      2,
		tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING:    3,
		tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED:     4,
		tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING:   5,
		tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED:     6,
		tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED: 7,
	}
	currentRank, currentKnown := rank[current]
	nextRank, nextKnown := rank[next]
	return currentKnown && (!nextKnown || nextRank < currentRank)
}

func timestampTime(timestamp *timestamppb.Timestamp) time.Time {
	if timestamp == nil || timestamp.CheckValid() != nil {
		return time.Time{}
	}
	return timestamp.AsTime()
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

// mergeDurableSandboxDisposition keeps lifecycle causality that a resource
// provider cannot reconstruct. It is safe only for the same terminal
// generation; a new generation or a non-terminal readback starts clean.
func mergeDurableSandboxDisposition(current, incoming *tgsrlv1.Sandbox) *tgsrlv1.Sandbox {
	next := cloneProjectedSandbox(incoming)
	if next == nil || current == nil || current.GetGeneration() != next.GetGeneration() ||
		current.GetRunId() == "" || current.GetRunId() != next.GetRunId() ||
		!terminalRuntimeState(current.GetState()) || !terminalRuntimeState(next.GetState()) ||
		protocolmeta.HasRunRetirementDisposition(next.GetSemanticContext()) ||
		!protocolmeta.HasRunRetirementDisposition(current.GetSemanticContext()) {
		return next
	}
	if next.SemanticContext == nil {
		next.SemanticContext = &tgsrlv1.SemanticEnvelope{}
	}
	if next.SemanticContext.Attributes == nil {
		next.SemanticContext.Attributes = make(map[string]string)
	}
	next.SemanticContext.Attributes[protocolmeta.RunRetirementAttribute] = current.GetSemanticContext().GetAttributes()[protocolmeta.RunRetirementAttribute]
	return next
}

func cloneSemanticEnvelope(envelope *tgsrlv1.SemanticEnvelope) *tgsrlv1.SemanticEnvelope {
	if envelope == nil {
		return nil
	}
	return proto.Clone(envelope).(*tgsrlv1.SemanticEnvelope)
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
