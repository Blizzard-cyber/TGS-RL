package provider

import (
	"context"
	"errors"
	"fmt"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Capabilities returns a clone of the currently advertised mock capabilities.
func (p *MockResourceProvider) Capabilities(ctx context.Context) (*tgsrlv1.CapabilitySet, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return cloneCapabilities(p.capabilities), nil
}

// Snapshot returns a cloned scheduler snapshot of the logical devices.
func (p *MockResourceProvider) Snapshot(ctx context.Context) (*tgsrlv1.ClusterSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked(), nil
}

// ListDevices returns cloned logical devices.
func (p *MockResourceProvider) ListDevices(ctx context.Context) ([]*tgsrlv1.Device, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return cloneDevices(p.devices), nil
}

// ListSandboxes returns stable-ID-ordered cloned sandbox state.
func (p *MockResourceProvider) ListSandboxes(ctx context.Context) ([]Sandbox, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]Sandbox, 0, len(p.sandboxes))
	for _, sandbox := range p.sandboxes {
		result = append(result, cloneSandbox(sandbox))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SandboxID < result[j].SandboxID })
	return result, nil
}

// GetSandbox returns a cloned sandbox state.
func (p *MockResourceProvider) GetSandbox(ctx context.Context, sandboxID string) (Sandbox, error) {
	if err := ctx.Err(); err != nil {
		return Sandbox{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	sandbox, ok := p.sandboxes[sandboxID]
	if !ok {
		return Sandbox{}, fmt.Errorf("%w: sandbox %q", ErrNotFound, sandboxID)
	}
	return cloneSandbox(sandbox), nil
}

// ID returns the stable provider identity for logs, health, and watch events.
func (p *MockResourceProvider) ID(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.providerID, nil
}

// Health returns a conservative health summary for scheduler callers.
func (p *MockResourceProvider) Health(ctx context.Context) (*HealthStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	status := &HealthStatus{
		ProviderID: p.providerID,
		Source:     p.source,
		Healthy:    true,
		CheckedAt:  p.now(),
	}
	for _, device := range p.devices {
		if device.GetHealth() != tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY {
			status.Healthy = false
			status.Reason = "device health is degraded"
			break
		}
	}
	return status, nil
}

// WatchResources replays retained resource events after cursor and then tails
// future events until the context ends.
func (p *MockResourceProvider) WatchResources(ctx context.Context, cursor uint64) (<-chan WatchedResourceEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := validateWatchCursor(cursor, p.resourceLog); err != nil {
		return nil, err
	}
	id := p.nextWatchID + 1
	p.nextWatchID = id
	ch := make(chan WatchedResourceEvent, p.retention+1)
	for _, entry := range p.resourceLog {
		if entry.Cursor > cursor {
			ch <- cloneWatchedResourceEvent(entry)
		}
	}
	p.resourceSubs[id] = ch
	go p.closeResourceWatcherOnDone(ctx, id)
	return ch, nil
}

// WatchSandboxes replays retained sandbox events after cursor and then tails
// future events until the context ends.
func (p *MockResourceProvider) WatchSandboxes(ctx context.Context, cursor uint64) (<-chan WatchedSandboxEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := validateWatchCursor(cursor, p.sandboxLog); err != nil {
		return nil, err
	}
	id := p.nextWatchID + 1
	p.nextWatchID = id
	ch := make(chan WatchedSandboxEvent, p.retention+1)
	for _, entry := range p.sandboxLog {
		if entry.Cursor > cursor {
			ch <- cloneWatchedSandboxEvent(entry)
		}
	}
	p.sandboxSubs[id] = ch
	go p.closeSandboxWatcherOnDone(ctx, id)
	return ch, nil
}

// ReconcilePlan returns the provider view of the supplied plan without
// re-executing it.
func (p *MockResourceProvider) ReconcilePlan(ctx context.Context, plan *tgsrlv1.PlacementPlan) (*PlanRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plan == nil || plan.GetPlanId() == "" {
		return nil, fmt.Errorf("%w: plan_id is required", ErrInvalidArgument)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	record := p.planRecords[plan.GetPlanId()]
	if record == nil {
		return nil, fmt.Errorf("%w: plan %q", ErrNotFound, plan.GetPlanId())
	}
	if cached := p.planRequests[plan.GetPlanId()]; cached != nil && !proto.Equal(cached, plan) {
		return nil, fmt.Errorf("%w: plan_id %q was reused with different content", ErrIdempotencyConflict, plan.GetPlanId())
	}
	return clonePlanRecord(record), nil
}

// RecoverInFlightPlans returns the bounded set of plans still marked in-flight.
func (p *MockResourceProvider) RecoverInFlightPlans(ctx context.Context) ([]*PlanRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var records []*PlanRecord
	for _, record := range p.planRecords {
		if record.Status == PlanStatusInFlight {
			records = append(records, clonePlanRecord(record))
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Plan.GetPlanId() < records[j].Plan.GetPlanId()
	})
	return records, nil
}

func (p *MockResourceProvider) snapshotLocked() *tgsrlv1.ClusterSnapshot {
	return &tgsrlv1.ClusterSnapshot{
		SnapshotId: "mock-snapshot-" + fmt.Sprint(p.revision),
		Revision:   p.revision,
		ObservedAt: timestamppb.New(p.now()),
		Devices:    cloneDevices(p.devices),
		Annotations: map[string]string{
			"provider":            p.providerID,
			"provider_source":     p.source,
			"tgsrl.io/safe-point": "true",
		},
	}
}

func (p *MockResourceProvider) sortedSandboxIDsLocked() []string {
	ids := make([]string, 0, len(p.sandboxes))
	for sandboxID := range p.sandboxes {
		ids = append(ids, sandboxID)
	}
	sort.Strings(ids)
	return ids
}

func (p *MockResourceProvider) closeResourceWatcherOnDone(ctx context.Context, id uint64) {
	<-ctx.Done()
	p.mu.Lock()
	defer p.mu.Unlock()
	ch, ok := p.resourceSubs[id]
	if !ok {
		return
	}
	delete(p.resourceSubs, id)
	close(ch)
}

func (p *MockResourceProvider) closeSandboxWatcherOnDone(ctx context.Context, id uint64) {
	<-ctx.Done()
	p.mu.Lock()
	defer p.mu.Unlock()
	ch, ok := p.sandboxSubs[id]
	if !ok {
		return
	}
	delete(p.sandboxSubs, id)
	close(ch)
}

func (entry WatchedResourceEvent) GetCursor() uint64 { return entry.Cursor }
func (entry WatchedSandboxEvent) GetCursor() uint64  { return entry.Cursor }

func validateWatchCursor[T interface{ GetCursor() uint64 }](cursor uint64, log []T) error {
	if cursor == 0 || len(log) == 0 {
		return nil
	}
	oldest := log[0].GetCursor()
	if cursor < oldest {
		return fmt.Errorf("%w: cursor %d predates retained cursor %d", ErrCursorExpired, cursor, oldest)
	}
	return nil
}

func (p *MockResourceProvider) publishCapabilityRefreshLocked() {
	event := &tgsrlv1.ResourceEvent{
		EventId:          fmt.Sprintf("%s-resource-%d", p.providerID, p.resourceSeq+1),
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_CAPABILITY_REFRESHED,
		ProviderRevision: p.revision,
		Provider:         p.providerID,
		ObservedAt:       timestamppb.New(p.now()),
		IdempotencyKey:   fmt.Sprintf("%s-capabilities-%d", p.providerID, p.revision),
		Capabilities:     cloneCapabilities(p.capabilities),
	}
	p.publishResourceEventLocked(event)
}

func (p *MockResourceProvider) publishSnapshotLocked(eventType tgsrlv1.ResourceEventType) {
	event := &tgsrlv1.ResourceEvent{
		EventId:          fmt.Sprintf("%s-resource-%d", p.providerID, p.resourceSeq+1),
		EventType:        eventType,
		ProviderRevision: p.revision,
		Provider:         p.providerID,
		ObservedAt:       timestamppb.New(p.now()),
		IdempotencyKey:   fmt.Sprintf("%s-snapshot-%d", p.providerID, p.revision),
		Snapshot:         p.snapshotLocked(),
	}
	p.publishResourceEventLocked(event)
}

func (p *MockResourceProvider) publishSandboxSnapshotLocked(sandbox Sandbox, detail string) {
	occurredAt := sandbox.UpdatedAt
	if occurredAt.IsZero() {
		occurredAt = p.now()
	}
	nextRevision := p.revision
	if nextRevision == 0 {
		nextRevision = p.sandboxSeq + 1
	}
	share, priority, safePoint, offloaded := sandbox.Share, sandbox.Priority, sandbox.SafePoint, sandbox.Offloaded
	event := &tgsrlv1.SandboxEvent{
		EventId:          fmt.Sprintf("%s-sandbox-%d", p.providerID, p.sandboxSeq+1),
		EventType:        sandboxStateToEventType(sandbox.State),
		SandboxId:        sandbox.SandboxID,
		Generation:       sandbox.Generation,
		State:            sandboxStateToRuntimeState(sandbox.State),
		Binding:          cloneBinding(sandbox.Binding),
		SemanticContext:  cloneSemanticEnvelope(sandbox.SemanticContext),
		Share:            &share,
		Priority:         &priority,
		SafePoint:        &safePoint,
		Offloaded:        &offloaded,
		Detail:           detail,
		OccurredAt:       timestamppb.New(occurredAt),
		ProviderRevision: nextRevision,
		IdempotencyKey:   fmt.Sprintf("%s-sandbox-%s-%d", p.providerID, sandbox.SandboxID, nextRevision),
	}
	if record := latestPlanForSandbox(p.planRecords, sandbox.SandboxID); record != nil {
		event.DecisionId = record.Plan.GetDecisionId()
		event.PlanId = record.Plan.GetPlanId()
		for _, result := range record.Results {
			if result != nil && result.GetObservedRevision() == nextRevision {
				event.ActionId = result.GetActionId()
				event.IdempotencyKey = result.GetIdempotencyKey()
				break
			}
		}
	}
	p.publishSandboxEventLocked(event)
}

func latestPlanForSandbox(records map[string]*PlanRecord, sandboxID string) *PlanRecord {
	var latest *PlanRecord
	for _, record := range records {
		if record == nil || record.Plan == nil {
			continue
		}
		for _, action := range record.Plan.GetActions() {
			if actionSandboxID(action) != sandboxID {
				continue
			}
			if latest == nil || record.ObservedRevision > latest.ObservedRevision || record.ObservedRevision == latest.ObservedRevision && record.Plan.GetPlanId() > latest.Plan.GetPlanId() {
				latest = record
			}
		}
	}
	return latest
}

func (p *MockResourceProvider) publishResourceEventLocked(event *tgsrlv1.ResourceEvent) {
	p.resourceSeq++
	entry := WatchedResourceEvent{Cursor: p.resourceSeq, Event: cloneProto(event)}
	p.resourceLog = appendRetained(p.resourceLog, entry, p.retention)
	for _, subscriber := range p.resourceSubs {
		subscriber <- cloneWatchedResourceEvent(entry)
	}
}

func (p *MockResourceProvider) publishSandboxEventLocked(event *tgsrlv1.SandboxEvent) {
	p.sandboxSeq++
	entry := WatchedSandboxEvent{Cursor: p.sandboxSeq, Event: cloneProto(event)}
	p.sandboxLog = appendRetained(p.sandboxLog, entry, p.retention)
	for _, subscriber := range p.sandboxSubs {
		subscriber <- cloneWatchedSandboxEvent(entry)
	}
}

func appendRetained[T any](values []T, value T, retention int) []T {
	values = append(values, value)
	if len(values) <= retention {
		return values
	}
	cut := len(values) - retention
	trimmed := make([]T, retention)
	copy(trimmed, values[cut:])
	return trimmed
}

func cloneProto[T proto.Message](message T) T {
	if any(message) == nil {
		return message
	}
	return proto.Clone(message).(T)
}

func cloneWatchedResourceEvent(event WatchedResourceEvent) WatchedResourceEvent {
	event.Event = cloneProto(event.Event)
	return event
}

func cloneWatchedSandboxEvent(event WatchedSandboxEvent) WatchedSandboxEvent {
	event.Event = cloneProto(event.Event)
	return event
}

func clonePlacementPlan(plan *tgsrlv1.PlacementPlan) *tgsrlv1.PlacementPlan {
	if plan == nil {
		return nil
	}
	return proto.Clone(plan).(*tgsrlv1.PlacementPlan)
}

func clonePlanRecord(record *PlanRecord) *PlanRecord {
	if record == nil {
		return nil
	}
	return &PlanRecord{
		Plan:             clonePlacementPlan(record.Plan),
		Status:           record.Status,
		Results:          cloneActionResults(record.Results),
		ErrorCode:        record.ErrorCode,
		ErrorMessage:     record.ErrorMessage,
		ObservedRevision: record.ObservedRevision,
		UpdatedAt:        record.UpdatedAt,
	}
}

func clonePlanRecords(records []*PlanRecord) []*PlanRecord {
	clones := make([]*PlanRecord, len(records))
	for i, record := range records {
		clones[i] = clonePlanRecord(record)
	}
	return clones
}

func (p *MockResourceProvider) setPlanRecordLocked(plan *tgsrlv1.PlacementPlan, status PlanStatus, results []*tgsrlv1.ActionResult, err error) {
	if plan == nil || plan.GetPlanId() == "" {
		return
	}
	record := &PlanRecord{
		Plan:             clonePlacementPlan(plan),
		Status:           status,
		Results:          cloneActionResults(results),
		ObservedRevision: p.revision,
		UpdatedAt:        p.now(),
	}
	if err != nil {
		record.ErrorMessage = err.Error()
		var providerError *Error
		if errors.As(err, &providerError) {
			record.ErrorCode = providerError.Code
		}
	}
	p.planRecords[plan.GetPlanId()] = record
	p.planRequests[plan.GetPlanId()] = clonePlacementPlan(plan)
}

func (p *MockResourceProvider) setActionPlanLocked(action *tgsrlv1.Action) {
	if action == nil || action.GetIdempotencyKey() == "" || action.GetPlanId() == "" {
		return
	}
	p.actionPlans[action.GetIdempotencyKey()] = action.GetPlanId()
}

func (p *MockResourceProvider) updatePlanRecordForActionResultLocked(action *tgsrlv1.Action, result *tgsrlv1.ActionResult, err error) {
	if action == nil || action.GetPlanId() == "" {
		return
	}
	planID := action.GetPlanId()
	record := p.planRecords[planID]
	if record == nil {
		record = &PlanRecord{
			Plan:             &tgsrlv1.PlacementPlan{PlanId: planID, SnapshotRevision: action.GetExpectedSnapshotRevision(), Actions: []*tgsrlv1.Action{cloneAction(action)}},
			Status:           PlanStatusInFlight,
			ObservedRevision: p.revision,
			UpdatedAt:        p.now(),
		}
	} else if record.Plan != nil && len(record.Plan.GetActions()) == 0 {
		record.Plan.Actions = []*tgsrlv1.Action{cloneAction(action)}
	}
	updated := false
	for index, existing := range record.Results {
		if existing != nil && existing.GetActionId() == result.GetActionId() {
			record.Results[index] = cloneActionResult(result)
			updated = true
			break
		}
	}
	if !updated {
		record.Results = append(record.Results, cloneActionResult(result))
	}
	record.ObservedRevision = p.revision
	record.UpdatedAt = p.now()
	if err != nil {
		record.Status = PlanStatusFailed
		record.ErrorMessage = err.Error()
		var providerError *Error
		if errors.As(err, &providerError) {
			record.ErrorCode = providerError.Code
		}
	} else if action.GetPlanId() != "" {
		record.Status = PlanStatusSucceeded
		record.ErrorCode = ""
		record.ErrorMessage = ""
	}
	p.planRecords[planID] = clonePlanRecord(record)
}

func sandboxStateToRuntimeState(state SandboxState) tgsrlv1.RuntimeState {
	switch state {
	case SandboxStateRequested:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_REQUESTED
	case SandboxStateBound:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND
	case SandboxStateRunning:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING
	case SandboxStatePaused:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED
	case SandboxStateSleeping:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING
	case SandboxStateFailed:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED
	case SandboxStateTerminated:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED
	default:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN
	}
}

func sandboxStateToEventType(state SandboxState) tgsrlv1.SandboxEventType {
	switch state {
	case SandboxStateRequested:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_REQUESTED
	case SandboxStateBound:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_BOUND
	case SandboxStateRunning:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_RUNNING
	case SandboxStatePaused:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSED
	case SandboxStateSleeping:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_SLEEPING
	case SandboxStateFailed:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_FAILED
	case SandboxStateTerminated:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_TERMINATED
	default:
		return tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_UNKNOWN
	}
}
