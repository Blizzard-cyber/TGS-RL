package nvidia

import (
	"context"
	"fmt"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	base "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (p *Provider) ID(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return p.providerID, nil
}

func (p *Provider) Health(ctx context.Context) (*base.HealthStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return &base.HealthStatus{
		ProviderID: p.providerID,
		Source:     ProviderID,
		Healthy:    p.healthy,
		Reason:     p.healthReason,
		CheckedAt:  p.now(),
	}, nil
}

func (p *Provider) Capabilities(ctx context.Context) (*tgsrlv1.CapabilitySet, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return cloneCapabilities(p.capabilities), nil
}

func (p *Provider) Snapshot(ctx context.Context) (*tgsrlv1.ClusterSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked(), nil
}

func (p *Provider) ListDevices(ctx context.Context) ([]*tgsrlv1.Device, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return cloneDevices(p.devices), nil
}

func (p *Provider) ListSandboxes(ctx context.Context) ([]base.Sandbox, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]base.Sandbox, 0, len(p.sandboxes))
	for _, sandbox := range p.sandboxes {
		result = append(result, cloneSandbox(sandbox))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SandboxID < result[j].SandboxID })
	return result, nil
}

func (p *Provider) GetSandbox(ctx context.Context, sandboxID string) (base.Sandbox, error) {
	if err := ctx.Err(); err != nil {
		return base.Sandbox{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	sandbox, ok := p.sandboxes[sandboxID]
	if !ok {
		return base.Sandbox{}, fmt.Errorf("%w: sandbox %q", base.ErrNotFound, sandboxID)
	}
	return cloneSandbox(sandbox), nil
}

func (p *Provider) WatchResources(ctx context.Context, cursor uint64) (<-chan base.WatchedResourceEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := validateResourceCursor(cursor, p.resourceLog); err != nil {
		return nil, err
	}
	id := p.nextWatchID + 1
	p.nextWatchID = id
	ch := make(chan base.WatchedResourceEvent, p.retention+1)
	for _, entry := range p.resourceLog {
		if entry.Cursor > cursor {
			ch <- cloneWatchedResourceEvent(entry)
		}
	}
	p.resourceSubs[id] = ch
	go p.closeResourceWatcherOnDone(ctx, id)
	return ch, nil
}

func (p *Provider) WatchSandboxes(ctx context.Context, cursor uint64) (<-chan base.WatchedSandboxEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := validateSandboxCursor(cursor, p.sandboxLog); err != nil {
		return nil, err
	}
	id := p.nextWatchID + 1
	p.nextWatchID = id
	ch := make(chan base.WatchedSandboxEvent, p.retention+1)
	for _, entry := range p.sandboxLog {
		if entry.Cursor > cursor {
			ch <- cloneWatchedSandboxEvent(entry)
		}
	}
	p.sandboxSubs[id] = ch
	go p.closeSandboxWatcherOnDone(ctx, id)
	return ch, nil
}

func (p *Provider) ReconcilePlan(ctx context.Context, plan *tgsrlv1.PlacementPlan) (*base.PlanRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plan == nil || plan.GetPlanId() == "" {
		return nil, fmt.Errorf("%w: plan_id is required", base.ErrInvalidArgument)
	}
	p.mu.Lock()
	record := p.plans[plan.GetPlanId()]
	if record == nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("%w: plan %q", base.ErrNotFound, plan.GetPlanId())
	}
	if !proto.Equal(record.Plan, plan) {
		p.mu.Unlock()
		return nil, fmt.Errorf("%w: plan_id %q was reused with different content", base.ErrIdempotencyConflict, plan.GetPlanId())
	}
	state := p.driverStateLocked()
	p.mu.Unlock()
	reconciled, err := p.driver.Reconcile(ctx, state, plan)
	if err != nil {
		return nil, normalizeDriverError(firstPlanAction(plan), err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applyReconcileStateLocked(reconciled)
	record = p.plans[plan.GetPlanId()]
	return clonePlanRecord(record), nil
}

func (p *Provider) RecoverInFlightPlans(ctx context.Context) ([]*base.PlanRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var records []*base.PlanRecord
	for _, record := range p.plans {
		if record.Status == base.PlanStatusInFlight {
			records = append(records, clonePlanRecord(record))
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Plan.GetPlanId() < records[j].Plan.GetPlanId() })
	return records, nil
}

func unavailableDevices(reason string) []*tgsrlv1.Device {
	return []*tgsrlv1.Device{{
		DeviceId:     "nvidia-unavailable",
		Kind:         tgsrlv1.DeviceKind_DEVICE_KIND_GPU,
		Health:       tgsrlv1.DeviceHealth_DEVICE_HEALTH_UNAVAILABLE,
		Capacity:     &tgsrlv1.ResourceVector{},
		Allocatable:  &tgsrlv1.ResourceVector{},
		Capabilities: unavailableCapabilities(reason),
		Labels: map[string]string{
			"provider": ProviderID,
			"status":   "unavailable",
			"reason":   reason,
		},
	}}
}

func (p *Provider) snapshotLocked() *tgsrlv1.ClusterSnapshot {
	return &tgsrlv1.ClusterSnapshot{
		SnapshotId: "nvidia-snapshot-" + fmt.Sprint(p.revision),
		Revision:   p.revision,
		ObservedAt: timestamppb.New(p.now()),
		Devices:    cloneDevices(p.devices),
		Annotations: map[string]string{
			"provider":        ProviderID,
			"provider_source": ProviderID,
		},
	}
}

func (p *Provider) publishCapabilityRefreshLocked() {
	event := &tgsrlv1.ResourceEvent{
		EventId:          fmt.Sprintf("%s-resource-%d", ProviderID, p.resourceSeq+1),
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_CAPABILITY_REFRESHED,
		ProviderRevision: p.revision,
		Provider:         ProviderID,
		ObservedAt:       timestamppb.New(p.now()),
		IdempotencyKey:   fmt.Sprintf("%s-capabilities-%d", ProviderID, p.revision),
		Capabilities:     cloneCapabilities(p.capabilities),
	}
	p.publishResourceEventLocked(event)
}

func (p *Provider) publishSnapshotLocked(eventType tgsrlv1.ResourceEventType) {
	event := &tgsrlv1.ResourceEvent{
		EventId:          fmt.Sprintf("%s-resource-%d", ProviderID, p.resourceSeq+1),
		EventType:        eventType,
		ProviderRevision: p.revision,
		Provider:         ProviderID,
		ObservedAt:       timestamppb.New(p.now()),
		IdempotencyKey:   fmt.Sprintf("%s-snapshot-%d", ProviderID, p.revision),
		Snapshot:         p.snapshotLocked(),
	}
	p.publishResourceEventLocked(event)
}

func (p *Provider) publishSandboxSnapshotLocked(sandbox base.Sandbox, detail string) {
	event := &tgsrlv1.SandboxEvent{
		EventId:    fmt.Sprintf("%s-sandbox-%d", ProviderID, p.sandboxSeq+1),
		EventType:  sandboxStateToEventType(sandbox.State),
		SandboxId:  sandbox.SandboxID,
		Generation: sandbox.Generation,
		State:      sandboxStateToRuntimeState(sandbox.State),
		Binding:    cloneBinding(sandbox.Binding),
		SafePoint:  sandbox.SafePoint,
		Detail:     detail,
		OccurredAt: timestamppb.New(nonZeroTime(sandbox.UpdatedAt, p.now())),
	}
	p.publishSandboxEventLocked(event)
}

func (p *Provider) publishResourceEventLocked(event *tgsrlv1.ResourceEvent) {
	p.resourceSeq++
	entry := base.WatchedResourceEvent{Cursor: p.resourceSeq, Event: cloneResourceEvent(event)}
	p.resourceLog = appendResourceEvent(p.resourceLog, entry, p.retention)
	for _, subscriber := range p.resourceSubs {
		subscriber <- cloneWatchedResourceEvent(entry)
	}
}

func (p *Provider) publishSandboxEventLocked(event *tgsrlv1.SandboxEvent) {
	p.sandboxSeq++
	entry := base.WatchedSandboxEvent{Cursor: p.sandboxSeq, Event: cloneRuntimeSandboxEvent(event)}
	p.sandboxLog = appendSandboxEvent(p.sandboxLog, entry, p.retention)
	for _, subscriber := range p.sandboxSubs {
		subscriber <- cloneWatchedSandboxEvent(entry)
	}
}

func (p *Provider) closeResourceWatcherOnDone(ctx context.Context, id uint64) {
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

func (p *Provider) closeSandboxWatcherOnDone(ctx context.Context, id uint64) {
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

func validateResourceCursor(cursor uint64, log []base.WatchedResourceEvent) error {
	if cursor == 0 || len(log) == 0 {
		return nil
	}
	oldest := log[0].Cursor
	if cursor < oldest {
		return fmt.Errorf("%w: cursor %d predates retained cursor %d", base.ErrCursorExpired, cursor, oldest)
	}
	return nil
}

func validateSandboxCursor(cursor uint64, log []base.WatchedSandboxEvent) error {
	if cursor == 0 || len(log) == 0 {
		return nil
	}
	oldest := log[0].Cursor
	if cursor < oldest {
		return fmt.Errorf("%w: cursor %d predates retained cursor %d", base.ErrCursorExpired, cursor, oldest)
	}
	return nil
}
