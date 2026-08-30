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

// DriverInventory returns the last detached v2 hardware inventory.
func (p *Provider) DriverInventory(ctx context.Context) (*InventorySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	driver, ok := p.driver.(V2Driver)
	p.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: NVIDIA Driver v2 is not enabled", base.ErrUnsupported)
	}
	return driver.Inventory(), nil
}

// DriverPartitions returns the last detached v2 partition discovery.
func (p *Provider) DriverPartitions(ctx context.Context) (*PartitionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	driver, ok := p.driver.(V2Driver)
	p.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: NVIDIA Driver v2 is not enabled", base.ErrUnsupported)
	}
	return driver.Partitions(), nil
}

// PlanAction returns a v2 dry-run command plan without changing provider state.
func (p *Provider) PlanAction(ctx context.Context, action *tgsrlv1.Action) (*ActionExecution, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	driver, ok := p.driver.(V2Driver)
	state := p.driverStateLocked()
	p.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: NVIDIA Driver v2 is not enabled", base.ErrUnsupported)
	}
	return driver.PlanAction(ctx, state, action)
}

// DriverAuditRecords returns detached v2 audit records.
func (p *Provider) DriverAuditRecords(ctx context.Context) ([]AuditRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	driver, ok := p.driver.(V2Driver)
	p.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: NVIDIA Driver v2 is not enabled", base.ErrUnsupported)
	}
	return driver.AuditRecords(), nil
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

// Rediscover refreshes inventory, capabilities, and recovered runtime state.
func (p *Provider) Rediscover(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	state := p.driverStateLocked()
	p.mu.Unlock()
	reconciled, err := p.driver.Reconcile(ctx, state, nil)
	if err != nil {
		return normalizeDriverError(nil, err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applyReconciledDiscoveryLocked(reconciled)
	return nil
}

func (p *Provider) applyReconciledDiscoveryLocked(reconciled *ReconcileState) {
	if reconciled == nil {
		return
	}
	previousHealthy := p.healthy
	previousReason := p.healthReason
	previousDevices := cloneDevices(p.devices)
	previousCapabilities := cloneCapabilities(p.capabilities)
	previousSandboxes := make(map[string]base.Sandbox, len(p.sandboxes))
	for sandboxID, sandbox := range p.sandboxes {
		previousSandboxes[sandboxID] = cloneSandbox(sandbox)
	}
	p.applyReconcileStateLocked(reconciled)
	capabilitiesChanged := !equalCapabilityContent(previousCapabilities, p.capabilities)
	sandboxesChanged := !equalSandboxMaps(previousSandboxes, p.sandboxes)
	if !sandboxesChanged {
		for sandboxID, previous := range previousSandboxes {
			if current, exists := p.sandboxes[sandboxID]; exists {
				current.UpdatedAt = previous.UpdatedAt
				current.StateChangedAt = previous.StateChangedAt
				p.sandboxes[sandboxID] = current
			}
		}
	}
	changed := previousHealthy != p.healthy || previousReason != p.healthReason || !equalDeviceContent(previousDevices, p.devices) || capabilitiesChanged || sandboxesChanged
	if !changed {
		return
	}
	p.revision++
	if capabilitiesChanged {
		p.publishCapabilityRefreshLocked()
	}
	if reconciled.SandboxesAuthoritative {
		for _, sandboxID := range p.sortedSandboxIDsLocked() {
			if previous, exists := previousSandboxes[sandboxID]; !exists || !equalSandbox(previous, p.sandboxes[sandboxID]) {
				p.publishSandboxSnapshotLocked(p.sandboxes[sandboxID], "authoritative runtime discovery reconciled")
			}
		}
	}
	if reconciled.SandboxesAuthoritative {
		for sandboxID, previous := range previousSandboxes {
			if _, exists := p.sandboxes[sandboxID]; exists {
				continue
			}
			tombstone := cloneSandbox(previous)
			tombstone.State = base.SandboxStateTerminated
			tombstone.UpdatedAt = p.now()
			tombstone.StateChangedAt = tombstone.UpdatedAt
			p.publishSandboxSnapshotLocked(tombstone, "runtime discovery removed sandbox")
		}
	}
	p.publishSnapshotLocked(tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED)
}

func equalCapabilityContent(left, right *tgsrlv1.CapabilitySet) bool {
	left, right = cloneCapabilities(left), cloneCapabilities(right)
	for _, capabilities := range []*tgsrlv1.CapabilitySet{left, right} {
		if capabilities == nil {
			continue
		}
		capabilities.MeasuredAt = nil
		capabilities.Revision = 0
		delete(capabilities.Attributes, "capability_content_digest")
		delete(capabilities.Attributes, "observation_cursor")
		for _, component := range capabilities.ComponentVersions {
			component.ObservedAt = nil
			component.Revision = 0
		}
		for _, evidence := range capabilities.Evidence {
			evidence.ObservedAt = nil
			evidence.Revision = 0
		}
	}
	return proto.Equal(left, right)
}

func equalDeviceContent(left, right []*tgsrlv1.Device) bool {
	left, right = cloneDevices(left), cloneDevices(right)
	for _, devices := range [][]*tgsrlv1.Device{left, right} {
		for _, device := range devices {
			if device == nil || device.Capabilities == nil {
				continue
			}
			device.Capabilities.MeasuredAt = nil
			device.Capabilities.Revision = 0
			delete(device.Capabilities.Attributes, "capability_content_digest")
			delete(device.Capabilities.Attributes, "observation_cursor")
			for _, component := range device.Capabilities.ComponentVersions {
				component.ObservedAt = nil
				component.Revision = 0
			}
			for _, evidence := range device.Capabilities.Evidence {
				evidence.ObservedAt = nil
				evidence.Revision = 0
			}
		}
	}
	return proto.Equal(&tgsrlv1.ClusterSnapshot{Devices: left}, &tgsrlv1.ClusterSnapshot{Devices: right})
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
	nextRevision := p.revision
	if nextRevision == 0 {
		nextRevision = p.sandboxSeq + 1
	}
	share, priority, safePoint, offloaded := sandbox.Share, sandbox.Priority, sandbox.SafePoint, sandbox.Offloaded
	event := &tgsrlv1.SandboxEvent{
		EventId:          fmt.Sprintf("%s-sandbox-%d", ProviderID, p.sandboxSeq+1),
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
		OccurredAt:       timestamppb.New(nonZeroTime(sandbox.UpdatedAt, p.now())),
		ProviderRevision: nextRevision,
		IdempotencyKey:   fmt.Sprintf("%s-sandbox-%s-%d", ProviderID, sandbox.SandboxID, nextRevision),
	}
	p.publishSandboxEventLocked(event)
}

func (p *Provider) publishSandboxShareReadbackLocked(action *tgsrlv1.Action, share float64, detail string) {
	if action == nil {
		return
	}
	sandbox, ok := p.sandboxes[actionSandboxID(action)]
	if !ok {
		return
	}
	event := &tgsrlv1.SandboxEvent{
		EventId:          fmt.Sprintf("%s-sandbox-%d", ProviderID, p.sandboxSeq+1),
		EventType:        sandboxStateToEventType(sandbox.State),
		SandboxId:        sandbox.SandboxID,
		Generation:       sandbox.Generation,
		State:            sandboxStateToRuntimeState(sandbox.State),
		Share:            &share,
		Detail:           detail,
		OccurredAt:       timestamppb.New(p.now()),
		PlanId:           action.GetPlanId(),
		ActionId:         action.GetActionId(),
		ProviderRevision: p.revision,
		IdempotencyKey:   action.GetIdempotencyKey(),
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
