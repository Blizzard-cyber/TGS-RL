package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/eventloop"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	providerWatchReconnectBaseDelay = 25 * time.Millisecond
	providerWatchReconnectMaxDelay  = 250 * time.Millisecond
)

func (s *Server) startProviderWatches() error {
	s.markProjectionLiveNotReady("all")
	s.setProviderWatchUnhealthy("resource", "provider resource watch has not completed live bootstrap")
	s.setProviderWatchUnhealthy("sandbox", "provider sandbox watch has not completed live bootstrap")
	ctx := s.backgroundContext()
	if err := s.bootstrapLiveProviderProjection(ctx, s.provider); err != nil {
		return err
	}
	resourceStream, err := s.provider.WatchResources(ctx, 0)
	if err != nil {
		s.markProjectionLiveNotReady("resource")
		s.setProviderWatchUnhealthy("resource", err.Error())
		s.recorder.IncCounter("provider_watch_start_failures", 1)
		return fmt.Errorf("service: start resource watch: %w", err)
	}
	sandboxStream, err := s.provider.WatchSandboxes(ctx, 0)
	if err != nil {
		s.markProjectionLiveNotReady("sandbox")
		s.setProviderWatchUnhealthy("sandbox", err.Error())
		s.recorder.IncCounter("provider_watch_start_failures", 1)
		return fmt.Errorf("service: start sandbox watch: %w", err)
	}
	s.markProviderWatchEventHealthy("resource")
	s.markProviderWatchEventHealthy("sandbox")
	s.markProjectionLiveReady("resource")
	s.markProjectionLiveReady("sandbox")
	go s.runResourceWatch(ctx, s.provider, resourceStream)
	go s.runSandboxWatch(ctx, s.provider, sandboxStream)
	return nil
}

func (s *Server) bootstrapLiveProviderProjection(ctx context.Context, complete provider.CompleteResourceProvider) error {
	s.markProjectionLiveAttempt("resource")
	snapshot, err := complete.Snapshot(ctx)
	if err != nil {
		s.markProjectionLiveNotReady("resource")
		s.setProviderWatchUnhealthy("resource", err.Error())
		s.recorder.IncCounter("provider_watch_start_failures", 1)
		return fmt.Errorf("service: bootstrap resource snapshot: %w", err)
	}
	providerID := "provider"
	if id, idErr := complete.ID(ctx); idErr == nil && id != "" {
		providerID = id
	}
	event := &tgsrlv1.ResourceEvent{
		EventId:          "bootstrap-snapshot",
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED,
		Provider:         providerID,
		ProviderRevision: snapshot.GetRevision(),
		ObservedAt:       snapshot.GetObservedAt(),
		Snapshot:         snapshot,
	}
	if _, err := s.applyAuthoritativeResourceEvent(event); err != nil {
		s.markProjectionLiveNotReady("resource")
		s.setProviderWatchUnhealthy("resource", err.Error())
		s.recorder.IncCounter("provider_watch_event_apply_failures", 1)
		return fmt.Errorf("service: bootstrap resource projection: %w", err)
	}
	s.markProjectionLiveAttempt("sandbox")
	sandboxes, err := complete.ListSandboxes(ctx)
	if err != nil {
		s.markProjectionLiveNotReady("sandbox")
		s.setProviderWatchUnhealthy("sandbox", err.Error())
		s.recorder.IncCounter("provider_watch_start_failures", 1)
		return fmt.Errorf("service: bootstrap sandbox projection: %w", err)
	}
	bootstrapped := make([]*tgsrlv1.Sandbox, 0, len(sandboxes))
	for _, sandbox := range sandboxes {
		bootstrapped = append(bootstrapped, sandboxToProto(sandbox))
	}
	s.planMu.Lock()
	if _, err := s.store.ReplaceProjectedSandboxes(bootstrapped); err != nil {
		s.planMu.Unlock()
		s.markProjectionLiveNotReady("sandbox")
		s.setProviderWatchUnhealthy("sandbox", err.Error())
		s.recorder.IncCounter("provider_watch_event_apply_failures", 1)
		return fmt.Errorf("service: bootstrap sandbox projection: %w", err)
	}
	s.planMu.Unlock()
	return nil
}

func (s *Server) runResourceWatch(ctx context.Context, complete provider.CompleteResourceProvider, stream <-chan provider.WatchedResourceEvent) {
	current := stream
	reconnects := 0
	for {
		err := s.consumeResourceWatch(ctx, current)
		if err == nil || ctx.Err() != nil {
			return
		}
		reconnects++
		s.markProjectionLiveNotReady("resource")
		s.providerWatch.mu.Lock()
		s.providerWatch.resourceReconnects = reconnects
		s.providerWatch.mu.Unlock()
		s.setProviderWatchUnhealthy("resource", err.Error())
		s.recorder.IncCounter("provider_watch_resource_reconnects", 1)
		slog.Warn("resource watch disconnected", "error", err, "reconnects", reconnects)
		delay := providerWatchReconnectDelay(reconnects)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if resyncErr := s.bootstrapLiveProviderProjection(ctx, complete); resyncErr != nil {
			s.setProviderWatchUnhealthy("resource", resyncErr.Error())
			s.recorder.IncCounter("provider_watch_resource_restart_failures", 1)
			slog.Warn("resource watch resync failed", "error", resyncErr, "reconnects", reconnects)
			continue
		}
		next, watchErr := complete.WatchResources(ctx, 0)
		if watchErr != nil {
			s.markProjectionLiveNotReady("resource")
			s.recorder.IncCounter("provider_watch_resource_restart_failures", 1)
			slog.Warn("resource watch restart failed", "error", watchErr, "reconnects", reconnects)
			continue
		}
		// Opening a replacement stream is not evidence that a previously
		// malformed stream is trustworthy again. A poisoned watch remains
		// fail-closed until the replacement stream yields one valid event.
		s.setProviderWatchHealthy("resource")
		s.markProjectionLiveReady("resource")
		current = next
	}
}

func (s *Server) consumeResourceWatch(ctx context.Context, stream <-chan provider.WatchedResourceEvent) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-stream:
			if !ok {
				s.markProjectionLiveNotReady("resource")
				return errors.New("resource watch channel closed")
			}
			if event.Event != nil {
				changed, err := s.applyAuthoritativeResourceEvent(event.Event)
				if err != nil {
					s.markProjectionLiveNotReady("resource")
					s.poisonProviderWatch("resource", err.Error())
					s.recorder.IncCounter("provider_watch_event_apply_failures", 1)
					return fmt.Errorf("resource event apply failed: %w", err)
				}
				s.markProviderWatchEventHealthy("resource")
				s.markProjectionLiveReady("resource")
				if changed {
					s.enqueueStoredIntents()
				}
			}
		}
	}
}

func (s *Server) runSandboxWatch(ctx context.Context, complete provider.CompleteResourceProvider, stream <-chan provider.WatchedSandboxEvent) {
	current := stream
	reconnects := 0
	for {
		err := s.consumeSandboxWatch(ctx, current)
		if err == nil || ctx.Err() != nil {
			return
		}
		reconnects++
		s.markProjectionLiveNotReady("sandbox")
		s.providerWatch.mu.Lock()
		s.providerWatch.sandboxReconnects = reconnects
		s.providerWatch.mu.Unlock()
		s.setProviderWatchUnhealthy("sandbox", err.Error())
		s.recorder.IncCounter("provider_watch_sandbox_reconnects", 1)
		slog.Warn("sandbox watch disconnected", "error", err, "reconnects", reconnects)
		delay := providerWatchReconnectDelay(reconnects)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if resyncErr := s.bootstrapLiveProviderProjection(ctx, complete); resyncErr != nil {
			s.setProviderWatchUnhealthy("sandbox", resyncErr.Error())
			s.recorder.IncCounter("provider_watch_sandbox_restart_failures", 1)
			slog.Warn("sandbox watch resync failed", "error", resyncErr, "reconnects", reconnects)
			continue
		}
		next, watchErr := complete.WatchSandboxes(ctx, 0)
		if watchErr != nil {
			s.markProjectionLiveNotReady("sandbox")
			s.recorder.IncCounter("provider_watch_sandbox_restart_failures", 1)
			slog.Warn("sandbox watch restart failed", "error", watchErr, "reconnects", reconnects)
			continue
		}
		// Preserve an apply-error poison across reconnect. Only successfully
		// applying a subsequent event may clear it.
		s.setProviderWatchHealthy("sandbox")
		s.markProjectionLiveReady("sandbox")
		current = next
	}
}

func (s *Server) consumeSandboxWatch(ctx context.Context, stream <-chan provider.WatchedSandboxEvent) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-stream:
			if !ok {
				s.markProjectionLiveNotReady("sandbox")
				return errors.New("sandbox watch channel closed")
			}
			if event.Event != nil {
				changed, err := s.applyAuthoritativeSandboxEvent(event.Event)
				if err != nil {
					s.markProjectionLiveNotReady("sandbox")
					s.poisonProviderWatch("sandbox", err.Error())
					s.recorder.IncCounter("provider_watch_event_apply_failures", 1)
					return fmt.Errorf("sandbox event apply failed: %w", err)
				}
				s.markProviderWatchEventHealthy("sandbox")
				s.markProjectionLiveReady("sandbox")
				if changed {
					s.enqueueStoredIntents()
				}
			}
		}
	}
}

func (s *Server) enqueueStoredIntents() {
	for _, intent := range s.store.ExportDurableState().Intents {
		if intent == nil {
			continue
		}
		s.enqueueThroughRuntime(s.backgroundContext(), intent, &eventloop.Trigger{
			ExecutionID:         intent.GetExecutionId(),
			StageID:             intent.GetStageId(),
			TickKind:            tgsrlv1.TickKind_TICK_KIND_FAST,
			Cause:               "provider_event",
			ContractObservation: cloneContractObservation(intent.GetContractObservation()),
		})
	}
}

func (s *Server) applyAuthoritativeResourceEvent(event *tgsrlv1.ResourceEvent) (bool, error) {
	s.planMu.Lock()
	defer s.planMu.Unlock()
	if event == nil {
		return false, nil
	}
	if event.GetEventType() == tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED && event.GetSnapshot() != nil {
		_, changed, err := s.store.BootstrapProviderSnapshot(event.GetProvider(), event.GetSnapshot())
		return changed, err
	}
	_, changed, err := s.store.ApplyProviderResourceEvent(event)
	return changed, err
}

func (s *Server) applyAuthoritativeSandboxEvent(event *tgsrlv1.SandboxEvent) (bool, error) {
	s.planMu.Lock()
	defer s.planMu.Unlock()
	if event == nil {
		return false, nil
	}
	_, changed, err := s.store.ApplyProviderSandboxEvent(event)
	return changed, err
}

func providerWatchReconnectDelay(reconnects int) time.Duration {
	delay := providerWatchReconnectBaseDelay
	for attempt := 1; attempt < reconnects; attempt++ {
		delay *= 2
		if delay >= providerWatchReconnectMaxDelay {
			return providerWatchReconnectMaxDelay
		}
	}
	if delay > providerWatchReconnectMaxDelay {
		return providerWatchReconnectMaxDelay
	}
	return delay
}

func runtimeStateFromProvider(state provider.SandboxState) tgsrlv1.RuntimeState {
	switch state {
	case provider.SandboxStateRequested:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_REQUESTED
	case provider.SandboxStateBound:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND
	case provider.SandboxStateRunning:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING
	case provider.SandboxStatePaused:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED
	case provider.SandboxStateSleeping:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING
	case provider.SandboxStateFailed:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED
	case provider.SandboxStateTerminated:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED
	default:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN
	}
}

func cloneBindingToProto(binding *tgsrlv1.Binding) *tgsrlv1.Binding {
	if binding == nil {
		return nil
	}
	return proto.Clone(binding).(*tgsrlv1.Binding)
}

func sandboxToProto(sandbox provider.Sandbox) *tgsrlv1.Sandbox {
	result := &tgsrlv1.Sandbox{
		SandboxId:       sandbox.SandboxID,
		State:           runtimeStateFromProvider(sandbox.State),
		Generation:      sandbox.Generation,
		Binding:         cloneBindingToProto(sandbox.Binding),
		Share:           sandbox.Share,
		Priority:        sandbox.Priority,
		SafePoint:       sandbox.SafePoint,
		Offloaded:       sandbox.Offloaded,
		SemanticContext: cloneSemanticEnvelope(sandbox.SemanticContext),
	}
	if !sandbox.UpdatedAt.IsZero() {
		result.ObservedAt = timestamppb.New(sandbox.UpdatedAt.UTC())
	}
	if !sandbox.StateChangedAt.IsZero() {
		result.StateChangedAt = timestamppb.New(sandbox.StateChangedAt.UTC())
	}
	return result
}
