package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/cache"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	providerWatchReconnectBaseDelay = 25 * time.Millisecond
	providerWatchReconnectMaxDelay  = 250 * time.Millisecond
)

func (s *Server) startProviderWatches() error {
	if s.eventLoop == nil {
		return nil
	}
	complete, ok := s.provider.(provider.CompleteResourceProvider)
	if !ok {
		return nil
	}
	s.setProviderWatchHealthy("resource")
	s.setProviderWatchHealthy("sandbox")
	ctx := s.backgroundContext()
	resourceStream, err := complete.WatchResources(ctx, 0)
	if err != nil {
		s.setProviderWatchUnhealthy("resource", err.Error())
		s.recorder.IncCounter("provider_watch_start_failures", 1)
		return fmt.Errorf("service: start resource watch: %w", err)
	}
	sandboxStream, err := complete.WatchSandboxes(ctx, 0)
	if err != nil {
		s.setProviderWatchUnhealthy("sandbox", err.Error())
		s.recorder.IncCounter("provider_watch_start_failures", 1)
		return fmt.Errorf("service: start sandbox watch: %w", err)
	}
	go s.runResourceWatch(ctx, complete, resourceStream)
	go s.runSandboxWatch(ctx, complete, sandboxStream)
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
		next, watchErr := complete.WatchResources(ctx, 0)
		if watchErr != nil {
			s.recorder.IncCounter("provider_watch_resource_restart_failures", 1)
			slog.Warn("resource watch restart failed", "error", watchErr, "reconnects", reconnects)
			continue
		}
		s.setProviderWatchHealthy("resource")
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
				return errors.New("resource watch channel closed")
			}
			if event.Event != nil {
				snapshot, changed, err := s.applyAuthoritativeResourceEvent(event.Event)
				if err != nil {
					s.recorder.IncCounter("provider_watch_event_apply_failures", 1)
					return fmt.Errorf("resource event apply failed: %w", err)
				}
				if changed {
					s.enqueueSnapshotIntents(snapshot)
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
		next, watchErr := complete.WatchSandboxes(ctx, 0)
		if watchErr != nil {
			s.recorder.IncCounter("provider_watch_sandbox_restart_failures", 1)
			slog.Warn("sandbox watch restart failed", "error", watchErr, "reconnects", reconnects)
			continue
		}
		s.setProviderWatchHealthy("sandbox")
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
				return errors.New("sandbox watch channel closed")
			}
			if event.Event != nil {
				snapshot, changed, err := s.applyAuthoritativeSandboxEvent(event.Event)
				if err != nil {
					s.recorder.IncCounter("provider_watch_event_apply_failures", 1)
					return fmt.Errorf("sandbox event apply failed: %w", err)
				}
				if changed {
					s.enqueueSnapshotIntents(snapshot)
				}
			}
		}
	}
}

func (s *Server) enqueueSnapshotIntents(snapshot cache.Snapshot) {
	keys := make([]string, 0, len(snapshot.Intents))
	for key := range snapshot.Intents {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		intent := snapshot.Intents[key]
		if intent == nil {
			continue
		}
		s.triggerReconcile(s.backgroundContext(), intent.GetExecutionId(), intent.GetStageId())
	}
}

func (s *Server) seedEventLoop(ctx context.Context) {
	if s.eventLoop == nil {
		return
	}
	providerID := "provider"
	if complete, ok := s.provider.(provider.CompleteResourceProvider); ok {
		if id, err := complete.ID(ctx); err == nil && id != "" {
			providerID = id
		}
	}
	if snapshot, err := s.provider.Snapshot(ctx); err == nil {
		event := &tgsrlv1.ResourceEvent{
			EventId:          "bootstrap-snapshot",
			EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED,
			Provider:         providerID,
			ProviderRevision: snapshot.GetRevision(),
			ObservedAt:       snapshot.GetObservedAt(),
			Snapshot:         snapshot,
		}
		if _, _, err := s.applyAuthoritativeResourceEvent(event); err != nil {
			s.setProviderWatchUnhealthy("resource", err.Error())
			s.recorder.IncCounter("provider_watch_event_apply_failures", 1)
		}
	}
	sandboxes, err := s.provider.ListSandboxes(ctx)
	if err != nil {
		return
	}
	bootstrapped := make([]*tgsrlv1.Sandbox, 0, len(sandboxes))
	for _, sandbox := range sandboxes {
		bootstrapped = append(bootstrapped, sandboxToProto(sandbox))
	}
	s.planMu.Lock()
	defer s.planMu.Unlock()
	if _, err := s.store.ReplaceProjectedSandboxes(bootstrapped); err == nil {
		for _, sandbox := range bootstrapped {
			s.eventLoop.PublishSandboxEvent(&tgsrlv1.SandboxEvent{
				EventId:    "bootstrap-sandbox-" + sandbox.GetSandboxId(),
				SandboxId:  sandbox.GetSandboxId(),
				RunId:      sandbox.GetRunId(),
				JobId:      sandbox.GetJobId(),
				TraceId:    sandbox.GetTraceId(),
				Generation: sandbox.GetGeneration(),
				State:      sandbox.GetState(),
				Binding:    cloneBindingToProto(sandbox.GetBinding()),
				SafePoint:  sandbox.GetSafePoint(),
				OccurredAt: sandbox.GetObservedAt(),
				DataKind:   sandbox.GetDataKind(),
			})
		}
	}
}

func (s *Server) applyAuthoritativeResourceEvent(event *tgsrlv1.ResourceEvent) (cache.Snapshot, bool, error) {
	s.planMu.Lock()
	defer s.planMu.Unlock()
	if s.eventLoop == nil {
		return cache.Snapshot{}, false, nil
	}
	if event == nil {
		return s.eventLoop.View(), false, nil
	}
	if event.GetEventType() == tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_SNAPSHOT_PUBLISHED && event.GetSnapshot() != nil {
		if _, changed, err := s.store.BootstrapProviderSnapshot(event.GetProvider(), event.GetSnapshot()); err == nil && changed {
			snapshot, applied := s.eventLoop.ApplyResourceEvent(event)
			return snapshot, applied, nil
		} else if err == nil {
			return s.eventLoop.View(), false, nil
		} else {
			return s.eventLoop.View(), false, err
		}
	}
	if _, changed, err := s.store.ApplyProviderResourceEvent(event); err == nil && changed {
		snapshot, applied := s.eventLoop.ApplyResourceEvent(event)
		return snapshot, applied, nil
	} else if err == nil {
		return s.eventLoop.View(), false, nil
	} else {
		return s.eventLoop.View(), false, err
	}
}

func (s *Server) applyAuthoritativeSandboxEvent(event *tgsrlv1.SandboxEvent) (cache.Snapshot, bool, error) {
	s.planMu.Lock()
	defer s.planMu.Unlock()
	if s.eventLoop == nil {
		return cache.Snapshot{}, false, nil
	}
	if event == nil {
		return s.eventLoop.View(), false, nil
	}
	if _, changed, err := s.store.ApplyProviderSandboxEvent(event); err == nil && changed {
		snapshot, applied := s.eventLoop.ApplySandboxEvent(event)
		return snapshot, applied, nil
	} else if err == nil {
		return s.eventLoop.View(), false, nil
	} else {
		return s.eventLoop.View(), false, err
	}
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
	observedAt := sandbox.UpdatedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	return &tgsrlv1.Sandbox{
		SandboxId:  sandbox.SandboxID,
		State:      runtimeStateFromProvider(sandbox.State),
		Generation: sandbox.Generation,
		Binding:    cloneBindingToProto(sandbox.Binding),
		SafePoint:  sandbox.SafePoint,
		ObservedAt: timestamppb.New(observedAt),
	}
}
