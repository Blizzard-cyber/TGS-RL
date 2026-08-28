package eventloop

import (
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/cache"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
)

// EventLoop owns the immutable cache mirrored from intents plus provider/runtime
// events. Authoritative decision generation remains in service/server.
type EventLoop struct {
	state    *cache.State
	recorder observability.Recorder
	clock    cache.Clock
}

// New constructs an event loop that owns immutable cache state. Runtime trigger
// queues live in Runtime, not here.
func New(state *cache.State, clock cache.Clock, recorder observability.Recorder) *EventLoop {
	if state == nil {
		state = cache.NewState(nil, nil)
	}
	if recorder == nil {
		recorder = observability.NopRecorder{}
	}
	if clock == nil {
		clock = cache.ClockFunc(func() time.Time { return time.Now().UTC() })
	}
	return &EventLoop{
		state:    state,
		recorder: recorder,
		clock:    clock,
	}
}

// PublishIntent applies an immutable intent and rebuilds pending units.
func (l *EventLoop) PublishIntent(intent *tgsrlv1.SchedulingIntent) cache.Snapshot {
	return l.state.Update(func(next *cache.Snapshot) bool {
		changed := next.UpsertIntent(intent)
		if changed {
			next.RebuildPendingUnits(l.clock.Now().UTC())
			l.recorder.IncCounter("intent_published", 1)
		}
		return changed
	})
}

// PublishResourceEvent applies a resource event with cursor deduplication.
func (l *EventLoop) PublishResourceEvent(event *tgsrlv1.ResourceEvent) cache.Snapshot {
	snapshot, _ := l.ApplyResourceEvent(event)
	return snapshot
}

// ApplyResourceEvent applies a provider resource event and reports whether the
// immutable cache state advanced.
func (l *EventLoop) ApplyResourceEvent(event *tgsrlv1.ResourceEvent) (cache.Snapshot, bool) {
	changed := false
	snapshot := l.state.Update(func(next *cache.Snapshot) bool {
		changed = next.ApplyResourceEvent(event)
		if changed {
			l.recorder.IncCounter("resource_event_applied", 1)
		} else {
			l.recorder.IncCounter("resource_event_deduped", 1)
		}
		return changed
	})
	return snapshot, changed
}

// PublishSandboxEvent applies a generation-fenced runtime event.
func (l *EventLoop) PublishSandboxEvent(event *tgsrlv1.SandboxEvent) cache.Snapshot {
	snapshot, _ := l.ApplySandboxEvent(event)
	return snapshot
}

// ApplySandboxEvent applies a runtime event and reports whether the immutable
// cache state advanced.
func (l *EventLoop) ApplySandboxEvent(event *tgsrlv1.SandboxEvent) (cache.Snapshot, bool) {
	changed := false
	snapshot := l.state.Update(func(next *cache.Snapshot) bool {
		changed = next.ApplySandboxEvent(event)
		if changed {
			l.recorder.IncCounter("sandbox_event_applied", 1)
		} else {
			l.recorder.IncCounter("sandbox_event_fenced", 1)
		}
		return changed
	})
	return snapshot, changed
}

// View returns the current immutable cache state clone.
func (l *EventLoop) View() cache.Snapshot {
	return l.state.View()
}
