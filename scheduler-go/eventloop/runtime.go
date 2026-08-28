package eventloop

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/loops"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/queue"
	"google.golang.org/protobuf/proto"
)

// Runtime wires keyed queues to fast/medium/slow scheduler ticks.
type Runtime struct {
	trigger        TriggerFunc
	fastQueue      *queue.Keyed[*Trigger]
	mediumQueue    *queue.Keyed[*Trigger]
	slowQueue      *queue.Keyed[*Trigger]
	fastTicker     *loops.Ticker
	mediumTicker   *loops.Ticker
	slowTicker     *loops.Ticker
	startOnce      sync.Once
	stopOnce       sync.Once
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	fastInterval   time.Duration
	mediumInterval time.Duration
	slowInterval   time.Duration
}

// Trigger describes one authoritative runtime reconcile request.
type Trigger struct {
	ExecutionID                  string
	StageID                      string
	TickKind                     tgsrlv1.TickKind
	Cause                        string
	ObservedRevision             uint64
	ContractObservation          *tgsrlv1.ContractObservation
	CompatibilityDefaultsApplied bool
}

// TriggerFunc consumes one runtime key through the caller's authoritative
// scheduling path. Implementations must be safe for concurrent use.
type TriggerFunc func(context.Context, *Trigger)

// RuntimeConfig controls background loop periods.
type RuntimeConfig struct {
	FastInterval   time.Duration
	MediumInterval time.Duration
	SlowInterval   time.Duration
}

// NewRuntime constructs a background runtime around one event loop's trigger
// cadence. The runtime does not own authoritative scheduling state.
func NewRuntime(loop *EventLoop, cfg RuntimeConfig) *Runtime {
	if cfg.FastInterval <= 0 {
		cfg.FastInterval = 25 * time.Millisecond
	}
	if cfg.MediumInterval <= 0 {
		cfg.MediumInterval = 100 * time.Millisecond
	}
	if cfg.SlowInterval <= 0 {
		cfg.SlowInterval = 250 * time.Millisecond
	}
	var recorder observability.Recorder = observability.NopRecorder{}
	if loop != nil && loop.recorder != nil {
		recorder = loop.recorder
	}
	return &Runtime{
		fastQueue:      queue.NewKeyed(MergeTrigger),
		mediumQueue:    queue.NewKeyed(MergeTrigger),
		slowQueue:      queue.NewKeyed(MergeTrigger),
		fastTicker:     loops.NewTicker(loops.NewRunner(loops.TickFast, recorder), cfg.FastInterval),
		mediumTicker:   loops.NewTicker(loops.NewRunner(loops.TickMedium, recorder), cfg.MediumInterval),
		slowTicker:     loops.NewTicker(loops.NewRunner(loops.TickSlow, recorder), cfg.SlowInterval),
		fastInterval:   cfg.FastInterval,
		mediumInterval: cfg.MediumInterval,
		slowInterval:   cfg.SlowInterval,
	}
}

// SetTrigger installs the callback used when a queued key is drained.
func (r *Runtime) SetTrigger(trigger TriggerFunc) {
	if r == nil {
		return
	}
	r.trigger = trigger
}

// Start launches the background loop goroutines. Repeated calls are safe.
func (r *Runtime) Start(ctx context.Context) {
	if r == nil {
		return
	}
	r.startOnce.Do(func() {
		runCtx, cancel := context.WithCancel(ctx)
		r.cancel = cancel
		r.wg.Add(3)
		go func() {
			defer r.wg.Done()
			r.fastTicker.Run(runCtx, func(context.Context) { r.drain(runCtx, r.fastQueue, tgsrlv1.TickKind_TICK_KIND_FAST) })
		}()
		go func() {
			defer r.wg.Done()
			r.mediumTicker.Run(runCtx, func(context.Context) { r.drain(runCtx, r.mediumQueue, tgsrlv1.TickKind_TICK_KIND_MEDIUM) })
		}()
		go func() {
			defer r.wg.Done()
			r.slowTicker.Run(runCtx, func(context.Context) { r.drain(runCtx, r.slowQueue, tgsrlv1.TickKind_TICK_KIND_SLOW) })
		}()
	})
}

// Stop terminates all background loop goroutines.
func (r *Runtime) Stop() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		r.wg.Wait()
	})
}

// Enqueue schedules one execution/stage key on all loop frequencies.
func (r *Runtime) Enqueue(trigger *Trigger) {
	if r == nil {
		return
	}
	trigger = CloneTrigger(trigger)
	if trigger == nil || trigger.ExecutionID == "" || trigger.StageID == "" {
		return
	}
	key := triggerQueueKey(trigger.ExecutionID, trigger.StageID)
	fast := CloneTrigger(trigger)
	fast.TickKind = tgsrlv1.TickKind_TICK_KIND_FAST
	medium := CloneTrigger(trigger)
	medium.TickKind = tgsrlv1.TickKind_TICK_KIND_MEDIUM
	slow := CloneTrigger(trigger)
	slow.TickKind = tgsrlv1.TickKind_TICK_KIND_SLOW
	r.fastQueue.Push(key, fast)
	r.mediumQueue.Push(key, medium)
	r.slowQueue.Push(key, slow)
}

func (r *Runtime) drain(ctx context.Context, q *queue.Keyed[*Trigger], kind tgsrlv1.TickKind) {
	for _, item := range q.Drain() {
		trigger := CloneTrigger(item.Value)
		if trigger == nil || trigger.ExecutionID == "" || trigger.StageID == "" {
			continue
		}
		trigger.TickKind = kind
		select {
		case <-ctx.Done():
			return
		default:
			if r.trigger == nil {
				continue
			}
			r.trigger(ctx, trigger)
		}
	}
}

// MergeTrigger coalesces two triggers without losing the newest revision or
// observation. The result never aliases either input.
func MergeTrigger(current *Trigger, incoming *Trigger) *Trigger {
	switch {
	case current == nil:
		return CloneTrigger(incoming)
	case incoming == nil:
		return CloneTrigger(current)
	}
	merged := CloneTrigger(current)
	if merged.ExecutionID == "" {
		merged.ExecutionID = incoming.ExecutionID
	}
	if merged.StageID == "" {
		merged.StageID = incoming.StageID
	}
	newerRevision := incoming.ObservedRevision > merged.ObservedRevision
	if newerRevision {
		merged.ObservedRevision = incoming.ObservedRevision
	}
	merged.Cause = mergeCauses(merged.Cause, incoming.Cause)
	if incoming.ContractObservation != nil && (merged.ContractObservation == nil ||
		newerRevision ||
		observationLess(merged.ContractObservation, incoming.ContractObservation)) {
		merged.ContractObservation = proto.Clone(incoming.ContractObservation).(*tgsrlv1.ContractObservation)
	}
	merged.CompatibilityDefaultsApplied = merged.CompatibilityDefaultsApplied || incoming.CompatibilityDefaultsApplied
	if merged.TickKind == tgsrlv1.TickKind_TICK_KIND_UNKNOWN {
		merged.TickKind = incoming.TickKind
	}
	return merged
}

func triggerQueueKey(executionID, stageID string) string {
	return strconv.Itoa(len(executionID)) + ":" + executionID + stageID
}

func observationLess(left, right *tgsrlv1.ContractObservation) bool {
	leftTime, rightTime := left.GetObservedAt(), right.GetObservedAt()
	if leftTime.GetSeconds() != rightTime.GetSeconds() {
		return leftTime.GetSeconds() < rightTime.GetSeconds()
	}
	if leftTime.GetNanos() != rightTime.GetNanos() {
		return leftTime.GetNanos() < rightTime.GetNanos()
	}
	return left.GetEventId() < right.GetEventId()
}

func mergeCauses(values ...string) string {
	seen := make(map[string]struct{})
	for _, value := range values {
		for _, item := range strings.Split(value, "|") {
			item = strings.TrimSpace(item)
			if item != "" {
				seen[item] = struct{}{}
			}
		}
	}
	merged := make([]string, 0, len(seen))
	for item := range seen {
		merged = append(merged, item)
	}
	sort.Strings(merged)
	return strings.Join(merged, "|")
}

// CloneTrigger returns a detached copy suitable for transfer between queues.
func CloneTrigger(trigger *Trigger) *Trigger {
	if trigger == nil {
		return nil
	}
	cloned := *trigger
	if trigger.ContractObservation != nil {
		cloned.ContractObservation = proto.Clone(trigger.ContractObservation).(*tgsrlv1.ContractObservation)
	}
	return &cloned
}
