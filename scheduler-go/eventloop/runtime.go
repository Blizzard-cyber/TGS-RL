package eventloop

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"google.golang.org/protobuf/proto"
)

// Runtime wires keyed queues to fast/medium/slow scheduler ticks. Enqueued
// keys stay active until Forget is called or the runtime stops.
type Runtime struct {
	trigger        TriggerFunc
	fastQueue      *keyedQueue
	mediumQueue    *keyedQueue
	slowQueue      *keyedQueue
	fastTicker     *tickerLoop
	mediumTicker   *tickerLoop
	slowTicker     *tickerLoop
	startOnce      sync.Once
	stopOnce       sync.Once
	mu             sync.Mutex
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	active         map[string]activeTrigger
	nextGeneration uint64
	stopped        bool
}

type activeTrigger struct {
	trigger    *Trigger
	generation uint64
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
func NewRuntime(recorder observability.Recorder, cfg RuntimeConfig) *Runtime {
	if cfg.FastInterval <= 0 {
		cfg.FastInterval = 25 * time.Millisecond
	}
	if cfg.MediumInterval <= 0 {
		cfg.MediumInterval = 100 * time.Millisecond
	}
	if cfg.SlowInterval <= 0 {
		cfg.SlowInterval = 250 * time.Millisecond
	}
	if recorder == nil {
		recorder = observability.NopRecorder{}
	}
	return &Runtime{
		fastQueue:    newKeyedQueue(),
		mediumQueue:  newKeyedQueue(),
		slowQueue:    newKeyedQueue(),
		fastTicker:   newTickerLoop(newTickRunner("fast", recorder), cfg.FastInterval),
		mediumTicker: newTickerLoop(newTickRunner("medium", recorder), cfg.MediumInterval),
		slowTicker:   newTickerLoop(newTickRunner("slow", recorder), cfg.SlowInterval),
		active:       make(map[string]activeTrigger),
	}
}

// SetTrigger installs the callback used when a queued key is drained.
func (r *Runtime) SetTrigger(trigger TriggerFunc) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.trigger = trigger
}

// Start launches the background loop goroutines. Repeated calls are safe.
func (r *Runtime) Start(ctx context.Context) {
	if r == nil {
		return
	}
	r.startOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		r.mu.Lock()
		if r.stopped {
			r.mu.Unlock()
			return
		}
		runCtx, cancel := context.WithCancel(ctx)
		r.cancel = cancel
		r.wg.Add(4)
		r.mu.Unlock()
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
		go func() {
			defer r.wg.Done()
			<-runCtx.Done()
			r.deactivate()
		}()
	})
}

// Stop terminates all background loop goroutines.
func (r *Runtime) Stop() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		r.mu.Lock()
		r.stopped = true
		clear(r.active)
		cancel := r.cancel
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		r.wg.Wait()
		r.clearQueues()
	})
}

// Enqueue activates one execution/stage key on all loop frequencies until it
// is forgotten or the runtime stops. Repeated calls coalesce metadata and keep
// at most one pending item per frequency.
func (r *Runtime) Enqueue(trigger *Trigger) {
	if r == nil {
		return
	}
	trigger = CloneTrigger(trigger)
	if trigger == nil || trigger.ExecutionID == "" || trigger.StageID == "" {
		return
	}
	key := triggerQueueKey(trigger.ExecutionID, trigger.StageID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	if current, ok := r.active[key]; ok {
		trigger = MergeTrigger(current.trigger, trigger)
	}
	r.nextGeneration++
	if r.nextGeneration == 0 {
		r.nextGeneration++
	}
	generation := r.nextGeneration
	r.active[key] = activeTrigger{trigger: trigger, generation: generation}
	r.fastQueue.Push(key, generation)
	r.mediumQueue.Push(key, generation)
	r.slowQueue.Push(key, generation)
}

// Forget stops periodic scheduling for one execution/stage key. A trigger that
// is already in flight may finish, but stale queued generations are ignored.
func (r *Runtime) Forget(executionID, stageID string) bool {
	if r == nil || executionID == "" || stageID == "" {
		return false
	}
	key := triggerQueueKey(executionID, stageID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.active[key]; !ok {
		return false
	}
	delete(r.active, key)
	return true
}

func (r *Runtime) drain(ctx context.Context, q *keyedQueue, kind tgsrlv1.TickKind) {
	for _, item := range q.Drain() {
		r.mu.Lock()
		active, ok := r.active[item.key]
		if r.stopped || !ok || active.generation != item.generation {
			r.mu.Unlock()
			continue
		}
		trigger := CloneTrigger(active.trigger)
		callback := r.trigger
		r.mu.Unlock()
		if trigger == nil || trigger.ExecutionID == "" || trigger.StageID == "" {
			continue
		}
		trigger.TickKind = kind
		select {
		case <-ctx.Done():
			return
		default:
			if callback != nil {
				callback(ctx, trigger)
			}
		}

		// Requeue only the generation that was just dispatched. A concurrent
		// Enqueue has already queued its newer generation, while Forget and Stop
		// deliberately leave no active entry to requeue.
		r.mu.Lock()
		active, ok = r.active[item.key]
		if !r.stopped && ctx.Err() == nil && ok && active.generation == item.generation {
			q.Push(item.key, item.generation)
		}
		r.mu.Unlock()
	}
}

func (r *Runtime) deactivate() {
	r.mu.Lock()
	r.stopped = true
	clear(r.active)
	r.mu.Unlock()
	r.clearQueues()
}

func (r *Runtime) clearQueues() {
	r.fastQueue.Drain()
	r.mediumQueue.Drain()
	r.slowQueue.Drain()
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

type queueItem struct {
	key        string
	generation uint64
}

type keyedQueue struct {
	mu      sync.Mutex
	pending []string
	items   map[string]uint64
}

func newKeyedQueue() *keyedQueue {
	return &keyedQueue{items: make(map[string]uint64)}
}

func (q *keyedQueue) Push(key string, generation uint64) bool {
	if key == "" {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, exists := q.items[key]; exists {
		q.items[key] = generation
		return false
	}
	q.items[key] = generation
	q.pending = append(q.pending, key)
	return true
}

func (q *keyedQueue) Drain() []queueItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	items := make([]queueItem, 0, len(q.pending))
	for _, key := range q.pending {
		generation, exists := q.items[key]
		if exists {
			items = append(items, queueItem{key: key, generation: generation})
		}
	}
	q.pending = q.pending[:0]
	clear(q.items)
	return items
}

func (q *keyedQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

type tickRunner struct {
	kind        string
	recorder    observability.Recorder
	inFlight    atomic.Int32
	triggered   atomic.Int64
	skipped     atomic.Int64
	completions atomic.Int64
}

func newTickRunner(kind string, recorder observability.Recorder) *tickRunner {
	return &tickRunner{kind: kind, recorder: recorder}
}

func (r *tickRunner) trigger(ctx context.Context, fn func(context.Context)) bool {
	if !r.inFlight.CompareAndSwap(0, 1) {
		r.skipped.Add(1)
		r.recorder.IncCounter("ticks_skipped_"+r.kind, 1)
		return false
	}
	r.triggered.Add(1)
	r.recorder.IncCounter("ticks_started_"+r.kind, 1)
	startedAt := time.Now()
	go func() {
		defer r.inFlight.Store(0)
		defer r.completions.Add(1)
		defer r.recorder.IncCounter("ticks_completed_"+r.kind, 1)
		defer func() {
			r.recorder.ObserveHistogram("tick_latency_"+r.kind, time.Since(startedAt).Seconds())
		}()
		fn(ctx)
	}()
	return true
}

func (r *tickRunner) stats() (triggered, skipped, completed int64) {
	return r.triggered.Load(), r.skipped.Load(), r.completions.Load()
}

type tickerLoop struct {
	runner   *tickRunner
	interval time.Duration
}

func newTickerLoop(runner *tickRunner, interval time.Duration) *tickerLoop {
	return &tickerLoop{runner: runner, interval: interval}
}

func (t *tickerLoop) Run(ctx context.Context, fn func(context.Context)) {
	if t == nil || t.runner == nil || fn == nil {
		return
	}
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-ticker.C:
			wg.Add(1)
			if !t.runner.trigger(ctx, func(ctx context.Context) {
				defer wg.Done()
				fn(ctx)
			}) {
				wg.Done()
			}
		}
	}
}
