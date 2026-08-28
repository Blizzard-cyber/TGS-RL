package eventloop

import (
	"context"
	"sync"
	"time"

	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/loops"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/queue"
)

// Runtime wires keyed queues to fast/medium/slow scheduler ticks.
type Runtime struct {
	trigger        TriggerFunc
	fastQueue      *queue.Keyed
	mediumQueue    *queue.Keyed
	slowQueue      *queue.Keyed
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

// TriggerFunc consumes one runtime key through the caller's authoritative
// scheduling path. Implementations must be safe for concurrent use.
type TriggerFunc func(context.Context, string, string)

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
		fastQueue:      queue.NewKeyed(),
		mediumQueue:    queue.NewKeyed(),
		slowQueue:      queue.NewKeyed(),
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
			r.fastTicker.Run(runCtx, func(context.Context) { r.drain(runCtx, r.fastQueue) })
		}()
		go func() {
			defer r.wg.Done()
			r.mediumTicker.Run(runCtx, func(context.Context) { r.drain(runCtx, r.mediumQueue) })
		}()
		go func() {
			defer r.wg.Done()
			r.slowTicker.Run(runCtx, func(context.Context) { r.drain(runCtx, r.slowQueue) })
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
func (r *Runtime) Enqueue(executionID, stageID string) {
	if r == nil {
		return
	}
	key := executionID + "/" + stageID
	r.fastQueue.Push(key)
	r.mediumQueue.Push(key)
	r.slowQueue.Push(key)
}

func (r *Runtime) drain(ctx context.Context, q *queue.Keyed) {
	for _, key := range q.Drain() {
		executionID, stageID, ok := splitKey(key)
		if !ok {
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
			if r.trigger == nil {
				continue
			}
			r.trigger(ctx, executionID, stageID)
		}
	}
}

func splitKey(key string) (string, string, bool) {
	for index := 0; index < len(key); index++ {
		if key[index] == '/' {
			return key[:index], key[index+1:], true
		}
	}
	return "", "", false
}
