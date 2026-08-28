package loops

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
)

// TickKind separates fast, medium, and slow loops.
type TickKind string

const (
	TickFast   TickKind = "fast"
	TickMedium TickKind = "medium"
	TickSlow   TickKind = "slow"
)

// Runner executes one callback per trigger and skips overlapping ticks.
type Runner struct {
	kind        TickKind
	recorder    observability.Recorder
	inFlight    atomic.Int32
	triggered   atomic.Int64
	skipped     atomic.Int64
	completions atomic.Int64
}

// NewRunner constructs a tick runner.
func NewRunner(kind TickKind, recorder observability.Recorder) *Runner {
	if recorder == nil {
		recorder = observability.NopRecorder{}
	}
	return &Runner{kind: kind, recorder: recorder}
}

// Trigger runs fn if no other tick of the same runner is currently executing.
func (r *Runner) Trigger(ctx context.Context, fn func(context.Context)) bool {
	if !r.inFlight.CompareAndSwap(0, 1) {
		r.skipped.Add(1)
		r.recorder.IncCounter("ticks_skipped_"+string(r.kind), 1)
		return false
	}
	r.triggered.Add(1)
	r.recorder.IncCounter("ticks_started_"+string(r.kind), 1)
	start := time.Now()
	go func() {
		defer r.inFlight.Store(0)
		defer r.completions.Add(1)
		defer r.recorder.IncCounter("ticks_completed_"+string(r.kind), 1)
		defer func() {
			r.recorder.ObserveHistogram("tick_latency_"+string(r.kind), time.Since(start).Seconds())
		}()
		fn(ctx)
	}()
	return true
}

// Ticker wires a time.Ticker to the runner.
type Ticker struct {
	runner   *Runner
	interval time.Duration
}

// NewTicker constructs a fixed-interval ticker loop.
func NewTicker(runner *Runner, interval time.Duration) *Ticker {
	return &Ticker{runner: runner, interval: interval}
}

// Run starts the ticker until ctx is cancelled.
func (t *Ticker) Run(ctx context.Context, fn func(context.Context)) {
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
			started := t.runner.Trigger(ctx, func(ctx context.Context) {
				defer wg.Done()
				fn(ctx)
			})
			if !started {
				wg.Done()
			}
		}
	}
}

// Stats reports runner activity.
func (r *Runner) Stats() (triggered, skipped, completed int64) {
	return r.triggered.Load(), r.skipped.Load(), r.completions.Load()
}
