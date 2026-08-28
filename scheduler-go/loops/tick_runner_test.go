package loops

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
)

func TestRunnerSkipsOverlappingTicks(t *testing.T) {
	runner := NewRunner(TickFast, observability.NewInMemoryRecorder())
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	var calls atomic.Int32

	if ok := runner.Trigger(context.Background(), func(context.Context) {
		calls.Add(1)
		started <- struct{}{}
		<-block
	}); !ok {
		t.Fatal("first trigger was skipped")
	}
	<-started
	if ok := runner.Trigger(context.Background(), func(context.Context) {
		calls.Add(1)
	}); ok {
		t.Fatal("second trigger unexpectedly started while first tick was in-flight")
	}
	close(block)
	time.Sleep(50 * time.Millisecond)

	triggered, skipped, completed := runner.Stats()
	if triggered != 1 || skipped != 1 || completed != 1 {
		t.Fatalf("stats = (%d,%d,%d), want (1,1,1)", triggered, skipped, completed)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}
