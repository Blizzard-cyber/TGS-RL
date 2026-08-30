package eventloop

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRuntimeSingleEnqueueRepeatsAcrossMediumIntervalsAndStopQuiesces(t *testing.T) {
	const mediumInterval = 15 * time.Millisecond
	runtime := NewRuntime(nil, RuntimeConfig{
		FastInterval:   time.Hour,
		MediumInterval: mediumInterval,
		SlowInterval:   time.Hour,
	})
	var calls atomic.Int64
	received := make(chan struct{}, 8)
	runtime.SetTrigger(func(_ context.Context, trigger *Trigger) {
		if trigger.TickKind != tgsrlv1.TickKind_TICK_KIND_MEDIUM {
			return
		}
		calls.Add(1)
		select {
		case received <- struct{}{}:
		default:
		}
	})
	runtime.Start(context.Background())
	defer runtime.Stop()
	runtime.Enqueue(&Trigger{ExecutionID: "exec", StageID: "stage", Cause: "intent"})

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for range 3 {
		select {
		case <-received:
		case <-deadline.C:
			t.Fatalf("medium callbacks = %d, want at least 3 after one Enqueue", calls.Load())
		}
	}
	if got := runtime.mediumQueue.Len(); got > 1 {
		t.Fatalf("medium queue length = %d, want at most one pending item per key", got)
	}

	stopped := make(chan struct{})
	go func() {
		runtime.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not terminate runtime goroutines")
	}
	afterStop := calls.Load()
	time.Sleep(3 * mediumInterval)
	if got := calls.Load(); got != afterStop {
		t.Fatalf("callbacks after Stop = %d, want 0", got-afterStop)
	}
	runtime.mu.Lock()
	active := len(runtime.active)
	runtime.mu.Unlock()
	if active != 0 {
		t.Fatalf("active keys after Stop = %d, want 0", active)
	}
	if got := runtime.fastQueue.Len() + runtime.mediumQueue.Len() + runtime.slowQueue.Len(); got != 0 {
		t.Fatalf("pending queue items after Stop = %d, want 0", got)
	}
}

func TestRuntimeForgetStopsPeriodicTriggersAndAllowsReactivation(t *testing.T) {
	const mediumInterval = 10 * time.Millisecond
	runtime := NewRuntime(nil, RuntimeConfig{
		FastInterval:   time.Hour,
		MediumInterval: mediumInterval,
		SlowInterval:   time.Hour,
	})
	var calls atomic.Int64
	started := make(chan int64, 4)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseFirst) })
		runtime.Stop()
	})
	runtime.SetTrigger(func(_ context.Context, trigger *Trigger) {
		if trigger.TickKind != tgsrlv1.TickKind_TICK_KIND_MEDIUM {
			return
		}
		n := calls.Add(1)
		select {
		case started <- n:
		default:
		}
		if n == 1 {
			<-releaseFirst
		}
	})
	runtime.Start(context.Background())
	runtime.Enqueue(&Trigger{ExecutionID: "exec", StageID: "stage", Cause: "intent"})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first medium callback did not start")
	}
	if !runtime.Forget("exec", "stage") {
		t.Fatal("Forget did not remove active key")
	}
	if runtime.Forget("exec", "stage") {
		t.Fatal("second Forget unexpectedly removed the key again")
	}
	releaseOnce.Do(func() { close(releaseFirst) })
	time.Sleep(4 * mediumInterval)
	if got := calls.Load(); got != 1 {
		t.Fatalf("callbacks after Forget = %d, want only the already in-flight callback", got)
	}

	runtime.Enqueue(&Trigger{ExecutionID: "exec", StageID: "stage", Cause: "reactivated"})
	select {
	case n := <-started:
		if n < 2 {
			t.Fatalf("reactivated callback number = %d, want at least 2", n)
		}
	case <-time.After(time.Second):
		t.Fatal("Enqueue did not reactivate forgotten key")
	}
}

func TestRuntimeConcurrentEnqueueDuringDrainKeepsNewestTriggerBounded(t *testing.T) {
	runtime := NewRuntime(nil, RuntimeConfig{})
	defer runtime.Stop()
	firstStarted := make(chan *Trigger, 1)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	runtime.SetTrigger(func(_ context.Context, trigger *Trigger) {
		firstStarted <- CloneTrigger(trigger)
		<-releaseFirst
	})
	runtime.Enqueue(&Trigger{ExecutionID: "exec", StageID: "stage", Cause: "old", ObservedRevision: 1})

	drained := make(chan struct{})
	go func() {
		runtime.drain(context.Background(), runtime.mediumQueue, tgsrlv1.TickKind_TICK_KIND_MEDIUM)
		close(drained)
	}()
	select {
	case trigger := <-firstStarted:
		if trigger.ObservedRevision != 1 {
			t.Fatalf("first revision = %d, want 1", trigger.ObservedRevision)
		}
	case <-time.After(time.Second):
		t.Fatal("first drain did not invoke callback")
	}

	runtime.Enqueue(&Trigger{ExecutionID: "exec", StageID: "stage", Cause: "new", ObservedRevision: 2})
	if got := runtime.mediumQueue.Len(); got != 1 {
		t.Fatalf("medium queue length during concurrent Enqueue = %d, want 1", got)
	}
	releaseOnce.Do(func() { close(releaseFirst) })
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("first drain did not finish")
	}
	if got := runtime.mediumQueue.Len(); got != 1 {
		t.Fatalf("medium queue length after stale drain = %d, want 1", got)
	}

	latest := make(chan *Trigger, 1)
	runtime.SetTrigger(func(_ context.Context, trigger *Trigger) {
		latest <- CloneTrigger(trigger)
	})
	runtime.drain(context.Background(), runtime.mediumQueue, tgsrlv1.TickKind_TICK_KIND_MEDIUM)
	select {
	case trigger := <-latest:
		if trigger.ObservedRevision != 2 {
			t.Fatalf("latest revision = %d, want 2", trigger.ObservedRevision)
		}
		if trigger.Cause != "new|old" {
			t.Fatalf("latest cause = %q, want merged causes", trigger.Cause)
		}
	default:
		t.Fatal("newest trigger was not dispatched")
	}
	if got := runtime.mediumQueue.Len(); got != 1 {
		t.Fatalf("medium queue length after periodic requeue = %d, want 1", got)
	}
}

func TestRuntimeStopWaitsForInFlightTriggerAndPreventsRequeue(t *testing.T) {
	const mediumInterval = 10 * time.Millisecond
	runtime := NewRuntime(nil, RuntimeConfig{
		FastInterval:   time.Hour,
		MediumInterval: mediumInterval,
		SlowInterval:   time.Hour,
	})
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		runtime.Stop()
	})
	runtime.SetTrigger(func(_ context.Context, trigger *Trigger) {
		if trigger.TickKind != tgsrlv1.TickKind_TICK_KIND_MEDIUM {
			return
		}
		close(started)
		<-release
	})
	runtime.Start(context.Background())
	runtime.Enqueue(&Trigger{ExecutionID: "exec", StageID: "stage"})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("medium callback did not start")
	}
	stopped := make(chan struct{})
	go func() {
		runtime.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned before the in-flight callback completed")
	case <-time.After(3 * mediumInterval):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after the in-flight callback completed")
	}

	runtime.mu.Lock()
	active := len(runtime.active)
	runtime.mu.Unlock()
	if active != 0 {
		t.Fatalf("active keys after Stop = %d, want 0", active)
	}
	if got := runtime.fastQueue.Len() + runtime.mediumQueue.Len() + runtime.slowQueue.Len(); got != 0 {
		t.Fatalf("pending queue items after Stop = %d, want 0", got)
	}
}

func TestRuntimeQueueKeyKeepsDelimitedIdentitiesDistinct(t *testing.T) {
	runtime := NewRuntime(nil, RuntimeConfig{})
	defer runtime.Stop()
	runtime.Enqueue(&Trigger{ExecutionID: "execution/one", StageID: "stage"})
	runtime.Enqueue(&Trigger{ExecutionID: "execution", StageID: "one/stage"})

	for name, got := range map[string]int{
		"fast":   runtime.fastQueue.Len(),
		"medium": runtime.mediumQueue.Len(),
		"slow":   runtime.slowQueue.Len(),
	} {
		if got != 2 {
			t.Fatalf("%s queue length = %d, want two distinct identities", name, got)
		}
	}
}

func TestRuntimePeriodicRequeueStaysBoundedPerKey(t *testing.T) {
	runtime := NewRuntime(nil, RuntimeConfig{})
	defer runtime.Stop()
	for revision := uint64(1); revision <= 100; revision++ {
		runtime.Enqueue(&Trigger{ExecutionID: "exec", StageID: "stage", ObservedRevision: revision})
	}

	for range 100 {
		runtime.drain(context.Background(), runtime.fastQueue, tgsrlv1.TickKind_TICK_KIND_FAST)
		runtime.drain(context.Background(), runtime.mediumQueue, tgsrlv1.TickKind_TICK_KIND_MEDIUM)
		runtime.drain(context.Background(), runtime.slowQueue, tgsrlv1.TickKind_TICK_KIND_SLOW)
		for name, got := range map[string]int{
			"fast":   runtime.fastQueue.Len(),
			"medium": runtime.mediumQueue.Len(),
			"slow":   runtime.slowQueue.Len(),
		} {
			if got != 1 {
				t.Fatalf("%s queue length = %d, want one pending item after periodic requeue", name, got)
			}
		}
	}
}

func TestMergeTriggerUsesNewestObservationAndStableCauses(t *testing.T) {
	older := &Trigger{
		ExecutionID:      "execution",
		StageID:          "stage",
		Cause:            "sandbox|intent",
		ObservedRevision: 4,
		ContractObservation: &tgsrlv1.ContractObservation{
			EventId:    "observation-old",
			ObservedAt: timestamppb.New(time.Unix(10, 0)),
		},
	}
	newer := &Trigger{
		ExecutionID:      "execution",
		StageID:          "stage",
		Cause:            "resource|intent",
		ObservedRevision: 5,
		ContractObservation: &tgsrlv1.ContractObservation{
			EventId:    "observation-new",
			ObservedAt: timestamppb.New(time.Unix(11, 0)),
		},
	}

	merged := MergeTrigger(older, newer)
	if merged.ObservedRevision != 5 {
		t.Fatalf("observed revision = %d, want 5", merged.ObservedRevision)
	}
	if got := merged.ContractObservation.GetEventId(); got != "observation-new" {
		t.Fatalf("observation = %q, want newest observation", got)
	}
	if got := merged.Cause; got != "intent|resource|sandbox" {
		t.Fatalf("cause = %q, want deterministic merged causes", got)
	}
	merged.ContractObservation.EventId = "caller-mutation"
	if newer.ContractObservation.GetEventId() != "observation-new" {
		t.Fatal("MergeTrigger aliased the incoming observation")
	}
}

func TestMergeTriggerUsesNewerObservationWithoutRevisionChange(t *testing.T) {
	current := &Trigger{
		ExecutionID: "execution", StageID: "stage", ObservedRevision: 5,
		ContractObservation: &tgsrlv1.ContractObservation{
			EventId: "old", ObservedAt: timestamppb.New(time.Unix(10, 0)),
		},
	}
	incoming := &Trigger{
		ExecutionID: "execution", StageID: "stage", ObservedRevision: 5,
		ContractObservation: &tgsrlv1.ContractObservation{
			EventId: "new", ObservedAt: timestamppb.New(time.Unix(11, 0)),
		},
	}

	if got := MergeTrigger(current, incoming).ContractObservation.GetEventId(); got != "new" {
		t.Fatalf("observation = %q, want causally newer observation", got)
	}
}
