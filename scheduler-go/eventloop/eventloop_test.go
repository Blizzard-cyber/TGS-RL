package eventloop

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/cache"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/observability"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEventLoopPublishesImmutableStateAndRuntimeDispatchesTriggers(t *testing.T) {
	recorder := observability.NewInMemoryRecorder()
	now := time.Date(2026, 8, 28, 2, 0, 0, 0, time.UTC)
	clock := cache.ClockFunc(func() time.Time { return now })
	state := cache.NewState(&tgsrlv1.ClusterSnapshot{
		SnapshotId: "snapshot-1",
		Revision:   1,
		Devices: []*tgsrlv1.Device{{
			DeviceId:    "device-1",
			Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
			Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
			Capabilities: &tgsrlv1.CapabilitySet{
				Names: []string{"logical-cpu"},
			},
		}},
	}, clock)
	loop := New(state, clock, recorder)

	intent := &tgsrlv1.SchedulingIntent{
		ExecutionId:      "exec",
		StageId:          "stage",
		Version:          1,
		JobId:            "job",
		PolicyVersion:    "policy-1",
		IdempotencyKey:   "intent-key",
		SubmittedAt:      timestamppb.New(now),
		ValidUntil:       timestamppb.New(now.Add(5 * time.Minute)),
		UnitCount:        1,
		ResourcesPerUnit: &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 128, AcceleratorUnits: 0.25},
		RequiredCapabilities: &tgsrlv1.CapabilitySet{
			Names: []string{"logical-cpu"},
		},
	}
	loop.PublishIntent(intent)
	loop.PublishResourceEvent(&tgsrlv1.ResourceEvent{
		EventId:          "resource-1",
		EventType:        tgsrlv1.ResourceEventType_RESOURCE_EVENT_TYPE_DEVICE_CHANGED,
		Provider:         "mock",
		ProviderRevision: 2,
		Device: &tgsrlv1.Device{
			DeviceId:    "device-1",
			Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
			Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
			Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
			Capabilities: &tgsrlv1.CapabilitySet{
				Names: []string{"logical-cpu"},
			},
		},
	})
	loop.PublishSandboxEvent(&tgsrlv1.SandboxEvent{
		EventId:    "sandbox-1",
		SandboxId:  "sb-1",
		Generation: 1,
		State:      tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		OccurredAt: timestamppb.New(now),
	})

	view := loop.View()
	if got := len(view.Snapshot.GetPendingUnits()); got != 1 {
		t.Fatalf("pending units = %d, want 1", got)
	}
	if sandbox := view.Sandboxes["sb-1"]; sandbox == nil || sandbox.GetState() != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("sandbox = %+v, want running sandbox state", sandbox)
	}

	runtime := NewRuntime(loop, RuntimeConfig{
		FastInterval:   10 * time.Millisecond,
		MediumInterval: 20 * time.Millisecond,
		SlowInterval:   30 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		mu       sync.Mutex
		drained  []string
		received = make(chan tgsrlv1.TickKind, 16)
	)
	runtime.SetTrigger(func(_ context.Context, trigger *Trigger) {
		if trigger == nil {
			return
		}
		mu.Lock()
		drained = append(drained, trigger.ExecutionID+"/"+trigger.StageID+":"+trigger.TickKind.String())
		mu.Unlock()
		select {
		case received <- trigger.TickKind:
		default:
		}
	})
	runtime.Start(ctx)
	defer runtime.Stop()
	runtime.Enqueue(&Trigger{ExecutionID: "exec", StageID: "stage", Cause: "test-enqueue", ObservedRevision: 7})
	seen := make(map[tgsrlv1.TickKind]bool, 3)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(seen) < 3 {
		select {
		case kind := <-received:
			seen[kind] = true
		case <-deadline.C:
			t.Fatal("runtime did not dispatch all three trigger queues")
		}
	}
	runtime.Forget("exec", "stage")
	mu.Lock()
	defer mu.Unlock()
	want := map[string]bool{
		"exec/stage:TICK_KIND_FAST":   false,
		"exec/stage:TICK_KIND_MEDIUM": false,
		"exec/stage:TICK_KIND_SLOW":   false,
	}
	for _, key := range drained {
		if _, ok := want[key]; !ok {
			t.Fatalf("trigger key = %q, want one of fast/medium/slow drains for exec/stage", key)
		}
		want[key] = true
	}
	for key, seen := range want {
		if !seen {
			t.Fatalf("missing trigger drain %q", key)
		}
	}
}

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

func TestEventLoopPublishesIntentWithInjectedClock(t *testing.T) {
	fixed := time.Date(2026, 8, 27, 15, 0, 0, 0, time.UTC)
	recorder := observability.NewInMemoryRecorder()
	clock := cache.ClockFunc(func() time.Time { return fixed })

	buildLoop := func() *EventLoop {
		state := cache.NewState(&tgsrlv1.ClusterSnapshot{
			SnapshotId: "snapshot-fixed",
			Revision:   9,
			ObservedAt: timestamppb.New(fixed.Add(-time.Minute)),
			Devices: []*tgsrlv1.Device{{
				DeviceId:    "device-1",
				Health:      tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY,
				Capacity:    &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
				Allocatable: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 1024, AcceleratorUnits: 1},
				Capabilities: &tgsrlv1.CapabilitySet{
					Names: []string{"logical-cpu"},
				},
			}},
		}, clock)
		return New(state, clock, recorder)
	}

	intent := &tgsrlv1.SchedulingIntent{
		ExecutionId:      "exec",
		StageId:          "stage",
		Version:          1,
		JobId:            "job",
		PolicyVersion:    "policy-1",
		IdempotencyKey:   "intent-key",
		SubmittedAt:      timestamppb.New(fixed),
		ValidUntil:       timestamppb.New(fixed.Add(time.Minute)),
		UnitCount:        1,
		ResourcesPerUnit: &tgsrlv1.ResourceVector{CpuMillis: 100, MemoryBytes: 128, AcceleratorUnits: 0.25},
		RequiredCapabilities: &tgsrlv1.CapabilitySet{
			Names: []string{"logical-cpu"},
		},
	}

	firstLoop := buildLoop()
	firstLoop.PublishIntent(intent)
	first := firstLoop.View()
	if got := first.Snapshot.GetObservedAt().AsTime(); !got.Equal(fixed) {
		t.Fatalf("ObservedAt = %s, want %s", got, fixed)
	}
	if got := len(first.Snapshot.GetPendingUnits()); got != 1 {
		t.Fatalf("pending units = %d, want 1", got)
	}

	secondLoop := buildLoop()
	secondLoop.PublishIntent(intent)
	second := secondLoop.View()
	if got := second.Snapshot.GetObservedAt().AsTime(); !got.Equal(first.Snapshot.GetObservedAt().AsTime()) {
		t.Fatalf("ObservedAt mismatch: first=%s second=%s", first.Snapshot.GetObservedAt().AsTime(), got)
	}
	if got := second.Snapshot.GetPendingUnits()[0].GetPendingUnitId(); got != first.Snapshot.GetPendingUnits()[0].GetPendingUnitId() {
		t.Fatalf("pending unit mismatch: first=%q second=%q", first.Snapshot.GetPendingUnits()[0].GetPendingUnitId(), got)
	}
}
