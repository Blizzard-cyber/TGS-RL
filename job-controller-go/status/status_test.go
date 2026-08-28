package status

import (
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMergeAggregatedStatusPreservesObservations(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 12, 20, 0, 0, time.UTC)
	first := &tgsrlv1.ComponentStatus{
		Component:  "runtime",
		Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
		Detail:     "ready",
		Source:     "scheduler",
		Revision:   1,
		ObservedAt: timestamppb.New(now),
		JobId:      "job-1",
		RunId:      "run-1",
	}
	aggregateA := MergeAggregatedStatus(nil, first)
	aggregateB := MergeAggregatedStatus([]*tgsrlv1.ComponentStatus{aggregateA}, &tgsrlv1.ComponentStatus{
		Component:  "runtime",
		Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED,
		Detail:     "crashed",
		Source:     "runtime",
		Revision:   2,
		ObservedAt: timestamppb.New(now.Add(time.Minute)),
		JobId:      "job-1",
		RunId:      "run-1",
	})
	if aggregateB.GetHealth() != tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED {
		t.Fatalf("aggregate health = %s, want FAILED", aggregateB.GetHealth())
	}
	if aggregateB.GetAnnotations()["observation.scheduler.detail"] != "ready" {
		t.Fatalf("scheduler observation missing: %+v", aggregateB.GetAnnotations())
	}
	if aggregateB.GetAnnotations()["observation.runtime.detail"] != "crashed" {
		t.Fatalf("runtime observation missing: %+v", aggregateB.GetAnnotations())
	}
}

func TestUpsertAggregatedStatusReplacesByComponent(t *testing.T) {
	t.Parallel()

	current := []*tgsrlv1.ComponentStatus{{
		Component: "runtime",
		Source:    "aggregated",
		Detail:    "old",
	}, {
		Component: "scheduler",
		Source:    "aggregated",
		Detail:    "keep",
	}}
	next := UpsertAggregatedStatus(current, &tgsrlv1.ComponentStatus{
		Component: "runtime",
		Source:    "aggregated",
		Detail:    "new",
	})
	if len(next) != 2 {
		t.Fatalf("len = %d, want 2", len(next))
	}
	if next[0].GetComponent() != "runtime" || next[0].GetDetail() != "new" {
		t.Fatalf("runtime aggregate = %+v, want replaced entry", next[0])
	}
}

func TestMergeAggregatedStatusIgnoresOutOfOrderUpdateFromSameSource(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 12, 30, 0, 0, time.UTC)
	aggregate := MergeAggregatedStatus(nil, &tgsrlv1.ComponentStatus{
		Component:  "runtime",
		Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
		Detail:     "scheduler ready",
		Source:     "scheduler",
		Revision:   5,
		ObservedAt: timestamppb.New(now.Add(time.Minute)),
		JobId:      "job-1",
		RunId:      "run-1",
	})
	aggregate = MergeAggregatedStatus([]*tgsrlv1.ComponentStatus{aggregate}, &tgsrlv1.ComponentStatus{
		Component:  "runtime",
		Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED,
		Detail:     "stale scheduler report",
		Source:     "scheduler",
		Revision:   4,
		ObservedAt: timestamppb.New(now),
		JobId:      "job-1",
		RunId:      "run-1",
	})
	if aggregate.GetAnnotations()["observation.scheduler.detail"] != "scheduler ready" {
		t.Fatalf("scheduler detail = %q, want latest observation kept", aggregate.GetAnnotations()["observation.scheduler.detail"])
	}
	if aggregate.GetAnnotations()["observation.scheduler.revision"] != "5" {
		t.Fatalf("scheduler revision = %q, want 5", aggregate.GetAnnotations()["observation.scheduler.revision"])
	}
}

func TestMergeAggregatedStatusRetainsConflictingSources(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 12, 35, 0, 0, time.UTC)
	aggregate := MergeAggregatedStatus(nil, &tgsrlv1.ComponentStatus{
		Component:  "runtime",
		Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_PROGRESSING,
		Detail:     "queueing",
		Source:     "scheduler",
		Revision:   3,
		ObservedAt: timestamppb.New(now),
		JobId:      "job-1",
		RunId:      "run-1",
	})
	for _, observation := range []*tgsrlv1.ComponentStatus{
		{
			Component:  "runtime",
			Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
			Detail:     "node assigned",
			Source:     "provider",
			Revision:   7,
			ObservedAt: timestamppb.New(now.Add(time.Minute)),
			JobId:      "job-1",
			RunId:      "run-1",
		},
		{
			Component:  "runtime",
			Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_DEGRADED,
			Detail:     "disk pressure",
			Source:     "operator",
			Revision:   2,
			ObservedAt: timestamppb.New(now.Add(2 * time.Minute)),
			JobId:      "job-1",
			RunId:      "run-1",
		},
		{
			Component:  "runtime",
			Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED,
			Detail:     "container crash",
			Source:     "runtime",
			Revision:   9,
			ObservedAt: timestamppb.New(now.Add(3 * time.Minute)),
			JobId:      "job-1",
			RunId:      "run-1",
		},
	} {
		aggregate = MergeAggregatedStatus([]*tgsrlv1.ComponentStatus{aggregate}, observation)
	}
	if aggregate.GetHealth() != tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED {
		t.Fatalf("aggregate health = %s, want FAILED", aggregate.GetHealth())
	}
	if aggregate.GetAnnotations()["observation.sources"] != "operator,provider,runtime,scheduler" {
		t.Fatalf("observation.sources = %q", aggregate.GetAnnotations()["observation.sources"])
	}
	if aggregate.GetAnnotations()["observation.provider.detail"] != "node assigned" {
		t.Fatalf("provider observation missing: %+v", aggregate.GetAnnotations())
	}
	if aggregate.GetAnnotations()["observation.operator.detail"] != "disk pressure" {
		t.Fatalf("operator observation missing: %+v", aggregate.GetAnnotations())
	}
	if aggregate.GetAnnotations()["observation.runtime.detail"] != "container crash" {
		t.Fatalf("runtime observation missing: %+v", aggregate.GetAnnotations())
	}
	if aggregate.GetAnnotations()["aggregation.strategy"] != "retain-latest-per-source-worst-health" {
		t.Fatalf("aggregation.strategy = %q", aggregate.GetAnnotations()["aggregation.strategy"])
	}
}

func TestMergeAggregatedStatusProjectsLatestRuntimeTypedAnnotations(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 12, 40, 0, 0, time.UTC)
	aggregate := MergeAggregatedStatus(nil, &tgsrlv1.ComponentStatus{
		Component:  "runtime",
		Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_PROGRESSING,
		Detail:     "queued",
		Source:     "scheduler",
		Revision:   1,
		ObservedAt: timestamppb.New(now),
		JobId:      "job-1",
		RunId:      "run-1",
	})
	aggregate = MergeAggregatedStatus([]*tgsrlv1.ComponentStatus{aggregate}, &tgsrlv1.ComponentStatus{
		Component:  "runtime",
		Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
		Detail:     "running",
		Source:     "runtime-observation",
		Revision:   2,
		ObservedAt: timestamppb.New(now.Add(time.Minute)),
		Annotations: map[string]string{
			"runtime.state":     tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING.String(),
			"runtime.converged": "true",
		},
		JobId: "job-1",
		RunId: "run-1",
	})
	aggregate = MergeAggregatedStatus([]*tgsrlv1.ComponentStatus{aggregate}, &tgsrlv1.ComponentStatus{
		Component:  "runtime",
		Health:     tgsrlv1.ComponentHealth_COMPONENT_HEALTH_DEGRADED,
		Detail:     "stale paused",
		Source:     "runtime-observation",
		Revision:   1,
		ObservedAt: timestamppb.New(now.Add(-time.Minute)),
		Annotations: map[string]string{
			"runtime.state":     tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED.String(),
			"runtime.converged": "false",
		},
		JobId: "job-1",
		RunId: "run-1",
	})
	if aggregate.GetAnnotations()["runtime.state"] != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING.String() {
		t.Fatalf("runtime.state = %q, want RUNNING", aggregate.GetAnnotations()["runtime.state"])
	}
	if aggregate.GetAnnotations()["runtime.converged"] != "true" {
		t.Fatalf("runtime.converged = %q, want true", aggregate.GetAnnotations()["runtime.converged"])
	}
	if aggregate.GetAnnotations()["observation.runtime-observation.runtime_state"] != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING.String() {
		t.Fatalf("observation.runtime-observation.runtime_state = %q, want RUNNING", aggregate.GetAnnotations()["observation.runtime-observation.runtime_state"])
	}
	if aggregate.GetAnnotations()["observation.runtime-observation.runtime_converged"] != "true" {
		t.Fatalf("observation.runtime-observation.runtime_converged = %q, want true", aggregate.GetAnnotations()["observation.runtime-observation.runtime_converged"])
	}
	if aggregate.GetObservedRuntimeState() != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("observed_runtime_state = %s, want RUNNING", aggregate.GetObservedRuntimeState())
	}
	if !aggregate.GetConverged() {
		t.Fatal("converged = false, want true")
	}
	if aggregate.GetAnnotations()["runtime.source"] != "runtime-observation" {
		t.Fatalf("runtime.source = %q, want runtime-observation", aggregate.GetAnnotations()["runtime.source"])
	}
}

func TestMergeAggregatedStatusSupportsTypedOnlyRuntimeSignal(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	aggregate := MergeAggregatedStatus(nil, &tgsrlv1.ComponentStatus{
		Component:            "runtime",
		Health:               tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
		Detail:               "typed running",
		Source:               "runtime-observation",
		Revision:             3,
		ObservedAt:           timestamppb.New(now),
		ObservedRuntimeState: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		Converged:            true,
		JobId:                "job-1",
		RunId:                "run-1",
	})
	if aggregate.GetObservedRuntimeState() != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("observed_runtime_state = %s, want RUNNING", aggregate.GetObservedRuntimeState())
	}
	if !aggregate.GetConverged() {
		t.Fatal("converged = false, want true")
	}
	if aggregate.GetAnnotations()["runtime.state"] != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING.String() {
		t.Fatalf("legacy runtime.state = %q, want RUNNING", aggregate.GetAnnotations()["runtime.state"])
	}
	if aggregate.GetAnnotations()["runtime.converged"] != "true" {
		t.Fatalf("legacy runtime.converged = %q, want true", aggregate.GetAnnotations()["runtime.converged"])
	}
	if aggregate.GetAnnotations()["runtime.source"] != "runtime-observation" {
		t.Fatalf("runtime.source = %q, want runtime-observation", aggregate.GetAnnotations()["runtime.source"])
	}
}

func TestMergeAggregatedStatusPrefersTypedRuntimeSignalOverConflictingAnnotation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 9, 5, 0, 0, time.UTC)
	aggregate := MergeAggregatedStatus(nil, &tgsrlv1.ComponentStatus{
		Component:            "runtime",
		Health:               tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
		Detail:               "typed wins",
		Source:               "runtime-observation",
		Revision:             4,
		ObservedAt:           timestamppb.New(now),
		ObservedRuntimeState: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		Converged:            true,
		Annotations: map[string]string{
			"runtime.state":     tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED.String(),
			"runtime.converged": "false",
		},
		JobId: "job-1",
		RunId: "run-1",
	})
	if aggregate.GetObservedRuntimeState() != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("observed_runtime_state = %s, want RUNNING", aggregate.GetObservedRuntimeState())
	}
	if !aggregate.GetConverged() {
		t.Fatal("converged = false, want true")
	}
	if aggregate.GetAnnotations()["runtime.state"] != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING.String() {
		t.Fatalf("legacy runtime.state = %q, want RUNNING", aggregate.GetAnnotations()["runtime.state"])
	}
	if aggregate.GetAnnotations()["runtime.converged"] != "true" {
		t.Fatalf("legacy runtime.converged = %q, want true", aggregate.GetAnnotations()["runtime.converged"])
	}
}

func TestMergeAggregatedStatusIgnoresNonAuthoritativeTypedRuntimeSignal(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 9, 10, 0, 0, time.UTC)
	aggregate := MergeAggregatedStatus(nil, &tgsrlv1.ComponentStatus{
		Component:            "runtime",
		Health:               tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
		Detail:               "scheduler running",
		Source:               "scheduler",
		Revision:             5,
		ObservedAt:           timestamppb.New(now),
		ObservedRuntimeState: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		Converged:            true,
		JobId:                "job-1",
		RunId:                "run-1",
	})
	if got := aggregate.GetObservedRuntimeState(); got != tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN {
		t.Fatalf("observed_runtime_state = %s, want UNKNOWN", got)
	}
	if aggregate.GetConverged() {
		t.Fatal("converged = true, want false")
	}
	if got := aggregate.GetAnnotations()["runtime.source"]; got != "" {
		t.Fatalf("runtime.source = %q, want empty", got)
	}
	if got := aggregate.GetAnnotations()["observation.scheduler.runtime_state"]; got != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING.String() {
		t.Fatalf("observation.scheduler.runtime_state = %q, want RUNNING", got)
	}
}

func TestMergeAggregatedStatusKeepsLatestAuthoritativeRuntimeObservation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 9, 15, 0, 0, time.UTC)
	aggregate := MergeAggregatedStatus(nil, &tgsrlv1.ComponentStatus{
		Component:            "runtime",
		Health:               tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
		Detail:               "running",
		Source:               "runtime-observation",
		Revision:             8,
		ObservedAt:           timestamppb.New(now.Add(time.Minute)),
		ObservedRuntimeState: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
		Converged:            true,
		JobId:                "job-1",
		RunId:                "run-1",
	})
	aggregate = MergeAggregatedStatus([]*tgsrlv1.ComponentStatus{aggregate}, &tgsrlv1.ComponentStatus{
		Component:            "runtime",
		Health:               tgsrlv1.ComponentHealth_COMPONENT_HEALTH_DEGRADED,
		Detail:               "stale paused",
		Source:               "runtime-observation",
		Revision:             7,
		ObservedAt:           timestamppb.New(now),
		ObservedRuntimeState: tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED,
		Converged:            false,
		JobId:                "job-1",
		RunId:                "run-1",
	})
	if got := aggregate.GetObservedRuntimeState(); got != tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING {
		t.Fatalf("observed_runtime_state = %s, want RUNNING", got)
	}
	if !aggregate.GetConverged() {
		t.Fatal("converged = false, want true")
	}
	if got := aggregate.GetAnnotations()["runtime.source"]; got != "runtime-observation" {
		t.Fatalf("runtime.source = %q, want runtime-observation", got)
	}
	if got := aggregate.GetAnnotations()["observation.runtime-observation.detail"]; got != "running" {
		t.Fatalf("runtime-observation detail = %q, want running", got)
	}
}
