package scoring

import (
	"reflect"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

func TestComponentsHasExactlyAuthoritativeSurface(t *testing.T) {
	got := Components(0.7, 0.2, 0.1, 0.3)
	want := map[string]float64{
		"capacity_headroom": 0.7,
		"share_headroom":    0.2,
		"ready":             0.1,
		"trace_affinity":    0.3,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Components() = %#v, want %#v", got, want)
	}
}

func TestTraceAffinityLabelVariants(t *testing.T) {
	for _, test := range []struct {
		name   string
		intent *tgsrlv1.SchedulingIntent
		key    string
		value  string
		want   float64
	}{
		{name: "trace", intent: &tgsrlv1.SchedulingIntent{TraceId: "TRACE-123"}, key: "trace", value: "trace-123", want: 1},
		{name: "trace underscore", intent: &tgsrlv1.SchedulingIntent{TraceId: "trace-123"}, key: "trace_id", value: "trace-123", want: 1},
		{name: "trace hyphen", intent: &tgsrlv1.SchedulingIntent{TraceId: "trace-123"}, key: "trace-id", value: "prefix/trace-123/suffix", want: 0.9},
		{name: "job", intent: &tgsrlv1.SchedulingIntent{JobId: "job-1"}, key: "job", value: "job-1", want: 0.6},
		{name: "job underscore", intent: &tgsrlv1.SchedulingIntent{JobId: "job-1"}, key: "job_id", value: "job-1", want: 0.6},
		{name: "job hyphen", intent: &tgsrlv1.SchedulingIntent{JobId: "job-1"}, key: "job-id", value: "prefix/job-1/suffix", want: 0.5},
	} {
		t.Run(test.name, func(t *testing.T) {
			device := &tgsrlv1.Device{Labels: map[string]string{test.key: test.value}}
			if got := TraceAffinity(test.intent, device); got != test.want {
				t.Fatalf("TraceAffinity() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestTraceAffinityDoesNotCrossMatchLabelKinds(t *testing.T) {
	intent := &tgsrlv1.SchedulingIntent{TraceId: "trace-1", JobId: "job-1"}
	device := &tgsrlv1.Device{Labels: map[string]string{
		"job":   "trace-1",
		"trace": "job-1",
	}}
	if got := TraceAffinity(intent, device); got != 0 {
		t.Fatalf("TraceAffinity() = %v, want 0 for crossed label kinds", got)
	}
}

func TestTraceAffinityPrefersTraceOverJob(t *testing.T) {
	intent := &tgsrlv1.SchedulingIntent{TraceId: "trace-1", JobId: "job-1"}
	device := &tgsrlv1.Device{Labels: map[string]string{
		"trace_id": "prefix-trace-1",
		"job_id":   "job-1",
	}}
	if got := TraceAffinity(intent, device); got != 0.9 {
		t.Fatalf("TraceAffinity() = %v, want 0.9", got)
	}
}

func TestTraceAffinityHandlesMissingInput(t *testing.T) {
	if got := TraceAffinity(nil, &tgsrlv1.Device{}); got != 0 {
		t.Fatalf("TraceAffinity(nil, device) = %v, want 0", got)
	}
	if got := TraceAffinity(&tgsrlv1.SchedulingIntent{TraceId: "trace-1"}, nil); got != 0 {
		t.Fatalf("TraceAffinity(intent, nil) = %v, want 0", got)
	}
}
