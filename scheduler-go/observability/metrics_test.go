package observability

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPrometheusRecorderHandler(t *testing.T) {
	recorder := NewPrometheusRecorder()
	recorder.IncCounter("decisions selected", 2)
	recorder.SetGauge("decision_cursor", 9)
	recorder.ObserveHistogram("schedule_latency_seconds", .25)
	recorder.ObserveHistogram("schedule_latency_seconds", .75)

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	recorder.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	body := response.Body.String()
	for _, expected := range []string{
		"tgsrl_decisions_selected 2",
		"tgsrl_decision_cursor 9",
		"tgsrl_schedule_latency_seconds_count 2",
		"tgsrl_schedule_latency_seconds_sum 1",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("metrics body %q does not contain %q", body, expected)
		}
	}
}

func TestSchedulerTelemetryUsesBoundedCorrelationLabels(t *testing.T) {
	recorder := NewPrometheusRecorder()
	correlation := Correlation{JobID: "private-job-id", RunID: "private-run-id", TraceID: "private-trace-id"}
	recorder.RecordQueueDepth(correlation, 3)
	recorder.RecordAction(correlation, "SUCCEEDED")
	recorder.RecordDecision(correlation, true, "PROVIDER_UNAVAILABLE: private detail")
	recorder.ObserveSchedule(correlation, 0.125)
	recorder.ObserveLoop("fast", 10, 10.02, 10.07)

	response := httptest.NewRecorder()
	recorder.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	for _, raw := range []string{correlation.JobID, correlation.RunID, correlation.TraceID, "private detail"} {
		if strings.Contains(body, raw) {
			t.Fatalf("metrics leaked raw high-cardinality value %q: %s", raw, body)
		}
	}
	for _, expected := range []string{
		"tgsrl_scheduler_queue_length{",
		"status=\"succeeded\"",
		"outcome=\"fallback\"",
		"reason=\"provider_unavailable\"",
		"tgsrl_scheduler_degradations_total{",
		"tgsrl_scheduler_schedule_latency_seconds_count{",
		"tgsrl_scheduler_loop_jitter_seconds_count{loop=\"fast\"} 1",
		"tgsrl_scheduler_loop_latency_seconds_count{loop=\"fast\"} 1",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("metrics body does not contain %q: %s", expected, body)
		}
	}
}

func TestUnknownDimensionsCollapseIntoBoundedClasses(t *testing.T) {
	recorder := NewPrometheusRecorder()
	recorder.RecordAction(Correlation{}, "vendor-specific-secret-status")
	recorder.RecordDecision(Correlation{}, true, "arbitrary private error text")

	response := httptest.NewRecorder()
	recorder.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	if strings.Contains(body, "vendor-specific") || strings.Contains(body, "arbitrary private") {
		t.Fatalf("unbounded dimension leaked into metrics: %s", body)
	}
	if !strings.Contains(body, "status=\"unknown\"") || !strings.Contains(body, "reason=\"other\"") {
		t.Fatalf("unknown dimensions were not collapsed: %s", body)
	}
}

func TestCorrelationCardinalityIsBounded(t *testing.T) {
	seen := map[string]struct{}{}
	for index := 0; index < 10_000; index++ {
		labels := correlationLabels(Correlation{JobID: fmt.Sprintf("job-%d", index), RunID: fmt.Sprintf("run-%d", index), TraceID: fmt.Sprintf("trace-%d", index)})
		seen[labels["correlation_bucket"]] = struct{}{}
	}
	if len(seen) > correlationBucketCount {
		t.Fatalf("correlation buckets = %d, want <= %d", len(seen), correlationBucketCount)
	}
}
