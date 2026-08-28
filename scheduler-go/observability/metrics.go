package observability

import (
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const correlationBucketCount = 64

// Correlation identifies a request without exporting high-cardinality raw IDs.
// Each identifier is mapped into one of a fixed number of stable buckets.
type Correlation struct {
	JobID   string
	RunID   string
	TraceID string
}

// SchedulerTelemetry is an optional, domain-oriented extension to Recorder.
// Callers should use a type assertion so lightweight Recorder implementations
// remain source compatible.
type SchedulerTelemetry interface {
	RecordQueueDepth(Correlation, int)
	RecordAction(Correlation, string)
	RecordDecision(Correlation, bool, string)
	ObserveSchedule(Correlation, float64)
	ObserveLoop(kind string, scheduled, started, completed float64)
}

// Recorder captures lightweight scheduler package metrics without imposing a
// concrete metrics backend on the caller.
type Recorder interface {
	IncCounter(name string, delta int64)
	SetGauge(name string, value int64)
	ObserveHistogram(name string, value float64)
}

// NopRecorder ignores all metrics.
type NopRecorder struct{}

func (NopRecorder) IncCounter(string, int64)         {}
func (NopRecorder) SetGauge(string, int64)           {}
func (NopRecorder) ObserveHistogram(string, float64) {}

// InMemoryRecorder is test-friendly and race-safe.
type InMemoryRecorder struct {
	mu         sync.Mutex
	counters   map[string]int64
	gauges     map[string]int64
	histograms map[string][]float64
}

func (r *InMemoryRecorder) RecordQueueDepth(c Correlation, depth int) {
	r.mu.Lock()
	r.gauges[seriesKey("scheduler_queue_length", correlationLabels(c))] = int64(depth)
	r.mu.Unlock()
}

func (r *InMemoryRecorder) RecordAction(c Correlation, status string) {
	labels := correlationLabels(c)
	labels["status"] = boundedValue(status, "unknown", "succeeded", "failed", "skipped", "rolled_back")
	r.mu.Lock()
	r.counters[seriesKey("scheduler_actions_total", labels)]++
	r.mu.Unlock()
}

func (r *InMemoryRecorder) RecordDecision(c Correlation, fallback bool, reason string) {
	labels := correlationLabels(c)
	labels["outcome"] = "selected"
	if fallback {
		labels["outcome"] = "fallback"
	}
	labels["reason"] = reasonClass(reason)
	r.mu.Lock()
	r.counters[seriesKey("scheduler_decisions_total", labels)]++
	if fallback {
		r.counters[seriesKey("scheduler_degradations_total", labels)]++
	}
	r.mu.Unlock()
}

func (r *InMemoryRecorder) ObserveSchedule(c Correlation, seconds float64) {
	r.mu.Lock()
	r.histograms[seriesKey("scheduler_schedule_latency_seconds", correlationLabels(c))] = append(r.histograms[seriesKey("scheduler_schedule_latency_seconds", correlationLabels(c))], seconds)
	r.mu.Unlock()
}

func (r *InMemoryRecorder) ObserveLoop(kind string, scheduled, started, completed float64) {
	labels := map[string]string{"loop": boundedValue(kind, "unknown", "fast", "medium", "slow")}
	r.mu.Lock()
	jitterKey := seriesKey("scheduler_loop_jitter_seconds", labels)
	latencyKey := seriesKey("scheduler_loop_latency_seconds", labels)
	r.histograms[jitterKey] = append(r.histograms[jitterKey], math.Max(0, started-scheduled))
	r.histograms[latencyKey] = append(r.histograms[latencyKey], math.Max(0, completed-started))
	r.mu.Unlock()
}

// PrometheusRecorder is a dependency-free Prometheus text collector. Metric
// names are sanitized and histograms expose count/sum without invented buckets.
type PrometheusRecorder struct {
	mu         sync.RWMutex
	counters   map[string]float64
	gauges     map[string]float64
	histograms map[string]histogramValue
}

// RecordQueueDepth records the current keyed-worker depth.
func (r *PrometheusRecorder) RecordQueueDepth(c Correlation, depth int) {
	r.setGaugeSeries("scheduler_queue_length", correlationLabels(c), float64(depth))
}

// RecordAction records a terminal provider action status.
func (r *PrometheusRecorder) RecordAction(c Correlation, status string) {
	labels := correlationLabels(c)
	labels["status"] = boundedValue(status, "unknown", "succeeded", "failed", "skipped", "rolled_back")
	r.addCounterSeries("scheduler_actions_total", labels, 1)
}

// RecordDecision records selected, fallback, or degraded decisions.
func (r *PrometheusRecorder) RecordDecision(c Correlation, fallback bool, reason string) {
	labels := correlationLabels(c)
	labels["outcome"] = "selected"
	if fallback {
		labels["outcome"] = "fallback"
	}
	labels["reason"] = reasonClass(reason)
	r.addCounterSeries("scheduler_decisions_total", labels, 1)
	if fallback {
		r.addCounterSeries("scheduler_degradations_total", labels, 1)
	}
}

// ObserveSchedule records end-to-end evaluation latency in seconds.
func (r *PrometheusRecorder) ObserveSchedule(c Correlation, seconds float64) {
	r.observeHistogramSeries("scheduler_schedule_latency_seconds", correlationLabels(c), seconds)
}

// ObserveLoop records loop latency and start jitter. Times are Unix seconds.
func (r *PrometheusRecorder) ObserveLoop(kind string, scheduled, started, completed float64) {
	labels := map[string]string{"loop": boundedValue(kind, "unknown", "fast", "medium", "slow")}
	r.observeHistogramSeries("scheduler_loop_jitter_seconds", labels, math.Max(0, started-scheduled))
	r.observeHistogramSeries("scheduler_loop_latency_seconds", labels, math.Max(0, completed-started))
}

func (r *PrometheusRecorder) addCounterSeries(name string, labels map[string]string, delta float64) {
	r.mu.Lock()
	r.counters[seriesKey(name, labels)] += delta
	r.mu.Unlock()
}

func (r *PrometheusRecorder) setGaugeSeries(name string, labels map[string]string, value float64) {
	r.mu.Lock()
	r.gauges[seriesKey(name, labels)] = value
	r.mu.Unlock()
}

func (r *PrometheusRecorder) observeHistogramSeries(name string, labels map[string]string, value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	r.mu.Lock()
	key := seriesKey(name, labels)
	histogram := r.histograms[key]
	histogram.count++
	histogram.sum += value
	r.histograms[key] = histogram
	r.mu.Unlock()
}

type histogramValue struct {
	count uint64
	sum   float64
}

// NewPrometheusRecorder constructs an empty Prometheus collector.
func NewPrometheusRecorder() *PrometheusRecorder {
	return &PrometheusRecorder{counters: make(map[string]float64), gauges: make(map[string]float64), histograms: make(map[string]histogramValue)}
}

func (r *PrometheusRecorder) IncCounter(name string, delta int64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.counters[sanitizeMetricName(name)] += float64(delta)
	r.mu.Unlock()
}

func (r *PrometheusRecorder) SetGauge(name string, value int64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.gauges[sanitizeMetricName(name)] = float64(value)
	r.mu.Unlock()
}

func (r *PrometheusRecorder) ObserveHistogram(name string, value float64) {
	if r == nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	name = sanitizeMetricName(name)
	r.mu.Lock()
	histogram := r.histograms[name]
	histogram.count++
	histogram.sum += value
	r.histograms[name] = histogram
	r.mu.Unlock()
}

// Counter returns a current counter value for diagnostics and tests.
func (r *PrometheusRecorder) Counter(name string) float64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.counters[sanitizeMetricName(name)]
}

// Gauge returns a current gauge value for diagnostics and tests.
func (r *PrometheusRecorder) Gauge(name string) float64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.gauges[sanitizeMetricName(name)]
}

// WritePrometheus writes one stable Prometheus text snapshot.
func (r *PrometheusRecorder) WritePrometheus(writer io.Writer) error {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	counters := cloneFloatMap(r.counters)
	gauges := cloneFloatMap(r.gauges)
	histograms := make(map[string]histogramValue, len(r.histograms))
	for name, value := range r.histograms {
		histograms[name] = value
	}
	r.mu.RUnlock()
	lastType := ""
	for _, key := range sortedMetricNames(counters) {
		name, labels := splitSeriesKey(key)
		if name != lastType {
			if _, err := fmt.Fprintf(writer, "# TYPE %s counter\n", name); err != nil {
				return err
			}
			lastType = name
		}
		if _, err := fmt.Fprintf(writer, "%s%s %s\n", name, labels, formatMetric(counters[key])); err != nil {
			return err
		}
	}
	lastType = ""
	for _, key := range sortedMetricNames(gauges) {
		name, labels := splitSeriesKey(key)
		if name != lastType {
			if _, err := fmt.Fprintf(writer, "# TYPE %s gauge\n", name); err != nil {
				return err
			}
			lastType = name
		}
		if _, err := fmt.Fprintf(writer, "%s%s %s\n", name, labels, formatMetric(gauges[key])); err != nil {
			return err
		}
	}
	keys := make([]string, 0, len(histograms))
	for name := range histograms {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	lastType = ""
	for _, key := range keys {
		value := histograms[key]
		name, labels := splitSeriesKey(key)
		if name != lastType {
			if _, err := fmt.Fprintf(writer, "# TYPE %s summary\n", name); err != nil {
				return err
			}
			lastType = name
		}
		if _, err := fmt.Fprintf(writer, "%s_count%s %d\n%s_sum%s %s\n", name, labels, value.count, name, labels, formatMetric(value.sum)); err != nil {
			return err
		}
	}
	return nil
}

// Handler exposes metrics using Prometheus text format.
func (r *PrometheusRecorder) Handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := r.WritePrometheus(writer); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
	})
}

func sanitizeMetricName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "tgsrl_unnamed"
	}
	var builder strings.Builder
	for index, char := range name {
		valid := char == '_' || char == ':' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || index > 0 && char >= '0' && char <= '9'
		if valid {
			builder.WriteRune(char)
		} else {
			builder.WriteByte('_')
		}
	}
	return "tgsrl_" + builder.String()
}

func cloneFloatMap(source map[string]float64) map[string]float64 {
	result := make(map[string]float64, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func sortedMetricNames(values map[string]float64) []string {
	result := make([]string, 0, len(values))
	for name := range values {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func formatMetric(value float64) string { return strconv.FormatFloat(value, 'g', -1, 64) }

func correlationLabels(c Correlation) map[string]string {
	material := c.JobID + "\x00" + c.RunID + "\x00" + c.TraceID
	if strings.TrimSpace(c.JobID) == "" && strings.TrimSpace(c.RunID) == "" && strings.TrimSpace(c.TraceID) == "" {
		material = ""
	}
	return map[string]string{
		"correlation_bucket": hashBucket(material),
		"has_job":            presence(c.JobID),
		"has_run":            presence(c.RunID),
		"has_trace":          presence(c.TraceID),
	}
}

func presence(value string) string {
	if strings.TrimSpace(value) == "" {
		return "false"
	}
	return "true"
}

func hashBucket(value string) string {
	if strings.TrimSpace(value) == "" {
		return "none"
	}
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("b%02d", int(sum[0])%correlationBucketCount)
}

func boundedValue(value, fallback string, allowed ...string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	for _, candidate := range allowed {
		if normalized == candidate {
			return normalized
		}
	}
	return fallback
}

func reasonClass(reason string) string {
	normalized := strings.ToUpper(strings.TrimSpace(reason))
	if index := strings.IndexAny(normalized, ": "); index >= 0 {
		normalized = normalized[:index]
	}
	switch normalized {
	case "":
		return "none"
	case "NO_CANDIDATE", "INTENT_EXPIRED", "STALE_INTENT", "REVISION_MISMATCH",
		"SAFE_POINT_REQUIRED", "PROVIDER_UNAVAILABLE", "PROVIDER_EXECUTION_FAILED",
		"PERSISTENCE_UNAVAILABLE", "STATE_FINALIZE_FAILED", "EVALUATION_FAILED",
		"INTENT_SUPERSEDED", "BUDGET_EXHAUSTED", "COOLDOWN", "HYSTERESIS",
		"CIRCUIT_OPEN", "NO_OP_POLICY", "STATIC", "STATIC_EMPTY":
		return strings.ToLower(normalized)
	default:
		return "other"
	}
}

func seriesKey(name string, labels map[string]string) string {
	name = sanitizeMetricName(name)
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, sanitizeLabelName(key)+"="+escapeLabelValue(labels[key]))
	}
	return name + "|" + strings.Join(parts, ",")
}

func splitSeriesKey(key string) (string, string) {
	name, encoded, found := strings.Cut(key, "|")
	if !found {
		return name, ""
	}
	parts := strings.Split(encoded, ",")
	labels := make([]string, 0, len(parts))
	for _, part := range parts {
		label, value, ok := strings.Cut(part, "=")
		if ok {
			labels = append(labels, label+"=\""+value+"\"")
		}
	}
	return name, "{" + strings.Join(labels, ",") + "}"
}

func sanitizeLabelName(value string) string {
	var builder strings.Builder
	for index, char := range value {
		if char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || index > 0 && char >= '0' && char <= '9' {
			builder.WriteRune(char)
		} else {
			builder.WriteByte('_')
		}
	}
	return builder.String()
}

func escapeLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return strings.ReplaceAll(value, "\n", `\n`)
}

// NewInMemoryRecorder constructs an empty in-memory recorder.
func NewInMemoryRecorder() *InMemoryRecorder {
	return &InMemoryRecorder{
		counters:   map[string]int64{},
		gauges:     map[string]int64{},
		histograms: map[string][]float64{},
	}
}

func (r *InMemoryRecorder) IncCounter(name string, delta int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters[name] += delta
}

func (r *InMemoryRecorder) SetGauge(name string, value int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[name] = value
}

func (r *InMemoryRecorder) ObserveHistogram(name string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.histograms[name] = append(r.histograms[name], value)
}

// Counter returns the current counter value.
func (r *InMemoryRecorder) Counter(name string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counters[name]
}

// Gauge returns the current gauge value.
func (r *InMemoryRecorder) Gauge(name string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gauges[name]
}

// Histogram returns a copy of all observed histogram values.
func (r *InMemoryRecorder) Histogram(name string) []float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]float64(nil), r.histograms[name]...)
}
