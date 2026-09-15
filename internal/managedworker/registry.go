package managedworker

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Blizzard-cyber/TGS-RL/internal/bootstrapauth"
)

const WorkerRegistryTokenHeader = "X-TGSRL-Worker-Token"

const maxRegistryPayloadBytes = 64 << 10
const maxTracePayloadBytes = 1 << 20

// WorkerObserver receives authoritative bootstrap lifecycle observations after
// the durable registry update succeeds. Callers must make observations
// idempotent because a bootstrap may retry after losing the HTTP response.
type WorkerObserver func(context.Context, Worker) error

// WorkerAuthority validates that a bootstrap registration still matches the
// Scheduler's current binding and generation before it becomes controllable.
type WorkerAuthority func(context.Context, Worker) error

// WorkerTracePublisher forwards an authenticated worker trace payload to the
// Runtime authority. The registry intentionally treats the protobuf bytes as
// opaque after binding and generation authorization.
type WorkerTracePublisher func(context.Context, Worker, WorkerTraceRequest) (WorkerTraceResponse, error)

type exitReport struct {
	SandboxID    string `json:"sandbox_id"`
	Generation   uint64 `json:"generation"`
	InstanceID   string `json:"instance_id"`
	ProcessToken string `json:"process_token"`
	State        string `json:"state"`
	ExitCode     int    `json:"exit_code"`
	Detail       string `json:"detail,omitempty"`
}

// WorkerActionRequest is the narrow Operator-to-registry lifecycle contract.
// Authentication remains scoped to one exact binding through the bootstrap
// registration token; callers cannot enumerate or target unrelated workers.
type WorkerActionRequest struct {
	Action         string `json:"action"`
	SandboxID      string `json:"sandbox_id"`
	Generation     uint64 `json:"generation"`
	IdempotencyKey string `json:"idempotency_key"`
}

var scopedWorkerActions = map[string]struct{}{
	"pause": {}, "checkpoint": {}, "resume": {}, "sleep": {}, "offload": {}, "reload": {}, "stop": {},
}

// WorkerStatusRequest identifies one exact registered workload generation.
type WorkerStatusRequest struct {
	SandboxID  string `json:"sandbox_id"`
	Generation uint64 `json:"generation"`
}

// WorkerTraceRequest carries one deterministic protobuf TraceEventBatch.
// Encoding []byte through JSON uses standard base64 without exposing the
// registry credential to the workload process.
type WorkerTraceRequest struct {
	SandboxID      string `json:"sandbox_id"`
	Generation     uint64 `json:"generation"`
	IdempotencyKey string `json:"idempotency_key"`
	Batch          []byte `json:"batch"`
}

type WorkerTraceResponse struct {
	AcceptedEventCount uint64 `json:"accepted_event_count"`
	Idempotent         bool   `json:"idempotent"`
	Cursor             string `json:"cursor,omitempty"`
}

type workerActionResponse struct {
	Accepted                 bool   `json:"accepted"`
	Worker                   Worker `json:"worker"`
	OffloadedBefore          bool   `json:"offloaded_before,omitempty"`
	GPUMemoryObservedBefore  bool   `json:"gpu_memory_observed_before,omitempty"`
	GPUMemoryAllocatedBefore uint64 `json:"gpu_memory_allocated_before_bytes,omitempty"`
	GPUMemoryReservedBefore  uint64 `json:"gpu_memory_reserved_before_bytes,omitempty"`
}

type registryHandler struct {
	controller   *Controller
	store        *Store
	signingKey   []byte
	authorize    WorkerAuthority
	observe      WorkerObserver
	publishTrace WorkerTracePublisher
}

func NewRegistryHandler(controller *Controller, store *Store, signingKey []byte, authorize WorkerAuthority, observe WorkerObserver, tracePublishers ...WorkerTracePublisher) (http.Handler, error) {
	if controller == nil || store == nil {
		return nil, errors.New("runtime controller and store are required")
	}
	if len(signingKey) < 32 {
		return nil, errors.New("worker registry signing key must contain at least 32 bytes")
	}
	if len(tracePublishers) > 1 {
		return nil, errors.New("worker registry accepts at most one trace publisher")
	}
	var publishTrace WorkerTracePublisher
	if len(tracePublishers) == 1 {
		publishTrace = tracePublishers[0]
	}
	handler := &registryHandler{controller: controller, store: store, signingKey: append([]byte(nil), signingKey...), authorize: authorize, observe: observe, publishTrace: publishTrace}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handler.health)
	mux.HandleFunc("/v1/workers/register", handler.register)
	mux.HandleFunc("/v1/workers/exit", handler.reportExit)
	mux.HandleFunc("/v1/workers/action", handler.applyAction)
	mux.HandleFunc("/v1/workers/status", handler.status)
	mux.HandleFunc("/v1/workers/trace", handler.publishWorkerTrace)
	return mux, nil
}

func (h *registryHandler) publishWorkerTrace(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if h.publishTrace == nil {
		http.Error(w, "worker trace publication is unavailable", http.StatusServiceUnavailable)
		return
	}
	var trace WorkerTraceRequest
	if err := decodeRegistryRequestLimit(request, &trace, maxTracePayloadBytes); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	trace.SandboxID = strings.TrimSpace(trace.SandboxID)
	trace.IdempotencyKey = strings.TrimSpace(trace.IdempotencyKey)
	if trace.SandboxID == "" || trace.Generation == 0 || trace.IdempotencyKey == "" || len(trace.Batch) == 0 {
		http.Error(w, "worker trace requires sandbox, generation, idempotency_key, and batch", http.StatusBadRequest)
		return
	}
	state, err := h.store.Snapshot()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	worker, found := state.Workers[trace.SandboxID]
	if !found || !authorizedRegisteredWorker(request, worker) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if worker.Generation != trace.Generation {
		http.Error(w, "worker generation mismatch", http.StatusConflict)
		return
	}
	if h.authorize != nil {
		if err := h.authorize(request.Context(), worker); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
	}
	response, err := h.publishTrace(request.Context(), worker, trace)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeRegistryResponse(w, http.StatusOK, response)
}

func (h *registryHandler) applyAction(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var action WorkerActionRequest
	if err := decodeRegistryRequest(request, &action); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	action.Action = strings.TrimSpace(action.Action)
	action.SandboxID = strings.TrimSpace(action.SandboxID)
	action.IdempotencyKey = strings.TrimSpace(action.IdempotencyKey)
	if action.Action == "terminate" {
		action.Action = "stop"
	}
	if action.SandboxID == "" || action.Generation == 0 || action.IdempotencyKey == "" {
		http.Error(w, "worker action requires sandbox, generation, and idempotency_key", http.StatusBadRequest)
		return
	}
	if _, supported := scopedWorkerActions[action.Action]; !supported {
		http.Error(w, "unsupported worker action", http.StatusBadRequest)
		return
	}
	state, err := h.store.Snapshot()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	worker, found := state.Workers[action.SandboxID]
	if !found || !authorizedRegisteredWorker(request, worker) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if worker.Generation != action.Generation {
		http.Error(w, "worker generation mismatch", http.StatusConflict)
		return
	}
	if _, replay := state.Receipts[action.IdempotencyKey]; !replay {
		worker, err = h.controller.DiscoverWorker(
			request.Context(), action.SandboxID, action.Generation,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
	}
	digest := RequestDigest(action.Action, action.SandboxID, action.Generation)
	updated, err := h.controller.Apply(request.Context(), ActionRequest{
		Operation:             action.Action,
		SandboxID:             action.SandboxID,
		Generation:            action.Generation,
		IdempotencyKey:        action.IdempotencyKey,
		StepIndex:             0,
		ExpectedActions:       1,
		TransactionGeneration: action.Generation,
		PlanDigest:            digest,
		CommandDigest:         digest,
		ActionID:              action.IdempotencyKey,
		PlanID:                "operator-lifecycle",
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	current, err := h.store.Snapshot()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	receipt, found := current.Receipts[action.IdempotencyKey]
	if !found {
		http.Error(w, "worker action receipt is unavailable", http.StatusInternalServerError)
		return
	}
	writeRegistryResponse(w, http.StatusOK, workerActionResponse{
		Accepted:                 true,
		Worker:                   publicWorker(updated),
		OffloadedBefore:          receipt.SourceOffloaded,
		GPUMemoryObservedBefore:  receipt.SourceGPUMemoryObserved,
		GPUMemoryAllocatedBefore: receipt.SourceGPUMemoryAllocated,
		GPUMemoryReservedBefore:  receipt.SourceGPUMemoryReserved,
	})
}

func (h *registryHandler) status(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var query WorkerStatusRequest
	if err := decodeRegistryRequest(request, &query); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	state, err := h.store.Snapshot()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	worker, found := state.Workers[strings.TrimSpace(query.SandboxID)]
	if !found {
		http.Error(w, "worker is not registered", http.StatusNotFound)
		return
	}
	if !authorizedRegisteredWorker(request, worker) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if query.Generation == 0 || worker.Generation != query.Generation {
		http.Error(w, "worker generation mismatch", http.StatusConflict)
		return
	}
	worker, err = h.controller.DiscoverWorker(request.Context(), query.SandboxID, query.Generation)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeRegistryResponse(w, http.StatusOK, workerActionResponse{Accepted: true, Worker: publicWorker(worker)})
}

func publicWorker(worker Worker) Worker {
	worker.ProcessToken = ""
	worker.ControlSocket = ""
	worker.ControlURL = ""
	worker.ControlToken = ""
	worker.RegistrationTokenHash = ""
	worker.MPSServerProcessToken = ""
	worker.SafePointFile = ""
	worker.ReadinessFile = ""
	return worker
}

func (h *registryHandler) health(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *registryHandler) register(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var worker Worker
	if err := decodeRegistryRequest(request, &worker); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !h.authorizedWorker(request, worker) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	worker.RegistrationTokenHash = registryTokenHash(request.Header.Get(WorkerRegistryTokenHeader))
	if err := validateBootstrapEndpoint(worker.ControlURL, request.RemoteAddr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if h.authorize != nil {
		if err := h.authorize(request.Context(), worker); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
	}
	if err := h.controller.Register(request.Context(), worker); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	state, err := h.store.Snapshot()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	registered := state.Workers[worker.SandboxID]
	if h.observe != nil {
		if err := h.observe(request.Context(), registered); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
	}
	writeRegistryResponse(w, http.StatusOK, map[string]any{"accepted": true})
}

func (h *registryHandler) reportExit(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var report exitReport
	if err := decodeRegistryRequest(request, &report); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	state, snapshotErr := h.store.Snapshot()
	if snapshotErr != nil {
		http.Error(w, snapshotErr.Error(), http.StatusInternalServerError)
		return
	}
	current, found := state.Workers[report.SandboxID]
	if !found || !authorizedRegisteredWorker(request, current) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	updated, matched, err := h.store.ReportExit(report.SandboxID, report.Generation, report.InstanceID, report.ProcessToken, report.State, report.ExitCode, report.Detail)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	// A deliberate stop is observed through the Operator backend so the
	// terminal event carries the command request/idempotency causality. The
	// registry still persists the real process exit code and reports crashes or
	// otherwise uncommanded exits directly.
	commandedStop := current.State == "terminated" && current.LastOperation == "stop"
	if matched && !commandedStop && h.observe != nil {
		state, snapshotErr := h.store.Snapshot()
		if snapshotErr != nil {
			http.Error(w, snapshotErr.Error(), http.StatusInternalServerError)
			return
		}
		worker := state.Workers[report.SandboxID]
		if err := h.observe(request.Context(), worker); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
	}
	writeRegistryResponse(w, http.StatusOK, map[string]any{"accepted": true, "updated": updated, "matched": matched})
}

func (h *registryHandler) authorizedWorker(request *http.Request, worker Worker) bool {
	return bootstrapauth.Verify(h.signingKey, registrationClaims(worker), request.Header.Get(WorkerRegistryTokenHeader))
}

func authorizedRegisteredWorker(request *http.Request, worker Worker) bool {
	expected := []byte(strings.TrimSpace(worker.RegistrationTokenHash))
	provided := []byte(registryTokenHash(request.Header.Get(WorkerRegistryTokenHeader)))
	return len(expected) != 0 && len(expected) == len(provided) && subtle.ConstantTimeCompare(expected, provided) == 1
}

func credentialHash(token string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return fmt.Sprintf("sha256:%x", digest[:])
}

func registryTokenHash(token string) string { return credentialHash(token) }

func decodeRegistryRequest(request *http.Request, value any) error {
	return decodeRegistryRequestLimit(request, value, maxRegistryPayloadBytes)
}

func decodeRegistryRequestLimit(request *http.Request, value any, limit int64) error {
	if request.Body == nil {
		return errors.New("request body is required")
	}
	decoder := json.NewDecoder(io.LimitReader(request.Body, limit+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode worker registry request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("worker registry request must contain one JSON object")
	}
	return nil
}

func writeRegistryResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// RegisterRemoteWorker registers a bootstrap-owned process with the central
// durable registry. The registry URL is a base URL, not a credential carrier.
func RegisterRemoteWorker(ctx context.Context, registryURL, token string, worker Worker) error {
	return postRegistry(ctx, registryURL, token, "/v1/workers/register", worker)
}

// ReportRemoteWorkerExit records the terminal state of one exact bootstrap
// generation. A stale generation is acknowledged as a no-op by the registry.
func ReportRemoteWorkerExit(ctx context.Context, registryURL, token string, worker Worker) error {
	report := exitReport{SandboxID: worker.SandboxID, Generation: worker.Generation, InstanceID: worker.InstanceID, ProcessToken: worker.ProcessToken, State: worker.State, ExitCode: worker.ExitCode, Detail: worker.Detail}
	return postRegistry(ctx, registryURL, token, "/v1/workers/exit", report)
}

// ApplyRemoteWorkerAction applies one generation-fenced lifecycle action to a
// previously registered worker. The registration token is never placed in the
// URL or response.
func ApplyRemoteWorkerAction(ctx context.Context, registryURL, token string, action WorkerActionRequest) (Worker, error) {
	var response workerActionResponse
	if err := postRegistryResponse(ctx, registryURL, token, "/v1/workers/action", action, &response); err != nil {
		return Worker{}, err
	}
	if !response.Accepted {
		return Worker{}, errors.New("worker registry did not accept lifecycle action")
	}
	return response.Worker, nil
}

// GetRemoteWorker returns one scoped registration without exposing registry
// enumeration. A not-yet-registered worker returns found=false.
func GetRemoteWorker(ctx context.Context, registryURL, token string, query WorkerStatusRequest) (Worker, bool, error) {
	var response workerActionResponse
	status, err := postRegistryStatus(ctx, registryURL, token, "/v1/workers/status", query, &response)
	if err != nil {
		return Worker{}, false, err
	}
	if status == http.StatusNotFound {
		return Worker{}, false, nil
	}
	if status < 200 || status >= 300 {
		return Worker{}, false, fmt.Errorf("worker registry returned HTTP %d", status)
	}
	return response.Worker, response.Accepted, nil
}

// PublishRemoteWorkerTrace sends one authenticated typed trace batch through
// the registry without exposing the registration token to the worker process.
func PublishRemoteWorkerTrace(ctx context.Context, registryURL, token string, trace WorkerTraceRequest) (WorkerTraceResponse, error) {
	var response WorkerTraceResponse
	if err := postRegistryResponse(ctx, registryURL, token, "/v1/workers/trace", trace, &response); err != nil {
		return WorkerTraceResponse{}, err
	}
	return response, nil
}

func postRegistry(ctx context.Context, baseURL, token, path string, value any) error {
	return postRegistryResponse(ctx, baseURL, token, path, value, nil)
}

func postRegistryResponse(ctx context.Context, baseURL, token, path string, value, output any) error {
	status, err := postRegistryStatus(ctx, baseURL, token, path, value, output)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("worker registry returned HTTP %d", status)
	}
	return nil
}

func postRegistryStatus(ctx context.Context, baseURL, token, path string, value, output any) (int, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return 0, errors.New("worker registry URL must be an absolute HTTP(S) base URL without credentials, query, or fragment")
	}
	if strings.TrimSpace(token) == "" {
		return 0, errors.New("worker registry token is required")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + path
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), strings.NewReader(string(payload)))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(WorkerRegistryTokenHeader, token)
	client := &http.Client{
		Timeout:       30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		if response.StatusCode == http.StatusNotFound {
			return response.StatusCode, nil
		}
		return response.StatusCode, fmt.Errorf("worker registry returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(detail)))
	}
	if output != nil {
		limit := int64(maxRegistryPayloadBytes)
		if path == "/v1/workers/trace" {
			limit = maxTracePayloadBytes
		}
		decoder := json.NewDecoder(io.LimitReader(response.Body, limit+1))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(output); err != nil {
			return response.StatusCode, fmt.Errorf("decode worker registry response: %w", err)
		}
	}
	return response.StatusCode, nil
}

func validateBootstrapEndpoint(endpoint, remoteAddress string) error {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("bootstrap control URL must be an absolute HTTP(S) URL without credentials, query, or fragment")
	}
	remoteHost, _, err := net.SplitHostPort(remoteAddress)
	if err != nil {
		return errors.New("bootstrap remote address is invalid")
	}
	endpointIP, remoteIP := net.ParseIP(parsed.Hostname()), net.ParseIP(remoteHost)
	if endpointIP == nil || remoteIP == nil || !sameRegistrationAddress(endpointIP, remoteIP) {
		return errors.New("bootstrap control URL must advertise the registration source IP")
	}
	return nil
}

func sameRegistrationAddress(endpointIP, remoteIP net.IP) bool {
	if endpointIP.Equal(remoteIP) {
		return true
	}
	return endpointIP.IsLoopback() && remoteIP.IsLoopback()
}

func registrationClaims(worker Worker) bootstrapauth.Claims {
	return bootstrapauth.Claims{RunID: worker.RunID, JobID: worker.JobID, RuntimeUnitID: worker.RuntimeUnitID, SandboxID: worker.SandboxID, BindingID: worker.BindingID, Generation: worker.Generation, DeviceIDs: worker.AllDeviceIDs()}
}
