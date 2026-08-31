package runtimehelper

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

	"github.com/Blizzard-cyber/TGS-RL/internal/bootstrapauth"
)

const WorkerRegistryTokenHeader = "X-TGSRL-Worker-Token"

const maxRegistryPayloadBytes = 64 << 10

// WorkerObserver receives authoritative bootstrap lifecycle observations after
// the durable registry update succeeds. Callers must make observations
// idempotent because a bootstrap may retry after losing the HTTP response.
type WorkerObserver func(context.Context, Worker) error

// WorkerAuthority validates that a bootstrap registration still matches the
// Scheduler's current binding and generation before it becomes controllable.
type WorkerAuthority func(context.Context, Worker) error

type exitReport struct {
	SandboxID    string `json:"sandbox_id"`
	Generation   uint64 `json:"generation"`
	InstanceID   string `json:"instance_id"`
	ProcessToken string `json:"process_token"`
	State        string `json:"state"`
	ExitCode     int    `json:"exit_code"`
	Detail       string `json:"detail,omitempty"`
}

type registryHandler struct {
	controller *Controller
	store      *Store
	signingKey []byte
	authorize  WorkerAuthority
	observe    WorkerObserver
}

// NewRegistryHandler exposes the narrow bootstrap registration boundary. It
// intentionally does not expose lifecycle action execution; Scheduler helpers
// read the durable registry and contact each bootstrap's private control URL.
func NewRegistryHandler(controller *Controller, store *Store, signingKey []byte, authorize WorkerAuthority, observe WorkerObserver) (http.Handler, error) {
	if controller == nil || store == nil {
		return nil, errors.New("runtime controller and store are required")
	}
	if len(signingKey) < 32 {
		return nil, errors.New("worker registry signing key must contain at least 32 bytes")
	}
	handler := &registryHandler{controller: controller, store: store, signingKey: append([]byte(nil), signingKey...), authorize: authorize, observe: observe}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handler.health)
	mux.HandleFunc("/v1/workers/register", handler.register)
	mux.HandleFunc("/v1/workers/exit", handler.reportExit)
	return mux, nil
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
	if matched && h.observe != nil {
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

func registryTokenHash(token string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return fmt.Sprintf("sha256:%x", digest[:])
}

func decodeRegistryRequest(request *http.Request, value any) error {
	if request.Body == nil {
		return errors.New("request body is required")
	}
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxRegistryPayloadBytes+1))
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

func postRegistry(ctx context.Context, baseURL, token, path string, value any) error {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("worker registry URL must be an absolute HTTP(S) base URL without credentials, query, or fragment")
	}
	if strings.TrimSpace(token) == "" {
		return errors.New("worker registry token is required")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + path
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), strings.NewReader(string(payload)))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(WorkerRegistryTokenHeader, token)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("worker registry returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(detail)))
	}
	return nil
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
