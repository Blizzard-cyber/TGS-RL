// Package runtimehelper implements durable, generation-fenced lifecycle state
// for the tgsrl-nvidia-runtime command. It never infers GPU offload from a
// process signal: offload requires an explicit managed-worker acknowledgement.
package managedworker

import (
	"crypto/sha256"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Blizzard-cyber/TGS-RL/internal/helperstate"
)

const SchemaVersion = 1

type Worker struct {
	RunID                 string    `json:"run_id,omitempty"`
	JobID                 string    `json:"job_id,omitempty"`
	TraceID               string    `json:"trace_id,omitempty"`
	RuntimeUnitID         string    `json:"runtime_unit_id,omitempty"`
	SandboxID             string    `json:"sandbox_id"`
	Generation            uint64    `json:"generation"`
	PID                   int       `json:"pid"`
	ProcessToken          string    `json:"process_token"`
	InstanceID            string    `json:"instance_id,omitempty"`
	State                 string    `json:"state"`
	SafePoint             bool      `json:"safe_point"`
	Offloaded             bool      `json:"offloaded"`
	Ready                 bool      `json:"ready"`
	Priority              int32     `json:"priority"`
	BindingID             string    `json:"binding_id,omitempty"`
	DeviceID              string    `json:"device_id,omitempty"`
	Share                 float64   `json:"share,omitempty"`
	ControlSocket         string    `json:"control_socket,omitempty"`
	ControlURL            string    `json:"control_url,omitempty"`
	ControlToken          string    `json:"control_token,omitempty"`
	RegistrationTokenHash string    `json:"registration_token_hash,omitempty"`
	DeviceIDs             []string  `json:"device_ids,omitempty"`
	MPSServerPID          uint32    `json:"mps_server_pid,omitempty"`
	MPSServerProcessToken string    `json:"mps_server_process_token,omitempty"`
	SafePointFile         string    `json:"safe_point_file,omitempty"`
	ReadinessFile         string    `json:"readiness_file,omitempty"`
	CheckpointRef         string    `json:"checkpoint_ref,omitempty"`
	GPUMemoryObserved     bool      `json:"gpu_memory_observed,omitempty"`
	GPUMemoryAllocated    uint64    `json:"gpu_memory_allocated_bytes,omitempty"`
	GPUMemoryReserved     uint64    `json:"gpu_memory_reserved_bytes,omitempty"`
	LastOperation         string    `json:"last_operation,omitempty"`
	LastUpdatedAt         time.Time `json:"last_updated_at"`
	ExitCode              int       `json:"exit_code,omitempty"`
	Detail                string    `json:"detail,omitempty"`
}

type Receipt struct {
	Operation                string  `json:"operation"`
	Phase                    string  `json:"phase"`
	StepIndex                int     `json:"step_index"`
	ExpectedActions          int     `json:"expected_actions"`
	Committed                bool    `json:"committed"`
	PlanDigest               string  `json:"plan_digest"`
	ActionID                 string  `json:"action_id"`
	PlanID                   string  `json:"plan_id"`
	IdempotencyKey           string  `json:"idempotency_key"`
	SandboxID                string  `json:"sandbox_id"`
	TransactionGeneration    uint64  `json:"transaction_generation"`
	ActionGeneration         uint64  `json:"action_generation"`
	TargetGeneration         uint64  `json:"target_generation,omitempty"`
	TargetDeviceID           string  `json:"target_device_id,omitempty"`
	TargetProfile            string  `json:"target_profile,omitempty"`
	TargetParentID           string  `json:"target_parent_id,omitempty"`
	TargetBindingID          string  `json:"target_binding_id,omitempty"`
	TargetShare              float64 `json:"target_share,omitempty"`
	SourceBindingID          string  `json:"source_binding_id,omitempty"`
	SourceDeviceID           string  `json:"source_device_id,omitempty"`
	SourceOffloaded          bool    `json:"source_offloaded,omitempty"`
	SourceGPUMemoryObserved  bool    `json:"source_gpu_memory_observed,omitempty"`
	SourceGPUMemoryAllocated uint64  `json:"source_gpu_memory_allocated_bytes,omitempty"`
	SourceGPUMemoryReserved  uint64  `json:"source_gpu_memory_reserved_bytes,omitempty"`
	BindingSynchronized      bool    `json:"binding_synchronized,omitempty"`
	Completed                bool    `json:"completed"`
	Succeeded                bool    `json:"succeeded"`
	ErrorCode                string  `json:"error_code,omitempty"`
	ErrorMessage             string  `json:"error_message,omitempty"`
	CommandDigest            string  `json:"command_digest"`
	RequestDigest            string  `json:"request_digest"`
	Recoverable              bool    `json:"recoverable,omitempty"`
}

type State struct {
	SchemaVersion int                `json:"schema_version"`
	Workers       map[string]Worker  `json:"workers"`
	Receipts      map[string]Receipt `json:"receipts"`
	Heads         map[string]string  `json:"heads"`
}

type ActionRequest struct {
	Operation             string
	SandboxID             string
	Generation            uint64
	IdempotencyKey        string
	StepIndex             int
	ExpectedActions       int
	TransactionGeneration uint64
	PlanDigest            string
	CommandDigest         string
	ActionID              string
	PlanID                string
	Recoverable           bool
	TargetGeneration      uint64
	TargetDeviceID        string
	TargetProfile         string
	TargetBindingID       string
	TargetShare           float64
	TargetParentID        string
	SourceBindingID       string
	SourceDeviceID        string
}

type Store struct{ state *helperstate.Store[State] }

func NewStore(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("runtime state path is required")
	}
	state, err := helperstate.New(path, emptyState, validateState)
	if err != nil {
		return nil, err
	}
	return &Store{state: state}, nil
}

func (s *Store) Snapshot() (State, error) {
	state, err := s.state.Snapshot()
	return cloneState(state), err
}

func (s *Store) Register(worker Worker) error {
	return s.state.Update(func(state *State) error {
		if err := validateWorker(worker); err != nil {
			return err
		}
		if key := state.Heads[worker.SandboxID]; key != "" && !state.Receipts[key].Completed {
			return fmt.Errorf("sandbox %q has an unresolved lifecycle action", worker.SandboxID)
		}
		if current, ok := state.Workers[worker.SandboxID]; ok {
			if worker.Generation < current.Generation {
				return fmt.Errorf("stale generation %d; current generation is %d", worker.Generation, current.Generation)
			}
			if worker.Generation == current.Generation {
				if !sameWorkerRegistrationIdentity(current, worker) {
					return fmt.Errorf("generation %d is already registered with different worker identity", worker.Generation)
				}
				if current.State == "failed" || current.State == "terminated" {
					return fmt.Errorf("generation %d already has terminal worker state %q", worker.Generation, current.State)
				}
				return nil
			}
			delete(state.Heads, worker.SandboxID)
		}
		state.Workers[worker.SandboxID] = cloneWorker(worker)
		return nil
	})
}

func (s *Store) Unregister(sandboxID string, generation uint64) error {
	return s.state.Update(func(state *State) error {
		worker, ok := state.Workers[sandboxID]
		if !ok {
			return fmt.Errorf("sandbox %q is not registered", sandboxID)
		}
		if worker.Generation != generation {
			return fmt.Errorf("generation fence failed: expected %d, current generation is %d", generation, worker.Generation)
		}
		if key := state.Heads[sandboxID]; key != "" && !state.Receipts[key].Completed {
			return fmt.Errorf("sandbox %q has an unresolved lifecycle action", sandboxID)
		}
		delete(state.Workers, sandboxID)
		delete(state.Heads, sandboxID)
		return nil
	})
}

// ReportExit records a terminal remote-worker observation without allowing an
// old bootstrap generation or instance to overwrite its replacement.
func (s *Store) ReportExit(sandboxID string, generation uint64, instanceID, processToken, state string, exitCode int, detail string) (bool, bool, error) {
	updated, matched := false, false
	err := s.state.Update(func(current *State) error {
		worker, ok := current.Workers[sandboxID]
		if !ok {
			return fmt.Errorf("sandbox %q is not registered", sandboxID)
		}
		if generation < worker.Generation {
			return nil
		}
		if generation > worker.Generation {
			return fmt.Errorf("future generation %d; current generation is %d", generation, worker.Generation)
		}
		if worker.InstanceID != instanceID || worker.ProcessToken != processToken {
			return nil
		}
		matched = true
		if state != "terminated" && state != "failed" {
			return fmt.Errorf("worker exit state must be terminated or failed")
		}
		trimmedDetail := strings.TrimSpace(detail)
		if worker.State == "terminated" || worker.State == "failed" {
			if worker.State != state || worker.ExitCode != exitCode || worker.Detail != trimmedDetail {
				if worker.LastOperation != "stop" || worker.Detail != "" {
					return fmt.Errorf("worker terminal outcome is already recorded")
				}
				worker.ExitCode, worker.Detail, worker.LastUpdatedAt = exitCode, trimmedDetail, time.Now().UTC()
				current.Workers[sandboxID] = worker
				updated = true
			}
			return nil
		}
		if worker.State == state && worker.ExitCode == exitCode && worker.Detail == trimmedDetail {
			return nil
		}
		worker.State, worker.SafePoint, worker.Offloaded, worker.Ready = state, false, false, false
		worker.ExitCode, worker.Detail, worker.LastUpdatedAt = exitCode, trimmedDetail, time.Now().UTC()
		current.Workers[sandboxID] = worker
		updated = true
		return nil
	})
	return updated, matched, err
}

func (s *Store) Refresh(worker Worker) error {
	return s.state.Update(func(state *State) error {
		current, ok := state.Workers[worker.SandboxID]
		if !ok || current.ProcessToken != worker.ProcessToken {
			return fmt.Errorf("worker %q changed during refresh", worker.SandboxID)
		}
		if current.Generation != worker.Generation {
			head := state.Receipts[state.Heads[worker.SandboxID]]
			if head.Completed || (head.Operation != "rebind" && head.Operation != "recreate") || head.ActionGeneration != current.Generation || head.TargetGeneration != worker.Generation {
				return fmt.Errorf("worker %q changed during refresh", worker.SandboxID)
			}
		}
		state.Workers[worker.SandboxID] = cloneWorker(worker)
		return nil
	})
}

func (s *Store) Begin(request ActionRequest) (Worker, Receipt, bool, error) {
	var worker Worker
	var receipt Receipt
	var replay bool
	err := s.state.Update(func(state *State) error {
		current, ok := state.Workers[request.SandboxID]
		if !ok {
			return fmt.Errorf("sandbox %q is not registered", request.SandboxID)
		}
		candidate := receiptFor(request)
		candidate.SourceOffloaded = current.Offloaded
		candidate.SourceGPUMemoryObserved = current.GPUMemoryObserved
		candidate.SourceGPUMemoryAllocated = current.GPUMemoryAllocated
		candidate.SourceGPUMemoryReserved = current.GPUMemoryReserved
		if err := validateReceipt(candidate); err != nil {
			return err
		}
		if existing, ok := state.Receipts[request.IdempotencyKey]; ok {
			if existing.RequestDigest != candidate.RequestDigest || existing.CommandDigest != candidate.CommandDigest || existing.PlanDigest != candidate.PlanDigest || existing.ActionID != candidate.ActionID || existing.PlanID != candidate.PlanID {
				return errors.New("idempotency key was reused with different lifecycle content")
			}
			worker, receipt, replay = cloneWorker(current), existing, true
			return nil
		}
		if current.Generation != request.Generation {
			return fmt.Errorf("generation fence failed: expected %d, current generation is %d", request.Generation, current.Generation)
		}
		if request.Operation == "rebind" || request.Operation == "recreate" {
			if current.BindingID != request.SourceBindingID || current.DeviceID != request.SourceDeviceID {
				return errors.New("MIG source binding does not match registered worker")
			}
		}
		if key := state.Heads[request.SandboxID]; key != "" && !state.Receipts[key].Completed {
			return fmt.Errorf("sandbox %q has an unresolved lifecycle action", request.SandboxID)
		}
		if err := validateTransition(current, request.Operation); err != nil {
			return err
		}
		state.Receipts[request.IdempotencyKey] = candidate
		state.Heads[request.SandboxID] = request.IdempotencyKey
		worker, receipt = cloneWorker(current), candidate
		return nil
	})
	return worker, receipt, replay, err
}

func (s *Store) Complete(key string, worker Worker, succeeded bool, code, message string) error {
	return s.state.Update(func(state *State) error {
		receipt, ok := state.Receipts[key]
		if !ok {
			return fmt.Errorf("receipt %q does not exist", key)
		}
		if state.Heads[receipt.SandboxID] != key {
			return fmt.Errorf("receipt %q is no longer the sandbox head", key)
		}
		wantGeneration := receipt.ActionGeneration
		if succeeded && (receipt.Operation == "rebind" || receipt.Operation == "recreate") {
			wantGeneration = receipt.TargetGeneration
		}
		if worker.SandboxID != receipt.SandboxID || worker.Generation != wantGeneration {
			return errors.New("completed worker does not match receipt identity")
		}
		receipt.Completed, receipt.Succeeded, receipt.Phase = true, succeeded, "completed"
		receipt.ErrorCode, receipt.ErrorMessage = strings.TrimSpace(code), strings.TrimSpace(message)
		state.Receipts[key] = receipt
		if succeeded {
			state.Workers[worker.SandboxID] = cloneWorker(worker)
		}
		return nil
	})
}

func (s *Store) Advance(key, phase string, worker Worker) error {
	return s.state.Update(func(state *State) error {
		receipt, ok := state.Receipts[key]
		if !ok || receipt.Completed || state.Heads[receipt.SandboxID] != key {
			return fmt.Errorf("receipt %q is not the active mutation", key)
		}
		current := state.Workers[receipt.SandboxID]
		if worker.SandboxID != receipt.SandboxID || worker.ProcessToken != current.ProcessToken {
			return errors.New("advanced worker does not match receipt identity")
		}
		if !validPhaseTransition(receipt.Phase, phase) {
			return fmt.Errorf("invalid runtime receipt transition %q -> %q", receipt.Phase, phase)
		}
		receipt.Phase = phase
		state.Receipts[key] = receipt
		state.Workers[worker.SandboxID] = cloneWorker(worker)
		return nil
	})
}

func (s *Store) MarkBindingSynchronized(key string) error {
	return s.state.Update(func(state *State) error {
		receipt, ok := state.Receipts[key]
		if !ok {
			return fmt.Errorf("receipt %q does not exist", key)
		}
		if !receipt.Completed || !receipt.Succeeded || (receipt.Operation != "rebind" && receipt.Operation != "recreate") {
			return fmt.Errorf("receipt %q is not a completed MIG reconfiguration", key)
		}
		receipt.BindingSynchronized = true
		state.Receipts[key] = receipt
		return nil
	})
}

func (s *Store) BindingSyncPending() ([]Receipt, error) {
	state, err := s.Snapshot()
	if err != nil {
		return nil, err
	}
	result := make([]Receipt, 0)
	for _, receipt := range state.Receipts {
		if receipt.Completed && receipt.Succeeded && (receipt.Operation == "rebind" || receipt.Operation == "recreate") && !receipt.BindingSynchronized {
			result = append(result, receipt)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].PlanID != result[j].PlanID {
			return result[i].PlanID < result[j].PlanID
		}
		return result[i].StepIndex < result[j].StepIndex
	})
	return result, nil
}

func validateState(state State) error {
	if state.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported runtime state schema version %d", state.SchemaVersion)
	}
	if state.Workers == nil || state.Receipts == nil || state.Heads == nil {
		return errors.New("runtime state maps are required")
	}
	for key, worker := range state.Workers {
		if key != worker.SandboxID {
			return fmt.Errorf("worker state key %q does not match sandbox %q", key, worker.SandboxID)
		}
		if err := validateWorker(worker); err != nil {
			return fmt.Errorf("invalid worker %q: %w", key, err)
		}
	}
	type receiptSet struct {
		digest   string
		expected int
		steps    map[int]struct{}
	}
	sets := map[string]*receiptSet{}
	for key, receipt := range state.Receipts {
		if key != receipt.IdempotencyKey {
			return fmt.Errorf("receipt state key %q does not match idempotency key %q", key, receipt.IdempotencyKey)
		}
		if err := validateReceipt(receipt); err != nil {
			return fmt.Errorf("invalid receipt %q: %w", key, err)
		}
		if !receipt.Recoverable {
			continue
		}
		setKey := receipt.PlanID + "\x00" + strconv.FormatUint(receipt.TransactionGeneration, 10)
		set := sets[setKey]
		if set == nil {
			set = &receiptSet{digest: receipt.PlanDigest, expected: receipt.ExpectedActions, steps: map[int]struct{}{}}
			sets[setKey] = set
		}
		if set.digest != receipt.PlanDigest || set.expected != receipt.ExpectedActions {
			return fmt.Errorf("runtime receipt set for plan %q is inconsistent", receipt.PlanID)
		}
		if _, duplicate := set.steps[receipt.StepIndex]; duplicate {
			return fmt.Errorf("runtime receipt set for plan %q duplicates step %d", receipt.PlanID, receipt.StepIndex)
		}
		set.steps[receipt.StepIndex] = struct{}{}
	}
	for sandboxID, key := range state.Heads {
		receipt, ok := state.Receipts[key]
		if _, workerExists := state.Workers[sandboxID]; !workerExists || !ok || receipt.SandboxID != sandboxID {
			return fmt.Errorf("runtime head %q references invalid receipt %q", sandboxID, key)
		}
	}
	return nil
}

func emptyState() State {
	return State{SchemaVersion: SchemaVersion, Workers: map[string]Worker{}, Receipts: map[string]Receipt{}, Heads: map[string]string{}}
}

func cloneState(state State) State {
	result := emptyState()
	for key, worker := range state.Workers {
		result.Workers[key] = cloneWorker(worker)
	}
	for key, receipt := range state.Receipts {
		result.Receipts[key] = receipt
	}
	for key, head := range state.Heads {
		result.Heads[key] = head
	}
	return result
}

func cloneWorker(worker Worker) Worker {
	worker.DeviceIDs = append([]string(nil), worker.DeviceIDs...)
	return worker
}

func receiptFor(request ActionRequest) Receipt {
	return Receipt{Operation: request.Operation, Phase: "pending", StepIndex: request.StepIndex, ExpectedActions: request.ExpectedActions, PlanDigest: request.PlanDigest, ActionID: request.ActionID, PlanID: request.PlanID, IdempotencyKey: request.IdempotencyKey, SandboxID: request.SandboxID, TransactionGeneration: request.TransactionGeneration, ActionGeneration: request.Generation, TargetGeneration: request.TargetGeneration, TargetDeviceID: request.TargetDeviceID, TargetProfile: request.TargetProfile, TargetParentID: request.TargetParentID, TargetBindingID: request.TargetBindingID, TargetShare: request.TargetShare, SourceBindingID: request.SourceBindingID, SourceDeviceID: request.SourceDeviceID, ErrorCode: "OUTCOME_UNKNOWN", ErrorMessage: "runtime mutation outcome is not confirmed", CommandDigest: request.CommandDigest, RequestDigest: RequestDigest(request.Operation, request.SandboxID, request.Generation, request.SourceBindingID, request.SourceDeviceID, strconv.FormatUint(request.TargetGeneration, 10), request.TargetParentID, request.TargetDeviceID, request.TargetProfile, request.TargetBindingID, strconv.FormatFloat(request.TargetShare, 'g', -1, 64)), Recoverable: request.Recoverable}
}

func RequestDigest(operation, sandboxID string, generation uint64, target ...string) string {
	material := []string{operation, sandboxID, strconv.FormatUint(generation, 10)}
	material = append(material, target...)
	digest := sha256.Sum256([]byte(strings.Join(material, "\x00")))
	return fmt.Sprintf("sha256:%x", digest[:])
}

func validateWorker(worker Worker) error {
	if strings.TrimSpace(worker.SandboxID) == "" || worker.Generation == 0 || worker.PID <= 0 || strings.TrimSpace(worker.ProcessToken) == "" {
		return errors.New("worker requires sandbox, generation, pid, and process token")
	}
	if worker.ControlURL != "" && (worker.RunID == "" || worker.JobID == "" || worker.RuntimeUnitID == "" || worker.InstanceID == "" || worker.ControlToken == "") {
		return errors.New("remote worker requires run, job, runtime unit, instance identity, and control token")
	}
	if worker.ControlURL != "" && worker.ControlSocket != "" {
		return errors.New("worker cannot use both remote and Unix control endpoints")
	}
	if worker.InstanceID != "" && worker.ControlURL == "" {
		return errors.New("remote worker instance identity requires a control URL")
	}
	if worker.MPSServerPID > 0 && worker.MPSServerProcessToken == "" {
		return errors.New("MPS server PID requires a process identity token")
	}
	switch worker.State {
	case "running", "paused", "sleeping", "failed", "terminated":
	default:
		return fmt.Errorf("unsupported worker state %q", worker.State)
	}
	deviceIDs := append([]string(nil), worker.DeviceIDs...)
	if len(deviceIDs) == 0 && worker.DeviceID != "" {
		deviceIDs = []string{worker.DeviceID}
	}
	hasAcceleratorBinding := len(deviceIDs) != 0 || worker.Share != 0
	if hasAcceleratorBinding && (worker.BindingID == "" || len(deviceIDs) == 0 || math.IsNaN(worker.Share) || math.IsInf(worker.Share, 0) || worker.Share < 0 || worker.Share > 1) {
		return errors.New("worker binding requires binding ID, device identities, and optional share within [0,1]")
	}
	seen := make(map[string]struct{}, len(deviceIDs))
	for _, deviceID := range deviceIDs {
		if strings.TrimSpace(deviceID) == "" {
			return errors.New("worker device identities must be nonblank")
		}
		if _, duplicate := seen[deviceID]; duplicate {
			return errors.New("worker device identities must be unique")
		}
		seen[deviceID] = struct{}{}
	}
	return nil
}

func validateReceipt(receipt Receipt) error {
	if receipt.Committed || receipt.Operation == "" || !validReceiptPhase(receipt.Phase) || receipt.StepIndex < 0 || receipt.ExpectedActions <= 0 || receipt.StepIndex >= receipt.ExpectedActions || receipt.TransactionGeneration == 0 || receipt.ActionGeneration == 0 || receipt.PlanDigest == "" || receipt.ActionID == "" || receipt.PlanID == "" || receipt.IdempotencyKey == "" || receipt.SandboxID == "" || receipt.CommandDigest == "" || receipt.RequestDigest == "" {
		return errors.New("runtime receipt identity is incomplete or invalid")
	}
	if (receipt.Operation == "rebind" || receipt.Operation == "recreate") && (receipt.SourceBindingID == "" || receipt.SourceDeviceID == "" || receipt.TargetGeneration != receipt.ActionGeneration+1 || receipt.TargetParentID == "" || receipt.TargetDeviceID == "" || receipt.TargetProfile == "" || receipt.TargetBindingID == "" || receipt.TargetShare <= 0 || receipt.TargetShare > 1) {
		return errors.New("runtime reconfiguration receipt target is invalid")
	}
	return nil
}

func validReceiptPhase(phase string) bool {
	switch phase {
	case "pending", "prepared", "checkpointed", "stopped", "reconfigured", "reloaded", "completed":
		return true
	default:
		return false
	}
}

func validPhaseTransition(current, next string) bool {
	order := map[string]int{"pending": 0, "prepared": 1, "checkpointed": 2, "stopped": 3, "reconfigured": 4, "reloaded": 5, "completed": 6}
	currentOrder, currentOK := order[current]
	nextOrder, nextOK := order[next]
	return currentOK && nextOK && nextOrder == currentOrder+1
}

func sameWorkerRegistrationIdentity(left, right Worker) bool {
	return left.RunID == right.RunID && left.JobID == right.JobID && left.TraceID == right.TraceID && left.RuntimeUnitID == right.RuntimeUnitID && left.SandboxID == right.SandboxID && left.Generation == right.Generation && left.PID == right.PID && left.ProcessToken == right.ProcessToken && left.InstanceID == right.InstanceID && left.Priority == right.Priority && left.BindingID == right.BindingID && left.DeviceID == right.DeviceID && equalStrings(left.DeviceIDs, right.DeviceIDs) && left.MPSServerPID == right.MPSServerPID && left.MPSServerProcessToken == right.MPSServerProcessToken && left.Share == right.Share && left.ControlSocket == right.ControlSocket && left.ControlURL == right.ControlURL && left.ControlToken == right.ControlToken && left.RegistrationTokenHash == right.RegistrationTokenHash && left.SafePointFile == right.SafePointFile && left.ReadinessFile == right.ReadinessFile
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (w Worker) AllDeviceIDs() []string {
	result := append([]string(nil), w.DeviceIDs...)
	if len(result) == 0 && w.DeviceID != "" {
		result = []string{w.DeviceID}
	}
	return result
}

func validateTransition(worker Worker, operation string) error {
	allowed := false
	switch operation {
	case "pause":
		allowed = worker.State == "running"
	case "checkpoint":
		allowed = worker.State == "paused" && worker.SafePoint
	case "resume":
		allowed = worker.State == "paused" || worker.State == "sleeping"
	case "sleep":
		allowed = worker.State == "running" || worker.State == "paused"
	case "offload":
		allowed = worker.State == "running" || worker.State == "paused" || worker.State == "sleeping"
	case "reload":
		allowed = worker.State == "sleeping" && worker.Offloaded && worker.CheckpointRef != ""
	case "rebind", "recreate":
		allowed = worker.ControlSocket != "" && (worker.State == "running" || worker.State == "paused" || worker.State == "sleeping" || worker.State == "failed")
	case "stop":
		allowed = worker.State == "running" || worker.State == "paused" || worker.State == "sleeping"
	default:
		return fmt.Errorf("unsupported lifecycle operation %q", operation)
	}
	if !allowed {
		return fmt.Errorf("lifecycle operation %q is invalid from state %q", operation, worker.State)
	}
	return nil
}

func WriteWorkersCSV(w io.Writer, state State) error {
	writer := csv.NewWriter(w)
	ids := make([]string, 0, len(state.Workers))
	for id := range state.Workers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		worker := state.Workers[id]
		record := []string{worker.SandboxID, strconv.FormatUint(worker.Generation, 10), worker.State, strconv.FormatBool(worker.SafePoint), strconv.FormatBool(worker.Offloaded), strconv.FormatInt(int64(worker.Priority), 10)}
		deviceIDs := append([]string(nil), worker.DeviceIDs...)
		if len(deviceIDs) == 0 && worker.DeviceID != "" {
			deviceIDs = []string{worker.DeviceID}
		}
		if worker.BindingID != "" && len(deviceIDs) > 0 && worker.Share > 0 {
			record = append(record, worker.BindingID, strings.Join(deviceIDs, ";"), strconv.FormatFloat(worker.Share, 'g', -1, 64))
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func WriteReceiptsCSV(w io.Writer, state State) error {
	writer := csv.NewWriter(w)
	activeTransactions := make(map[string]bool)
	for _, receipt := range state.Receipts {
		if !receipt.Recoverable {
			continue
		}
		key := receipt.PlanID + "\x00" + strconv.FormatUint(receipt.TransactionGeneration, 10)
		if _, exists := activeTransactions[key]; !exists {
			activeTransactions[key] = true
		}
		if state.Heads[receipt.SandboxID] != receipt.IdempotencyKey {
			activeTransactions[key] = false
		}
	}
	receipts := make([]Receipt, 0, len(state.Receipts))
	for _, receipt := range state.Receipts {
		if receipt.Recoverable {
			receipts = append(receipts, receipt)
		}
	}
	sort.Slice(receipts, func(i, j int) bool {
		if receipts[i].PlanID != receipts[j].PlanID {
			return receipts[i].PlanID < receipts[j].PlanID
		}
		if receipts[i].TransactionGeneration != receipts[j].TransactionGeneration {
			return receipts[i].TransactionGeneration < receipts[j].TransactionGeneration
		}
		return receipts[i].StepIndex < receipts[j].StepIndex
	})
	for _, receipt := range receipts {
		succeeded, code, message := receipt.Succeeded, receipt.ErrorCode, receipt.ErrorMessage
		if !receipt.Completed {
			succeeded, code, message = false, "OUTCOME_UNKNOWN", "runtime mutation outcome is not confirmed"
		} else if (receipt.Operation == "rebind" || receipt.Operation == "recreate") && !receipt.BindingSynchronized {
			succeeded, code, message = false, "OUTCOME_UNKNOWN", "binding authority is not synchronized"
		} else if !activeTransactions[receipt.PlanID+"\x00"+strconv.FormatUint(receipt.TransactionGeneration, 10)] {
			succeeded, code, message = false, "SUPERSEDED", "runtime state no longer matches this transaction"
		}
		if err := writer.Write([]string{strconv.Itoa(receipt.StepIndex), strconv.Itoa(receipt.ExpectedActions), "false", receipt.PlanDigest, receipt.ActionID, receipt.PlanID, receipt.IdempotencyKey, receipt.SandboxID, strconv.FormatUint(receipt.TransactionGeneration, 10), strconv.FormatUint(receipt.ActionGeneration, 10), strconv.FormatBool(succeeded), code, message, receipt.CommandDigest}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}
