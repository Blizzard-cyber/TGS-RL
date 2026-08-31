package managedworker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	WorkerControlTokenHeader = "X-TGSRL-Worker-Control-Token"
	WorkerTraceTokenHeader   = "X-TGSRL-Worker-Trace-Token"
)

type ControlRequest struct {
	Action          string  `json:"action"`
	SandboxID       string  `json:"sandbox_id"`
	Generation      uint64  `json:"generation"`
	IdempotencyKey  string  `json:"idempotency_key"`
	CheckpointRef   string  `json:"checkpoint_ref,omitempty"`
	DeviceID        string  `json:"device_id,omitempty"`
	Profile         string  `json:"profile,omitempty"`
	BindingID       string  `json:"binding_id,omitempty"`
	Share           float64 `json:"share,omitempty"`
	PreserveProcess bool    `json:"preserve_process,omitempty"`
}

type ControlResponse struct {
	Accepted      bool    `json:"accepted"`
	State         string  `json:"state,omitempty"`
	Generation    uint64  `json:"generation,omitempty"`
	SafePoint     bool    `json:"safe_point,omitempty"`
	Offloaded     bool    `json:"offloaded,omitempty"`
	Ready         bool    `json:"ready,omitempty"`
	CheckpointRef string  `json:"checkpoint_ref,omitempty"`
	BindingID     string  `json:"binding_id,omitempty"`
	DeviceID      string  `json:"device_id,omitempty"`
	Share         float64 `json:"share,omitempty"`
	InstanceID    string  `json:"instance_id,omitempty"`
	PID           int     `json:"pid,omitempty"`
	ProcessToken  string  `json:"process_token,omitempty"`
	Error         string  `json:"error,omitempty"`
}

var ErrOutcomeUnknown = errors.New("managed-worker outcome is unknown")
var ErrRequestNotDelivered = errors.New("managed-worker request was not delivered")

type Controller struct {
	store         *Store
	now           func() time.Time
	signal        func(int, syscall.Signal) error
	control       func(context.Context, string, ControlRequest) (ControlResponse, error)
	remoteControl func(context.Context, string, string, ControlRequest) (ControlResponse, error)
}

func NewController(store *Store) (*Controller, error) {
	if store == nil {
		return nil, errors.New("runtime store is required")
	}
	return &Controller{store: store, now: time.Now, signal: signalProcess, control: callUnixControl, remoteControl: callHTTPControl}, nil
}

func (c *Controller) Register(ctx context.Context, worker Worker) error {
	ctx = nonNilContext(ctx)
	if worker.ControlURL != "" {
		if worker.ProcessToken == "" || worker.InstanceID == "" || worker.ControlToken == "" {
			return errors.New("remote worker registration requires process token, instance identity, and control token")
		}
		response, err := c.controlRemote(ctx, worker, ControlRequest{Action: "status", SandboxID: worker.SandboxID, Generation: worker.Generation, IdempotencyKey: fmt.Sprintf("register:%s:%d", worker.SandboxID, worker.Generation)})
		if err != nil {
			return err
		}
		if !response.Accepted || response.Generation != worker.Generation || response.InstanceID != worker.InstanceID || response.PID != worker.PID || response.ProcessToken != worker.ProcessToken {
			return errors.New("remote worker status does not match registration")
		}
		if response.BindingID != "" && response.BindingID != worker.BindingID {
			return errors.New("remote worker binding does not match registration")
		}
		if response.DeviceID != "" && len(worker.AllDeviceIDs()) == 1 && response.DeviceID != worker.AllDeviceIDs()[0] {
			return errors.New("remote worker device does not match registration")
		}
		worker.State, worker.SafePoint, worker.Offloaded, worker.Ready = response.State, response.SafePoint, response.Offloaded, response.Ready
		worker.CheckpointRef = response.CheckpointRef
		if response.DeviceID != "" && len(worker.AllDeviceIDs()) <= 1 {
			worker.DeviceID, worker.DeviceIDs = response.DeviceID, []string{response.DeviceID}
		}
	} else {
		token, err := ProcessToken(ctx, worker.PID)
		if err != nil {
			return err
		}
		worker.ProcessToken = token
	}
	if worker.ControlSocket != "" {
		response, err := c.control(ctx, worker.ControlSocket, ControlRequest{Action: "status", SandboxID: worker.SandboxID, Generation: worker.Generation, IdempotencyKey: fmt.Sprintf("register:%s:%d", worker.SandboxID, worker.Generation)})
		if err != nil {
			return err
		}
		if !response.Accepted || (response.Generation != 0 && response.Generation != worker.Generation) {
			return errors.New("managed worker status does not match registration")
		}
		worker.State, worker.SafePoint, worker.Offloaded, worker.Ready = response.State, response.SafePoint, response.Offloaded, response.Ready
		worker.CheckpointRef = response.CheckpointRef
	} else if worker.ControlURL == "" {
		if worker.SafePointFile == "" || worker.ReadinessFile == "" {
			return errors.New("signal-managed worker requires safe-point and readiness marker files")
		}
		safePoint, safePointAvailable := readMarker(worker.SafePointFile)
		ready, readinessAvailable := readMarker(worker.ReadinessFile)
		if !safePointAvailable || !readinessAvailable {
			return errors.New("signal-managed worker marker files are unavailable")
		}
		worker.State, worker.SafePoint, worker.Ready = "running", safePoint, ready
	}
	worker.LastUpdatedAt = c.now().UTC()
	return c.store.Register(worker)
}

func (c *Controller) Discover(ctx context.Context) (State, error) {
	ctx = nonNilContext(ctx)
	state, err := c.store.Snapshot()
	if err != nil {
		return State{}, err
	}
	for _, worker := range state.Workers {
		if _, err := c.discoverWorker(ctx, state, worker); err != nil {
			return State{}, err
		}
	}
	return c.store.Snapshot()
}

// DiscoverWorker refreshes one exact worker without allowing a scoped caller
// to probe or mutate unrelated registrations.
func (c *Controller) DiscoverWorker(ctx context.Context, sandboxID string, generation uint64) (Worker, error) {
	ctx = nonNilContext(ctx)
	state, err := c.store.Snapshot()
	if err != nil {
		return Worker{}, err
	}
	worker, ok := state.Workers[strings.TrimSpace(sandboxID)]
	if !ok {
		return Worker{}, fmt.Errorf("sandbox %q is not registered", sandboxID)
	}
	if generation == 0 || worker.Generation != generation {
		return Worker{}, fmt.Errorf("generation fence failed: expected %d, current generation is %d", generation, worker.Generation)
	}
	return c.discoverWorker(ctx, state, worker)
}

func (c *Controller) discoverWorker(ctx context.Context, state State, worker Worker) (Worker, error) {
	if worker.State == "failed" || worker.State == "terminated" {
		if key := state.Heads[worker.SandboxID]; key != "" {
			receipt := state.Receipts[key]
			if !receipt.Completed {
				if err := c.reconcilePending(worker, receipt); err != nil {
					return Worker{}, err
				}
			}
		}
		return worker, nil
	}
	if worker.ControlURL != "" {
		response, statusErr := c.controlRemote(ctx, worker, ControlRequest{Action: "status", SandboxID: worker.SandboxID, Generation: worker.Generation, IdempotencyKey: fmt.Sprintf("discover:%s:%d", worker.SandboxID, worker.Generation)})
		if statusErr != nil {
			worker.State, worker.SafePoint, worker.Offloaded, worker.Ready = "failed", false, false, false
			worker.Detail = "remote worker is unreachable: " + statusErr.Error()
		} else if !response.Accepted || response.Generation != worker.Generation || response.InstanceID != worker.InstanceID || response.PID != worker.PID || response.ProcessToken != worker.ProcessToken {
			worker.State, worker.SafePoint, worker.Offloaded, worker.Ready = "failed", false, false, false
			worker.Detail = "remote worker status does not match registration"
		} else {
			worker.State, worker.SafePoint, worker.Offloaded, worker.Ready = response.State, response.SafePoint, response.Offloaded, response.Ready
			worker.CheckpointRef = response.CheckpointRef
			if response.BindingID != "" || response.DeviceID != "" || response.Share != 0 {
				worker.BindingID, worker.DeviceID, worker.Share = response.BindingID, response.DeviceID, response.Share
				if response.DeviceID != "" && len(worker.AllDeviceIDs()) <= 1 {
					worker.DeviceIDs = []string{response.DeviceID}
				}
			}
		}
	} else if current, tokenErr := ProcessToken(ctx, worker.PID); tokenErr != nil || current != worker.ProcessToken {
		worker.State, worker.SafePoint, worker.Offloaded, worker.Ready = "failed", false, false, false
	} else if worker.ControlSocket != "" {
		response, statusErr := c.control(ctx, worker.ControlSocket, ControlRequest{Action: "status", SandboxID: worker.SandboxID, Generation: worker.Generation, IdempotencyKey: fmt.Sprintf("discover:%s:%d", worker.SandboxID, worker.Generation)})
		if statusErr != nil || !response.Accepted {
			if statusErr == nil {
				statusErr = fmt.Errorf("managed worker rejected status: %s", response.Error)
			}
			return Worker{}, statusErr
		}
		if response.Generation != 0 && response.Generation != worker.Generation {
			return Worker{}, fmt.Errorf("managed worker generation %d does not match %d", response.Generation, worker.Generation)
		}
		worker.State, worker.SafePoint, worker.Offloaded, worker.Ready = response.State, response.SafePoint, response.Offloaded, response.Ready
		worker.CheckpointRef = response.CheckpointRef
		if response.BindingID != "" || response.DeviceID != "" || response.Share != 0 {
			worker.BindingID, worker.DeviceID, worker.Share = response.BindingID, response.DeviceID, response.Share
		}
	} else {
		stopped, stateErr := processStopped(ctx, worker.PID)
		if stateErr != nil {
			return Worker{}, stateErr
		}
		if stopped {
			if worker.State != "sleeping" {
				worker.State = "paused"
			}
			worker.Ready = false
		} else {
			worker.State, worker.Offloaded = "running", false
		}
		worker.SafePoint, _ = readMarker(worker.SafePointFile)
		worker.Ready, _ = readMarker(worker.ReadinessFile)
	}
	worker.LastUpdatedAt = c.now().UTC()
	if err := c.store.Refresh(worker); err != nil {
		return Worker{}, err
	}
	if key := state.Heads[worker.SandboxID]; key != "" {
		receipt := state.Receipts[key]
		if !receipt.Completed {
			if err := c.reconcilePending(worker, receipt); err != nil {
				return Worker{}, err
			}
		}
	}
	return worker, nil
}

func (c *Controller) reconcilePending(worker Worker, receipt Receipt) error {
	if worker.State == "failed" {
		return c.store.Complete(receipt.IdempotencyKey, worker, false, "WORKER_UNAVAILABLE", "worker process is unavailable or its identity changed")
	}
	if (receipt.Operation == "rebind" || receipt.Operation == "recreate") && (worker.Generation != receipt.TargetGeneration || worker.BindingID != receipt.TargetBindingID || worker.DeviceID != receipt.TargetDeviceID || worker.Share != receipt.TargetShare) {
		return nil
	}
	if lifecycleObserved(receipt.Operation, worker) {
		worker.LastOperation, worker.LastUpdatedAt = receipt.Operation, c.now().UTC()
		return c.store.Complete(receipt.IdempotencyKey, worker, true, "", "")
	}
	return nil
}

func lifecycleObserved(operation string, worker Worker) bool {
	switch operation {
	case "pause":
		return worker.State == "paused" && worker.SafePoint
	case "sleep":
		return worker.State == "sleeping"
	case "offload":
		return worker.State == "sleeping" && worker.Offloaded && worker.CheckpointRef != ""
	case "resume":
		return worker.State == "running" && worker.Ready && !worker.Offloaded
	default:
		return false
	}
}

func (c *Controller) Apply(ctx context.Context, request ActionRequest) (Worker, error) {
	ctx = nonNilContext(ctx)
	worker, receipt, replay, err := c.store.Begin(request)
	if err != nil {
		return Worker{}, err
	}
	if replay {
		if !receipt.Completed {
			return Worker{}, errors.New("runtime mutation outcome is unknown; reconciliation is required")
		}
		if !receipt.Succeeded {
			if state, snapshotErr := c.store.Snapshot(); snapshotErr == nil && state.Heads[request.SandboxID] != request.IdempotencyKey {
				return Worker{}, errors.New("runtime mutation result has been superseded")
			}
			return Worker{}, fmt.Errorf("runtime mutation previously failed: %s", receipt.ErrorMessage)
		}
		if state, snapshotErr := c.store.Snapshot(); snapshotErr == nil && state.Heads[request.SandboxID] != request.IdempotencyKey {
			return Worker{}, errors.New("runtime mutation result has been superseded")
		}
		return worker, nil
	}
	updated, applyErr := c.apply(ctx, worker, request)
	if applyErr != nil {
		if errors.Is(applyErr, ErrOutcomeUnknown) {
			return Worker{}, applyErr
		}
		if completeErr := c.store.Complete(request.IdempotencyKey, worker, false, "FAILED_PRECONDITION", applyErr.Error()); completeErr != nil {
			return Worker{}, errors.Join(applyErr, completeErr)
		}
		return Worker{}, applyErr
	}
	updated.LastOperation, updated.LastUpdatedAt = request.Operation, c.now().UTC()
	if err := c.store.Complete(request.IdempotencyKey, updated, true, "", ""); err != nil {
		return Worker{}, fmt.Errorf("%w: persist completed runtime mutation: %v", ErrOutcomeUnknown, err)
	}
	return updated, nil
}

// Reconfigure executes the managed-worker half of a MIG replacement around
// one caller-supplied, GPU-scoped mutation. The mutation callback must not
// return until the replacement device identity has been read back.
func (c *Controller) Reconfigure(ctx context.Context, request ActionRequest, mutate func(context.Context, Worker) (string, error)) (Worker, error) {
	ctx = nonNilContext(ctx)
	if mutate == nil {
		return Worker{}, errors.New("MIG mutation callback is required")
	}
	if request.TargetGeneration != request.Generation+1 || strings.TrimSpace(request.TargetDeviceID) == "" || strings.TrimSpace(request.TargetProfile) == "" {
		return Worker{}, errors.New("MIG reconfiguration requires the next generation, target device, and target profile")
	}
	worker, receipt, replay, err := c.store.Begin(request)
	if err != nil {
		return Worker{}, err
	}
	if replay {
		if !receipt.Completed {
			return c.resumeReconfiguration(ctx, request, worker, receipt, mutate)
		}
		if !receipt.Succeeded {
			return Worker{}, fmt.Errorf("runtime reconfiguration previously failed: %s", receipt.ErrorMessage)
		}
		return worker, nil
	}
	if err := c.verifyWorker(ctx, worker); err != nil {
		_ = c.store.Complete(request.IdempotencyKey, worker, false, "WORKER_UNAVAILABLE", err.Error())
		return Worker{}, err
	}
	if worker.ControlSocket == "" && worker.ControlURL == "" {
		err := errors.New("MIG reconfiguration requires a managed-worker control socket")
		_ = c.store.Complete(request.IdempotencyKey, worker, false, "FAILED_PRECONDITION", err.Error())
		return Worker{}, err
	}
	return c.resumeReconfiguration(ctx, request, worker, receipt, mutate)
}

func (c *Controller) resumeReconfiguration(ctx context.Context, request ActionRequest, worker Worker, receipt Receipt, mutate func(context.Context, Worker) (string, error)) (Worker, error) {
	if err := c.verifyWorker(ctx, worker); err != nil {
		return Worker{}, err
	}
	if worker.ControlSocket == "" && worker.ControlURL == "" {
		return Worker{}, errors.New("MIG reconfiguration requires a managed-worker control socket")
	}
	phase := receipt.Phase
	if phase == "pending" {
		prepared, err := c.call(ctx, worker, request, "prepare_pause", "")
		if err != nil || !prepared.SafePoint {
			if err == nil {
				err = errors.New("managed worker did not confirm a safe point")
			}
			return Worker{}, c.completeDefiniteOrUnknown(request, worker, err)
		}
		worker.SafePoint, worker.Ready = true, false
		if err := c.store.Advance(request.IdempotencyKey, "prepared", worker); err != nil {
			return Worker{}, err
		}
		phase = "prepared"
	}
	if phase == "prepared" {
		checkpoint, err := c.call(ctx, worker, request, "checkpoint", "")
		if err != nil || checkpoint.CheckpointRef == "" {
			if err == nil {
				err = errors.New("managed worker did not return a checkpoint reference")
			}
			return Worker{}, c.completeDefiniteOrUnknown(request, worker, err)
		}
		worker.CheckpointRef = checkpoint.CheckpointRef
		if err := c.store.Advance(request.IdempotencyKey, "checkpointed", worker); err != nil {
			return Worker{}, err
		}
		phase = "checkpointed"
	}
	if phase == "checkpointed" {
		if _, err := c.call(ctx, worker, request, "stop", worker.CheckpointRef); err != nil {
			return Worker{}, c.completeDefiniteOrUnknown(request, worker, err)
		}
		worker.State, worker.Ready = "terminated", false
		if err := c.store.Advance(request.IdempotencyKey, "stopped", worker); err != nil {
			return Worker{}, err
		}
		phase = "stopped"
	}
	if phase == "stopped" {
		newDeviceID, err := mutate(ctx, worker)
		if err != nil {
			if errors.Is(err, ErrRequestNotDelivered) {
				_ = c.store.Complete(request.IdempotencyKey, worker, false, "MIG_MUTATION_NOT_DELIVERED", err.Error())
				return Worker{}, err
			}
			return Worker{}, fmt.Errorf("%w: MIG mutation failed after worker stop: %v", ErrOutcomeUnknown, err)
		}
		if strings.TrimSpace(newDeviceID) == "" {
			return Worker{}, fmt.Errorf("%w: MIG mutation returned no device identity", ErrOutcomeUnknown)
		}
		worker.Generation = request.TargetGeneration
		worker.BindingID, worker.DeviceID, worker.Share = request.TargetBindingID, newDeviceID, request.TargetShare
		worker.DeviceIDs = []string{newDeviceID}
		worker.State, worker.SafePoint, worker.Offloaded, worker.Ready = "sleeping", true, true, false
		if err := c.store.Advance(request.IdempotencyKey, "reconfigured", worker); err != nil {
			return Worker{}, fmt.Errorf("%w: persist post-MIG worker state: %v", ErrOutcomeUnknown, err)
		}
		phase = "reconfigured"
	}
	reloadRequest := request
	reloadRequest.Generation = request.TargetGeneration
	if phase == "reconfigured" {
		if _, err := c.call(ctx, worker, reloadRequest, "reload", worker.CheckpointRef); err != nil {
			return Worker{}, err
		}
		worker.Offloaded = false
		if err := c.store.Advance(request.IdempotencyKey, "reloaded", worker); err != nil {
			return Worker{}, fmt.Errorf("%w: persist reloaded worker state: %v", ErrOutcomeUnknown, err)
		}
		phase = "reloaded"
	}
	if phase != "reloaded" {
		return Worker{}, fmt.Errorf("unsupported persisted MIG phase %q", phase)
	}
	response, err := c.call(ctx, worker, reloadRequest, "resume", worker.CheckpointRef)
	if err != nil || !response.Ready {
		if err == nil {
			err = fmt.Errorf("%w: managed worker did not confirm readiness after MIG mutation", ErrOutcomeUnknown)
		}
		return Worker{}, err
	}
	worker.State, worker.SafePoint, worker.Offloaded, worker.Ready = "running", false, false, true
	worker.LastOperation, worker.LastUpdatedAt = request.Operation, c.now().UTC()
	if err := c.store.Complete(request.IdempotencyKey, worker, true, "", ""); err != nil {
		return Worker{}, fmt.Errorf("%w: persist completed runtime reconfiguration: %v", ErrOutcomeUnknown, err)
	}
	return worker, nil
}

func (c *Controller) completeDefiniteOrUnknown(request ActionRequest, worker Worker, err error) error {
	if errors.Is(err, ErrOutcomeUnknown) {
		return err
	}
	if completeErr := c.store.Complete(request.IdempotencyKey, worker, false, "FAILED_PRECONDITION", err.Error()); completeErr != nil {
		return errors.Join(err, completeErr)
	}
	return err
}

func (c *Controller) apply(ctx context.Context, worker Worker, request ActionRequest) (Worker, error) {
	if err := c.verifyWorker(ctx, worker); err != nil {
		return Worker{}, err
	}
	switch request.Operation {
	case "pause", "sleep":
		if worker.ControlSocket != "" || worker.ControlURL != "" {
			prepared, err := c.call(ctx, worker, request, "prepare_pause", "")
			if err != nil || !prepared.SafePoint {
				if err == nil {
					err = errors.New("managed worker did not confirm a safe point")
				}
				return Worker{}, err
			}
			response, err := c.call(ctx, worker, request, request.Operation, "")
			if err != nil {
				return Worker{}, err
			}
			wantedState := "paused"
			if request.Operation == "sleep" {
				wantedState = "sleeping"
			}
			if response.State != wantedState {
				return Worker{}, fmt.Errorf("%w: managed worker reported state %q after %s", ErrOutcomeUnknown, response.State, request.Operation)
			}
			worker.SafePoint = prepared.SafePoint || response.SafePoint
		} else {
			if value, ok := readMarker(worker.SafePointFile); ok {
				worker.SafePoint = value
			}
			if !worker.SafePoint {
				return Worker{}, errors.New("worker is not at a safe point")
			}
			if err := c.signal(worker.PID, syscall.SIGSTOP); err != nil {
				return Worker{}, fmt.Errorf("pause worker: %w", err)
			}
			if err := waitProcessState(ctx, worker.PID, true); err != nil {
				return Worker{}, fmt.Errorf("%w: confirm paused worker: %v", ErrOutcomeUnknown, err)
			}
		}
		worker.State = "paused"
		if request.Operation == "sleep" {
			worker.State = "sleeping"
		}
		worker.Ready = false
	case "offload":
		if worker.ControlSocket == "" && worker.ControlURL == "" {
			return Worker{}, errors.New("offload requires a managed-worker control socket")
		}
		prepared, err := c.call(ctx, worker, request, "prepare_pause", "")
		if err != nil || !prepared.SafePoint {
			if err == nil {
				err = errors.New("managed worker did not confirm a safe point")
			}
			return Worker{}, err
		}
		checkpoint, err := c.call(ctx, worker, request, "checkpoint", "")
		if err != nil || checkpoint.CheckpointRef == "" {
			if err == nil {
				err = errors.New("managed worker did not return a checkpoint reference")
			}
			return Worker{}, err
		}
		response, err := c.call(ctx, worker, request, "offload", checkpoint.CheckpointRef)
		if err != nil || !response.Offloaded {
			if err == nil {
				err = fmt.Errorf("%w: managed worker did not confirm offload", ErrOutcomeUnknown)
			}
			return Worker{}, err
		}
		worker.State, worker.SafePoint, worker.Offloaded, worker.Ready, worker.CheckpointRef = "sleeping", true, true, false, checkpoint.CheckpointRef
	case "resume":
		if worker.ControlSocket != "" || worker.ControlURL != "" {
			if worker.Offloaded {
				if _, err := c.call(ctx, worker, request, "reload", worker.CheckpointRef); err != nil {
					return Worker{}, err
				}
			}
			response, err := c.call(ctx, worker, request, "resume", worker.CheckpointRef)
			if err != nil || !response.Ready {
				if worker.Offloaded && err != nil && !errors.Is(err, ErrOutcomeUnknown) {
					err = fmt.Errorf("%w: resume after reload: %v", ErrOutcomeUnknown, err)
				}
				if err == nil {
					err = fmt.Errorf("%w: managed worker did not confirm readiness", ErrOutcomeUnknown)
				}
				return Worker{}, err
			}
		} else {
			if err := c.signal(worker.PID, syscall.SIGCONT); err != nil {
				return Worker{}, fmt.Errorf("resume worker: %w", err)
			}
			if err := waitProcessState(ctx, worker.PID, false); err != nil {
				return Worker{}, fmt.Errorf("%w: confirm resumed worker: %v", ErrOutcomeUnknown, err)
			}
			if worker.ReadinessFile != "" {
				if err := waitMarker(ctx, worker.ReadinessFile, true); err != nil {
					return Worker{}, fmt.Errorf("confirm worker readiness: %w", err)
				}
			}
		}
		worker.State, worker.SafePoint, worker.Offloaded, worker.Ready = "running", false, false, true
	case "stop":
		if worker.ControlSocket == "" && worker.ControlURL == "" {
			return Worker{}, errors.New("stop requires a managed-worker control endpoint")
		}
		response, err := c.call(ctx, worker, request, "stop", worker.CheckpointRef)
		if err != nil {
			return Worker{}, err
		}
		if response.State != "terminated" {
			return Worker{}, fmt.Errorf("%w: managed worker reported state %q after stop", ErrOutcomeUnknown, response.State)
		}
		worker.State, worker.SafePoint, worker.Offloaded, worker.Ready = "terminated", false, false, false
	default:
		return Worker{}, fmt.Errorf("unsupported lifecycle operation %q", request.Operation)
	}
	return worker, nil
}

func (c *Controller) call(ctx context.Context, worker Worker, request ActionRequest, action, checkpointRef string) (ControlResponse, error) {
	controlRequest := ControlRequest{Action: action, SandboxID: worker.SandboxID, Generation: worker.Generation, IdempotencyKey: request.IdempotencyKey + ":" + action, CheckpointRef: checkpointRef, DeviceID: request.TargetDeviceID, Profile: request.TargetProfile, BindingID: request.TargetBindingID, Share: request.TargetShare, PreserveProcess: action == "stop" && (request.Operation == "rebind" || request.Operation == "recreate")}
	var response ControlResponse
	var err error
	if worker.ControlURL != "" {
		response, err = c.controlRemote(ctx, worker, controlRequest)
	} else {
		response, err = c.control(ctx, worker.ControlSocket, controlRequest)
	}
	if err != nil {
		if errors.Is(err, ErrRequestNotDelivered) {
			return ControlResponse{}, err
		}
		return ControlResponse{}, fmt.Errorf("%w: %v", ErrOutcomeUnknown, err)
	}
	if !response.Accepted {
		return ControlResponse{}, fmt.Errorf("managed worker rejected %s: %s", action, response.Error)
	}
	if response.Generation != 0 && response.Generation != worker.Generation {
		return ControlResponse{}, fmt.Errorf("%w: managed worker generation %d does not match %d", ErrOutcomeUnknown, response.Generation, worker.Generation)
	}
	if worker.ControlURL != "" && (response.InstanceID != worker.InstanceID || response.PID != worker.PID || response.ProcessToken != worker.ProcessToken) {
		return ControlResponse{}, fmt.Errorf("%w: remote worker identity changed", ErrOutcomeUnknown)
	}
	return response, nil
}

func (c *Controller) verifyWorker(ctx context.Context, worker Worker) error {
	if worker.ControlURL != "" {
		response, err := c.controlRemote(ctx, worker, ControlRequest{Action: "status", SandboxID: worker.SandboxID, Generation: worker.Generation, IdempotencyKey: fmt.Sprintf("verify:%s:%d", worker.SandboxID, worker.Generation)})
		if err != nil {
			return err
		}
		if !response.Accepted || response.Generation != worker.Generation || response.InstanceID != worker.InstanceID || response.PID != worker.PID || response.ProcessToken != worker.ProcessToken {
			return errors.New("remote worker identity changed")
		}
		return nil
	}
	token, err := ProcessToken(ctx, worker.PID)
	if err != nil {
		return err
	}
	if token != worker.ProcessToken {
		return errors.New("worker PID identity changed")
	}
	return nil
}

func (c *Controller) controlRemote(ctx context.Context, worker Worker, request ControlRequest) (ControlResponse, error) {
	if c.remoteControl == nil {
		return ControlResponse{}, errors.New("remote worker control is unavailable")
	}
	return c.remoteControl(ctx, worker.ControlURL, worker.ControlToken, request)
}

func ProcessToken(ctx context.Context, pid int) (string, error) {
	ctx = nonNilContext(ctx)
	if pid <= 0 {
		return "", errors.New("worker pid must be positive")
	}
	if err := syscall.Kill(pid, 0); err != nil && !errors.Is(err, syscall.EPERM) {
		return "", fmt.Errorf("worker process %d is not alive: %w", pid, err)
	}
	if runtime.GOOS == "linux" {
		data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		if err == nil {
			end := strings.LastIndexByte(string(data), ')')
			if end >= 0 {
				fields := strings.Fields(string(data)[end+1:])
				if len(fields) > 19 {
					return "linux:" + strconv.Itoa(pid) + ":" + fields[19], nil
				}
			}
		}
	}
	if token, err := platformProcessToken(pid); err == nil && token != "" {
		return token, nil
	}
	psBinary, err := exec.LookPath("ps")
	if err != nil && runtime.GOOS == "darwin" {
		psBinary = "/bin/ps"
	}
	command := exec.CommandContext(ctx, psBinary, "-o", "lstart=", "-p", strconv.Itoa(pid))
	output, err := command.Output()
	if err != nil || strings.TrimSpace(string(output)) == "" {
		return "", fmt.Errorf("read worker process identity: %w", err)
	}
	return runtime.GOOS + ":" + strconv.Itoa(pid) + ":" + strings.TrimSpace(string(output)), nil
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func processStopped(ctx context.Context, pid int) (bool, error) {
	if _, err := ProcessToken(ctx, pid); err != nil {
		return false, err
	}
	if runtime.GOOS == "linux" {
		data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
		if err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(line, "State:") {
					fields := strings.Fields(line)
					return len(fields) >= 2 && (fields[1] == "T" || fields[1] == "t"), nil
				}
			}
		}
	}
	if stopped, err := platformProcessStopped(pid); err == nil {
		return stopped, nil
	}
	output, err := exec.CommandContext(ctx, "ps", "-o", "state=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return false, fmt.Errorf("read worker process state: %w", err)
	}
	state := strings.TrimSpace(string(output))
	return strings.HasPrefix(state, "T") || strings.HasPrefix(state, "t"), nil
}

func waitProcessState(ctx context.Context, pid int, stopped bool) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := processStopped(ctx, pid)
		if err != nil {
			return err
		}
		if current == stopped {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitMarker(ctx context.Context, path string, expected bool) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		value, available := readMarker(path)
		if available && value == expected {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func signalProcess(pid int, signal syscall.Signal) error {
	return syscall.Kill(pid, signal)
}

func readMarker(path string) (bool, bool) {
	if strings.TrimSpace(path) == "" {
		return false, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false, false
	}
	switch strings.ToLower(strings.TrimSpace(string(data))) {
	case "1", "true", "ready", "yes":
		return true, true
	default:
		return false, true
	}
}

func callUnixControl(ctx context.Context, socketPath string, request ControlRequest) (ControlResponse, error) {
	if strings.TrimSpace(socketPath) == "" {
		return ControlResponse{}, errors.New("managed-worker control socket is required")
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	if err != nil {
		return ControlResponse{}, fmt.Errorf("%w: connect managed-worker socket: %v", ErrRequestNotDelivered, err)
	}
	defer connection.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = connection.SetDeadline(deadline)
	}
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		return ControlResponse{}, fmt.Errorf("%w: write managed-worker request: %v", ErrOutcomeUnknown, err)
	}
	var response ControlResponse
	decoder := json.NewDecoder(bufio.NewReader(io.LimitReader(connection, 64<<10)))
	if err := decoder.Decode(&response); err != nil {
		return ControlResponse{}, fmt.Errorf("%w: read managed-worker response: %v", ErrOutcomeUnknown, err)
	}
	return response, nil
}

func callHTTPControl(ctx context.Context, endpoint, token string, request ControlRequest) (ControlResponse, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return ControlResponse{}, errors.New("managed-worker control URL must be an absolute HTTP(S) URL without credentials, query, or fragment")
	}
	if strings.TrimSpace(token) == "" {
		return ControlResponse{}, errors.New("managed-worker control token is required")
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return ControlResponse{}, fmt.Errorf("encode managed-worker request: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), strings.NewReader(string(payload)))
	if err != nil {
		return ControlResponse{}, fmt.Errorf("build managed-worker request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set(WorkerControlTokenHeader, token)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	httpResponse, err := client.Do(httpRequest)
	if err != nil {
		return ControlResponse{}, fmt.Errorf("%w: call managed-worker endpoint: %v", ErrRequestNotDelivered, err)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		return ControlResponse{}, fmt.Errorf("managed-worker endpoint returned HTTP %d", httpResponse.StatusCode)
	}
	var response ControlResponse
	decoder := json.NewDecoder(io.LimitReader(httpResponse.Body, 64<<10))
	if err := decoder.Decode(&response); err != nil {
		return ControlResponse{}, fmt.Errorf("%w: decode managed-worker response: %v", ErrOutcomeUnknown, err)
	}
	return response, nil
}
