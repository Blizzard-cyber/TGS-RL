package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/internal/bootstrapauth"
	"github.com/Blizzard-cyber/TGS-RL/internal/managedworker"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type runtimeEventPublisher interface {
	PublishSandboxEvent(context.Context, *tgsrlv1.PublishSandboxEventRequest, ...grpc.CallOption) (*tgsrlv1.PublishSandboxEventResponse, error)
	PublishTraceBatch(context.Context, *tgsrlv1.PublishTraceBatchRequest, ...grpc.CallOption) (*tgsrlv1.PublishTraceBatchResponse, error)
	GetRuntimeStatus(context.Context, *tgsrlv1.GetRuntimeStatusRequest, ...grpc.CallOption) (*tgsrlv1.GetRuntimeStatusResponse, error)
}

func signTraceRequest(signingKey []byte, request *tgsrlv1.PublishTraceBatchRequest) error {
	request.AuthenticationTag = nil
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return fmt.Errorf("marshal worker trace request: %w", err)
	}
	request.AuthenticationTag, err = bootstrapauth.SignPayload(signingKey, payload)
	return err
}

func startWorkerRegistry(address, statePath, signingKeyFile string, resourceProvider provider.CompleteResourceProvider, runtimeClient runtimeEventPublisher) (*http.Server, net.Listener, error) {
	if strings.TrimSpace(address) == "" {
		return nil, nil, nil
	}
	if resourceProvider == nil {
		return nil, nil, errors.New("worker registry requires a resource provider")
	}
	if runtimeClient == nil {
		return nil, nil, errors.New("worker registry requires a Runtime event publisher")
	}
	signingKey := strings.TrimSpace(os.Getenv("TGSRL_WORKER_REGISTRY_SIGNING_KEY"))
	if strings.TrimSpace(signingKeyFile) != "" {
		data, err := os.ReadFile(signingKeyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("read worker registry signing key: %w", err)
		}
		signingKey = strings.TrimSpace(string(data))
	}
	if signingKey == "" {
		return nil, nil, errors.New("worker registry signing key is required through TGSRL_WORKER_REGISTRY_SIGNING_KEY or -worker-registry-signing-key-file")
	}
	store, err := managedworker.NewStore(statePath)
	if err != nil {
		return nil, nil, err
	}
	controller, err := managedworker.NewController(store)
	if err != nil {
		return nil, nil, err
	}
	authorize := func(ctx context.Context, worker managedworker.Worker) error {
		current, err := resourceProvider.GetSandbox(ctx, worker.SandboxID)
		if err != nil {
			return fmt.Errorf("resolve scheduler binding: %w", err)
		}
		if current.Generation != worker.Generation || current.Binding == nil {
			return errors.New("worker generation does not match the scheduler binding")
		}
		if current.Binding.GetBindingId() != worker.BindingID || registeredRuntimeUnitID(current.Binding) != worker.RuntimeUnitID || !sameDeviceSet(current.Binding.GetDeviceIds(), worker.AllDeviceIDs()) {
			return errors.New("worker binding identity does not match the scheduler binding")
		}
		if math.Abs(current.Share-worker.Share) > 1e-9 {
			return errors.New("worker accelerator share does not match the scheduler binding")
		}
		if worker.State == "running" {
			status, err := runtimeClient.GetRuntimeStatus(ctx, &tgsrlv1.GetRuntimeStatusRequest{RunId: worker.RunID})
			if err != nil {
				return fmt.Errorf("read Runtime state before worker registration: %w", err)
			}
			bound := false
			for _, sandbox := range status.GetSandboxes() {
				if sandbox.GetSandboxId() == worker.SandboxID && sandbox.GetGeneration() == worker.Generation && (sandbox.GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND || sandbox.GetState() == tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING) {
					bound = true
					break
				}
			}
			if !bound {
				return errors.New("runtime has not observed the bound or running workload generation")
			}
		}
		return nil
	}
	observe := func(ctx context.Context, worker managedworker.Worker) error {
		current, err := resourceProvider.GetSandbox(ctx, worker.SandboxID)
		if err != nil {
			return fmt.Errorf("resolve scheduler binding for observation: %w", err)
		}
		state, eventType := runtimeStateForWorker(worker.State)
		if state == tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN {
			return fmt.Errorf("unsupported worker state %q", worker.State)
		}
		if current.Generation != worker.Generation || current.Binding == nil || current.Binding.GetBindingId() != worker.BindingID || registeredRuntimeUnitID(current.Binding) != worker.RuntimeUnitID || !sameDeviceSet(current.Binding.GetDeviceIds(), worker.AllDeviceIDs()) {
			return errors.New("worker observation no longer matches the scheduler binding")
		}
		share, priority, safePoint, offloaded := worker.Share, worker.Priority, worker.SafePoint, worker.Offloaded
		detail := strings.TrimSpace(worker.Detail)
		if detail == "" {
			detail = "workload bootstrap registered worker"
		}
		observedAt := worker.LastUpdatedAt
		if observedAt.IsZero() {
			observedAt = time.Now().UTC()
		}
		processIdentity := sha256.Sum256([]byte(worker.ProcessToken))
		lifecycleIdentity := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%t\x00%t\x00%t\x00%s\x00%s\x00%d", worker.State, worker.SafePoint, worker.Offloaded, worker.Ready, worker.CheckpointRef, worker.LastOperation, worker.ExitCode)))
		eventID := fmt.Sprintf("bootstrap:%s:%x:%d:%x", worker.InstanceID, processIdentity[:8], worker.Generation, lifecycleIdentity[:8])
		event := &tgsrlv1.SandboxEvent{
			EventId:        eventID,
			EventType:      eventType,
			SandboxId:      worker.SandboxID,
			RunId:          worker.RunID,
			JobId:          worker.JobID,
			TraceId:        worker.TraceID,
			RuntimeUnitId:  worker.RuntimeUnitID,
			Generation:     worker.Generation,
			State:          state,
			Binding:        current.Binding,
			SafePoint:      &safePoint,
			Share:          &share,
			Priority:       &priority,
			Offloaded:      &offloaded,
			Detail:         detail,
			OccurredAt:     timestamppb.New(observedAt),
			IdempotencyKey: eventID,
		}
		stored, err := runtimeClient.PublishSandboxEvent(ctx, &tgsrlv1.PublishSandboxEventRequest{Event: event})
		if err != nil {
			return err
		}
		if stored.GetEvent() == nil {
			return errors.New("runtime accepted no worker lifecycle event")
		}
		_, err = resourceProvider.ObserveSandbox(ctx, stored.GetEvent())
		return err
	}
	publishTrace := func(ctx context.Context, worker managedworker.Worker, request managedworker.WorkerTraceRequest) (managedworker.WorkerTraceResponse, error) {
		var batch tgsrlv1.TraceEventBatch
		if err := proto.Unmarshal(request.Batch, &batch); err != nil {
			return managedworker.WorkerTraceResponse{}, fmt.Errorf("decode worker trace batch: %w", err)
		}
		if err := validateWorkerTraceBatch(worker, request, &batch); err != nil {
			return managedworker.WorkerTraceResponse{}, err
		}
		runtimeRequest := &tgsrlv1.PublishTraceBatchRequest{
			BatchPayload:   append([]byte(nil), request.Batch...),
			RuntimeUnitId:  worker.RuntimeUnitID,
			SandboxId:      worker.SandboxID,
			BindingId:      worker.BindingID,
			Generation:     worker.Generation,
			DeviceIds:      worker.AllDeviceIDs(),
			IdempotencyKey: request.IdempotencyKey,
		}
		if err := signTraceRequest([]byte(signingKey), runtimeRequest); err != nil {
			return managedworker.WorkerTraceResponse{}, err
		}
		response, err := runtimeClient.PublishTraceBatch(ctx, runtimeRequest)
		if err != nil {
			return managedworker.WorkerTraceResponse{}, err
		}
		return managedworker.WorkerTraceResponse{AcceptedEventCount: response.GetAcceptedEventCount(), Idempotent: response.GetIdempotent(), Cursor: response.GetCursor()}, nil
	}
	handler, err := managedworker.NewRegistryHandler(controller, store, []byte(signingKey), authorize, observe, publishTrace)
	if err != nil {
		return nil, nil, err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, nil, fmt.Errorf("listen for worker registry on %s: %w", address, err)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	return server, listener, nil
}

func validateWorkerTraceBatch(worker managedworker.Worker, request managedworker.WorkerTraceRequest, batch *tgsrlv1.TraceEventBatch) error {
	if batch == nil || len(batch.GetEvents()) == 0 {
		return errors.New("worker trace batch is empty")
	}
	digest := sha256.Sum256(request.Batch)
	if request.IdempotencyKey != fmt.Sprintf("trace-sha256-%x", digest[:]) {
		return errors.New("worker trace idempotency key does not match the batch")
	}
	if batch.GetExecutionId() == "" || batch.GetRunId() != worker.RunID || batch.GetTraceId() != worker.TraceID {
		return errors.New("worker trace batch identity does not match the registration")
	}
	for _, event := range batch.GetEvents() {
		if event.GetExecutionId() != batch.GetExecutionId() || event.GetRunId() != worker.RunID || event.GetJobId() != worker.JobID || event.GetTraceId() != worker.TraceID || event.GetSandboxId() != worker.SandboxID || event.GetGeneration() != worker.Generation || event.GetAttributes()["runtime_unit_id"] != worker.RuntimeUnitID {
			return errors.New("worker trace event identity does not match the registration")
		}
		if ids := splitDeviceIDs(event.GetAttributes()["device_ids"]); !sameDeviceSet(ids, worker.AllDeviceIDs()) {
			return errors.New("worker trace device identity does not match the registration")
		}
	}
	return nil
}

func splitDeviceIDs(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}

func sameDeviceSet(left, right []string) bool {
	left, right = append([]string(nil), left...), append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
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

func registeredRuntimeUnitID(binding *tgsrlv1.Binding) string {
	if binding.GetRuntimeUnitId() != "" {
		return binding.GetRuntimeUnitId()
	}
	return binding.GetPendingUnitId()
}

func runtimeStateForWorker(state string) (tgsrlv1.RuntimeState, tgsrlv1.SandboxEventType) {
	switch state {
	case "running":
		return tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_RUNNING
	case "paused":
		return tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSED
	case "sleeping":
		return tgsrlv1.RuntimeState_RUNTIME_STATE_SLEEPING, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_SLEEPING
	case "failed":
		return tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_FAILED
	case "terminated":
		return tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_TERMINATED
	default:
		return tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_UNKNOWN
	}
}
