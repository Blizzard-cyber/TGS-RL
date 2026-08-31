package statuswatch

import (
	"context"
	"fmt"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/bundleadapter"
	"google.golang.org/protobuf/proto"
)

// Observer reports state read back from the execution backend. Applying a
// bundle is intentionally not an observation and must never make a run BOUND
// or RUNNING by itself.
type Observer interface {
	Watch(context.Context, Request) (Stream, error)
}

type Stream interface {
	Recv() (*Snapshot, error)
}

type Request struct {
	Bundle   *api.Bundle
	Bindings []*tgsrlv1.Binding
}

// Snapshot is the backend-independent subset of Kueue and Kubernetes status
// needed to derive the externally visible runtime state.
type Snapshot struct {
	ObservedGeneration      uint64
	WorkloadAdmitted        bool
	ResourceClaimsAllocated bool
	AllocatedDeviceIDs      []string
	JobActive               uint32
	JobSucceeded            uint32
	JobFailed               uint32
	JobPaused               bool
	JobDeleted              bool
	Reason                  string
	ObservedAt              time.Time
	ControlRequestID        string
	ControlIdempotencyKey   string
	ControlAction           tgsrlv1.JobCommandType
	ControlBackendRevision  uint64
	ControlCommitted        bool
}

func snapshotFromAdapter(value *bundleadapter.Snapshot) *Snapshot {
	if value == nil {
		return nil
	}
	return &Snapshot{
		ObservedGeneration:      value.ObservedGeneration,
		WorkloadAdmitted:        value.WorkloadAdmitted,
		ResourceClaimsAllocated: value.ResourceClaimsAllocated,
		AllocatedDeviceIDs:      append([]string(nil), value.AllocatedDeviceIDs...),
		JobActive:               value.JobActive,
		JobSucceeded:            value.JobSucceeded,
		JobFailed:               value.JobFailed,
		JobPaused:               value.JobPaused,
		JobDeleted:              value.JobDeleted,
		Reason:                  value.Reason,
		ObservedAt:              value.ObservedAt,
		ControlRequestID:        value.ControlRequestID,
		ControlIdempotencyKey:   value.ControlIdempotencyKey,
		ControlAction:           value.ControlAction,
		ControlBackendRevision:  value.ControlBackendRevision,
		ControlCommitted:        value.ControlCommitted,
	}
}

type Projection struct {
	EventType tgsrlv1.SandboxEventType
	State     tgsrlv1.RuntimeState
	Health    tgsrlv1.ComponentHealth
	Detail    string
	Ready     bool
}

// Project gives terminal observations precedence and otherwise enforces the
// Kueue admission -> resource allocation -> Job active ordering.
func Project(snapshot *Snapshot, claimRequired bool) Projection {
	if snapshot == nil {
		return Projection{}
	}
	detail := snapshot.Reason
	if snapshot.JobDeleted {
		return Projection{
			EventType: tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_TERMINATED,
			State:     tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED,
			Health:    tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
			Detail:    defaultDetail(detail, "kubernetes job deleted"),
			Ready:     true,
		}
	}
	if snapshot.JobFailed > 0 {
		return Projection{
			EventType: tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_FAILED,
			State:     tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED,
			Health:    tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED,
			Detail:    defaultDetail(detail, "kubernetes job failed"),
			Ready:     true,
		}
	}
	if snapshot.JobSucceeded > 0 {
		return Projection{
			EventType: tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_TERMINATED,
			State:     tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED,
			Health:    tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
			Detail:    defaultDetail(detail, "kubernetes job succeeded"),
			Ready:     true,
		}
	}
	if snapshot.JobPaused {
		return Projection{
			EventType: tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSED,
			State:     tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED,
			Health:    tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
			Detail:    defaultDetail(detail, "kubernetes job suspended"),
			Ready:     true,
		}
	}
	bound := snapshot.WorkloadAdmitted && (!claimRequired || snapshot.ResourceClaimsAllocated)
	if bound && snapshot.JobActive > 0 {
		return Projection{
			EventType: tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_RUNNING,
			State:     tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING,
			Health:    tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
			Detail:    defaultDetail(detail, "kubernetes job is active"),
			Ready:     true,
		}
	}
	if bound {
		return Projection{
			EventType: tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_BOUND,
			State:     tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND,
			Health:    tgsrlv1.ComponentHealth_COMPONENT_HEALTH_PROGRESSING,
			Detail:    defaultDetail(detail, "workload admitted and resources allocated"),
			Ready:     true,
		}
	}
	return Projection{
		Health: tgsrlv1.ComponentHealth_COMPONENT_HEALTH_PROGRESSING,
		Detail: defaultDetail(detail, "waiting for workload admission and resource allocation"),
	}
}

func ValidateRequest(request Request) error {
	if request.Bundle == nil {
		return fmt.Errorf("bundle is required")
	}
	if len(request.Bindings) == 0 {
		return fmt.Errorf("at least one binding is required")
	}
	return nil
}

func CloneRequest(request Request) (Request, error) {
	bundle, err := api.CloneBundle(request.Bundle)
	if err != nil {
		return Request{}, err
	}
	bindings := make([]*tgsrlv1.Binding, 0, len(request.Bindings))
	for _, binding := range request.Bindings {
		if binding == nil {
			continue
		}
		bindings = append(bindings, proto.Clone(binding).(*tgsrlv1.Binding))
	}
	return Request{Bundle: bundle, Bindings: bindings}, nil
}

func defaultDetail(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

type sliceStream struct {
	inner bundleadapter.Stream
}

func (s *sliceStream) Recv() (*Snapshot, error) {
	if s == nil || s.inner == nil {
		return nil, fmt.Errorf("stream is required")
	}
	snapshot, err := s.inner.Recv()
	if err != nil {
		return nil, err
	}
	return snapshotFromAdapter(snapshot), nil
}
