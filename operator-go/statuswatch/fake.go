package statuswatch

import (
	"context"
	"fmt"
	"io"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
)

// FakeObserver models the same observation boundary as a Kubernetes watcher.
// It is deterministic and only substitutes external infrastructure.
type FakeObserver struct {
	backend interface {
		Snapshots(context.Context, *api.Bundle) ([]*backend.ObservationSnapshot, error)
	}
}

func NewFake(values ...*backend.FakeBackend) *FakeObserver {
	shared := (*backend.FakeBackend)(nil)
	if len(values) > 0 {
		shared = values[0]
	}
	if shared == nil {
		shared = backend.NewFake()
	}
	return &FakeObserver{backend: shared}
}

func (o *FakeObserver) Watch(ctx context.Context, request Request) (Stream, error) {
	if err := ValidateRequest(request); err != nil {
		return nil, err
	}
	if o == nil || o.backend == nil {
		return nil, fmt.Errorf("fake backend is required")
	}
	snapshots, err := o.backend.Snapshots(ctx, request.Bundle)
	if err != nil {
		return nil, err
	}
	return &fakeStream{snapshots: snapshots}, nil
}

type fakeStream struct {
	snapshots []*backend.ObservationSnapshot
	index     int
}

func (s *fakeStream) Recv() (*Snapshot, error) {
	if s.index >= len(s.snapshots) {
		return nil, io.EOF
	}
	value := s.snapshots[s.index]
	s.index++
	return &Snapshot{ObservedGeneration: value.ObservedGeneration, WorkloadAdmitted: value.WorkloadAdmitted, ResourceClaimsAllocated: value.ResourceClaimsAllocated, AllocatedDeviceIDs: append([]string(nil), value.AllocatedDeviceIDs...), JobActive: value.JobActive, JobSucceeded: value.JobSucceeded, JobFailed: value.JobFailed, JobPaused: value.JobPaused, JobDeleted: value.JobDeleted, Reason: value.Reason, ObservedAt: value.ObservedAt, ControlRequestID: value.ControlRequestID, ControlIdempotencyKey: value.ControlIdempotencyKey, ControlAction: value.ControlAction, ControlBackendRevision: value.ControlBackendRevision, ControlCommitted: value.ControlCommitted}, nil
}
