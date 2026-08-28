package statuswatch

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/backend"
)

type SnapshotBackend interface {
	Snapshot(context.Context, *api.Bundle) (*backend.ObservationSnapshot, bool, error)
}

type BackendObserver struct {
	backend  SnapshotBackend
	interval time.Duration
}

func NewBackendObserver(source SnapshotBackend, interval time.Duration) (*BackendObserver, error) {
	if source == nil {
		return nil, fmt.Errorf("snapshot backend is required")
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &BackendObserver{backend: source, interval: interval}, nil
}

func (o *BackendObserver) Watch(ctx context.Context, request Request) (Stream, error) {
	if err := ValidateRequest(request); err != nil {
		return nil, err
	}
	clone, err := CloneRequest(request)
	if err != nil {
		return nil, err
	}
	return &backendStream{ctx: ctx, backend: o.backend, interval: o.interval, bundle: clone.Bundle}, nil
}

type backendStream struct {
	ctx      context.Context
	backend  SnapshotBackend
	interval time.Duration
	bundle   *api.Bundle
	done     bool
}

func (s *backendStream) Recv() (*Snapshot, error) {
	if s.done {
		return nil, io.EOF
	}
	value, terminal, err := s.backend.Snapshot(s.ctx, s.bundle)
	if err != nil {
		return nil, err
	}
	if terminal {
		s.done = true
	}
	if !terminal {
		timer := time.NewTimer(s.interval)
		defer timer.Stop()
		select {
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		case <-timer.C:
		}
	}
	return snapshotFromBackend(value), nil
}

func snapshotFromBackend(value *backend.ObservationSnapshot) *Snapshot {
	if value == nil {
		return nil
	}
	return &Snapshot{ObservedGeneration: value.ObservedGeneration, WorkloadAdmitted: value.WorkloadAdmitted, ResourceClaimsAllocated: value.ResourceClaimsAllocated, JobActive: value.JobActive, JobSucceeded: value.JobSucceeded, JobFailed: value.JobFailed, JobPaused: value.JobPaused, JobDeleted: value.JobDeleted, Reason: value.Reason, ObservedAt: value.ObservedAt, ControlRequestID: value.ControlRequestID, ControlIdempotencyKey: value.ControlIdempotencyKey, ControlAction: value.ControlAction, ControlBackendRevision: value.ControlBackendRevision, ControlCommitted: value.ControlCommitted}
}
