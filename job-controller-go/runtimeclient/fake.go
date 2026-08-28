package runtimeclient

import (
	"context"
	"fmt"
	"sync"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

// FakeDriver is a deterministic in-process runtime driver for tests.
type FakeDriver struct {
	mu sync.Mutex

	ValidateDiagnostics []string
	ValidateErr         error
	CommandErr          error
	StatusErr           error

	manifests map[string]*tgsrlv1.RuntimeManifest
	units     map[string][]*tgsrlv1.RuntimeUnit
}

// NewFakeDriver constructs a fake runtime driver.
func NewFakeDriver() *FakeDriver {
	return &FakeDriver{
		manifests: make(map[string]*tgsrlv1.RuntimeManifest),
		units:     make(map[string][]*tgsrlv1.RuntimeUnit),
	}
}

func (d *FakeDriver) ValidateRuntime(_ context.Context, request *tgsrlv1.ValidateRuntimeRequest) (*tgsrlv1.ValidateRuntimeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ValidateErr != nil {
		return nil, d.ValidateErr
	}
	valid := len(d.ValidateDiagnostics) == 0
	return &tgsrlv1.ValidateRuntimeResponse{
		Valid:              valid,
		NormalizedManifest: cloneManifest(request.GetManifest()),
		Diagnostics:        append([]string(nil), d.ValidateDiagnostics...),
		Cursor:             "validate",
	}, nil
}

func (d *FakeDriver) CompileRuntime(_ context.Context, request *tgsrlv1.CompileRuntimeRequest) (*tgsrlv1.CompileRuntimeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	manifest := cloneManifest(request.GetManifest())
	d.manifests[manifest.GetRunId()] = manifest
	unit := &tgsrlv1.RuntimeUnit{
		RuntimeUnitId: fmt.Sprintf("unit-%s", manifest.GetRunId()),
		RunId:         manifest.GetRunId(),
		JobId:         manifest.GetJobId(),
		TraceId:       manifest.GetTraceId(),
		State:         tgsrlv1.RuntimeState_RUNTIME_STATE_REQUESTED,
		Generation:    1,
	}
	d.units[manifest.GetRunId()] = []*tgsrlv1.RuntimeUnit{unit}
	return &tgsrlv1.CompileRuntimeResponse{
		Manifest:     manifest,
		RuntimeUnits: cloneUnits(d.units[manifest.GetRunId()]),
		Cursor:       "compile",
	}, nil
}

func (d *FakeDriver) PrepareRuntime(_ context.Context, request *tgsrlv1.PrepareRuntimeRequest) (*tgsrlv1.PrepareRuntimeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	runID := request.GetRunId()
	d.ensureRun(runID)
	for _, unit := range d.units[runID] {
		unit.State = tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND
	}
	return &tgsrlv1.PrepareRuntimeResponse{
		Manifest:     cloneManifest(d.manifests[runID]),
		RuntimeUnits: cloneUnits(d.units[runID]),
		Cursor:       "prepare",
	}, nil
}

func (d *FakeDriver) StartRuntime(_ context.Context, request *tgsrlv1.StartRuntimeRequest) (*tgsrlv1.StartRuntimeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.CommandErr != nil {
		return nil, d.CommandErr
	}
	runID := request.GetRunId()
	d.ensureRun(runID)
	for _, unit := range d.units[runID] {
		unit.State = tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING
	}
	return &tgsrlv1.StartRuntimeResponse{
		Manifest:     cloneManifest(d.manifests[runID]),
		RuntimeUnits: cloneUnits(d.units[runID]),
		Cursor:       "start",
	}, nil
}

func (d *FakeDriver) PauseRuntime(_ context.Context, request *tgsrlv1.PauseRuntimeRequest) (*tgsrlv1.PauseRuntimeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.CommandErr != nil {
		return nil, d.CommandErr
	}
	runID := request.GetRunId()
	d.ensureRun(runID)
	for _, unit := range d.units[runID] {
		unit.State = tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED
	}
	return &tgsrlv1.PauseRuntimeResponse{
		RuntimeUnits: cloneUnits(d.units[runID]),
		Cursor:       "pause",
	}, nil
}

func (d *FakeDriver) ResumeRuntime(_ context.Context, request *tgsrlv1.ResumeRuntimeRequest) (*tgsrlv1.ResumeRuntimeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.CommandErr != nil {
		return nil, d.CommandErr
	}
	runID := request.GetRunId()
	d.ensureRun(runID)
	for _, unit := range d.units[runID] {
		unit.State = tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING
	}
	return &tgsrlv1.ResumeRuntimeResponse{
		RuntimeUnits: cloneUnits(d.units[runID]),
		Cursor:       "resume",
	}, nil
}

func (d *FakeDriver) StopRuntime(_ context.Context, request *tgsrlv1.StopRuntimeRequest) (*tgsrlv1.StopRuntimeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.CommandErr != nil {
		return nil, d.CommandErr
	}
	runID := request.GetRunId()
	d.ensureRun(runID)
	for _, unit := range d.units[runID] {
		unit.State = tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED
	}
	return &tgsrlv1.StopRuntimeResponse{
		RuntimeUnits: cloneUnits(d.units[runID]),
		Cursor:       "stop",
	}, nil
}

func (d *FakeDriver) TerminateRuntime(_ context.Context, request *tgsrlv1.TerminateRuntimeRequest) (*tgsrlv1.TerminateRuntimeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.CommandErr != nil {
		return nil, d.CommandErr
	}
	runID := request.GetRunId()
	d.ensureRun(runID)
	for _, unit := range d.units[runID] {
		unit.State = tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED
	}
	return &tgsrlv1.TerminateRuntimeResponse{
		RuntimeUnits: cloneUnits(d.units[runID]),
		Cursor:       "terminate",
	}, nil
}

func (d *FakeDriver) GetRuntimeStatus(_ context.Context, request *tgsrlv1.GetRuntimeStatusRequest) (*tgsrlv1.GetRuntimeStatusResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.StatusErr != nil {
		return nil, d.StatusErr
	}
	runID := request.GetRunId()
	d.ensureRun(runID)
	return &tgsrlv1.GetRuntimeStatusResponse{
		Manifest:     cloneManifest(d.manifests[runID]),
		RuntimeUnits: cloneUnits(d.units[runID]),
		Cursor:       "status",
	}, nil
}

func (d *FakeDriver) Close() error { return nil }

func (d *FakeDriver) ensureRun(runID string) {
	if _, ok := d.manifests[runID]; !ok {
		d.manifests[runID] = &tgsrlv1.RuntimeManifest{RunId: runID}
	}
	if _, ok := d.units[runID]; !ok {
		d.units[runID] = []*tgsrlv1.RuntimeUnit{{
			RuntimeUnitId: fmt.Sprintf("unit-%s", runID),
			RunId:         runID,
			State:         tgsrlv1.RuntimeState_RUNTIME_STATE_REQUESTED,
			Generation:    1,
		}}
	}
}

func cloneManifest(manifest *tgsrlv1.RuntimeManifest) *tgsrlv1.RuntimeManifest {
	if manifest == nil {
		return nil
	}
	return proto.Clone(manifest).(*tgsrlv1.RuntimeManifest)
}

func cloneUnits(units []*tgsrlv1.RuntimeUnit) []*tgsrlv1.RuntimeUnit {
	clones := make([]*tgsrlv1.RuntimeUnit, 0, len(units))
	for _, unit := range units {
		clones = append(clones, proto.Clone(unit).(*tgsrlv1.RuntimeUnit))
	}
	return clones
}
