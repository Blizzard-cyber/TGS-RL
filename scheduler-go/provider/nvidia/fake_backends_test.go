package nvidia

import (
	"context"
	"sync"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// FakeInventoryBackend is a deterministic inventory backend for tests.
type FakeInventoryBackend struct {
	mu        sync.Mutex
	Snapshots []*InventorySnapshot
	Err       error
	Calls     int
}

// Discover returns the next configured snapshot, retaining the last snapshot.
func (b *FakeInventoryBackend) Discover(ctx context.Context) (*InventorySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Calls++
	if b.Err != nil {
		return nil, b.Err
	}
	if len(b.Snapshots) == 0 {
		return &InventorySnapshot{}, nil
	}
	result := cloneInventory(b.Snapshots[0])
	if len(b.Snapshots) > 1 {
		b.Snapshots = b.Snapshots[1:]
	}
	return result, nil
}

// FakePartitionBackend is a deterministic partition backend for tests.
type FakePartitionBackend struct {
	mu            sync.Mutex
	PartitionMode PartitionMode
	Snapshot      *PartitionSnapshot
	DiscoverErr   error
	ApplyResult   *BackendActionResult
	ApplyErr      error
	ApplyFunc     func(context.Context, BackendActionRequest) (*BackendActionResult, error)
	requests      []BackendActionRequest
}

// Mode returns the configured mode.
func (b *FakePartitionBackend) Mode() PartitionMode { return b.PartitionMode }

// Discover returns the configured snapshot.
func (b *FakePartitionBackend) Discover(ctx context.Context, _ *InventorySnapshot) (*PartitionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return clonePartitions(b.Snapshot), b.DiscoverErr
}

// Apply returns the configured result.
func (b *FakePartitionBackend) Apply(ctx context.Context, request BackendActionRequest) (*BackendActionResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.requests = append(b.requests, cloneBackendRequest(request))
	apply := b.ApplyFunc
	result, err := cloneBackendResult(b.ApplyResult), b.ApplyErr
	b.mu.Unlock()
	if apply != nil {
		return apply(ctx, cloneBackendRequest(request))
	}
	return result, err
}

// Requests returns detached apply requests in call order.
func (b *FakePartitionBackend) Requests() []BackendActionRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make([]BackendActionRequest, len(b.requests))
	for index, request := range b.requests {
		result[index] = cloneBackendRequest(request)
	}
	return result
}

func cloneBackendRequest(request BackendActionRequest) BackendActionRequest {
	request.State = cloneDriverState(request.State)
	request.Action = cloneAction(request.Action)
	request.Partitions = clonePartitions(request.Partitions)
	request.Binding = cloneDiscoveredBinding(request.Binding)
	return request
}

// FakeRuntimeBackend is a deterministic runtime backend for tests.
type FakeRuntimeBackend struct {
	Status      *RuntimeBackendStatus
	DiscoverErr error
	ApplyResult *BackendActionResult
	ApplyErr    error
}

// Discover returns the configured runtime capability surface.
func (b *FakeRuntimeBackend) Discover(ctx context.Context) (*RuntimeBackendStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.Status == nil {
		return nil, b.DiscoverErr
	}
	status := *b.Status
	status.SupportedActions = append([]tgsrlv1.ActionType(nil), b.Status.SupportedActions...)
	status.Sandboxes = cloneSandboxSlice(b.Status.Sandboxes)
	return &status, b.DiscoverErr
}

// Apply returns the configured result.
func (b *FakeRuntimeBackend) Apply(ctx context.Context, _ BackendActionRequest) (*BackendActionResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return cloneBackendResult(b.ApplyResult), b.ApplyErr
}

// FakeBindingBackend is a deterministic binding backend for tests.
type FakeBindingBackend struct {
	Snapshot    *BindingSnapshot
	DiscoverErr error
	ApplyResult *BackendActionResult
	ApplyErr    error
}

// Discover returns the configured binding snapshot.
func (b *FakeBindingBackend) Discover(ctx context.Context, _ *InventorySnapshot, _ *PartitionSnapshot) (*BindingSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.Snapshot == nil {
		return nil, b.DiscoverErr
	}
	result := *b.Snapshot
	result.Bindings = cloneBindings(b.Snapshot.Bindings)
	result.SupportedActions = append([]tgsrlv1.ActionType(nil), b.Snapshot.SupportedActions...)
	return &result, b.DiscoverErr
}

// Apply returns the configured result.
func (b *FakeBindingBackend) Apply(ctx context.Context, _ BackendActionRequest) (*BackendActionResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return cloneBackendResult(b.ApplyResult), b.ApplyErr
}
