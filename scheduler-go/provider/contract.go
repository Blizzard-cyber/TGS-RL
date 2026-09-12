package provider

import (
	"context"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// ResourceProvider is the vendor-neutral scheduler resource boundary.
// Implementations project vendor inventory into the shared Device contract and
// must clone protobuf messages at every ownership boundary.
type ResourceProvider interface {
	Capabilities(context.Context) (*tgsrlv1.CapabilitySet, error)
	Snapshot(context.Context) (*tgsrlv1.ClusterSnapshot, error)
	ListDevices(context.Context) ([]*tgsrlv1.Device, error)
	ListSandboxes(context.Context) ([]Sandbox, error)
	GetSandbox(context.Context, string) (Sandbox, error)
}

// HealthStatus reports provider readiness without exposing provider-internal
// implementation details.
type HealthStatus struct {
	ProviderID string
	Source     string
	Healthy    bool
	Reason     string
	CheckedAt  time.Time
}

// WatchedResourceEvent couples a replayable cursor with one immutable resource
// event.
type WatchedResourceEvent struct {
	Cursor uint64
	Event  *tgsrlv1.ResourceEvent
}

// WatchedSandboxEvent couples a replayable cursor with one immutable sandbox
// event.
type WatchedSandboxEvent struct {
	Cursor uint64
	Event  *tgsrlv1.SandboxEvent
}

// PlanStatus describes the terminal or recoverable execution state of a plan.
type PlanStatus string

const (
	PlanStatusUnknown   PlanStatus = "unknown"
	PlanStatusInFlight  PlanStatus = "in_flight"
	PlanStatusSucceeded PlanStatus = "succeeded"
	PlanStatusFailed    PlanStatus = "failed"
)

// PlanRecord is the recovery/reconciliation view of one provider plan.
type PlanRecord struct {
	Plan             *tgsrlv1.PlacementPlan
	Status           PlanStatus
	Results          []*tgsrlv1.ActionResult
	ErrorCode        string
	ErrorMessage     string
	ObservedRevision uint64
	UpdatedAt        time.Time
}

// CompleteResourceProvider is the production scheduler plug-in boundary.
// A hardware implementation supplies discovery, observations, durable
// transactions, health, watches, and recovery without changing scheduler
// candidate generation or scoring.
type CompleteResourceProvider interface {
	TransactionalResourceProvider
	ID(context.Context) (string, error)
	Health(context.Context) (*HealthStatus, error)
	WatchResources(context.Context, uint64) (<-chan WatchedResourceEvent, error)
	WatchSandboxes(context.Context, uint64) (<-chan WatchedSandboxEvent, error)
	ReconcilePlan(context.Context, *tgsrlv1.PlacementPlan) (*PlanRecord, error)
	RecoverInFlightPlans(context.Context) ([]*PlanRecord, error)
}
