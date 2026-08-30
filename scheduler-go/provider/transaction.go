package provider

import (
	"context"
	"errors"
	"fmt"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

var ErrTransactionalUnavailable = errors.New("provider: transactional execution unavailable")

type TransactionPhase string

const (
	TransactionPhaseUnknown    TransactionPhase = "unknown"
	TransactionPhasePrepared   TransactionPhase = "prepared"
	TransactionPhaseExecuting  TransactionPhase = "executing"
	TransactionPhaseCommitted  TransactionPhase = "committed"
	TransactionPhaseAborted    TransactionPhase = "aborted"
	TransactionPhaseReconciled TransactionPhase = "reconciled"
	TransactionPhaseDegraded   TransactionPhase = "degraded"
)

type EffectStatus string

const (
	EffectStatusUnknown     EffectStatus = "unknown"
	EffectStatusNotApplied  EffectStatus = "not_applied"
	EffectStatusApplied     EffectStatus = "applied"
	EffectStatusFailed      EffectStatus = "failed"
	EffectStatusCompensated EffectStatus = "compensated"
	EffectStatusDegraded    EffectStatus = "degraded"
)

type TransactionCapabilities struct {
	OrderedStepExecution   bool
	CompensatingAbort      bool
	StepIdempotency        bool
	GenerationFence        bool
	PartialEffectReporting bool
	AtomicReplacement      bool
}

type TransactionEffect struct {
	StepIndex             int
	ActionID              string
	IdempotencyKey        string
	Status                EffectStatus
	Generation            uint64
	Revision              uint64
	ErrorCode             string
	ErrorMessage          string
	CompensationAttempted bool
	Compensated           bool
}

type TransactionReceipt struct {
	TransactionID    string
	PlanID           string
	Generation       uint64
	Phase            TransactionPhase
	ObservedRevision uint64
	Effects          []TransactionEffect
	ErrorCode        string
	ErrorMessage     string
	PreparedAt       time.Time
	UpdatedAt        time.Time
}

// TransactionExecutor is the mutation-only dependency used by the durable
// plan executor. It deliberately excludes discovery and observation methods.
type TransactionExecutor interface {
	PreparePlan(context.Context, string, uint64, *tgsrlv1.PlacementPlan) (*TransactionReceipt, error)
	ExecuteStep(context.Context, string, uint64, int) (*TransactionReceipt, error)
	CommitPlan(context.Context, string, uint64) (*TransactionReceipt, error)
	AbortPlan(context.Context, string, uint64) (*TransactionReceipt, error)
	ReconcilePlanTransaction(context.Context, string, uint64) (*TransactionReceipt, error)
	DescribeCapabilities(context.Context) (TransactionCapabilities, error)
}

// TransactionalResourceProvider is the complete Scheduler provider boundary:
// discovery, observation ingestion, and durable plan execution.
type TransactionalResourceProvider interface {
	ResourceProvider
	TransactionExecutor
	ObserveSandbox(context.Context, *tgsrlv1.SandboxEvent) (*tgsrlv1.SandboxEvent, error)
}

// ValidateExecutionCapabilities is the single capability handshake for a
// mutating plan. Provider feature names come from CapabilitySet; transaction
// guarantees come from the executable transaction contract itself.
func ValidateExecutionCapabilities(ctx context.Context, resourceProvider TransactionalResourceProvider, plan *tgsrlv1.PlacementPlan) error {
	if resourceProvider == nil {
		return ErrTransactionalUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	available, err := resourceProvider.Capabilities(ctx)
	if err != nil {
		return err
	}
	if err := ValidateProviderCapabilityRequirements(available, plan); err != nil {
		return err
	}
	transactional, err := resourceProvider.DescribeCapabilities(ctx)
	if err != nil {
		return err
	}
	for _, required := range plan.GetCapabilityRequirements() {
		if required == nil {
			return fmt.Errorf("%w: nil plan capability requirement", ErrInvalidArgument)
		}
		if !required.GetRequired() || required.GetKind() == tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_PROVIDER_CAPABILITY {
			continue
		}
		versionOK, versionErr := versionAtLeast("1.0.0", required.GetMinVersion())
		if versionErr != nil || !versionOK || !supportsTransactionRequirement(transactional, required.GetKind()) {
			return &Error{Code: ErrorCodeUnsupported, Message: fmt.Sprintf("executor does not support plan capability %s", required.GetKind()), PlanID: plan.GetPlanId(), Cause: ErrUnsupported}
		}
	}
	return nil
}

func supportsTransactionRequirement(capabilities TransactionCapabilities, kind tgsrlv1.CapabilityRequirementKind) bool {
	switch kind {
	case tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ORDERED_ACTION_EXECUTION:
		return capabilities.OrderedStepExecution
	case tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_COMPENSATING_ROLLBACK:
		return capabilities.CompensatingAbort
	case tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_TRANSACTIONAL_PLAN_EXECUTION:
		return capabilities.StepIdempotency && capabilities.GenerationFence && capabilities.PartialEffectReporting
	case tgsrlv1.CapabilityRequirementKind_CAPABILITY_REQUIREMENT_KIND_ATOMIC_REPLACEMENT:
		return capabilities.AtomicReplacement && capabilities.OrderedStepExecution && capabilities.CompensatingAbort && capabilities.StepIdempotency && capabilities.GenerationFence && capabilities.PartialEffectReporting
	default:
		return false
	}
}
