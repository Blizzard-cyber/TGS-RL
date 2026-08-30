package scheduler

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/policy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/preemption"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/protection"
)

const (
	// SafePointAnnotation is the canonical snapshot annotation used to advertise
	// that the current execution state permits a safe mutation.
	SafePointAnnotation = "tgsrl.io/safe-point"

	FallbackReasonIntentExpired  = "INTENT_EXPIRED"
	FallbackReasonNoCandidate    = "NO_CANDIDATE"
	FallbackReasonStaleIntent    = "STALE_INTENT"
	FallbackReasonSafePoint      = "SAFE_POINT_REQUIRED"
	FallbackReasonRevision       = "REVISION_MISMATCH"
	FallbackReasonPendingCount   = "PENDING_UNIT_COUNT_MISMATCH"
	defaultCodeRevision          = "scheduler-go/v1"
	defaultConfigurationRevision = "default"
)

// FallbackMode controls the safe result returned when no new placement may be
// made. Neither mode creates an action: NoOp returns an empty plan, while Static
// describes already committed allocations matching the evaluated intent.
type FallbackMode string

const (
	FallbackNoOp   FallbackMode = "NO_OP"
	FallbackStatic FallbackMode = "STATIC"
)

// Clock is injectable so replay and tests never depend on wall-clock time.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

// Sequence supplies the monotonic cursor written to DecisionRecord.
type Sequence interface {
	Next() uint64
}

// SequenceFunc adapts a function to Sequence. Scheduler serializes calls to it.
type SequenceFunc func() uint64

func (f SequenceFunc) Next() uint64 { return f() }

// Config contains only scheduler-generic behavior and audit metadata.
type Config struct {
	Fallback            FallbackMode
	Clock               Clock
	Sequence            Sequence
	CodeRevision        string
	ConfigRevision      string
	DataKind            tgsrlv1.DataKind
	SafePointAnnotation string
	Policy              policy.Bundle
	Guard               *protection.Guard
	Preemption          preemption.Strategy
	// PlannerEvidenceBudget independently bounds adaptive-planner evidence.
	// A zero value uses DefaultPlannerEvidenceBudget.
	PlannerEvidenceBudget int
	// PlannerPerTickActionBudget bounds adaptive mutations proposed in one
	// evaluation. Store currently accepts one mutation action, so zero defaults
	// to DefaultPlannerActionBudget.
	PlannerPerTickActionBudget int
	// PlannerUtility holds fixed-point utility weights. An all-zero value uses
	// DefaultPlannerUtilityConfig.
	PlannerUtility PlannerUtilityConfig
	// PlannerIdleSleepAfter and PlannerIdleOffloadAfter are deterministic
	// thresholds evaluated against the explicit sandbox observed_at timestamp.
	PlannerIdleSleepAfter   time.Duration
	PlannerIdleOffloadAfter time.Duration
	// PlannerSandboxMaximumAge rejects missing, future, or stale runtime
	// observations before any adaptive tier can authorize a mutation.
	PlannerSandboxMaximumAge time.Duration
}

// ValidationError identifies a stable input field without coupling callers to
// an error string. Expected scheduling outcomes (expiry, stale data, or no
// candidate) are represented as fallback decisions rather than errors.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("scheduler: invalid %s: %s", e.Field, e.Reason)
}

// Scheduler evaluates immutable snapshot/intent pairs. It holds no resource or
// provider state; the mutex only protects a caller-supplied sequence generator.
type Scheduler struct {
	fallback       FallbackMode
	clock          Clock
	sequence       Sequence
	codeRevision   string
	configRevision string
	dataKind       tgsrlv1.DataKind
	safePointKey   string
	policyBundle   policy.Bundle
	policy         policy.Policy
	guard          *protection.Guard
	preemption     preemption.Strategy
	coordinator    *Coordinator
	sequenceMu     sync.Mutex
}

// DefaultConfig returns conservative production defaults. Callers performing
// replay should replace Clock and Sequence with deterministic implementations.
func DefaultConfig() Config {
	return Config{
		Fallback:            FallbackNoOp,
		Clock:               wallClock{},
		Sequence:            &atomicSequence{},
		CodeRevision:        defaultCodeRevision,
		ConfigRevision:      defaultConfigurationRevision,
		DataKind:            tgsrlv1.DataKind_DATA_KIND_UNKNOWN,
		SafePointAnnotation: SafePointAnnotation,
	}
}

// IsValidationError reports whether err represents malformed scheduler input.
func IsValidationError(err error) bool {
	var target *ValidationError
	return errors.As(err, &target)
}

// New constructs a Scheduler. Zero-valued Config fields receive conservative
// defaults, except invalid enum/configuration values which are rejected.
func New(config Config) (*Scheduler, error) {
	defaults := DefaultConfig()
	if config.Fallback == "" {
		config.Fallback = defaults.Fallback
	}
	if config.Fallback != FallbackNoOp && config.Fallback != FallbackStatic {
		return nil, &ValidationError{Field: "config.fallback", Reason: "must be NO_OP or STATIC"}
	}
	if config.Clock == nil {
		config.Clock = defaults.Clock
	}
	if config.Sequence == nil {
		config.Sequence = defaults.Sequence
	}
	if config.CodeRevision == "" {
		config.CodeRevision = defaults.CodeRevision
	}
	if config.ConfigRevision == "" {
		config.ConfigRevision = defaults.ConfigRevision
	}
	if config.SafePointAnnotation == "" {
		config.SafePointAnnotation = defaults.SafePointAnnotation
	}
	if config.DataKind < tgsrlv1.DataKind_DATA_KIND_UNKNOWN || config.DataKind > tgsrlv1.DataKind_DATA_KIND_LIVE {
		return nil, &ValidationError{Field: "config.data_kind", Reason: "unknown enum value"}
	}
	if config.Policy.ID == "" {
		config.Policy = policy.Bundle{
			ID:               "default",
			Version:          config.ConfigRevision,
			Strategy:         policy.StrategyScoreFirst,
			TopK:             1,
			PreemptionPolicy: "noop",
			RequireSafePoint: true,
		}
	}
	if config.Policy.TopK == 0 {
		config.Policy.TopK = 1
	}
	configuredPolicy, err := policy.New(config.Policy)
	if err != nil {
		return nil, &ValidationError{Field: "config.policy", Reason: err.Error()}
	}
	if config.Guard == nil {
		config.Guard = protection.NewGuard(protection.Config{}, schedulerProtectionClock{clock: config.Clock})
	}
	if config.Preemption == nil {
		config.Preemption = preemption.NoOp{}
	}
	scheduler := &Scheduler{
		fallback:       config.Fallback,
		clock:          config.Clock,
		sequence:       config.Sequence,
		codeRevision:   config.CodeRevision,
		configRevision: config.ConfigRevision,
		dataKind:       config.DataKind,
		safePointKey:   config.SafePointAnnotation,
		policyBundle:   config.Policy,
		policy:         configuredPolicy,
		guard:          config.Guard,
		preemption:     config.Preemption,
	}
	scheduler.coordinator = NewCoordinator(config)
	return scheduler, nil
}

// Guard returns the scheduler's configured protection guard.
func (s *Scheduler) Guard() *protection.Guard {
	if s == nil {
		return nil
	}
	return s.guard
}

func (s *Scheduler) nextSequence() uint64 {
	s.sequenceMu.Lock()
	defer s.sequenceMu.Unlock()
	return s.sequence.Next()
}

func (s *Scheduler) resolveDataKind(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) tgsrlv1.DataKind {
	if s.dataKind != tgsrlv1.DataKind_DATA_KIND_UNKNOWN {
		return s.dataKind
	}
	value := intent.GetLabels()["data_kind"]
	if value == "" {
		value = snapshot.GetAnnotations()["data_kind"]
	}
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "-", "_")) {
	case "synthetic":
		return tgsrlv1.DataKind_DATA_KIND_SYNTHETIC
	case "replay":
		return tgsrlv1.DataKind_DATA_KIND_REPLAY
	case "live":
		return tgsrlv1.DataKind_DATA_KIND_LIVE
	default:
		return tgsrlv1.DataKind_DATA_KIND_UNKNOWN
	}
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

type atomicSequence struct{ value atomic.Uint64 }

func (s *atomicSequence) Next() uint64 { return s.value.Add(1) }
