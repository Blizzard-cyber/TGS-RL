package config

import (
	"fmt"

	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/policy"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/preemption"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/protection"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scheduler"
)

// ApplyToScheduler projects a validated YAML PolicyBundle into the exact
// configuration consumed by the authoritative scheduler.Scheduler.
func (p PolicyBundle) ApplyToScheduler(base scheduler.Config) (scheduler.Config, error) {
	projection := p.SchedulerPolicy()
	bundle := policy.Bundle{
		ID:               projection.PolicyID,
		Version:          projection.PolicyVersion,
		Strategy:         policy.Strategy(projection.Strategy),
		AllowPreemption:  projection.Preemption.Enabled,
		PreemptionPolicy: projection.Preemption.Strategy,
		RequireSafePoint: projection.Preemption.RequireSafePoint,
	}
	if err := bundle.Validate(); err != nil {
		return scheduler.Config{}, fmt.Errorf("config: scheduler policy: %w", err)
	}
	guardConfig := protection.Config{
		Enabled:             projection.Protection.Enabled,
		Cooldown:            projection.Protection.Cooldown,
		Hysteresis:          projection.Protection.Hysteresis,
		MaxActionsPerWindow: projection.Protection.MaxActionsPerWindow,
		Window:              projection.Protection.Window,
		BreakerThreshold:    projection.Protection.BreakerThreshold,
		BreakerResetAfter:   projection.Protection.BreakerResetAfter,
	}
	var strategy preemption.Strategy = preemption.NoOp{}
	if projection.Preemption.Enabled {
		strategy = preemption.LowPriorityFirst{}
	}
	base.Policy = bundle
	base.Guard = protection.NewGuard(guardConfig, base.Clock)
	base.Preemption = strategy
	base.ConfigRevision = projection.PolicyVersion
	return base, nil
}
