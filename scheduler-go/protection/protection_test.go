package protection

import (
	"testing"
	"time"
)

type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time { return f.now }

func TestGuardCooldownBudgetAndBreaker(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)}
	guard := NewGuard(Config{
		Enabled:             true,
		Cooldown:            time.Second,
		Hysteresis:          0.2,
		MaxActionsPerWindow: 2,
		Window:              10 * time.Second,
		BreakerThreshold:    2,
		BreakerResetAfter:   5 * time.Second,
	}, clock)

	if decision := guard.Allow("job/stage", 1.0); !decision.Allowed {
		t.Fatalf("first decision = %+v, want allowed", decision)
	}
	if decision := guard.Allow("job/stage", 1.3); decision.Allowed || decision.Reason != "COOLDOWN" {
		t.Fatalf("cooldown decision = %+v, want COOLDOWN", decision)
	}

	clock.now = clock.now.Add(2 * time.Second)
	if decision := guard.Allow("job/stage", 1.1); decision.Allowed || decision.Reason != "HYSTERESIS" {
		t.Fatalf("hysteresis decision = %+v, want HYSTERESIS", decision)
	}
	if decision := guard.Allow("job/stage", 1.5); !decision.Allowed {
		t.Fatalf("second allowed decision = %+v, want allowed", decision)
	}

	clock.now = clock.now.Add(2 * time.Second)
	if decision := guard.Allow("job/stage", 1.8); decision.Allowed || decision.Reason != "BUDGET_EXHAUSTED" {
		t.Fatalf("budget decision = %+v, want BUDGET_EXHAUSTED", decision)
	}

	guard.Reject("job/stage")
	guard.Reject("job/stage")
	if decision := guard.Allow("job/stage", 2.0); decision.Allowed || decision.Reason != "CIRCUIT_OPEN" {
		t.Fatalf("breaker decision = %+v, want CIRCUIT_OPEN", decision)
	}

	clock.now = clock.now.Add(6 * time.Second)
	if decision := guard.Allow("job/stage", 2.0); !decision.Allowed {
		t.Fatalf("decision after breaker reset = %+v, want allowed", decision)
	}
}

func TestDisabledGuardDoesNotChangeSchedulerDefaults(t *testing.T) {
	guard := NewGuard(Config{}, nil)
	for iteration := 0; iteration < 100; iteration++ {
		if decision := guard.AllowN("job/stage", 1, 1000); !decision.Allowed {
			t.Fatalf("iteration %d decision = %+v, want disabled guard to allow", iteration, decision)
		}
	}
}
