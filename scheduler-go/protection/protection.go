package protection

import (
	"sync"
	"time"
)

// Clock makes protection behavior deterministic in tests.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// Config controls cooldown, hysteresis, budgets, and breaker behavior.
type Config struct {
	Enabled             bool
	Cooldown            time.Duration
	Hysteresis          float64
	MaxActionsPerWindow int
	Window              time.Duration
	BreakerThreshold    int
	BreakerResetAfter   time.Duration
}

// Decision reports whether an action may proceed.
type Decision struct {
	Allowed bool
	Reason  string
}

type actionState struct {
	lastAction    time.Time
	lastScore     float64
	windowStart   time.Time
	windowCount   int
	breakerCount  int
	breakerOpened time.Time
}

// Guard is a race-safe protection layer for scheduling actions.
type Guard struct {
	mu    sync.Mutex
	clock Clock
	cfg   Config
	state map[string]actionState
}

// Config returns a copy of the configured protection policy.
func (g *Guard) Config() Config {
	if g == nil {
		return Config{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cfg
}

// NewGuard constructs a protection guard with conservative defaults.
func NewGuard(cfg Config, clock Clock) *Guard {
	if clock == nil {
		clock = wallClock{}
	}
	if cfg.Enabled {
		if cfg.MaxActionsPerWindow == 0 {
			cfg.MaxActionsPerWindow = 1
		}
		if cfg.Window <= 0 {
			cfg.Window = time.Minute
		}
		if cfg.BreakerThreshold == 0 {
			cfg.BreakerThreshold = 3
		}
		if cfg.BreakerResetAfter <= 0 {
			cfg.BreakerResetAfter = 5 * time.Minute
		}
	}
	return &Guard{
		clock: clock,
		cfg:   cfg,
		state: make(map[string]actionState),
	}
}

// Allow checks cooldown, hysteresis, budgets, and circuit-breaker state.
func (g *Guard) Allow(key string, score float64) Decision {
	return g.AllowN(key, score, 1)
}

// AllowN checks protection state for a plan containing actionCount mutations.
func (g *Guard) AllowN(key string, score float64, actionCount int) Decision {
	if g == nil || !g.cfg.Enabled {
		return Decision{Allowed: true}
	}
	if key == "" {
		return Decision{Allowed: false, Reason: "EMPTY_KEY"}
	}
	if actionCount < 1 {
		return Decision{Allowed: false, Reason: "EMPTY_ACTION_SET"}
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock.Now()
	st := g.state[key]
	if !st.breakerOpened.IsZero() && now.Sub(st.breakerOpened) < g.cfg.BreakerResetAfter {
		return Decision{Allowed: false, Reason: "CIRCUIT_OPEN"}
	}
	if !st.breakerOpened.IsZero() && now.Sub(st.breakerOpened) >= g.cfg.BreakerResetAfter {
		st.breakerOpened = time.Time{}
		st.breakerCount = 0
	}
	if g.cfg.Cooldown > 0 && !st.lastAction.IsZero() && now.Sub(st.lastAction) < g.cfg.Cooldown {
		return Decision{Allowed: false, Reason: "COOLDOWN"}
	}
	if g.cfg.Hysteresis > 0 && st.lastScore > 0 && score < st.lastScore+g.cfg.Hysteresis {
		return Decision{Allowed: false, Reason: "HYSTERESIS"}
	}
	if st.windowStart.IsZero() || now.Sub(st.windowStart) >= g.cfg.Window {
		st.windowStart = now
		st.windowCount = 0
	}
	if actionCount > g.cfg.MaxActionsPerWindow-st.windowCount {
		return Decision{Allowed: false, Reason: "BUDGET_EXHAUSTED"}
	}

	st.lastAction = now
	st.lastScore = score
	st.windowCount += actionCount
	g.state[key] = st
	return Decision{Allowed: true}
}

// Reject records a failed scheduling attempt against the breaker.
func (g *Guard) Reject(key string) {
	if g == nil || !g.cfg.Enabled || key == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock.Now()
	st := g.state[key]
	st.breakerCount++
	if st.breakerCount >= g.cfg.BreakerThreshold {
		st.breakerOpened = now
	}
	g.state[key] = st
}

// Accept records a successful action and heals one accumulated breaker error.
func (g *Guard) Accept(key string) {
	if g == nil || !g.cfg.Enabled || key == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	st := g.state[key]
	if st.breakerCount > 0 {
		st.breakerCount--
	}
	if st.breakerCount == 0 {
		st.breakerOpened = time.Time{}
	}
	g.state[key] = st
}

// Snapshot returns a copy of the current internal state for tests.
func (g *Guard) Snapshot() map[string]Decision {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]Decision, len(g.state))
	now := g.clock.Now()
	for key, st := range g.state {
		reason := ""
		allowed := true
		if !st.breakerOpened.IsZero() && now.Sub(st.breakerOpened) < g.cfg.BreakerResetAfter {
			allowed = false
			reason = "CIRCUIT_OPEN"
		}
		out[key] = Decision{Allowed: allowed, Reason: reason}
	}
	return out
}
