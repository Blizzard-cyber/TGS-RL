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

// State is the durable protection snapshot used for restart recovery.
type State struct {
	Entries map[string]StateEntry `json:"entries,omitempty"`
}

// StateEntry is the durable form of one protection key.
type StateEntry struct {
	LastAction    time.Time `json:"last_action,omitempty"`
	LastScore     float64   `json:"last_score,omitempty"`
	WindowStart   time.Time `json:"window_start,omitempty"`
	WindowCount   int       `json:"window_count,omitempty"`
	BreakerCount  int       `json:"breaker_count,omitempty"`
	BreakerOpened time.Time `json:"breaker_opened,omitempty"`
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
	return g.CommitN(key, score, 1)
}

// AllowN applies protection state for a plan containing actionCount mutations.
func (g *Guard) AllowN(key string, score float64, actionCount int) Decision {
	return g.CommitN(key, score, actionCount)
}

// Check checks protection state without mutating it.
func (g *Guard) Check(key string, score float64) Decision {
	return g.CheckN(key, score, 1)
}

// CheckN checks protection state for a plan containing actionCount mutations
// without consuming cooldown, budget, or hysteresis state.
func (g *Guard) CheckN(key string, score float64, actionCount int) Decision {
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
	decision, _, _ := g.evaluateLocked(st, now, score, actionCount)
	return decision
}

// CommitN re-validates and then consumes protection state for an allowed plan.
func (g *Guard) CommitN(key string, score float64, actionCount int) Decision {
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
	decision, next, allowed := g.evaluateLocked(st, now, score, actionCount)
	if allowed {
		g.state[key] = next
	}
	return decision
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
	st = g.normalizeLocked(st, now)
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
	st = g.normalizeLocked(st, g.clock.Now())
	if st.breakerCount > 0 {
		st.breakerCount--
	}
	if st.breakerCount == 0 {
		st.breakerOpened = time.Time{}
	}
	g.state[key] = st
}

// ExportState returns a deep copy of the durable protection state.
func (g *Guard) ExportState() State {
	if g == nil {
		return State{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	out := State{Entries: make(map[string]StateEntry, len(g.state))}
	for key, st := range g.state {
		out.Entries[key] = StateEntry{
			LastAction:    st.lastAction,
			LastScore:     st.lastScore,
			WindowStart:   st.windowStart,
			WindowCount:   st.windowCount,
			BreakerCount:  st.breakerCount,
			BreakerOpened: st.breakerOpened,
		}
	}
	return out
}

// RestoreState replaces the in-memory state with a durable snapshot.
func (g *Guard) RestoreState(snapshot State) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state = make(map[string]actionState, len(snapshot.Entries))
	for key, entry := range snapshot.Entries {
		g.state[key] = actionState{
			lastAction:    entry.LastAction,
			lastScore:     entry.LastScore,
			windowStart:   entry.WindowStart,
			windowCount:   entry.WindowCount,
			breakerCount:  entry.BreakerCount,
			breakerOpened: entry.BreakerOpened,
		}
	}
}

// Snapshot returns a copy of the current internal state for tests.
func (g *Guard) Snapshot() map[string]Decision {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]Decision, len(g.state))
	now := g.clock.Now()
	for key, st := range g.state {
		st = g.normalizeLocked(st, now)
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

func (g *Guard) evaluateLocked(st actionState, now time.Time, score float64, actionCount int) (Decision, actionState, bool) {
	st = g.normalizeLocked(st, now)
	if !st.breakerOpened.IsZero() && now.Sub(st.breakerOpened) < g.cfg.BreakerResetAfter {
		return Decision{Allowed: false, Reason: "CIRCUIT_OPEN"}, st, false
	}
	if g.cfg.Cooldown > 0 && !st.lastAction.IsZero() && now.Sub(st.lastAction) < g.cfg.Cooldown {
		return Decision{Allowed: false, Reason: "COOLDOWN"}, st, false
	}
	if g.cfg.Hysteresis > 0 && st.lastScore > 0 && score < st.lastScore+g.cfg.Hysteresis {
		return Decision{Allowed: false, Reason: "HYSTERESIS"}, st, false
	}
	if st.windowStart.IsZero() || now.Sub(st.windowStart) >= g.cfg.Window {
		st.windowStart = now
		st.windowCount = 0
	}
	if actionCount > g.cfg.MaxActionsPerWindow-st.windowCount {
		return Decision{Allowed: false, Reason: "BUDGET_EXHAUSTED"}, st, false
	}

	st.lastAction = now
	st.lastScore = score
	st.windowCount += actionCount
	return Decision{Allowed: true}, st, true
}

func (g *Guard) normalizeLocked(st actionState, now time.Time) actionState {
	if !st.breakerOpened.IsZero() && now.Sub(st.breakerOpened) >= g.cfg.BreakerResetAfter {
		st.breakerOpened = time.Time{}
		st.breakerCount = 0
	}
	if !st.windowStart.IsZero() && now.Sub(st.windowStart) >= g.cfg.Window {
		st.windowStart = time.Time{}
		st.windowCount = 0
	}
	return st
}
