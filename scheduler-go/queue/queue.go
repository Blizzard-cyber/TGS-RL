package queue

import "sync"

// Keyed coalesces duplicate keys while preserving FIFO drain order.
type Keyed struct {
	mu      sync.Mutex
	pending []string
	seen    map[string]struct{}
}

// NewKeyed constructs an empty keyed queue.
func NewKeyed() *Keyed {
	return &Keyed{seen: make(map[string]struct{})}
}

// Push adds key if it is not already pending.
func (q *Keyed) Push(key string) bool {
	if key == "" {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.seen[key]; ok {
		return false
	}
	q.seen[key] = struct{}{}
	q.pending = append(q.pending, key)
	return true
}

// Drain returns pending keys in enqueue order and clears the queue.
func (q *Keyed) Drain() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := append([]string(nil), q.pending...)
	q.pending = q.pending[:0]
	clear(q.seen)
	return out
}

// Len returns the current pending key count.
func (q *Keyed) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}
