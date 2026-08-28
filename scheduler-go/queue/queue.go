package queue

import "sync"

// Item is one drained keyed value.
type Item[T any] struct {
	Key   string
	Value T
}

// Keyed coalesces duplicate keys while preserving FIFO drain order.
type Keyed[T any] struct {
	mu      sync.Mutex
	pending []string
	items   map[string]T
	merge   func(current T, incoming T) T
}

// NewKeyed constructs an empty keyed queue.
func NewKeyed[T any](merge func(current T, incoming T) T) *Keyed[T] {
	return &Keyed[T]{
		items: make(map[string]T),
		merge: merge,
	}
}

// Push adds key if it is not already pending. When the key is already pending,
// Push merges metadata in place and preserves the original FIFO position.
func (q *Keyed[T]) Push(key string, value T) bool {
	if key == "" {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if current, ok := q.items[key]; ok {
		if q.merge != nil {
			q.items[key] = q.merge(current, value)
		} else {
			q.items[key] = value
		}
		return false
	}
	q.items[key] = value
	q.pending = append(q.pending, key)
	return true
}

// Drain returns pending keys in enqueue order and clears the queue.
func (q *Keyed[T]) Drain() []Item[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Item[T], 0, len(q.pending))
	for _, key := range q.pending {
		value, ok := q.items[key]
		if !ok {
			continue
		}
		out = append(out, Item[T]{Key: key, Value: value})
	}
	q.pending = q.pending[:0]
	clear(q.items)
	return out
}

// Len returns the current pending key count.
func (q *Keyed[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}
