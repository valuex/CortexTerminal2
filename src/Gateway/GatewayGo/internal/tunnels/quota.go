package tunnels

import (
	"sync"
	"time"
)

// Quota is the per-tunnel fixed-window QPS limiter. Mirrors
// CortexTerminal.Gateway.Tunnels.TunnelQuota. In-memory only — single
// gateway instance. For a multi-instance gateway a Redis-backed counter
// would replace this; the slice keeps the local implementation.
type Quota struct {
	maxQPS int
	clock  func() time.Time

	mu       sync.Mutex
	counters map[string]*counter
}

type counter struct {
	window int64 // unix second the window started
	count  int
}

// NewQuota builds a Quota with the supplied QPS cap and clock source.
// Pass clock = time.Now if the test harness wants the real clock.
func NewQuota(maxQPS int, clock func() time.Time) *Quota {
	if clock == nil {
		clock = time.Now
	}
	return &Quota{
		maxQPS:  maxQPS,
		clock:   clock,
		counters: make(map[string]*counter),
	}
}

// TryAcquire attempts to consume one token from tunnelID's bucket.
// Returns true when under the limit, false when the per-second window
// is exhausted. Callers convert the false to HTTP 429.
func (q *Quota) TryAcquire(tunnelID string) bool {
	now := q.clock().UTC().Unix()
	q.mu.Lock()
	defer q.mu.Unlock()
	c, ok := q.counters[tunnelID]
	if !ok {
		c = &counter{window: now, count: 1}
		q.counters[tunnelID] = c
		return true
	}
	if c.window != now {
		c.window = now
		c.count = 1
		return true
	}
	c.count++
	return c.count <= q.maxQPS
}

// Sweep removes counter entries older than 60 seconds — keeps the map
// bounded when many short-lived tunnels pass through. Optional; the
// middleware never calls it directly today.
func (q *Quota) Sweep() {
	threshold := q.clock().UTC().Unix() - 60
	q.mu.Lock()
	defer q.mu.Unlock()
	for k, c := range q.counters {
		if c.window < threshold {
			delete(q.counters, k)
		}
	}
}