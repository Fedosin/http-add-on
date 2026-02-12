package queue

import (
	"sync"
	"sync/atomic"
	"time"
)

// CountReader represents the size of a virtual HTTP queue, possibly
// distributed across multiple HTTP server processes. It only can access
// the current size of the queue, not any other information about requests.
//
// It is concurrency safe.
type CountReader interface {
	// Current returns the current count of pending requests
	// for the given hostname
	Current() (*Counts, error)
}

// Counter represents a virtual HTTP queue, possibly distributed across
// multiple HTTP server processes. It can only increase or decrease the
// size of the queue or read the current size of the queue, but not read
// or modify any other information about it.
//
// Both the mutation and read functionality is concurrency safe, but
// the read functionality is point-in-time only
type Counter interface {
	CountReader
	// Increase increases the queue size by delta for the given host.
	Increase(host string, delta int) error
	// Decrease decreases the queue size by delta for the given host.
	Decrease(host string, delta int) error
	// EnsureKey ensures that host is represented in this counter.
	EnsureKey(host string, window, granularity time.Duration)
	// UpdateBuckets update request backets if there are changes
	UpdateBuckets(host string, window, granularity time.Duration)
	// RemoveKey tries to remove the given host and its
	// associated counts from the queue. returns true if it existed,
	// false otherwise.
	RemoveKey(host string) bool
}

// hostEntry bundles the concurrency counter and RPS buckets for a single host.
// The concurrency counter uses atomic operations for lock-free access on the
// hot path. The RPS buckets pointer is stored atomically so it can be replaced
// during configuration updates without affecting concurrent readers.
type hostEntry struct {
	concurrency atomic.Int64
	buckets     atomic.Pointer[RequestsBuckets]
}

// Memory implements Counter and CountReader
var _ Counter = (*Memory)(nil)
var _ CountReader = (*Memory)(nil)

// Memory is a Counter implementation that holds the HTTP queue in memory
// only. It uses sync.Map and atomic operations to avoid global locking on
// the hot path (Increase/Decrease). Always use NewMemory to create one of
// these.
type Memory struct {
	hosts sync.Map // map[string]*hostEntry
}

// NewMemoryQueue creates a new empty in-memory queue
func NewMemory() *Memory {
	return &Memory{}
}

// Increase changes the size of the queue adding delta.
// Uses atomic operations — no global lock required.
func (r *Memory) Increase(host string, delta int) error {
	val, ok := r.hosts.Load(host)
	if !ok {
		return nil
	}
	he := val.(*hostEntry)
	he.concurrency.Add(int64(delta))
	if buckets := he.buckets.Load(); buckets != nil {
		buckets.Record(time.Now(), delta)
	}
	return nil
}

// Decrease changes the size of the queue reducing delta.
// Uses a CAS loop to atomically decrement with clamping to zero.
func (r *Memory) Decrease(host string, delta int) error {
	val, ok := r.hosts.Load(host)
	if !ok {
		return nil
	}
	he := val.(*hostEntry)
	for {
		old := he.concurrency.Load()
		newVal := old - int64(delta)
		if newVal < 0 {
			newVal = 0
		}
		if he.concurrency.CompareAndSwap(old, newVal) {
			return nil
		}
	}
}

func (r *Memory) EnsureKey(host string, window, granularity time.Duration) {
	if _, ok := r.hosts.Load(host); ok {
		return
	}
	entry := &hostEntry{}
	entry.buckets.Store(NewRequestsBuckets(window, granularity))
	r.hosts.LoadOrStore(host, entry)
}

func (r *Memory) UpdateBuckets(host string, window, granularity time.Duration) {
	r.EnsureKey(host, window, granularity)
	val, ok := r.hosts.Load(host)
	if !ok {
		return
	}
	he := val.(*hostEntry)
	currentBuckets := he.buckets.Load()
	if currentBuckets != nil &&
		(currentBuckets.window != window ||
			currentBuckets.granularity != granularity) {
		he.buckets.Store(NewRequestsBuckets(window, granularity))
	}
}

func (r *Memory) RemoveKey(host string) bool {
	_, loaded := r.hosts.LoadAndDelete(host)
	return loaded
}

// Current returns the current size of the queue.
// Iterates over hosts using sync.Map.Range — no global lock required.
func (r *Memory) Current() (*Counts, error) {
	cts := NewCounts()
	now := time.Now()
	r.hosts.Range(func(key, value any) bool {
		host := key.(string)
		entry := value.(*hostEntry)
		buckets := entry.buckets.Load()
		var rps float64
		if buckets != nil {
			rps = buckets.WindowAverage(now)
		}
		cts.Counts[host] = Count{
			Concurrency: int(entry.concurrency.Load()),
			RPS:         rps,
		}
		return true
	})
	return cts, nil
}
