package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCurrent(t *testing.T) {
	r := require.New(t)
	memory := NewMemory()
	host := "host1"
	memory.EnsureKey(host, time.Minute, time.Second)
	err := memory.Increase(host, 1)
	r.NoError(err)
	current, err := memory.Current()
	r.NoError(err)
	r.Equal(1, current.Counts[host].Concurrency)
	r.Greater(current.Counts[host].RPS, 0.0)

	err = memory.Increase(host, 1)
	r.NoError(err)
	err = memory.Increase(host, 1)
	r.NoError(err)

	// The earlier snapshot should differ from the current live state
	newCurrent, err := memory.Current()
	r.NoError(err)
	r.Equal(3, newCurrent.Counts[host].Concurrency)
	r.NotEqual(current.Counts[host].Concurrency, newCurrent.Counts[host].Concurrency)
}

func TestIncreaseDecrease(t *testing.T) {
	r := require.New(t)
	memory := NewMemory()
	host := "host1"
	memory.EnsureKey(host, time.Minute, time.Second)

	// Increase by 5
	r.NoError(memory.Increase(host, 5))
	counts, err := memory.Current()
	r.NoError(err)
	r.Equal(5, counts.Counts[host].Concurrency)

	// Decrease by 3
	r.NoError(memory.Decrease(host, 3))
	counts, err = memory.Current()
	r.NoError(err)
	r.Equal(2, counts.Counts[host].Concurrency)

	// Decrease by more than current value should clamp to 0
	r.NoError(memory.Decrease(host, 10))
	counts, err = memory.Current()
	r.NoError(err)
	r.Equal(0, counts.Counts[host].Concurrency)
}

func TestIncreaseUnknownHost(t *testing.T) {
	r := require.New(t)
	memory := NewMemory()
	// Increase on an unknown host should not error
	r.NoError(memory.Increase("unknown", 1))
}

func TestDecreaseUnknownHost(t *testing.T) {
	r := require.New(t)
	memory := NewMemory()
	// Decrease on an unknown host should not error
	r.NoError(memory.Decrease("unknown", 1))
}

func TestEnsureKeyIdempotent(t *testing.T) {
	r := require.New(t)
	memory := NewMemory()
	host := "host1"

	memory.EnsureKey(host, time.Minute, time.Second)
	r.NoError(memory.Increase(host, 3))

	// Calling EnsureKey again should not reset the counter
	memory.EnsureKey(host, time.Minute, time.Second)
	counts, err := memory.Current()
	r.NoError(err)
	r.Equal(3, counts.Counts[host].Concurrency)
}

func TestRemoveKey(t *testing.T) {
	r := require.New(t)
	memory := NewMemory()
	host := "host1"
	memory.EnsureKey(host, time.Minute, time.Second)

	r.True(memory.RemoveKey(host))
	r.False(memory.RemoveKey(host)) // already removed

	counts, err := memory.Current()
	r.NoError(err)
	_, exists := counts.Counts[host]
	r.False(exists)
}

func TestUpdateBuckets(t *testing.T) {
	r := require.New(t)
	memory := NewMemory()
	host := "host1"

	// Create with initial window
	memory.EnsureKey(host, time.Minute, time.Second)
	r.NoError(memory.Increase(host, 1))

	// Update to different window — should replace the buckets
	memory.UpdateBuckets(host, 2*time.Minute, 2*time.Second)
	// RPS should be reset because the buckets were replaced
	counts, err := memory.Current()
	r.NoError(err)
	r.Equal(0.0, counts.Counts[host].RPS)
	// But concurrency should remain unchanged
	r.Equal(1, counts.Counts[host].Concurrency)
}
