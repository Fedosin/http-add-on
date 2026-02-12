//! In-memory queue counter with per-host concurrency tracking (atomic) and
//! RPS calculation via a Knative-inspired ring-buffer.
//!
//! **Hot-path guarantees:**
//! - `increase` / `decrease` → one `DashMap` read + one atomic op.
//! - RPS recording → one `parking_lot::Mutex` lock *per host* (no global lock).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::Mutex;
use serde::Serialize;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Concurrent per-host request counter.
pub struct QueueCounter {
    entries: DashMap<String, HostEntry>,
}

/// RAII guard that decrements the counter when dropped (after the response
/// body has been fully streamed to the client).
pub struct QueueGuard {
    queue: Arc<QueueCounter>,
    key: String,
}

impl Drop for QueueGuard {
    fn drop(&mut self) {
        self.queue.decrease_inner(&self.key);
    }
}

impl QueueCounter {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Ensure an entry exists for `key` (idempotent).
    pub fn ensure_key(&self, key: &str) {
        self.entries
            .entry(key.to_string())
            .or_insert_with(HostEntry::new);
    }

    /// Remove entry.
    #[allow(dead_code)]
    pub fn remove_key(&self, key: &str) {
        self.entries.remove(key);
    }

    /// Remove all keys **not** present in the given set.
    pub fn retain_keys(&self, keys: &std::collections::HashSet<String>) {
        self.entries.retain(|k, _| keys.contains(k));
    }

    /// Create / replace the RPS ring-buffer for `key`.
    pub fn update_buckets(&self, key: &str, window: Duration, granularity: Duration) {
        if let Some(entry) = self.entries.get(key) {
            let mut buckets = entry.rps_buckets.lock();
            *buckets = Some(RpsBuckets::new(window, granularity));
        }
    }

    /// Atomically increment the concurrency counter for `key` and record an
    /// RPS data-point.  Returns a [`QueueGuard`] that decrements on drop.
    pub fn increase(self: &Arc<Self>, key: &str) -> QueueGuard {
        if let Some(entry) = self.entries.get(key) {
            entry.concurrency.fetch_add(1, Ordering::Relaxed);

            // Record RPS data-point
            let mut buckets = entry.rps_buckets.lock();
            if let Some(ref mut b) = *buckets {
                b.record(Instant::now(), 1.0);
            }
        }
        QueueGuard {
            queue: Arc::clone(self),
            key: key.to_string(),
        }
    }

    /// Snapshot of all per-host counts (called by `GET /queue`).
    pub fn current(&self) -> HashMap<String, QueueCount> {
        let now = Instant::now();
        let mut result = HashMap::with_capacity(self.entries.len());
        for entry in self.entries.iter() {
            let concurrency = entry.concurrency.load(Ordering::Relaxed);
            let rps = {
                let buckets = entry.rps_buckets.lock();
                buckets.as_ref().map_or(0.0, |b| b.window_average(now))
            };
            result.insert(
                entry.key().clone(),
                QueueCount { concurrency, rps },
            );
        }
        result
    }

    // -- internal ------------------------------------------------------------

    fn decrease_inner(&self, key: &str) {
        if let Some(entry) = self.entries.get(key) {
            // CAS loop so the counter never goes below zero.
            loop {
                let cur = entry.concurrency.load(Ordering::Relaxed);
                if cur <= 0 {
                    break;
                }
                if entry
                    .concurrency
                    .compare_exchange_weak(cur, cur - 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Wire format (matches the Go JSON contract)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct QueueCount {
    #[serde(rename = "Concurrency")]
    pub concurrency: i64,
    #[serde(rename = "RPS")]
    pub rps: f64,
}

// ---------------------------------------------------------------------------
// Per-host entry
// ---------------------------------------------------------------------------

struct HostEntry {
    concurrency: AtomicI64,
    rps_buckets: Mutex<Option<RpsBuckets>>,
}

impl HostEntry {
    fn new() -> Self {
        Self {
            concurrency: AtomicI64::new(0),
            rps_buckets: Mutex::new(None),
        }
    }
}

// ---------------------------------------------------------------------------
// RPS ring-buffer (Knative-inspired)
// ---------------------------------------------------------------------------

struct RpsBuckets {
    buckets: Vec<f64>,
    window: Duration,
    granularity: Duration,
    last_write_time: Instant,
    last_write_idx: usize,
}

impl RpsBuckets {
    fn new(window: Duration, granularity: Duration) -> Self {
        let n = (window.as_nanos() / granularity.as_nanos().max(1)) as usize;
        let n = n.max(1);
        Self {
            buckets: vec![0.0; n],
            window,
            granularity,
            last_write_time: Instant::now(),
            last_write_idx: 0,
        }
    }

    fn record(&mut self, now: Instant, delta: f64) {
        self.advance_to(now);
        let idx = self.bucket_index(now);
        self.buckets[idx] += delta;
        self.last_write_time = now;
        self.last_write_idx = idx;
    }

    fn window_average(&self, now: Instant) -> f64 {
        if let Some(window_start) = now.checked_sub(self.window) {
            if self.last_write_time < window_start {
                return 0.0; // all data is stale
            }
        }
        let total: f64 = self.buckets.iter().sum();
        let secs = self.window.as_secs_f64();
        if secs > 0.0 {
            total / secs
        } else {
            0.0
        }
    }

    // -- helpers -------------------------------------------------------------

    fn bucket_index(&self, now: Instant) -> usize {
        let elapsed = now.duration_since(self.last_write_time);
        let steps = (elapsed.as_nanos() / self.granularity.as_nanos().max(1)) as usize;
        (self.last_write_idx + steps) % self.buckets.len()
    }

    fn advance_to(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.last_write_time);
        let steps = (elapsed.as_nanos() / self.granularity.as_nanos().max(1)) as usize;
        if steps == 0 {
            return;
        }
        let clear_count = steps.min(self.buckets.len());
        for i in 1..=clear_count {
            let idx = (self.last_write_idx + i) % self.buckets.len();
            self.buckets[idx] = 0.0;
        }
    }
}
