//! Lightweight diagnostics counters.
//!
//! ## `DiagCounters`
//!
//! Zero-overhead atomic counters exposed via `GET /debug/stats` on the admin
//! server.  The most important metric is `connection_reuse_pct` — if it's
//! close to 0 %, the pool isn't helping and most CPU is wasted on DNS + TCP
//! handshakes.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

// ---------------------------------------------------------------------------
// Atomic counters
// ---------------------------------------------------------------------------

/// Zero-overhead atomic counters — one relaxed `fetch_add` per event.
#[derive(Default)]
pub struct DiagCounters {
    /// Total proxy requests that entered `handle_proxy_request`.
    pub requests_total: AtomicU64,
    /// Requests that went through the cold-start path.
    pub requests_cold_start: AtomicU64,
    /// Times the backend pool created a *new* TCP connection (not reused
    /// from the pool).
    pub connections_established: AtomicU64,
    /// DNS cache hits (resolved from in-memory cache).
    pub dns_cache_hits: AtomicU64,
    /// DNS cache misses (had to call `getaddrinfo`).
    pub dns_cache_misses: AtomicU64,
    /// Requests that got a 404 (no matching route).
    pub requests_no_route: AtomicU64,
    /// Requests where the backend returned an error or timed out.
    pub requests_backend_error: AtomicU64,
}

impl DiagCounters {
    /// Produce a JSON-serialisable snapshot.
    pub fn snapshot(&self) -> DiagSnapshot {
        let requests = self.requests_total.load(Ordering::Relaxed);
        let connections = self.connections_established.load(Ordering::Relaxed);
        DiagSnapshot {
            requests_total: requests,
            requests_cold_start: self.requests_cold_start.load(Ordering::Relaxed),
            connections_established: connections,
            dns_cache_hits: self.dns_cache_hits.load(Ordering::Relaxed),
            dns_cache_misses: self.dns_cache_misses.load(Ordering::Relaxed),
            requests_no_route: self.requests_no_route.load(Ordering::Relaxed),
            requests_backend_error: self.requests_backend_error.load(Ordering::Relaxed),
            connection_reuse_pct: if requests > 0 {
                100.0 * (1.0 - connections as f64 / requests as f64)
            } else {
                0.0
            },
        }
    }
}

/// JSON snapshot returned by `GET /debug/stats`.
#[derive(Serialize)]
pub struct DiagSnapshot {
    pub requests_total: u64,
    pub requests_cold_start: u64,
    pub connections_established: u64,
    pub dns_cache_hits: u64,
    pub dns_cache_misses: u64,
    pub requests_no_route: u64,
    pub requests_backend_error: u64,
    /// Percentage of requests that reused an existing pooled connection.
    /// 0 % → every request opens a fresh TCP connection (pool not helping).
    /// ~100 % → excellent reuse (pool working well).
    pub connection_reuse_pct: f64,
}
