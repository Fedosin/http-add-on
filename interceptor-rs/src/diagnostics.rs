//! Lightweight diagnostic counters for profiling connection behaviour.
//!
//! These counters are **not** meant for production metrics (use Prometheus for
//! that).  They exist to answer one critical question during performance
//! analysis: *are backend connections being reused, or is the pool churning?*
//!
//! ## Usage
//!
//! Hit the admin server while the load test is running:
//!
//! ```sh
//! curl -s http://localhost:9090/debug/stats | python3 -m json.tool
//! ```
//!
//! Key number: `connection_reuse_pct`.  If it's close to 0% the pool isn't
//! helping and most CPU is wasted on DNS + TCP connect + TLS (if any).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use http::Uri;
use serde::Serialize;
use tower_service::Service;

// ---------------------------------------------------------------------------
// Atomic counters
// ---------------------------------------------------------------------------

/// Zero-overhead atomic counters — one relaxed fetch_add per event.
#[derive(Default)]
pub struct DiagCounters {
    /// Total proxy requests that entered `handle_proxy_request`.
    pub requests_total: AtomicU64,
    /// Requests that went through the cold-start path.
    pub requests_cold_start: AtomicU64,
    /// Times `HttpConnector::call()` was invoked — i.e. a *new* TCP connection
    /// was established (not reused from the pool).
    pub connections_established: AtomicU64,
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
    pub requests_no_route: u64,
    pub requests_backend_error: u64,
    /// Percentage of requests that reused an existing pooled connection.
    /// 0% → every request opens a fresh TCP connection (pool not helping).
    /// ~100% → excellent reuse (pool working well).
    pub connection_reuse_pct: f64,
}

// ---------------------------------------------------------------------------
// Counting connector wrapper
// ---------------------------------------------------------------------------

/// Transparent `tower::Service<Uri>` wrapper that increments
/// `connections_established` every time a new connection is opened.
///
/// Because `hyper_util::client::legacy::Client` only calls the connector when
/// the pool cannot satisfy the request, this counter directly measures pool
/// misses.
#[derive(Clone)]
pub struct CountingConnector<C> {
    inner: C,
    counters: Arc<DiagCounters>,
}

impl<C> CountingConnector<C> {
    pub fn new(inner: C, counters: Arc<DiagCounters>) -> Self {
        Self { inner, counters }
    }
}

impl<C, R> Service<Uri> for CountingConnector<C>
where
    C: Service<Uri, Response = R>,
{
    type Response = R;
    type Error = C::Error;
    type Future = C::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        self.counters
            .connections_established
            .fetch_add(1, Ordering::Relaxed);
        self.inner.call(req)
    }
}
