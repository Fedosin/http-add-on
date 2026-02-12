//! Lightweight diagnostics and optimised backend connector.
//!
//! ## `CachingConnector`
//!
//! Replaces the default `HttpConnector` + `GaiResolver` with:
//!
//! * **DNS caching** — avoids repeated blocking `getaddrinfo` syscalls for the
//!   same Kubernetes service.  Results are cached for a configurable TTL
//!   (default 30 s).  In-cluster service DNS is stable, so this is safe.
//! * **Pool-miss counting** — `connections_established` increments each time
//!   the hyper connection pool calls the connector (= a new TCP connection).
//! * **Direct `TcpStream` setup** — sets `TCP_NODELAY` immediately.
//!
//! ## `DiagCounters`
//!
//! Zero-overhead atomic counters exposed via `GET /debug/stats` on the admin
//! server.  The most important metric is `connection_reuse_pct` — if it's
//! close to 0 %, the pool isn't helping and most CPU is wasted on DNS + TCP
//! handshakes.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use http::Uri;
use hyper_util::rt::TokioIo;
use serde::Serialize;
use tokio::net::TcpStream;
use tower_service::Service;

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
    /// Times the connector was called — i.e. a *new* TCP connection was
    /// established (not reused from the pool).
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

// ---------------------------------------------------------------------------
// DNS cache entry
// ---------------------------------------------------------------------------

struct CachedDns {
    addrs: Vec<SocketAddr>,
    inserted_at: Instant,
}

// ---------------------------------------------------------------------------
// Caching connector
// ---------------------------------------------------------------------------

/// Optimised connector that replaces `HttpConnector` with:
/// 1. **DNS caching** — `DashMap` keyed by `host:port`, TTL-based expiry.
/// 2. **Pool-miss counting** — increments `DiagCounters::connections_established`.
/// 3. **Direct TCP connect** — `TcpStream::connect` with timeout and `TCP_NODELAY`.
///
/// The hyper connection pool only calls the connector for pool misses, so each
/// `call()` invocation = one brand-new TCP connection.
#[derive(Clone)]
pub struct CachingConnector {
    dns_cache: Arc<DashMap<String, CachedDns>>,
    dns_ttl: Duration,
    connect_timeout: Duration,
    nodelay: bool,
    counters: Arc<DiagCounters>,
}

impl CachingConnector {
    pub fn new(
        dns_ttl: Duration,
        connect_timeout: Duration,
        nodelay: bool,
        counters: Arc<DiagCounters>,
    ) -> Self {
        Self {
            dns_cache: Arc::new(DashMap::new()),
            dns_ttl,
            connect_timeout,
            nodelay,
            counters,
        }
    }

    /// Resolve `host:port` with caching.  Cache hits are zero-cost (a single
    /// `DashMap` read).  Misses fall back to `tokio::net::lookup_host` which
    /// calls `getaddrinfo` on the blocking thread-pool.
    async fn resolve(
        dns_cache: &DashMap<String, CachedDns>,
        dns_ttl: Duration,
        counters: &DiagCounters,
        host: &str,
        port: u16,
    ) -> std::io::Result<Vec<SocketAddr>> {
        let key = format!("{}:{}", host, port);

        // Fast path: cache hit
        if let Some(entry) = dns_cache.get(&key) {
            if entry.inserted_at.elapsed() < dns_ttl {
                counters.dns_cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.addrs.clone());
            }
        }

        // Slow path: resolve via OS (blocking, on tokio thread-pool)
        counters.dns_cache_misses.fetch_add(1, Ordering::Relaxed);
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();

        if !addrs.is_empty() {
            dns_cache.insert(
                key,
                CachedDns {
                    addrs: addrs.clone(),
                    inserted_at: Instant::now(),
                },
            );
        }

        Ok(addrs)
    }
}

impl Service<Uri> for CachingConnector {
    type Response = TokioIo<TcpStream>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        self.counters
            .connections_established
            .fetch_add(1, Ordering::Relaxed);

        let dns_cache = self.dns_cache.clone();
        let dns_ttl = self.dns_ttl;
        let connect_timeout = self.connect_timeout;
        let nodelay = self.nodelay;
        let counters = self.counters.clone();

        Box::pin(async move {
            let host = uri.host().ok_or("URI missing host")?;
            let port = uri.port_u16().unwrap_or(80);

            let addrs = Self::resolve(&dns_cache, dns_ttl, &counters, host, port).await?;
            if addrs.is_empty() {
                return Err("DNS resolved to zero addresses".into());
            }

            let stream = tokio::time::timeout(
                connect_timeout,
                TcpStream::connect(&addrs[..]),
            )
            .await
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                format!("connect timeout after {connect_timeout:?}").into()
            })?
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

            stream.set_nodelay(nodelay)?;

            Ok(TokioIo::new(stream))
        })
    }
}
