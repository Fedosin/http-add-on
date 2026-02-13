//! Lightweight async TCP connection pool with DNS caching.
//!
//! Replaces hyper's `Client` + connection pool with a simple `DashMap`-based
//! pool of raw `TcpStream`s keyed by authority (`host:port`).
//!
//! **Why this is faster than hyper's pool:**
//!
//! * No channel dispatch per request (`Checkout::poll`, `Sender::try_send`)
//! * No `Pooled<T>` wrapper with custom `Drop` that locks the pool
//! * No per-connection background task (`Connection` future)
//! * Direct `DashMap::get_mut` + `Vec::pop` instead of `HashMap` + `Mutex`
//!
//! DNS results are cached with a configurable TTL (default 30 s), avoiding
//! repeated `getaddrinfo` syscalls for stable Kubernetes service names.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::net::TcpStream;

use crate::diagnostics::DiagCounters;

// ---------------------------------------------------------------------------
// Pool entry
// ---------------------------------------------------------------------------

struct PooledConn {
    stream: TcpStream,
    idle_since: Instant,
}

// ---------------------------------------------------------------------------
// DNS cache
// ---------------------------------------------------------------------------

struct CachedDns {
    addrs: Vec<SocketAddr>,
    inserted_at: Instant,
}

// ---------------------------------------------------------------------------
// Backend pool
// ---------------------------------------------------------------------------

pub struct BackendPool {
    idle: DashMap<String, Vec<PooledConn>>,
    dns_cache: DashMap<String, CachedDns>,
    dns_ttl: Duration,
    pool_idle_timeout: Duration,
    connect_timeout: Duration,
    max_idle_per_host: usize,
    counters: Arc<DiagCounters>,
}

impl BackendPool {
    pub fn new(
        dns_ttl: Duration,
        connect_timeout: Duration,
        max_idle_per_host: usize,
        counters: Arc<DiagCounters>,
    ) -> Self {
        Self {
            idle: DashMap::new(),
            dns_cache: DashMap::new(),
            dns_ttl,
            pool_idle_timeout: Duration::from_secs(90),
            connect_timeout,
            max_idle_per_host,
            counters,
        }
    }

    /// Get a TCP connection to `authority` (host:port).
    /// Reuses an idle connection from the pool if available, otherwise creates
    /// a new one.
    pub async fn checkout(&self, authority: &str) -> io::Result<TcpStream> {
        // Try reuse from pool (fast path)
        if let Some(mut conns) = self.idle.get_mut(authority) {
            while let Some(pooled) = conns.pop() {
                // Skip connections that have been idle too long
                if pooled.idle_since.elapsed() < self.pool_idle_timeout {
                    // Quick liveness check: peek for errors without reading.
                    // If the peer RST'd the connection while idle, readable()
                    // will succeed but try_read will get 0 or error.  We skip
                    // the peek for speed — hyper didn't do it either — and
                    // rely on the caller retrying on write/read failure.
                    return Ok(pooled.stream);
                }
                // Stale — drop it (TcpStream closes on drop)
            }
        }

        // Pool miss — create a new connection
        self.counters
            .connections_established
            .fetch_add(1, Ordering::Relaxed);
        self.connect(authority).await
    }

    /// Return a TCP connection to the pool for reuse.
    /// The connection is silently dropped if the pool for this authority is full.
    pub fn checkin(&self, authority: &str, stream: TcpStream) {
        let mut conns = self.idle.entry(authority.to_string()).or_default();
        if conns.len() < self.max_idle_per_host {
            conns.push(PooledConn {
                stream,
                idle_since: Instant::now(),
            });
        }
        // else: pool full, drop the connection
    }

    // -----------------------------------------------------------------------
    // DNS + TCP connect
    // -----------------------------------------------------------------------

    async fn connect(&self, authority: &str) -> io::Result<TcpStream> {
        let (host, port) = parse_authority(authority);
        let addrs = self.resolve(host, port).await?;

        if addrs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "DNS resolved to zero addresses",
            ));
        }

        let stream = tokio::time::timeout(
            self.connect_timeout,
            TcpStream::connect(&addrs[..]),
        )
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("connect timeout after {:?}", self.connect_timeout),
            )
        })??;

        stream.set_nodelay(true)?;
        Ok(stream)
    }

    async fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        let key = format!("{}:{}", host, port);

        // Fast path: cache hit
        if let Some(entry) = self.dns_cache.get(&key) {
            if entry.inserted_at.elapsed() < self.dns_ttl {
                self.counters.dns_cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.addrs.clone());
            }
        }

        // Slow path: OS resolve
        self.counters
            .dns_cache_misses
            .fetch_add(1, Ordering::Relaxed);
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();

        if !addrs.is_empty() {
            self.dns_cache.insert(
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_authority(authority: &str) -> (&str, u16) {
    // Handle IPv6: [::1]:8080
    if let Some(bracket_end) = authority.find(']') {
        let host = &authority[..=bracket_end];
        let port = authority[bracket_end + 1..]
            .strip_prefix(':')
            .and_then(|p| p.parse().ok())
            .unwrap_or(80);
        return (host, port);
    }

    match authority.rsplit_once(':') {
        Some((host, port_str)) => {
            let port = port_str.parse().unwrap_or(80);
            (host, port)
        }
        None => (authority, 80),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_authority() {
        assert_eq!(parse_authority("example.com:8080"), ("example.com", 8080));
        assert_eq!(parse_authority("example.com"), ("example.com", 80));
        assert_eq!(parse_authority("svc.ns:80"), ("svc.ns", 80));
        assert_eq!(parse_authority("[::1]:8080"), ("[::1]", 8080));
    }
}
