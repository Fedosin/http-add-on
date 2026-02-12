//! Lock-free routing table.
//!
//! Reads are wait-free (a single `arc_swap::ArcSwap::load`).  The table is
//! rebuilt atomically on every `HTTPScaledObject` change.  The matching
//! algorithm mirrors the Go implementation: exact host → wildcard hosts →
//! catch-all `*`, each combined with longest-path-prefix and header filtering.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use http::HeaderMap;

use crate::config;
use crate::crd::HTTPScaledObject;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Thread-safe routing table with lock-free reads via `ArcSwap`.
pub struct RoutingTable {
    memory: ArcSwap<TableMemory>,
    synced: AtomicBool,
}

/// Information about a matched route – returned from the hot-path lookup.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct RouteInfo {
    pub queue_key: String,
    pub httpso_key: String,
    pub target_url: String,
    pub host: String,
    pub condition_wait_timeout: Option<Duration>,
    pub response_header_timeout: Option<Duration>,
    pub failover_url: Option<String>,
    pub failover_timeout: Option<Duration>,
    pub target_namespace: String,
    pub target_service: String,
    pub target_port: u16,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self {
            memory: ArcSwap::new(Arc::new(TableMemory::default())),
            synced: AtomicBool::new(false),
        }
    }

    /// Hot-path route lookup — lock-free.
    #[inline]
    pub fn route(&self, host: &str, path: &str, headers: &HeaderMap) -> Option<RouteInfo> {
        let table = self.memory.load();
        table.lookup(host, path, headers)
    }

    /// Atomically swap in a new routing table built from the given CRDs.
    pub fn rebuild(&self, objects: &[Arc<HTTPScaledObject>]) {
        let new_memory = TableMemory::build(objects);
        self.memory.store(Arc::new(new_memory));
        self.synced.store(true, Ordering::Release);
    }

    /// `true` once the table has been built at least once.
    pub fn has_synced(&self) -> bool {
        self.synced.load(Ordering::Acquire)
    }
}

// ---------------------------------------------------------------------------
// Internal: immutable snapshot
// ---------------------------------------------------------------------------

/// Immutable snapshot of all routes, swapped atomically.
#[derive(Default)]
struct TableMemory {
    /// `hostname → Vec<RouteEntry>` sorted by
    /// (path_prefix length **desc**, header-matcher count **desc**).
    routes: HashMap<String, Vec<RouteEntry>>,
}

#[derive(Clone, Debug)]
struct RouteEntry {
    path_prefix: String,
    header_matchers: Vec<HeaderMatcher>,
    queue_key: String,
    httpso_key: String,
    target_url: String,
    condition_wait_timeout: Option<Duration>,
    response_header_timeout: Option<Duration>,
    failover_url: Option<String>,
    failover_timeout: Option<Duration>,
    target_namespace: String,
    target_service: String,
    target_port: u16,
}

#[derive(Clone, Debug)]
struct HeaderMatcher {
    name: String,
    value: Option<String>,
}

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

impl TableMemory {
    fn build(objects: &[Arc<HTTPScaledObject>]) -> Self {
        let mut routes: HashMap<String, Vec<RouteEntry>> = HashMap::new();

        for httpso in objects {
            let namespace = httpso
                .metadata
                .namespace
                .as_deref()
                .unwrap_or("default");
            let name = httpso
                .metadata
                .name
                .as_deref()
                .unwrap_or("unknown");
            let spec = &httpso.spec;

            // -- resolve port ------------------------------------------------
            let port = spec.scale_target_ref.port.unwrap_or(80);
            let target_url = format!(
                "http://{}.{}:{}",
                spec.scale_target_ref.service, namespace, port
            );

            // -- failover ----------------------------------------------------
            let failover_url = spec
                .cold_start_timeout_failover_ref
                .as_ref()
                .map(|f| format!("http://{}.{}:{}", f.service, namespace, f.port));
            let failover_timeout = spec
                .cold_start_timeout_failover_ref
                .as_ref()
                .map(|f| Duration::from_secs(f.timeout_seconds.max(0) as u64));

            // -- timeouts ----------------------------------------------------
            let condition_wait_timeout = spec
                .timeouts
                .as_ref()
                .and_then(|t| t.condition_wait.as_ref())
                .and_then(|s| config::parse_go_duration(s));
            let response_header_timeout = spec
                .timeouts
                .as_ref()
                .and_then(|t| t.response_header.as_ref())
                .and_then(|s| config::parse_go_duration(s));

            // -- header matchers (lowercased for case-insensitive matching) --
            let header_matchers: Vec<HeaderMatcher> = spec
                .headers
                .iter()
                .map(|h| HeaderMatcher {
                    name: h.name.to_lowercase(),
                    value: h.value.clone(),
                })
                .collect();

            // -- hosts / path-prefixes (defaults) ----------------------------
            let hosts = if spec.hosts.is_empty() {
                vec!["*".to_string()]
            } else {
                spec.hosts.clone()
            };
            let path_prefixes = if spec.path_prefixes.is_empty() {
                vec!["/".to_string()]
            } else {
                spec.path_prefixes.clone()
            };

            let httpso_key = format!("{}/{}", namespace, name);

            for host in &hosts {
                for prefix in &path_prefixes {
                    let entry = RouteEntry {
                        path_prefix: normalize_path(prefix),
                        header_matchers: header_matchers.clone(),
                        queue_key: httpso_key.clone(),
                        httpso_key: httpso_key.clone(),
                        target_url: target_url.clone(),
                        condition_wait_timeout,
                        response_header_timeout,
                        failover_url: failover_url.clone(),
                        failover_timeout,
                        target_namespace: namespace.to_string(),
                        target_service: spec.scale_target_ref.service.clone(),
                        target_port: port as u16,
                    };
                    routes.entry(host.clone()).or_default().push(entry);
                }
            }
        }

        // Sort each host's entries: longest prefix first, then most headers.
        for entries in routes.values_mut() {
            entries.sort_by(|a, b| {
                b.path_prefix
                    .len()
                    .cmp(&a.path_prefix.len())
                    .then_with(|| {
                        b.header_matchers
                            .len()
                            .cmp(&a.header_matchers.len())
                    })
            });
        }

        Self { routes }
    }
}

// ---------------------------------------------------------------------------
// Lookup
// ---------------------------------------------------------------------------

impl TableMemory {
    fn lookup(&self, host: &str, path: &str, headers: &HeaderMap) -> Option<RouteInfo> {
        let host_stripped = strip_port(host);
        let path = if path.is_empty() { "/" } else { path };

        // 1. Exact hostname
        if let Some(info) = self.try_match(host_stripped, path, headers) {
            return Some(info);
        }

        // 2. Wildcard hostnames: *.example.com → *.com
        let parts: Vec<&str> = host_stripped.split('.').collect();
        for i in 1..parts.len() {
            let wildcard = format!("*.{}", parts[i..].join("."));
            if let Some(info) = self.try_match(&wildcard, path, headers) {
                return Some(info);
            }
        }

        // 3. Catch-all
        self.try_match("*", path, headers)
    }

    fn try_match(
        &self,
        hostname: &str,
        path: &str,
        headers: &HeaderMap,
    ) -> Option<RouteInfo> {
        let entries = self.routes.get(hostname)?;
        // Entries are pre-sorted; the first matching entry wins.
        for entry in entries {
            if !path.starts_with(&entry.path_prefix) {
                continue;
            }
            if !headers_match(&entry.header_matchers, headers) {
                continue;
            }
            return Some(RouteInfo {
                queue_key: entry.queue_key.clone(),
                httpso_key: entry.httpso_key.clone(),
                target_url: entry.target_url.clone(),
                host: hostname.to_string(),
                condition_wait_timeout: entry.condition_wait_timeout,
                response_header_timeout: entry.response_header_timeout,
                failover_url: entry.failover_url.clone(),
                failover_timeout: entry.failover_timeout,
                target_namespace: entry.target_namespace.clone(),
                target_service: entry.target_service.clone(),
                target_port: entry.target_port,
            });
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn headers_match(matchers: &[HeaderMatcher], headers: &HeaderMap) -> bool {
    matchers.iter().all(|m| {
        headers.get(&m.name).is_some_and(|val| match &m.value {
            Some(expected) => val.to_str().unwrap_or("") == expected.as_str(),
            None => true, // presence-only check
        })
    })
}

fn normalize_path(prefix: &str) -> String {
    if prefix.is_empty() {
        "/".to_string()
    } else if !prefix.starts_with('/') {
        format!("/{prefix}")
    } else {
        prefix.to_string()
    }
}

fn strip_port(host: &str) -> &str {
    // Preserve IPv6 addresses like [::1]:8080
    if host.starts_with('[') {
        return host;
    }
    host.split(':').next().unwrap_or(host)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_port_works() {
        assert_eq!(strip_port("example.com:8080"), "example.com");
        assert_eq!(strip_port("example.com"), "example.com");
        assert_eq!(strip_port("[::1]:8080"), "[::1]:8080");
    }

    #[test]
    fn normalize_path_works() {
        assert_eq!(normalize_path(""), "/");
        assert_eq!(normalize_path("api"), "/api");
        assert_eq!(normalize_path("/api"), "/api");
    }
}
