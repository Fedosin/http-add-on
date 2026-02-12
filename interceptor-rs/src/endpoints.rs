//! EndpointSlice cache.
//!
//! Maintains a derived `DashMap<service_key, ready_count>` for O(1) hot-path
//! lookups, plus a `broadcast` channel so the cold-start wait function can
//! block until a service becomes ready.

use std::time::Duration;

use dashmap::DashMap;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::runtime::reflector::Store;
use tokio::sync::broadcast;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub struct EndpointsCache {
    /// `"namespace/service"` → total number of ready endpoint addresses.
    ready_counts: DashMap<String, usize>,
    /// Fires the service key whenever counts change.
    notify_tx: broadcast::Sender<String>,
}

impl EndpointsCache {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(4096);
        Self {
            ready_counts: DashMap::new(),
            notify_tx: tx,
        }
    }

    /// Fast hot-path check: does the service have at least one ready endpoint?
    #[inline]
    pub fn has_ready_endpoints(&self, namespace: &str, service: &str) -> bool {
        let key = service_key(namespace, service);
        self.ready_counts
            .get(&key)
            .is_some_and(|count| *count > 0)
    }

    /// Wait until the service has at least one ready endpoint, or `timeout`
    /// elapses.  Returns `Ok(false)` for warm backends (already ready),
    /// `Ok(true)` when a cold-start was detected but the backend is now ready,
    /// and `Err(())` on timeout.
    pub async fn wait_for_ready(
        &self,
        namespace: &str,
        service: &str,
        timeout: Duration,
    ) -> Result<bool, ()> {
        let key = service_key(namespace, service);

        // ---- fast path (warm backend) ----
        if self.has_ready_endpoints(namespace, service) {
            return Ok(false);
        }

        let mut rx = self.notify_tx.subscribe();

        // Re-check after subscribing to close the race window.
        if self.has_ready_endpoints(namespace, service) {
            return Ok(true);
        }

        tracing::debug!(
            namespace = namespace,
            service = service,
            "cold-start: waiting for ready endpoints",
        );

        // ---- slow path (cold start) ----
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Ok(ref changed)) if changed == &key => {
                    if self.has_ready_endpoints(namespace, service) {
                        tracing::info!(
                            namespace = namespace,
                            service = service,
                            "cold-start: endpoints became ready",
                        );
                        return Ok(true); // cold-start resolved
                    }
                }
                Ok(Ok(_)) => continue, // different service
                Ok(Err(broadcast::error::RecvError::Lagged(n))) => {
                    // Receiver fell behind — some notifications were dropped.
                    // Re-check the current state; the service may have become
                    // ready while we were lagging.
                    tracing::debug!(
                        skipped = n,
                        namespace = namespace,
                        service = service,
                        "cold-start: broadcast receiver lagged, re-checking",
                    );
                    if self.has_ready_endpoints(namespace, service) {
                        tracing::info!(
                            namespace = namespace,
                            service = service,
                            "cold-start: endpoints became ready (detected after lag)",
                        );
                        return Ok(true);
                    }
                    continue;
                }
                Ok(Err(_)) => {
                    // Channel closed — sender dropped (shutdown).
                    tracing::warn!(
                        namespace = namespace,
                        service = service,
                        "cold-start: broadcast channel closed",
                    );
                    return Err(());
                }
                Err(_) => {
                    // Timeout — condition_wait_timeout elapsed.
                    tracing::warn!(
                        namespace = namespace,
                        service = service,
                        timeout_secs = timeout.as_secs(),
                        "cold-start: timed out waiting for ready endpoints",
                    );
                    return Err(());
                }
            }
        }
    }

    /// Recount ready endpoints for the service that `changed_slice` belongs to,
    /// reading the authoritative state from the reflector `store`.
    pub fn refresh_service(
        &self,
        store: &Store<EndpointSlice>,
        changed_slice: &EndpointSlice,
    ) {
        let (namespace, svc_name) = match extract_service(changed_slice) {
            Some(pair) => pair,
            None => return,
        };

        let key = service_key(&namespace, &svc_name);

        // Recount across *all* slices for this service in the reflector store.
        let mut total: usize = 0;
        for slice in store.state() {
            if let Some((ns, svc)) = extract_service(&slice) {
                if ns == namespace && svc == svc_name {
                    total += count_ready(&slice);
                }
            }
        }

        self.ready_counts.insert(key.clone(), total);
        // Best-effort notify; OK to lose if buffer is full.
        let _ = self.notify_tx.send(key);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn service_key(namespace: &str, service: &str) -> String {
    format!("{namespace}/{service}")
}

fn extract_service(slice: &EndpointSlice) -> Option<(String, String)> {
    let ns = slice
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".into());
    let svc = slice
        .metadata
        .labels
        .as_ref()?
        .get("kubernetes.io/service-name")?
        .clone();
    Some((ns, svc))
}

fn count_ready(slice: &EndpointSlice) -> usize {
    let mut total: usize = 0;
    for ep in &slice.endpoints {
        let conditions = ep.conditions.as_ref();
        let is_ready = conditions.and_then(|c| c.ready).unwrap_or(true);
        // Exclude terminating endpoints — they still report ready=true
        // briefly during the grace period, but kube-proxy may already be
        // draining them.
        let is_terminating = conditions
            .and_then(|c| c.terminating)
            .unwrap_or(false);
        if is_ready && !is_terminating {
            total += ep.addresses.len();
        }
    }
    total
}
