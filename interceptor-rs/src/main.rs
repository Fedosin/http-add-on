//! KEDA HTTP Add-on Interceptor — Rust reimplementation.
//!
//! A high-performance HTTP reverse proxy that:
//! 1. Matches incoming requests to `HTTPScaledObject` routing rules.
//! 2. Tracks per-host concurrency & RPS for KEDA autoscaling (including
//!    scale-to-zero and cold-start).
//! 3. Exposes Prometheus metrics.
//!
//! Target: ≥ 50 k RPS on 16 CPU cores.

// Use jemalloc for reduced contention under heavy concurrency.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::runtime::{reflector, watcher, WatchStreamExt};
use kube::{Api, Client};
use tracing_subscriber::EnvFilter;

mod admin;
mod backend_pool;
mod config;
mod crd;
mod diagnostics;
mod endpoints;
mod metrics;
mod proxy;
mod queue;
mod routing;

use crd::HTTPScaledObject;

// ===========================================================================
// Entry point
// ===========================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Log the effective RUST_LOG so we know what tracing overhead to expect.
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "(unset → default: info)".into());
    tracing::info!(RUST_LOG = %rust_log, "Tracing filter active");

    let cfg = config::Config::from_env()?;
    tracing::info!(
        proxy_port = cfg.proxy_port,
        admin_port = cfg.admin_port,
        metrics_port = cfg.metrics_port,
        "Starting interceptor-rs",
    );

    let kube_client = Client::try_default().await?;

    // -- subsystems ----------------------------------------------------------
    let queue_counter = Arc::new(queue::QueueCounter::new());
    let routing_table = Arc::new(routing::RoutingTable::new());
    let endpoints_cache = Arc::new(endpoints::EndpointsCache::new());
    let metrics_collector = Arc::new(metrics::MetricsCollector::new()?);
    let diag_counters = Arc::new(diagnostics::DiagCounters::default());

    let state = Arc::new(proxy::AppState::new(
        cfg.clone(),
        routing_table.clone(),
        queue_counter.clone(),
        endpoints_cache.clone(),
        metrics_collector.clone(),
        diag_counters,
    ));

    // -- background tasks ----------------------------------------------------
    let httpso_watcher = tokio::spawn(run_httpso_watcher(
        kube_client.clone(),
        routing_table.clone(),
        queue_counter.clone(),
        cfg.watch_namespace.clone(),
    ));
    let eps_watcher = tokio::spawn(run_endpointslice_watcher(
        kube_client.clone(),
        endpoints_cache.clone(),
    ));
    let proxy_server = tokio::spawn(proxy::serve(state.clone()));
    let admin_server = tokio::spawn(admin::serve(state.clone()));
    let metrics_server = tokio::spawn(metrics::serve(state.clone()));

    // -- wait for shutdown or fatal error ------------------------------------
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received shutdown signal");
        }
        res = httpso_watcher => {
            tracing::error!("HTTPScaledObject watcher exited: {res:?}");
        }
        res = eps_watcher => {
            tracing::error!("EndpointSlice watcher exited: {res:?}");
        }
        res = proxy_server => {
            tracing::error!("Proxy server exited: {res:?}");
        }
        res = admin_server => {
            tracing::error!("Admin server exited: {res:?}");
        }
        res = metrics_server => {
            tracing::error!("Metrics server exited: {res:?}");
        }
    }

    tracing::info!("Shutting down");
    Ok(())
}

// ===========================================================================
// HTTPScaledObject watcher → routing table + queue sync
// ===========================================================================

async fn run_httpso_watcher(
    client: Client,
    routing_table: Arc<routing::RoutingTable>,
    queue: Arc<queue::QueueCounter>,
    namespace: Option<String>,
) -> Result<()> {
    let api: Api<HTTPScaledObject> = match namespace {
        Some(ref ns) if !ns.is_empty() => Api::namespaced(client, ns),
        _ => Api::all(client),
    };

    let store_writer = reflector::store::Writer::default();
    let store_reader = store_writer.as_reader();

    // Use the raw event stream (not `touched_objects()`) so we can observe
    // `InitDone` — the signal that the initial list has completed.  Without
    // this the routing table would never be marked as synced when there are
    // zero HTTPScaledObjects, and the readiness probe would return 503 forever.
    let mut stream = watcher(api, watcher::Config::default())
        .default_backoff()
        .reflect(store_writer)
        .boxed();

    tracing::info!("HTTPScaledObject watcher started");

    while let Some(result) = stream.next().await {
        match result {
            Ok(event) => {
                let should_rebuild = matches!(
                    event,
                    watcher::Event::Apply(_)
                        | watcher::Event::Delete(_)
                        | watcher::Event::InitDone
                );
                if should_rebuild {
                    let objects = store_reader.state();
                    routing_table.rebuild(&objects);
                    sync_queue_keys(&queue, &objects);
                    tracing::debug!(count = objects.len(), "Routing table rebuilt");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "HTTPScaledObject watch error");
            }
        }
    }

    anyhow::bail!("HTTPScaledObject watcher stream ended unexpectedly")
}

/// Make sure the queue counter has exactly the keys that the current set of
/// `HTTPScaledObject`s requires, and that RPS buckets are configured.
fn sync_queue_keys(queue: &queue::QueueCounter, objects: &[Arc<HTTPScaledObject>]) {
    let mut new_keys = HashSet::new();

    for httpso in objects {
        let namespace = httpso.metadata.namespace.as_deref().unwrap_or("default");
        let name = httpso.metadata.name.as_deref().unwrap_or("unknown");

        let key = format!("{namespace}/{name}");
        queue.ensure_key(&key);
        new_keys.insert(key.clone());

        // Configure RPS buckets if a requestRate metric is defined.
        if let Some(ref sm) = httpso.spec.scaling_metric {
            if let Some(ref rr) = sm.request_rate {
                let window = config::parse_go_duration(&rr.window)
                    .unwrap_or(Duration::from_secs(60));
                let granularity = config::parse_go_duration(&rr.granularity)
                    .unwrap_or(Duration::from_secs(1));
                queue.update_buckets(&key, window, granularity);
            }
        }
    }

    queue.retain_keys(&new_keys);
}

// ===========================================================================
// EndpointSlice watcher → endpoints cache
// ===========================================================================

async fn run_endpointslice_watcher(
    client: Client,
    cache: Arc<endpoints::EndpointsCache>,
) -> Result<()> {
    let api: Api<EndpointSlice> = Api::all(client);

    let store_writer = reflector::store::Writer::<EndpointSlice>::default();
    let store_reader = store_writer.as_reader();

    let mut stream = watcher(api, watcher::Config::default())
        .default_backoff()
        .reflect(store_writer)
        .touched_objects()
        .boxed();

    tracing::info!("EndpointSlice watcher started");

    while let Some(result) = stream.next().await {
        match result {
            Ok(ref slice) => {
                cache.refresh_service(&store_reader, slice);
            }
            Err(e) => {
                tracing::error!(error = %e, "EndpointSlice watch error");
            }
        }
    }

    anyhow::bail!("EndpointSlice watcher stream ended unexpectedly")
}
