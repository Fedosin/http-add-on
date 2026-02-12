//! Prometheus metrics collector and server.
//!
//! Exposes two counters that match the Go implementation:
//!
//! * `keda_http_request_count{method, path, code, host}`
//! * `keda_http_pending_request_count{host}` (gauge)
//!
//! The server listens on `:2223` (configurable via
//! `KEDA_HTTP_OTEL_PROM_EXPORTER_PORT`).

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use prometheus::{Encoder, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};

use crate::proxy::AppState;

// ---------------------------------------------------------------------------
// Metrics collector
// ---------------------------------------------------------------------------

pub struct MetricsCollector {
    pub registry: Registry,
    pub request_count: IntCounterVec,
    pub pending_request_count: IntGaugeVec,
}

impl MetricsCollector {
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::new();

        let request_count = IntCounterVec::new(
            Opts::new(
                "keda_http_request_count",
                "Total completed HTTP requests",
            ),
            &["method", "path", "code", "host"],
        )?;
        registry.register(Box::new(request_count.clone()))?;

        let pending_request_count = IntGaugeVec::new(
            Opts::new(
                "keda_http_pending_request_count",
                "Current in-flight HTTP requests",
            ),
            &["host"],
        )?;
        registry.register(Box::new(pending_request_count.clone()))?;

        Ok(Self {
            registry,
            request_count,
            pending_request_count,
        })
    }

    /// Record a completed request (called after the response status is known).
    pub fn record_request(&self, method: &str, path: &str, code: u16, host: &str) {
        let _span = tracing::trace_span!("prometheus_inc").entered();
        self.request_count
            .with_label_values(&[method, path, &code.to_string(), host])
            .inc();
    }

    /// Set the pending (in-flight) gauge for a host.
    #[allow(dead_code)]
    pub fn record_pending(&self, host: &str, value: i64) {
        self.pending_request_count
            .with_label_values(&[host])
            .set(value);
    }
}

// ---------------------------------------------------------------------------
// HTTP server for /metrics
// ---------------------------------------------------------------------------

pub async fn serve(state: Arc<AppState>) -> anyhow::Result<()> {
    let addr: SocketAddr = ([0, 0, 0, 0], state.config.metrics_port).into();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "Metrics server listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();

        tokio::spawn(async move {
            let io = hyper_util::rt::TokioIo::new(stream);
            let service = hyper::service::service_fn(move |req| {
                let state = state.clone();
                async move { handle(req, state).await }
            });

            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                tracing::debug!(error = %e, "metrics connection error");
            }
        });
    }
}

async fn handle(
    _req: Request<Incoming>,
    state: Arc<AppState>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let encoder = TextEncoder::new();
    let metric_families = state.metrics.registry.gather();
    let mut buf = Vec::with_capacity(4096);
    if let Err(e) = encoder.encode(&metric_families, &mut buf) {
        tracing::error!(error = %e, "failed to encode metrics");
        return Ok(Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Full::new(Bytes::from("metrics encoding error")))
            .unwrap());
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", encoder.format_type())
        .body(Full::new(Bytes::from(buf)))
        .unwrap())
}
