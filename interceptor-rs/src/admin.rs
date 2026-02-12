//! Admin server — health probes, queue-counts, and (optional) profiling.
//!
//! Listens on `:9090` (configurable via `KEDA_HTTP_ADMIN_PORT`).
//! Traffic here is low-volume (probes + scaler polling), so HTTP/1.1 only is
//! fine and avoids the h2 negotiation overhead.
//!
//! ## Profiling (feature `profiling`)
//!
//! When built with `--features profiling`, the following endpoint is available:
//!
//! * `GET /debug/pprof/flamegraph?seconds=N` — CPU flamegraph (SVG).
//!   Defaults to a 5-second sample.  Open the SVG in a browser and click to
//!   zoom into hot call-stacks.
//!
//!   ```sh
//!   curl -s http://localhost:9090/debug/pprof/flamegraph?seconds=10 > flame.svg
//!   open flame.svg   # macOS; use `xdg-open` on Linux
//!   ```

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};

use crate::proxy::AppState;

pub async fn serve(state: Arc<AppState>) -> anyhow::Result<()> {
    let addr: SocketAddr = ([0, 0, 0, 0], state.config.admin_port).into();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "Admin server listening");

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
                tracing::debug!(error = %e, "admin connection error");
            }
        });
    }
}

async fn handle(
    req: Request<Incoming>,
    state: Arc<AppState>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    match req.uri().path() {
        // ----- Health probes ------------------------------------------------
        "/livez" | "/readyz" => {
            if state.routing_table.has_synced() {
                Ok(ok("OK"))
            } else {
                Ok(status(StatusCode::SERVICE_UNAVAILABLE, "Service Unavailable"))
            }
        }

        // ----- Queue counts (consumed by the Scaler) -----------------------
        "/queue" => {
            let counts = state.queue.current();
            match serde_json::to_vec(&counts) {
                Ok(json) => Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from(json)))
                    .unwrap()),
                Err(e) => {
                    tracing::error!(error = %e, "failed to serialize queue counts");
                    Ok(status(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Internal Server Error",
                    ))
                }
            }
        }

        // ----- CPU flamegraph (opt-in via `profiling` feature) -------------
        #[cfg(feature = "profiling")]
        p if p.starts_with("/debug/pprof/flamegraph") => {
            Ok(handle_flamegraph(req.uri().query()).await)
        }

        _ => Ok(status(StatusCode::NOT_FOUND, "Not Found")),
    }
}

// ---------------------------------------------------------------------------
// pprof flamegraph handler
// ---------------------------------------------------------------------------

#[cfg(feature = "profiling")]
async fn handle_flamegraph(query: Option<&str>) -> Response<Full<Bytes>> {
    use std::time::Duration;

    // Parse ?seconds=N from query string, default to 5.
    let seconds = query
        .and_then(|q| {
            q.split('&')
                .find_map(|kv| kv.strip_prefix("seconds="))
                .and_then(|v| v.parse::<u64>().ok())
        })
        .unwrap_or(5)
        .min(120); // cap at 2 minutes

    tracing::info!(seconds, "starting CPU profile");

    // Run the profiler in a blocking thread so we don't starve the runtime.
    let result = tokio::task::spawn_blocking(move || {
        tracing::info!("profiler: building guard");
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(997)       // prime to avoid aliasing
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .map_err(|e| format!("profiler build: {e}"))?;

        tracing::info!(seconds, "profiler: sampling");
        std::thread::sleep(Duration::from_secs(seconds));

        tracing::info!("profiler: building report");
        let report = guard
            .report()
            .build()
            .map_err(|e| format!("report build: {e}"))?;

        tracing::info!("profiler: rendering flamegraph");
        let mut svg = Vec::with_capacity(256 * 1024);
        report
            .flamegraph(&mut svg)
            .map_err(|e| format!("flamegraph render: {e}"))?;

        tracing::info!(svg_bytes = svg.len(), "profiler: done");
        Ok::<Vec<u8>, String>(svg)
    })
    .await;

    match result {
        Ok(Ok(svg)) => {
            tracing::info!(svg_bytes = svg.len(), "profiler: sending response");
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "image/svg+xml")
                .body(Full::new(Bytes::from(svg)))
                .unwrap()
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "pprof error");
            status(StatusCode::INTERNAL_SERVER_ERROR, "profiling error")
        }
        Err(e) => {
            tracing::error!(error = %e, "pprof task panicked");
            status(StatusCode::INTERNAL_SERVER_ERROR, "profiling task panicked")
        }
    }
}

// ---------------------------------------------------------------------------
// Tiny helpers
// ---------------------------------------------------------------------------

fn ok(body: &'static str) -> Response<Full<Bytes>> {
    Response::new(Full::new(Bytes::from(body)))
}

fn status(code: StatusCode, body: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(code)
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}
