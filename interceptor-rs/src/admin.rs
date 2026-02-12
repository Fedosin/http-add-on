//! Admin server — health probes and queue-counts endpoint.
//!
//! Listens on `:9090` (configurable via `KEDA_HTTP_ADMIN_PORT`).
//! Traffic here is low-volume (probes + scaler polling), so HTTP/1.1 only is
//! fine and avoids the h2 negotiation overhead.

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

        _ => Ok(status(StatusCode::NOT_FOUND, "Not Found")),
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
