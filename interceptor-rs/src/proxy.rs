//! Proxy server — the main performance-critical path.
//!
//! Design choices for throughput:
//!
//! * **Zero-copy body forwarding** — the client request's `Incoming` body is
//!   moved (not copied) into the outbound request to the backend.
//! * **No per-request allocation for warm backends** — route lookup is a
//!   `HashMap` read behind `ArcSwap`, concurrency tracking is one atomic op,
//!   endpoint check is one `DashMap` read.
//! * **Custom body enum** — avoids `BoxBody` dynamic dispatch; the enum has
//!   only two variants (`Proxied` for backend responses, `Fixed` for error
//!   pages) with statically dispatched `Body::poll_frame`.
//! * **jemalloc global allocator** — reduces contention vs. glibc malloc under
//!   many concurrent connections.
//! * **hyper 1.x `auto::Builder`** — supports both HTTP/1.1 and h2 on the
//!   same port with minimal overhead.
//! * **`hyper-util` pooled client** — reuses TCP connections to backends, keyed
//!   by authority.
//! * **DNS caching connector** — avoids repeated `getaddrinfo` syscalls for
//!   the same Kubernetes service names.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::header;
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use pin_project_lite::pin_project;

use crate::config::Config;
use crate::diagnostics::{CachingConnector, DiagCounters};
use crate::endpoints::EndpointsCache;
use crate::metrics::MetricsCollector;
use crate::queue::{QueueCounter, QueueGuard};
use crate::routing::RoutingTable;

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

pub struct AppState {
    pub config: Config,
    pub routing_table: Arc<RoutingTable>,
    pub queue: Arc<QueueCounter>,
    pub endpoints_cache: Arc<EndpointsCache>,
    pub metrics: Arc<MetricsCollector>,
    pub diag: Arc<DiagCounters>,
    pub http_client: Client<CachingConnector, Incoming>,
}

impl AppState {
    pub fn new(
        config: Config,
        routing_table: Arc<RoutingTable>,
        queue: Arc<QueueCounter>,
        endpoints_cache: Arc<EndpointsCache>,
        metrics: Arc<MetricsCollector>,
        diag: Arc<DiagCounters>,
    ) -> Self {
        // CachingConnector replaces HttpConnector + GaiResolver:
        //   - Caches DNS results (avoids blocking getaddrinfo per connection)
        //   - Counts pool misses (new TCP connections) for diagnostics
        //   - Sets TCP_NODELAY directly on new connections
        let connector = CachingConnector::new(
            config.dns_cache_ttl,
            config.connect_timeout,
            true, // TCP_NODELAY
            diag.clone(),
        );

        let http_client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(config.max_idle_conns_per_host)
            // Retry when a pooled connection turns out to be dead (server
            // closed it between our checkout and our write).  This prevents
            // spurious 502s from stale connections.
            .retry_canceled_requests(true)
            // Ensure the Host header is set on outgoing requests.
            .set_host(true)
            .build(connector);

        Self {
            config,
            routing_table,
            queue,
            endpoints_cache,
            metrics,
            diag,
            http_client,
        }
    }
}

// ---------------------------------------------------------------------------
// Proxy body (avoids BoxBody overhead)
// ---------------------------------------------------------------------------

pin_project! {
    /// Response body returned by the proxy handler.
    ///
    /// `Proxied` streams the backend response (zero-copy).
    /// `Fixed` returns a short error payload (404 / 502 / 500).
    ///
    /// Both variants optionally hold a [`QueueGuard`] that decrements the
    /// in-flight counter when the body is dropped (i.e. after the response has
    /// been fully sent to the client).
    #[project = ProxyBodyProj]
    pub enum ProxyBody {
        Proxied {
            #[pin]
            inner: Incoming,
            _guard: Option<QueueGuard>,
        },
        Fixed {
            data: Option<Bytes>,
            _guard: Option<QueueGuard>,
        },
    }
}

impl ProxyBody {
    fn proxied(body: Incoming, guard: QueueGuard) -> Self {
        Self::Proxied {
            inner: body,
            _guard: Some(guard),
        }
    }

    fn fixed(status_text: &'static str, guard: Option<QueueGuard>) -> Self {
        Self::Fixed {
            data: Some(Bytes::from(status_text)),
            _guard: guard,
        }
    }
}

impl Body for ProxyBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.project() {
            ProxyBodyProj::Proxied { inner, .. } => inner.poll_frame(cx),
            ProxyBodyProj::Fixed { data, .. } => {
                Poll::Ready(data.take().map(|b| Ok(Frame::data(b))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            ProxyBody::Proxied { inner, .. } => inner.is_end_stream(),
            ProxyBody::Fixed { data, .. } => data.is_none(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            ProxyBody::Proxied { inner, .. } => inner.size_hint(),
            ProxyBody::Fixed { data, .. } => {
                let mut hint = SizeHint::default();
                hint.set_exact(data.as_ref().map_or(0, |b| b.len()) as u64);
                hint
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

pub async fn serve(state: Arc<AppState>) -> anyhow::Result<()> {
    let addr: SocketAddr = ([0, 0, 0, 0], state.config.proxy_port).into();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "Proxy server listening");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let state = state.clone();

        tokio::spawn(async move {
            // TCP_NODELAY reduces latency for small writes.
            let _ = stream.set_nodelay(true);
            let io = hyper_util::rt::TokioIo::new(stream);

            // Pre-format peer IP once per connection (avoid per-request allocation).
            let peer_ip = peer_addr.ip().to_string();

            let service = hyper::service::service_fn(move |req| {
                let state = state.clone();
                let peer_ip = peer_ip.clone();
                async move { handle_proxy_request(req, state, peer_addr, peer_ip).await }
            });

            if let Err(e) = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, service)
                .await
            {
                // Suppress "connection reset by peer" noise.
                let msg = e.to_string();
                if !msg.contains("connection reset")
                    && !msg.contains("broken pipe")
                    && !msg.contains("connection closed")
                {
                    tracing::debug!(
                        error = %e,
                        peer = %peer_addr,
                        "proxy connection error",
                    );
                }
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Request handler
// ---------------------------------------------------------------------------

async fn handle_proxy_request(
    mut req: Request<Incoming>,
    state: Arc<AppState>,
    peer_addr: SocketAddr,
    peer_ip: String,
) -> Result<Response<ProxyBody>, Infallible> {
    let req_start = std::time::Instant::now();
    state
        .diag
        .requests_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string();
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    // ---- 1. Route lookup (lock-free ArcSwap read) ----
    let route = match state.routing_table.route(&host, &path, req.headers()) {
        Some(r) => r,
        None => {
            state
                .diag
                .requests_no_route
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            state
                .metrics
                .record_request(method.as_str(), &path, 404, &host);
            return Ok(response(StatusCode::NOT_FOUND, "Not Found", None));
        }
    };

    // ---- 2. Increment in-flight counter (atomic) ----
    let guard = state.queue.increase(&route.queue_key);

    // ---- 3. Wait for ready endpoints (cold-start support) ----
    let wait_timeout = route
        .condition_wait_timeout
        .or(route.failover_timeout)
        .unwrap_or(state.config.condition_wait_timeout);

    let (target_url, is_cold_start) = {
        let result = state
            .endpoints_cache
            .wait_for_ready(&route.target_namespace, &route.target_service, wait_timeout)
            .await;
        match result {
            Ok(cold_start) => (route.target_url.clone(), cold_start),
            Err(()) => {
                // Timeout — try failover if configured
                if let Some(ref failover_url) = route.failover_url {
                    (failover_url.clone(), true)
                } else {
                    state
                        .metrics
                        .record_request(method.as_str(), &path, 502, &host);
                    return Ok(response(
                        StatusCode::BAD_GATEWAY,
                        "Bad Gateway",
                        Some(guard),
                    ));
                }
            }
        }
    };

    // ---- 4. Build backend URI ----
    let backend_uri = {
        let path_and_query = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        match format!("{target_url}{path_and_query}").parse::<http::Uri>() {
            Ok(uri) => uri,
            Err(_) => {
                state
                    .metrics
                    .record_request(method.as_str(), &path, 500, &host);
                return Ok(response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Server Error",
                    Some(guard),
                ));
            }
        }
    };

    *req.uri_mut() = backend_uri.clone();

    // ---- 5. Add X-Forwarded-* headers ----
    add_forwarded_headers(&mut req, &host, &peer_ip);

    // ---- 6. Cold-start connectivity probe ----
    // After a cold start, kube-proxy may not have updated its iptables/IPVS
    // rules yet, even though the EndpointSlice already reports the pod as
    // ready.  Probe TCP connectivity first so we don't waste the original
    // request body on a guaranteed-to-fail connection.
    if is_cold_start {
        state
            .diag
            .requests_cold_start
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let probe_start = std::time::Instant::now();
        let authority = backend_uri
            .authority()
            .map(|a| a.as_str().to_string())
            .unwrap_or_else(|| format!("{}:{}", route.target_service, route.target_port));

        let probe_timeout = Duration::from_secs(5);
        let probe_deadline = tokio::time::Instant::now() + probe_timeout;
        let mut attempt = 0u32;

        loop {
            match tokio::time::timeout(
                Duration::from_secs(1),
                tokio::net::TcpStream::connect(&authority),
            )
            .await
            {
                Ok(Ok(_stream)) => {
                    // Connection succeeded — kube-proxy routing is in place.
                    tracing::info!(
                        host = %host,
                        authority = %authority,
                        attempt,
                        elapsed_us = probe_start.elapsed().as_micros() as u64,
                        "cold-start: backend reachable",
                    );
                    break;
                }
                Ok(Err(e)) => {
                    if tokio::time::Instant::now() >= probe_deadline {
                        tracing::error!(
                            error = %e,
                            host = %host,
                            authority = %authority,
                            "cold-start: backend unreachable after probe timeout",
                        );
                        state
                            .metrics
                            .record_request(method.as_str(), &path, 502, &host);
                        return Ok(response(
                            StatusCode::BAD_GATEWAY,
                            "Bad Gateway",
                            Some(guard),
                        ));
                    }
                    attempt += 1;
                    // Backoff: 100ms, 200ms, 400ms, 800ms, ...
                    let delay = Duration::from_millis(100 << attempt.min(4));
                    tracing::debug!(
                        host = %host,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        "cold-start: probing backend connectivity",
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(_) => {
                    // TCP connect timed out (1s per attempt)
                    if tokio::time::Instant::now() >= probe_deadline {
                        tracing::error!(
                            host = %host,
                            authority = %authority,
                            "cold-start: backend connect timed out after probe timeout",
                        );
                        state
                            .metrics
                            .record_request(method.as_str(), &path, 502, &host);
                        return Ok(response(
                            StatusCode::BAD_GATEWAY,
                            "Bad Gateway",
                            Some(guard),
                        ));
                    }
                    attempt += 1;
                }
            }
        }
    }

    // ---- 7. Forward to backend ----
    let resp_timeout = route
        .response_header_timeout
        .unwrap_or(state.config.response_header_timeout);

    let result =
        tokio::time::timeout(resp_timeout, state.http_client.request(req)).await;

    match result {
        Ok(Ok(resp)) => {
            let status = resp.status();
            state
                .metrics
                .record_request(method.as_str(), &path, status.as_u16(), &host);

            let (mut parts, body) = resp.into_parts();
            if is_cold_start {
                parts.headers.insert(
                    "x-keda-http-cold-start",
                    http::HeaderValue::from_static("true"),
                );
            }
            Ok(Response::from_parts(parts, ProxyBody::proxied(body, guard)))
        }
        Ok(Err(e)) => {
            state
                .diag
                .requests_backend_error
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::warn!(
                error = %e,
                host = %host,
                total_us = req_start.elapsed().as_micros() as u64,
                "proxy error",
            );
            state
                .metrics
                .record_request(method.as_str(), &path, 502, &host);
            Ok(response(
                StatusCode::BAD_GATEWAY,
                "Bad Gateway",
                Some(guard),
            ))
        }
        Err(_elapsed) => {
            state
                .diag
                .requests_backend_error
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::warn!(
                host = %host,
                timeout = ?resp_timeout,
                total_us = req_start.elapsed().as_micros() as u64,
                "response header timeout",
            );
            state
                .metrics
                .record_request(method.as_str(), &path, 502, &host);
            Ok(response(
                StatusCode::BAD_GATEWAY,
                "Bad Gateway",
                Some(guard),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn response(
    status: StatusCode,
    body: &'static str,
    guard: Option<QueueGuard>,
) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .body(ProxyBody::fixed(body, guard))
        .expect("static response")
}

fn add_forwarded_headers(req: &mut Request<Incoming>, host: &str, peer_ip: &str) {
    let headers = req.headers_mut();

    // X-Forwarded-For: append client IP (avoid format! when possible)
    if let Some(existing) = headers.get("x-forwarded-for") {
        let existing_bytes = existing.as_bytes();
        let mut buf = Vec::with_capacity(existing_bytes.len() + 2 + peer_ip.len());
        buf.extend_from_slice(existing_bytes);
        buf.extend_from_slice(b", ");
        buf.extend_from_slice(peer_ip.as_bytes());
        if let Ok(val) = http::HeaderValue::from_bytes(&buf) {
            headers.insert("x-forwarded-for", val);
        }
    } else if let Ok(val) = http::HeaderValue::from_str(peer_ip) {
        headers.insert("x-forwarded-for", val);
    }

    // X-Forwarded-Host
    if let Ok(val) = http::HeaderValue::from_str(host) {
        headers.insert("x-forwarded-host", val);
    }

    // X-Forwarded-Proto
    headers.insert(
        "x-forwarded-proto",
        http::HeaderValue::from_static("http"),
    );
}
