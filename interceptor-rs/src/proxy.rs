//! Proxy server — the main performance-critical path.
//!
//! ## Raw TCP forwarding (v2 architecture)
//!
//! Instead of using hyper's `Client` (which parses + re-encodes headers on
//! both the request and response legs), we:
//!
//! 1. **Keep hyper as the server** — it parses the incoming request so we can
//!    do routing, header matching, and queue counting.
//! 2. **Write the outgoing request as raw bytes** directly to a pooled
//!    `TcpStream` (avoids hyper client's `encode_headers` pass).
//! 3. **Parse the response status + headers with `httparse`** — much cheaper
//!    than going through hyper's client dispatcher; we construct an
//!    `http::Response` from the parsed data.
//! 4. **Stream the response body** back through hyper's server as a custom
//!    `Body` impl that reads directly from the backend `TcpStream`.
//!
//! This eliminates ~20% of CPU overhead from the hyper client's pool
//! (checkout/put/drop), request re-encoding, and dispatch machinery.

use std::convert::Infallible;
use std::io::Write as _;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::header;
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioExecutor;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::backend_pool::BackendPool;
use crate::config::Config;
use crate::diagnostics::DiagCounters;
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
    pub backend_pool: Arc<BackendPool>,
    pub diag: Arc<DiagCounters>,
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
        let backend_pool = Arc::new(BackendPool::new(
            config.dns_cache_ttl,
            config.connect_timeout,
            config.max_idle_conns_per_host,
            diag.clone(),
        ));

        Self {
            config,
            routing_table,
            queue,
            endpoints_cache,
            metrics,
            backend_pool,
            diag,
        }
    }
}

// ---------------------------------------------------------------------------
// Proxy body (avoids BoxBody overhead)
// ---------------------------------------------------------------------------

pin_project! {
    /// Response body returned by the proxy handler.
    ///
    /// * `RawStream` — reads from the backend `TcpStream` (raw TCP mode).
    /// * `Fixed` — short error payload (404 / 502 / 500).
    ///
    /// Both variants optionally hold a [`QueueGuard`] that decrements the
    /// in-flight counter when the body is dropped (i.e. after the response has
    /// been fully sent to the client).
    #[project = ProxyBodyProj]
    pub enum ProxyBody {
        RawStream {
            inner: RawResponseBody,
            _guard: Option<QueueGuard>,
        },
        Fixed {
            data: Option<Bytes>,
            _guard: Option<QueueGuard>,
        },
    }
}

impl ProxyBody {
    fn raw_stream(body: RawResponseBody, guard: QueueGuard) -> Self {
        Self::RawStream {
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
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.project() {
            ProxyBodyProj::RawStream { inner, .. } => {
                Pin::new(inner).poll_frame(cx)
            }
            ProxyBodyProj::Fixed { data, .. } => {
                Poll::Ready(data.take().map(|b| Ok(Frame::data(b))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            ProxyBody::RawStream { inner, .. } => inner.is_done(),
            ProxyBody::Fixed { data, .. } => data.is_none(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            ProxyBody::RawStream { inner, .. } => inner.size_hint(),
            ProxyBody::Fixed { data, .. } => {
                let mut hint = SizeHint::default();
                hint.set_exact(data.as_ref().map_or(0, |b| b.len()) as u64);
                hint
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Raw response body — reads from backend TcpStream
// ---------------------------------------------------------------------------

/// Streams a Content-Length-delimited or chunked HTTP response body from a
/// raw `TcpStream`.  When the body is fully consumed, the stream is returned
/// to the `BackendPool` for reuse.
pub struct RawResponseBody {
    /// Backend TCP stream (taken when body is done, for pool return).
    stream: Option<TcpStream>,
    /// Any leftover bytes from the header-parsing read buffer.
    buffered: BytesMut,
    /// Body framing.
    framing: BodyFraming,
    /// Pool + authority for returning the connection.
    pool: Option<Arc<BackendPool>>,
    authority: String,
}

enum BodyFraming {
    /// Known content length; `remaining` counts down to zero.
    ContentLength { remaining: u64 },
    /// Read until the connection closes (HTTP/1.0 or missing Content-Length).
    /// Cannot reuse the connection.
    ReadUntilClose,
}

impl RawResponseBody {
    fn new(
        stream: TcpStream,
        buffered: BytesMut,
        framing: BodyFraming,
        pool: Arc<BackendPool>,
        authority: String,
    ) -> Self {
        Self {
            stream: Some(stream),
            buffered,
            framing,
            pool: Some(pool),
            authority,
        }
    }

    fn is_done(&self) -> bool {
        match self.framing {
            BodyFraming::ContentLength { remaining } => remaining == 0 && self.buffered.is_empty(),
            BodyFraming::ReadUntilClose => self.stream.is_none(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self.framing {
            BodyFraming::ContentLength { remaining } => {
                let total = remaining + self.buffered.len() as u64;
                SizeHint::with_exact(total)
            }
            BodyFraming::ReadUntilClose => SizeHint::default(),
        }
    }

    /// Return the stream to the pool (only for Content-Length bodies that
    /// have been fully consumed).
    fn maybe_return_to_pool(&mut self) {
        if let BodyFraming::ContentLength { remaining: 0 } = self.framing {
            if let (Some(pool), Some(stream)) = (self.pool.take(), self.stream.take()) {
                pool.checkin(&self.authority, stream);
            }
        }
    }
}

impl Body for RawResponseBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        // 1. Serve buffered data first (leftover from header parsing)
        if !this.buffered.is_empty() {
            let chunk = match this.framing {
                BodyFraming::ContentLength { ref mut remaining } => {
                    let take = (*remaining as usize).min(this.buffered.len());
                    *remaining -= take as u64;
                    this.buffered.split_to(take).freeze()
                }
                BodyFraming::ReadUntilClose => this.buffered.split().freeze(),
            };
            if !chunk.is_empty() {
                // Check if we're done
                if let BodyFraming::ContentLength { remaining: 0 } = this.framing {
                    this.maybe_return_to_pool();
                }
                return Poll::Ready(Some(Ok(Frame::data(chunk))));
            }
        }

        // 2. Check if body is complete
        if let BodyFraming::ContentLength { remaining: 0 } = this.framing {
            this.maybe_return_to_pool();
            return Poll::Ready(None);
        }

        // 3. Read more from the stream
        let stream = match this.stream.as_mut() {
            Some(s) => s,
            None => return Poll::Ready(None), // stream already returned/closed
        };

        let mut buf = [0u8; 16384];
        let pin_stream = Pin::new(stream);
        let mut read_buf = tokio::io::ReadBuf::new(&mut buf);
        match pin_stream.poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let n = read_buf.filled().len();
                if n == 0 {
                    // EOF — stream closed
                    // For ReadUntilClose this is normal end-of-body.
                    // For ContentLength with remaining > 0, backend closed early.
                    this.stream = None;
                    this.pool = None;
                    return Poll::Ready(None);
                }

                let chunk = match this.framing {
                    BodyFraming::ContentLength { ref mut remaining } => {
                        let take = (*remaining as usize).min(n);
                        *remaining -= take as u64;
                        Bytes::copy_from_slice(&buf[..take])
                    }
                    BodyFraming::ReadUntilClose => Bytes::copy_from_slice(&buf[..n]),
                };

                if let BodyFraming::ContentLength { remaining: 0 } = this.framing {
                    this.maybe_return_to_pool();
                }

                Poll::Ready(Some(Ok(Frame::data(chunk))))
            }
            Poll::Ready(Err(e)) => {
                this.stream = None;
                this.pool = None;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Pending => Poll::Pending,
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

            let service = hyper::service::service_fn(move |req| {
                let state = state.clone();
                async move { handle_proxy_request(req, state, peer_addr).await }
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
    req: Request<Incoming>,
    state: Arc<AppState>,
    peer_addr: SocketAddr,
) -> Result<Response<ProxyBody>, Infallible> {
    let req_start = std::time::Instant::now();

    state.diag.requests_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // Extract what we need for routing/metrics before potentially moving `req`.
    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string();
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    // ---- 1. Route lookup (lock-free ArcSwap read) ----
    let route = {
        let _span = tracing::trace_span!("route_lookup").entered();
        match state.routing_table.route(&host, &path, req.headers()) {
            Some(r) => r,
            None => {
                state.diag.requests_no_route.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                state.metrics.record_request(method.as_str(), &path, 404, &host);
                return Ok(response(StatusCode::NOT_FOUND, "Not Found", None));
            }
        }
    };

    // ---- 2. Increment in-flight counter (atomic) ----
    let guard = {
        let _span = tracing::trace_span!("queue_increase").entered();
        state.queue.increase(&route.queue_key)
    };

    // ---- 3. Wait for ready endpoints (cold-start support) ----
    let wait_timeout = route
        .condition_wait_timeout
        .or(route.failover_timeout)
        .unwrap_or(state.config.condition_wait_timeout);

    // Use the pre-computed service_key from the routing table (avoids format! per request)
    let (target_url, is_cold_start) = {
        let t0 = std::time::Instant::now();
        let result = state
            .endpoints_cache
            .wait_for_ready_by_key(&route.service_key, wait_timeout)
            .await;
        tracing::trace!(elapsed_us = t0.elapsed().as_micros() as u64, "wait_endpoints");
        match result {
            Ok(cold_start) => (route.target_url.as_str(), cold_start),
            Err(()) => {
                // Timeout — try failover if configured
                if let Some(ref failover_url) = route.failover_url {
                    (failover_url.as_str(), true)
                } else {
                    state.metrics.record_request(method.as_str(), &path, 502, &host);
                    return Ok(response(
                        StatusCode::BAD_GATEWAY,
                        "Bad Gateway",
                        Some(guard),
                    ));
                }
            }
        }
    };

    if is_cold_start {
        state.diag.requests_cold_start.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    // ---- 4. Compute authority (host:port for the backend) ----
    let authority = &route.authority;

    // ---- 5. Cold-start connectivity probe ----
    if is_cold_start {
        let probe_start = std::time::Instant::now();
        let probe_timeout = Duration::from_secs(5);
        let probe_deadline = tokio::time::Instant::now() + probe_timeout;
        let mut attempt = 0u32;

        loop {
            match tokio::time::timeout(
                Duration::from_secs(1),
                tokio::net::TcpStream::connect(authority.as_str()),
            )
            .await
            {
                Ok(Ok(_stream)) => {
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

    // ---- 6. Forward to backend (raw TCP) ----
    let resp_timeout = route
        .response_header_timeout
        .unwrap_or(state.config.response_header_timeout);

    let result = {
        let t0 = std::time::Instant::now();
        let r = tokio::time::timeout(
            resp_timeout,
            forward_raw(&state, authority, target_url, req, peer_addr),
        )
        .await;
        tracing::trace!(elapsed_us = t0.elapsed().as_micros() as u64, "forward_request");
        r
    };

    match result {
        Ok(Ok((status_code, resp_headers, body))) => {
            let _span = tracing::trace_span!("record_metrics").entered();
            state
                .metrics
                .record_request(method.as_str(), &path, status_code, &host);
            drop(_span);

            let mut builder = Response::builder().status(status_code);
            if let Some(h) = builder.headers_mut() {
                *h = resp_headers;
                if is_cold_start {
                    h.insert(
                        "x-keda-http-cold-start",
                        http::HeaderValue::from_static("true"),
                    );
                }
            }

            tracing::trace!(
                total_us = req_start.elapsed().as_micros() as u64,
                status = status_code,
                host = %host,
                "request_complete",
            );
            Ok(builder
                .body(ProxyBody::raw_stream(body, guard))
                .unwrap_or_else(|_| response(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error", None)))
        }
        Ok(Err(e)) => {
            state.diag.requests_backend_error.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            state.diag.requests_backend_error.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
// Raw TCP forwarding
// ---------------------------------------------------------------------------

/// Forward a request to the backend over a raw TCP connection and return
/// the parsed status code, response headers, and a streaming body.
async fn forward_raw(
    state: &Arc<AppState>,
    authority: &str,
    _target_url: &str,
    req: Request<Incoming>,
    peer_addr: SocketAddr,
) -> std::io::Result<(u16, http::HeaderMap, RawResponseBody)> {
    let (parts, body) = req.into_parts();

    // 1. Get a pooled (or new) TCP connection
    let mut stream = state.backend_pool.checkout(authority).await?;

    // 2. Write request line + headers as raw bytes
    let mut head_buf = Vec::with_capacity(1024);

    // Request line: METHOD /path HTTP/1.1\r\n
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    write!(head_buf, "{} {} HTTP/1.1\r\n", parts.method, path_and_query)?;

    // Host header (use the backend authority, not the original host)
    write!(head_buf, "host: {}\r\n", authority)?;

    // Original headers (skip hop-by-hop and Host — we set our own)
    let host_orig = parts.headers.get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    for (name, value) in &parts.headers {
        // Skip headers we handle ourselves
        if name == header::HOST
            || name == header::TRANSFER_ENCODING
            || name == "x-forwarded-for"
            || name == "x-forwarded-host"
            || name == "x-forwarded-proto"
        {
            continue;
        }
        head_buf.extend_from_slice(name.as_str().as_bytes());
        head_buf.extend_from_slice(b": ");
        head_buf.extend_from_slice(value.as_bytes());
        head_buf.extend_from_slice(b"\r\n");
    }

    // X-Forwarded-* headers (written directly, no String formatting)
    head_buf.extend_from_slice(b"x-forwarded-for: ");
    write!(head_buf, "{}", peer_addr.ip())?;
    head_buf.extend_from_slice(b"\r\n");

    head_buf.extend_from_slice(b"x-forwarded-host: ");
    head_buf.extend_from_slice(host_orig.as_bytes());
    head_buf.extend_from_slice(b"\r\n");

    head_buf.extend_from_slice(b"x-forwarded-proto: http\r\n");

    // End of headers
    head_buf.extend_from_slice(b"\r\n");

    // Write the entire header block in one syscall
    stream.write_all(&head_buf).await?;

    // 3. Forward request body (if any)
    //    Consume the hyper Incoming body and write each chunk to the backend.
    use hyper::body::Body as _;
    if !body.is_end_stream() {
        use http_body_util::BodyExt;
        let mut body = body;
        while let Some(frame_result) = body.frame().await {
            match frame_result {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        stream.write_all(&data).await?;
                    }
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        format!("error reading request body: {e}"),
                    ));
                }
            }
        }
    }

    // Flush to ensure the request is sent.
    stream.flush().await?;

    // 4. Read response headers
    let mut read_buf = BytesMut::with_capacity(8192);
    let (status_code, headers, body_start) = read_response_head(&mut stream, &mut read_buf).await?;

    // 5. Determine body framing
    let content_length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    let framing = if let Some(len) = content_length {
        BodyFraming::ContentLength { remaining: len }
    } else {
        BodyFraming::ReadUntilClose
    };

    // 6. Create streaming body (any leftover bytes from header read are buffered)
    let body = RawResponseBody::new(
        stream,
        body_start,
        framing,
        state.backend_pool.clone(),
        authority.to_string(),
    );

    Ok((status_code, headers, body))
}

/// Read and parse the HTTP response status line + headers from the backend.
/// Returns (status_code, headers, leftover_bytes_after_headers).
async fn read_response_head(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
) -> std::io::Result<(u16, http::HeaderMap, BytesMut)> {
    loop {
        // Try to parse what we have so far
        let mut headers_buf = [httparse::EMPTY_HEADER; 64];
        let mut parsed = httparse::Response::new(&mut headers_buf);

        match parsed.parse(buf) {
            Ok(httparse::Status::Complete(header_len)) => {
                let status = parsed.code.unwrap_or(502);

                // Build HeaderMap from parsed headers
                let mut header_map = http::HeaderMap::with_capacity(parsed.headers.len());
                for h in parsed.headers.iter() {
                    if let (Ok(name), Ok(value)) = (
                        http::header::HeaderName::from_bytes(h.name.as_bytes()),
                        http::HeaderValue::from_bytes(h.value),
                    ) {
                        header_map.append(name, value);
                    }
                }

                // Split off the body portion (everything after the headers)
                let body_start = buf.split_off(header_len);
                // Drop the header portion
                buf.clear();

                return Ok((status, header_map, body_start));
            }
            Ok(httparse::Status::Partial) => {
                // Need more data — read from the stream
                let n = stream.read_buf(buf).await?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "backend closed connection before sending response headers",
                    ));
                }
            }
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid HTTP response from backend: {e}"),
                ));
            }
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
