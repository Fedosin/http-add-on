use std::env;
use std::time::Duration;

use anyhow::Result;

/// All interceptor configuration, parsed from environment variables.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct Config {
    pub proxy_port: u16,
    pub admin_port: u16,
    pub metrics_port: u16,
    pub connect_timeout: Duration,
    pub keep_alive: Duration,
    pub response_header_timeout: Duration,
    pub condition_wait_timeout: Duration,
    pub max_idle_conns_per_host: usize,
    pub force_http2: bool,
    pub tls_enabled: bool,
    pub watch_namespace: Option<String>,
    pub log_requests: bool,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            proxy_port: env_or("KEDA_HTTP_PROXY_PORT", 8080)?,
            admin_port: env_or("KEDA_HTTP_ADMIN_PORT", 9090)?,
            metrics_port: env_or("KEDA_HTTP_OTEL_PROM_EXPORTER_PORT", 2223)?,
            connect_timeout: env_duration(
                "KEDA_HTTP_CONNECT_TIMEOUT",
                Duration::from_millis(500),
            ),
            keep_alive: env_duration("KEDA_HTTP_KEEP_ALIVE", Duration::from_secs(1)),
            response_header_timeout: env_duration(
                "KEDA_HTTP_RESPONSE_HEADER_TIMEOUT",
                Duration::from_millis(500),
            ),
            condition_wait_timeout: env_duration(
                "KEDA_HTTP_WORKLOAD_REPLICAS_TIMEOUT",
                Duration::from_secs(20),
            ),
            max_idle_conns_per_host: env_or("KEDA_HTTP_MAX_IDLE_CONNS_PER_HOST", 20)?,
            force_http2: env_or("KEDA_HTTP_FORCE_HTTP2", false)?,
            tls_enabled: env_or("KEDA_HTTP_PROXY_TLS_ENABLED", false)?,
            watch_namespace: env::var("KEDA_HTTP_WATCH_NAMESPACE").ok(),
            log_requests: env_or("KEDA_HTTP_LOG_REQUESTS", false)?,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match env::var(key) {
        Ok(val) => val
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid value for {key}: {e}")),
        Err(_) => Ok(default),
    }
}

fn env_duration(key: &str, default: Duration) -> Duration {
    env::var(key)
        .ok()
        .and_then(|v| parse_go_duration(&v))
        .unwrap_or(default)
}

/// Parse Go-style duration strings: `500ms`, `1s`, `20s`, `1m`, `1m30s`.
pub fn parse_go_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Compound durations, e.g. "1m30s"
    if let Some(m_pos) = s.find('m') {
        let rest = &s[m_pos + 1..];
        if !rest.is_empty() && !rest.starts_with('s') {
            let mins: f64 = s[..m_pos].parse().ok()?;
            if let Some(secs_str) = rest.strip_suffix("ms") {
                let ms: f64 = secs_str.parse().ok()?;
                return Some(Duration::from_secs_f64(mins * 60.0 + ms / 1000.0));
            }
            if let Some(secs_str) = rest.strip_suffix('s') {
                let secs: f64 = secs_str.parse().ok()?;
                return Some(Duration::from_secs_f64(mins * 60.0 + secs));
            }
        }
    }

    if let Some(ms) = s.strip_suffix("ms") {
        return ms.parse::<u64>().ok().map(Duration::from_millis);
    }
    if let Some(secs) = s.strip_suffix('s') {
        return secs.parse::<f64>().ok().map(Duration::from_secs_f64);
    }
    if let Some(mins) = s.strip_suffix('m') {
        return mins
            .parse::<f64>()
            .ok()
            .map(|m| Duration::from_secs_f64(m * 60.0));
    }

    // Bare number → seconds
    s.parse::<f64>().ok().map(Duration::from_secs_f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_durations() {
        assert_eq!(parse_go_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_go_duration("1s"), Some(Duration::from_secs(1)));
        assert_eq!(parse_go_duration("20s"), Some(Duration::from_secs(20)));
        assert_eq!(parse_go_duration("1m"), Some(Duration::from_secs(60)));
        assert_eq!(
            parse_go_duration("1m30s"),
            Some(Duration::from_secs_f64(90.0))
        );
        assert_eq!(parse_go_duration(""), None);
    }
}
