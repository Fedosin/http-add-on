//! Kubernetes CRD types for `HTTPScaledObject`.
//!
//! These mirror the Go types in `operator/apis/http/v1alpha1/` just enough for
//! the Interceptor to read and route on them.  We use `kube::CustomResource` to
//! derive the top-level `HTTPScaledObject` wrapper automatically.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Top-level CRD
// ---------------------------------------------------------------------------

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "http.keda.sh",
    version = "v1alpha1",
    kind = "HTTPScaledObject",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct HTTPScaledObjectSpec {
    #[serde(default)]
    pub hosts: Vec<String>,

    #[serde(default)]
    pub path_prefixes: Vec<String>,

    #[serde(default)]
    pub headers: Vec<HeaderMatch>,

    pub scale_target_ref: ScaleTargetRef,

    #[serde(default)]
    pub cold_start_timeout_failover_ref: Option<ColdStartFailoverRef>,

    #[serde(default)]
    pub replicas: Option<ReplicaSpec>,

    #[serde(default)]
    pub scaling_metric: Option<ScalingMetricSpec>,

    #[serde(default)]
    pub timeouts: Option<TimeoutSpec>,
}

// ---------------------------------------------------------------------------
// Nested types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HeaderMatch {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScaleTargetRef {
    pub service: String,
    #[serde(default)]
    pub port: Option<i32>,
    #[serde(default)]
    pub port_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ColdStartFailoverRef {
    pub service: String,
    pub port: i32,
    #[serde(default = "default_failover_timeout")]
    pub timeout_seconds: i64,
}

fn default_failover_timeout() -> i64 {
    30
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSpec {
    #[serde(default)]
    pub min: Option<i32>,
    #[serde(default)]
    pub max: Option<i32>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScalingMetricSpec {
    #[serde(default)]
    pub concurrency: Option<ConcurrencyMetric>,
    #[serde(default)]
    pub request_rate: Option<RequestRateMetric>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConcurrencyMetric {
    pub target_value: i32,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RequestRateMetric {
    pub target_value: i32,
    #[serde(default = "default_window")]
    pub window: String,
    #[serde(default = "default_granularity")]
    pub granularity: String,
}

fn default_window() -> String {
    "1m".into()
}
fn default_granularity() -> String {
    "1s".into()
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TimeoutSpec {
    #[serde(default)]
    pub condition_wait: Option<String>,
    #[serde(default)]
    pub response_header: Option<String>,
}
