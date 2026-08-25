//! Version 1 wire DTOs for the read-only administration API.

use serde::{Deserialize, Serialize};

use crate::model::ServerStatsResponse;

pub(crate) const API_VERSION: &str = "v1";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperationalState {
    Ready,
    Degraded,
    External,
    Unsupported,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct Page {
    pub(super) limit: usize,
    pub(super) returned: usize,
    pub(super) next_cursor: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct StatusResponse {
    pub(super) api_version: &'static str,
    pub(super) shardline_version: &'static str,
    pub(super) observed_at_unix_seconds: u64,
    pub(super) state: OperationalState,
    pub(super) durable_storage_state: OperationalState,
    pub(super) cache_state: OperationalState,
    pub(super) server_role: String,
    pub(super) server_frontends: Vec<String>,
    pub(super) metadata_backend: String,
    pub(super) object_backend: String,
    pub(super) cache_backend: String,
    pub(super) plugin_registry: OperationalState,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct StorageResponse {
    pub(super) api_version: &'static str,
    pub(super) observed_at_unix_seconds: u64,
    pub(super) authoritative: ServerStatsResponse,
    pub(super) process_lifetime: StorageProcessCounters,
    pub(super) deduplication_ratio_state: OperationalState,
    pub(super) deduplication_ratio: Option<Ratio>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct StorageProcessCounters {
    pub(super) objects_written: u64,
    pub(super) object_bytes_written: u64,
    pub(super) xorbs_written: u64,
    pub(super) xorb_bytes_written: u64,
    pub(super) shards_written: i64,
    pub(super) deduplicated_bytes: u64,
    pub(super) compression_saved_bytes: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct Ratio {
    pub(super) numerator_bytes: u64,
    pub(super) denominator_bytes: u64,
    pub(super) basis_points: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct GcResponse {
    pub(super) api_version: &'static str,
    pub(super) observed_at_unix_seconds: u64,
    pub(super) state: OperationalState,
    pub(super) execution: OperationalState,
    pub(super) runs_observed_by_process: u64,
    pub(super) objects_collected_by_process: u64,
    pub(super) bytes_collected_by_process: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct IntegrityResponse {
    pub(super) api_version: &'static str,
    pub(super) observed_at_unix_seconds: u64,
    pub(super) state: OperationalState,
    pub(super) execution: OperationalState,
    pub(super) fsck_runs_observed_by_process: u64,
    pub(super) errors_observed_by_process: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct NodesResponse {
    pub(super) api_version: &'static str,
    pub(super) observed_at_unix_seconds: u64,
    pub(super) discovery: OperationalState,
    pub(super) nodes: Vec<Node>,
    pub(super) page: Page,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct Node {
    pub(super) scope: &'static str,
    pub(super) state: OperationalState,
    pub(super) server_role: String,
    pub(super) server_frontends: Vec<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct TasksResponse {
    pub(super) api_version: &'static str,
    pub(super) observed_at_unix_seconds: u64,
    pub(super) scheduler: OperationalState,
    pub(super) tasks: Vec<Task>,
    pub(super) page: Page,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct Task {
    pub(super) id: String,
    pub(super) state: OperationalState,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct MetricsResponse {
    pub(super) api_version: &'static str,
    pub(super) observed_at_unix_seconds: u64,
    pub(super) prometheus_path: &'static str,
    pub(super) active_connections: i64,
    pub(super) admitted_requests: u64,
    pub(super) queued_requests: u64,
    pub(super) rejected_requests: u64,
    pub(super) upload_requests: u64,
    pub(super) upload_bytes: u64,
    pub(super) download_requests: u64,
    pub(super) download_bytes: u64,
    pub(super) range_requests: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct PluginsResponse {
    pub(super) api_version: &'static str,
    pub(super) observed_at_unix_seconds: u64,
    pub(super) registry: OperationalState,
    pub(super) plugins: Vec<Plugin>,
    pub(super) page: Page,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct Plugin {
    pub(super) id: String,
    pub(super) version: String,
    pub(super) state: OperationalState,
    pub(super) capabilities: Vec<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct ReplicationResponse {
    pub(super) api_version: &'static str,
    pub(super) observed_at_unix_seconds: u64,
    pub(super) state: OperationalState,
    pub(super) coordinator: OperationalState,
    pub(super) replicas: Vec<Replica>,
    pub(super) page: Page,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct Replica {
    pub(super) id: String,
    pub(super) state: OperationalState,
}
