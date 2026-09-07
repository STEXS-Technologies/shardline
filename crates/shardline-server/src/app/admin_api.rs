use std::sync::Arc;

mod v1;

use axum::{
    Json,
    extract::{RawQuery, State},
    http::{
        HeaderMap, HeaderValue,
        header::{CACHE_CONTROL, CONTENT_SECURITY_POLICY, X_CONTENT_TYPE_OPTIONS},
    },
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::{
    ServerError, admission::weights, app::AppState, auth::authorize_static_bearer_token_hash,
    clock::unix_now_seconds_checked,
};

use self::v1::{
    API_VERSION as ADMIN_API_VERSION, GcResponse as AdminGcResponse,
    IntegrityResponse as AdminIntegrityResponse, MetricsResponse as AdminMetricsResponse,
    Node as AdminNode, NodesResponse as AdminNodesResponse, OperationalState, Page as AdminPage,
    Plugin as AdminPlugin, PluginsResponse as AdminPluginsResponse, Replica as AdminReplica,
    ReplicationResponse as AdminReplicationResponse, StatusResponse as AdminStatusResponse,
    StorageProcessCounters as AdminStorageProcessCounters, StorageResponse as AdminStorageResponse,
    Task as AdminTask, TasksResponse as AdminTasksResponse,
};

const NO_STORE: HeaderValue = HeaderValue::from_static("no-store");
const NOSNIFF: HeaderValue = HeaderValue::from_static("nosniff");
const API_CONTENT_SECURITY_POLICY: HeaderValue =
    HeaderValue::from_static("default-src 'none'; frame-ancestors 'none'");
const DEFAULT_PAGE_LIMIT: usize = 100;
const MAX_PAGE_LIMIT: usize = 1_000;
const MAX_ADMIN_QUERY_BYTES: usize = 4_096;
const MAX_ADMIN_FILTER_BYTES: usize = 128;
const MAX_ADMIN_CURSOR_BYTES: usize = 1_024;

type AdminJson<T> = (HeaderMap, Json<T>);

impl OperationalState {
    fn parse(value: &str) -> Result<Self, ServerError> {
        match value {
            "ready" => Ok(Self::Ready),
            "degraded" => Ok(Self::Degraded),
            "external" => Ok(Self::External),
            "unsupported" => Ok(Self::Unsupported),
            _ => Err(ServerError::InvalidAdminQuery),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AdminPageLimit(usize);

impl AdminPageLimit {
    fn parse(value: &str) -> Result<Self, ServerError> {
        let value = value
            .parse::<usize>()
            .map_err(|_error| ServerError::InvalidAdminQuery)?;
        if !(1..=MAX_PAGE_LIMIT).contains(&value) {
            return Err(ServerError::InvalidAdminQuery);
        }
        Ok(Self(value))
    }

    const fn get(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
struct AdminPrefix(String);

impl AdminPrefix {
    fn new(value: String) -> Result<Self, ServerError> {
        if value.len() > MAX_ADMIN_FILTER_BYTES || value.chars().any(char::is_control) {
            return Err(ServerError::InvalidAdminQuery);
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
struct AdminCapability(String);

impl AdminCapability {
    fn new(value: String) -> Result<Self, ServerError> {
        if value.is_empty()
            || value.len() > MAX_ADMIN_FILTER_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(ServerError::InvalidAdminQuery);
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AdminCursorToken(String);

impl AdminCursorToken {
    fn new(value: String) -> Result<Self, ServerError> {
        if value.is_empty() || value.len() > MAX_ADMIN_CURSOR_BYTES {
            return Err(ServerError::InvalidAdminQuery);
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
struct AdminItemKey(String);

impl AdminItemKey {
    fn new(value: String) -> Result<Self, ServerError> {
        if value.is_empty()
            || value.len() > MAX_ADMIN_FILTER_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(ServerError::InvalidAdminQuery);
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct AdminCollectionQuery {
    limit: Option<AdminPageLimit>,
    cursor: Option<AdminCursorToken>,
    state: Option<OperationalState>,
    prefix: Option<AdminPrefix>,
    capability: Option<AdminCapability>,
}

#[derive(Debug, Clone, Copy)]
struct AllowedFilters {
    state: bool,
    prefix: bool,
    capability: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct AdminCursor {
    version: u8,
    after: AdminItemKey,
    state: Option<OperationalState>,
    prefix: Option<AdminPrefix>,
    capability: Option<AdminCapability>,
}

fn authorize_admin(state: &AppState, headers: &HeaderMap) -> Result<(), ServerError> {
    let expected_hash = state
        .config
        .admin_read_token_sha256()
        .ok_or(ServerError::NotFound)?;
    authorize_static_bearer_token_hash(headers, expected_hash)
}

fn observed_at() -> Result<u64, ServerError> {
    unix_now_seconds_checked()
}

fn admin_json<T>(value: T) -> AdminJson<T> {
    let mut headers = HeaderMap::new();
    headers.insert(CACHE_CONTROL, NO_STORE);
    headers.insert(X_CONTENT_TYPE_OPTIONS, NOSNIFF);
    headers.insert(CONTENT_SECURITY_POLICY, API_CONTENT_SECURITY_POLICY);
    (headers, Json(value))
}

fn validate_raw_query(raw_query: Option<&str>) -> Result<&str, ServerError> {
    let raw_query = raw_query.unwrap_or_default();
    if raw_query.len() > MAX_ADMIN_QUERY_BYTES {
        return Err(ServerError::RequestQueryTooLarge);
    }
    let bytes = raw_query.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut encoded = bytes.iter().copied();
    while let Some(byte) = encoded.next() {
        if byte == b'%' {
            let Some(high_byte) = encoded.next() else {
                return Err(ServerError::InvalidAdminQuery);
            };
            let Some(low_byte) = encoded.next() else {
                return Err(ServerError::InvalidAdminQuery);
            };
            let high = (high_byte as char)
                .to_digit(16)
                .ok_or(ServerError::InvalidAdminQuery)?;
            let low = (low_byte as char)
                .to_digit(16)
                .ok_or(ServerError::InvalidAdminQuery)?;
            decoded.push(
                u8::try_from((high << 4) | low).map_err(|_error| ServerError::InvalidAdminQuery)?,
            );
        } else {
            decoded.push(byte);
        }
    }
    std::str::from_utf8(&decoded).map_err(|_error| ServerError::InvalidAdminQuery)?;
    Ok(raw_query)
}

fn reject_query(raw_query: Option<&str>) -> Result<(), ServerError> {
    if validate_raw_query(raw_query)?.is_empty() {
        Ok(())
    } else {
        Err(ServerError::InvalidAdminQuery)
    }
}

fn parse_collection_query(
    raw_query: Option<&str>,
    allowed: AllowedFilters,
) -> Result<AdminCollectionQuery, ServerError> {
    let raw_query = validate_raw_query(raw_query)?;
    let mut query = AdminCollectionQuery::default();
    let mut seen = std::collections::HashSet::new();
    for (name, value) in url::form_urlencoded::parse(raw_query.as_bytes()) {
        if name.is_empty() || !seen.insert(name.to_string()) {
            return Err(ServerError::InvalidAdminQuery);
        }
        if value.chars().any(char::is_control) {
            return Err(ServerError::InvalidAdminQuery);
        }
        match name.as_ref() {
            "limit" => {
                query.limit = Some(AdminPageLimit::parse(&value)?);
            }
            "cursor" => {
                query.cursor = Some(AdminCursorToken::new(value.into_owned())?);
            }
            "state" if allowed.state => query.state = Some(OperationalState::parse(&value)?),
            "prefix" if allowed.prefix => {
                query.prefix = Some(AdminPrefix::new(value.into_owned())?);
            }
            "capability" if allowed.capability => {
                query.capability = Some(AdminCapability::new(value.into_owned())?);
            }
            _ => return Err(ServerError::InvalidAdminQuery),
        }
    }
    Ok(query)
}

#[cfg(feature = "fuzzing")]
pub(crate) fn parse_admin_query_for_fuzzing(raw_query: &str) -> Result<(), ServerError> {
    let query = parse_collection_query(
        Some(raw_query),
        AllowedFilters {
            state: true,
            prefix: true,
            capability: true,
        },
    )?;
    if let Some(cursor) = query.cursor.as_ref() {
        decode_cursor(cursor.as_str(), &query)?;
    }
    Ok(())
}

#[cfg(feature = "fuzzing")]
pub(crate) fn parse_admin_cursor_for_fuzzing(bytes: &[u8]) -> Result<(), ServerError> {
    if bytes.len() > MAX_ADMIN_CURSOR_BYTES {
        return Err(ServerError::InvalidAdminQuery);
    }
    let encoded = URL_SAFE_NO_PAD.encode(bytes);
    let query = AdminCollectionQuery::default();
    decode_cursor(&encoded, &query).map(|_cursor| ())
}

fn encode_cursor(after: &str, query: &AdminCollectionQuery) -> Result<String, ServerError> {
    let cursor = AdminCursor {
        version: 1,
        after: AdminItemKey::new(after.to_owned())?,
        state: query.state,
        prefix: query.prefix.clone(),
        capability: query.capability.clone(),
    };
    let bytes = serde_json::to_vec(&cursor)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_cursor(encoded: &str, query: &AdminCollectionQuery) -> Result<AdminCursor, ServerError> {
    if encoded.len() > MAX_ADMIN_CURSOR_BYTES {
        return Err(ServerError::InvalidAdminQuery);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_error| ServerError::InvalidAdminQuery)?;
    if bytes.len() > MAX_ADMIN_CURSOR_BYTES {
        return Err(ServerError::InvalidAdminQuery);
    }
    let cursor: AdminCursor =
        serde_json::from_slice(&bytes).map_err(|_error| ServerError::InvalidAdminQuery)?;
    if cursor.version != 1
        || AdminItemKey::new(cursor.after.0.clone()).is_err()
        || cursor.state != query.state
        || cursor.prefix != query.prefix
        || cursor.capability != query.capability
        || cursor
            .prefix
            .as_ref()
            .is_some_and(|value| AdminPrefix::new(value.0.clone()).is_err())
        || cursor
            .capability
            .as_ref()
            .is_some_and(|value| AdminCapability::new(value.0.clone()).is_err())
    {
        return Err(ServerError::InvalidAdminQuery);
    }
    Ok(cursor)
}

fn paginate<T, Key, State, Capability>(
    mut items: Vec<T>,
    query: &AdminCollectionQuery,
    key: Key,
    state: State,
    has_capability: Capability,
) -> Result<(Vec<T>, AdminPage), ServerError>
where
    Key: Fn(&T) -> &str,
    State: Fn(&T) -> OperationalState,
    Capability: Fn(&T, &str) -> bool,
{
    items.retain(|item| {
        query.state.is_none_or(|expected| state(item) == expected)
            && query
                .prefix
                .as_ref()
                .is_none_or(|prefix| key(item).starts_with(prefix.as_str()))
            && query
                .capability
                .as_ref()
                .is_none_or(|capability| has_capability(item, capability.as_str()))
    });
    items.sort_by(|left, right| key(left).cmp(key(right)));

    let after = query
        .cursor
        .as_ref()
        .map(|cursor| decode_cursor(cursor.as_str(), query))
        .transpose()?
        .map(|cursor| cursor.after);
    let start = after.as_ref().map_or(0, |after| {
        items.partition_point(|item| key(item) <= after.as_str())
    });
    let limit = query.limit.map_or(DEFAULT_PAGE_LIMIT, AdminPageLimit::get);
    let has_more = items.len().saturating_sub(start) > limit;
    let page_items: Vec<T> = items.into_iter().skip(start).take(limit).collect();
    let next_cursor = if has_more {
        page_items
            .last()
            .map(|item| encode_cursor(key(item), query))
            .transpose()?
    } else {
        None
    };
    let page = AdminPage {
        limit,
        returned: page_items.len(),
        next_cursor,
    };
    Ok((page_items, page))
}

fn frontends(state: &AppState) -> Vec<String> {
    state
        .config
        .server_frontends()
        .iter()
        .map(|frontend| frontend.as_str().to_owned())
        .collect()
}

pub(super) async fn status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<AdminJson<AdminStatusResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    reject_query(raw_query.as_deref())?;
    let ready = state.backend.ready().await.is_ok();
    let cache_ready =
        !state.role.uses_reconstruction_cache() || state.reconstruction_cache.ready().await.is_ok();
    Ok(admin_json(AdminStatusResponse {
        api_version: ADMIN_API_VERSION,
        shardline_version: env!("CARGO_PKG_VERSION"),
        observed_at_unix_seconds: observed_at()?,
        state: if ready {
            OperationalState::Ready
        } else {
            OperationalState::Degraded
        },
        durable_storage_state: if ready {
            OperationalState::Ready
        } else {
            OperationalState::Degraded
        },
        cache_state: if cache_ready {
            OperationalState::Ready
        } else {
            OperationalState::Degraded
        },
        server_role: state.role.as_str().to_owned(),
        server_frontends: frontends(&state),
        metadata_backend: state.backend.backend_name().to_owned(),
        object_backend: state.backend.object_backend_name().to_owned(),
        cache_backend: state.reconstruction_cache.backend_name().to_owned(),
        plugin_registry: OperationalState::Unsupported,
    }))
}

pub(super) async fn storage(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<AdminJson<AdminStorageResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    reject_query(raw_query.as_deref())?;
    let _permit = state
        .admission
        .try_acquire(weights::STATS)
        .ok_or(ServerError::WorkQueueSaturated)?;
    let authoritative = state.backend.stats().await?;
    let metrics = shardline_metrics::metrics();
    Ok(admin_json(AdminStorageResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        authoritative,
        process_lifetime: AdminStorageProcessCounters {
            objects_written: metrics.storage.objects_total.get().try_into()?,
            object_bytes_written: metrics.storage.objects_bytes_total.get(),
            xorbs_written: metrics.storage.xorbs_total.get().try_into()?,
            xorb_bytes_written: metrics.storage.xorbs_bytes_total.get(),
            shards_written: metrics.storage.shards_total.get(),
            deduplicated_bytes: metrics.storage.dedup_saves_bytes_total.get(),
            compression_saved_bytes: metrics.storage.compression_saved_bytes_total.get(),
        },
        deduplication_ratio_state: OperationalState::Unsupported,
        // Exact logical bytes are not currently retained as an authoritative
        // aggregate. Returning null is safer than deriving a misleading ratio
        // from process-lifetime counters after restarts or across replicas.
        deduplication_ratio: None,
    }))
}

pub(super) async fn gc(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<AdminJson<AdminGcResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    reject_query(raw_query.as_deref())?;
    let metrics = shardline_metrics::metrics();
    Ok(admin_json(AdminGcResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        state: OperationalState::External,
        execution: OperationalState::External,
        runs_observed_by_process: metrics.gc.runs.get(),
        objects_collected_by_process: metrics.gc.objects_collected.get(),
        bytes_collected_by_process: metrics.gc.bytes_collected.get(),
    }))
}

pub(super) async fn integrity(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<AdminJson<AdminIntegrityResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    reject_query(raw_query.as_deref())?;
    let metrics = shardline_metrics::metrics();
    Ok(admin_json(AdminIntegrityResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        state: OperationalState::External,
        execution: OperationalState::External,
        fsck_runs_observed_by_process: metrics.fsck.runs.get(),
        errors_observed_by_process: metrics.fsck.errors_found.get(),
    }))
}

pub(super) async fn nodes(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<AdminJson<AdminNodesResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    let query = parse_collection_query(
        raw_query.as_deref(),
        AllowedFilters {
            state: true,
            prefix: true,
            capability: false,
        },
    )?;
    let ready = state.backend.ready().await.is_ok();
    let (nodes, page) = paginate(
        vec![AdminNode {
            scope: "current_process",
            state: if ready {
                OperationalState::Ready
            } else {
                OperationalState::Degraded
            },
            server_role: state.role.as_str().to_owned(),
            server_frontends: frontends(&state),
            bind_addr: state.config.bind_addr().to_string(),
            bin_version: env!("CARGO_PKG_VERSION").to_owned(),
            metadata_backend: state.backend.backend_name().to_owned(),
            object_backend: state.backend.object_backend_name().to_owned(),
            cache_backend: state.reconstruction_cache.backend_name().to_owned(),
        }],
        &query,
        |node| node.scope,
        |node| node.state,
        |_node, _capability| false,
    )?;
    Ok(admin_json(AdminNodesResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        discovery: OperationalState::Unsupported,
        nodes,
        page,
    }))
}

pub(super) async fn tasks(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<AdminJson<AdminTasksResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    let query = parse_collection_query(
        raw_query.as_deref(),
        AllowedFilters {
            state: true,
            prefix: true,
            capability: false,
        },
    )?;
    let after = query
        .cursor
        .as_ref()
        .map(|cursor| decode_cursor(cursor.as_str(), &query))
        .transpose()?
        .map(|cursor| cursor.after);
    let after_session_id = after.as_ref().map(|a| a.as_str());
    let prefix = query.prefix.as_ref().map(AdminPrefix::as_str);
    let limit = query.limit.map_or(DEFAULT_PAGE_LIMIT, AdminPageLimit::get);
    let sessions = match state
        .backend
        .list_inflight_resumable_sessions(after_session_id, prefix, limit.saturating_add(1))
        .await
    {
        Ok(sessions) => sessions,
        Err(ServerError::StaleResourceFence) => Vec::new(),
        Err(error) => return Err(error),
    };
    let has_more = sessions.len() > limit;
    let sessions = if has_more {
        sessions.get(..limit).unwrap_or(&sessions)
    } else {
        &sessions
    };
    let tasks: Vec<AdminTask> = sessions
        .iter()
        .map(|session| AdminTask {
            id: session.session_id().to_owned(),
            state: OperationalState::Ready,
        })
        .filter(|task| query.state.is_none_or(|expected| task.state == expected))
        .collect();
    let next_cursor = if has_more && !tasks.is_empty() {
        tasks
            .last()
            .map(|task| encode_cursor(&task.id, &query))
            .transpose()?
    } else {
        None
    };
    let page = AdminPage {
        limit,
        returned: tasks.len(),
        next_cursor,
    };
    Ok(admin_json(AdminTasksResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        scheduler: OperationalState::External,
        tasks,
        page,
    }))
}

pub(super) async fn metrics(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<AdminJson<AdminMetricsResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    reject_query(raw_query.as_deref())?;
    let metrics = shardline_metrics::metrics();
    Ok(admin_json(AdminMetricsResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        prometheus_path: "/metrics",
        active_connections: metrics.system.active_connections.get(),
        admitted_requests: metrics.system.admitted_total.get(),
        queued_requests: metrics.system.queued_total.get(),
        rejected_requests: metrics.system.rejected_total.get(),
        upload_requests: metrics.transfer.upload_requests.get(),
        upload_bytes: metrics.transfer.upload_bytes.get(),
        download_requests: metrics.transfer.download_requests.get(),
        download_bytes: metrics.transfer.download_bytes.get(),
        range_requests: metrics.transfer.range_requests.get(),
    }))
}

pub(super) async fn plugins(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<AdminJson<AdminPluginsResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    let query = parse_collection_query(
        raw_query.as_deref(),
        AllowedFilters {
            state: true,
            prefix: true,
            capability: true,
        },
    )?;
    let (plugins, page) = paginate(
        Vec::new(),
        &query,
        |plugin: &AdminPlugin| plugin.id.as_str(),
        |plugin| plugin.state,
        |plugin, capability| {
            plugin
                .capabilities
                .iter()
                .any(|candidate| candidate == capability)
        },
    )?;
    Ok(admin_json(AdminPluginsResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        registry: OperationalState::Unsupported,
        plugins,
        page,
    }))
}

pub(super) async fn replication(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<AdminJson<AdminReplicationResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    let query = parse_collection_query(
        raw_query.as_deref(),
        AllowedFilters {
            state: true,
            prefix: true,
            capability: false,
        },
    )?;
    let (replicas, page) = paginate(
        Vec::new(),
        &query,
        |replica: &AdminReplica| replica.id.as_str(),
        |replica| replica.state,
        |_replica, _capability| false,
    )?;
    Ok(admin_json(AdminReplicationResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        // Shardline coordinates writers over shared durable state. It does not
        // own an asynchronous replication controller whose lag could be
        // reported authoritatively, so keep this surface explicit and empty.
        state: OperationalState::External,
        coordinator: OperationalState::External,
        replicas,
        page,
    }))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request, StatusCode, header::AUTHORIZATION},
    };
    use proptest::prelude::*;
    use serde_json::Value;
    use tempfile::TempDir;
    use tower::ServiceExt;

    use super::*;
    use crate::{DeploymentMode, ServerConfig, ServerFrontend, ServerRole, app::router};

    const ADMIN_TOKEN: &str = "admin-read-secret";
    const ADMIN_PATHS: [&str; 9] = [
        "/api/v1/status",
        "/api/v1/storage",
        "/api/v1/gc",
        "/api/v1/integrity",
        "/api/v1/nodes",
        "/api/v1/tasks",
        "/api/v1/metrics",
        "/api/v1/plugins",
        "/api/v1/replication",
    ];

    proptest! {
        #[test]
        fn arbitrary_collection_queries_are_deterministic_and_never_panic(raw in any::<String>()) {
            let allowed = AllowedFilters {
                state: true,
                prefix: true,
                capability: true,
            };
            let first = parse_collection_query(Some(&raw), allowed);
            let second = parse_collection_query(Some(&raw), allowed);
            prop_assert_eq!(format!("{first:?}"), format!("{second:?}"));
        }

        #[test]
        fn arbitrary_v1_cursor_dtos_are_deterministic_and_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..=MAX_ADMIN_CURSOR_BYTES)) {
            let encoded = URL_SAFE_NO_PAD.encode(bytes);
            let query = AdminCollectionQuery::default();
            let first = decode_cursor(&encoded, &query);
            let second = decode_cursor(&encoded, &query);
            prop_assert_eq!(format!("{first:?}"), format!("{second:?}"));
        }
    }

    async fn app_with_config(
        admin_token: Option<&str>,
        metrics_token: Option<&str>,
        role: ServerRole,
    ) -> (axum::Router, TempDir) {
        let temp = TempDir::new().expect("temp dir");
        let mut config = ServerConfig::new(
            "127.0.0.1:0".parse().expect("bind address"),
            "http://127.0.0.1:8080".to_owned(),
            temp.path().to_path_buf(),
            NonZeroUsize::new(65_536).expect("chunk size"),
        )
        .with_server_frontends([ServerFrontend::Xet])
        .expect("frontends")
        .with_server_role(role);
        if let Some(token) = metrics_token {
            config = config
                .with_metrics_token(token.as_bytes().to_vec())
                .expect("metrics token");
        }
        if let Some(token) = admin_token {
            config = config
                .with_admin_read_token(token.as_bytes().to_vec())
                .expect("admin token");
        }
        (router(config).await.expect("router"), temp)
    }

    async fn app(admin_token: Option<&str>) -> (axum::Router, TempDir) {
        app_with_config(admin_token, None, ServerRole::All).await
    }

    fn request(method: Method, path: &str, token: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(path);
        if let Some(token) = token {
            builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        builder.body(Body::empty()).expect("request")
    }

    async fn json_body(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), 1_048_576)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("JSON response")
    }

    #[tokio::test]
    async fn disabled_admin_api_hides_every_endpoint() {
        let (app, _temp) = app(None).await;
        for path in ADMIN_PATHS {
            let response = app
                .clone()
                .oneshot(request(Method::GET, path, Some(ADMIN_TOKEN)))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[tokio::test]
    async fn admin_api_rejects_missing_and_wrong_credentials() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        for token in [None, Some("wrong-token")] {
            let response = app
                .clone()
                .oneshot(request(Method::GET, ADMIN_PATHS[0], token))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }

    #[tokio::test]
    async fn every_admin_endpoint_is_read_only_and_returns_versioned_json() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        for path in ADMIN_PATHS {
            let get_response = app
                .clone()
                .oneshot(request(Method::GET, path, Some(ADMIN_TOKEN)))
                .await
                .expect("GET response");
            assert_eq!(get_response.status(), StatusCode::OK, "{path}");
            assert_eq!(
                get_response.headers().get(CACHE_CONTROL),
                Some(&NO_STORE),
                "{path} must not be cached"
            );
            let body = json_body(get_response).await;
            assert_eq!(body["api_version"], ADMIN_API_VERSION, "{path}");
            if [
                "/api/v1/nodes",
                "/api/v1/tasks",
                "/api/v1/plugins",
                "/api/v1/replication",
            ]
            .contains(&path)
            {
                assert_eq!(body["page"]["limit"], DEFAULT_PAGE_LIMIT, "{path}");
                assert!(body["page"]["returned"].is_number(), "{path}");
                assert!(body["page"]["next_cursor"].is_null(), "{path}");
            }

            let head_response = app
                .clone()
                .oneshot(request(Method::HEAD, path, Some(ADMIN_TOKEN)))
                .await
                .expect("HEAD response");
            assert_eq!(head_response.status(), StatusCode::OK, "{path}");
            assert_eq!(head_response.headers().get(CACHE_CONTROL), Some(&NO_STORE));
            assert!(
                to_bytes(head_response.into_body(), 1)
                    .await
                    .expect("HEAD body")
                    .is_empty(),
                "{path} HEAD must not return a body"
            );

            for method in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
                let mutation_response = app
                    .clone()
                    .oneshot(request(method.clone(), path, Some(ADMIN_TOKEN)))
                    .await
                    .expect("mutation response");
                assert_eq!(
                    mutation_response.status(),
                    StatusCode::METHOD_NOT_ALLOWED,
                    "{path} must not expose {method}"
                );
            }
        }
    }

    #[tokio::test]
    async fn admin_api_rejects_ambiguous_authorization_headers() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/status")
            .header(AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
            .header(AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
            .body(Body::empty())
            .expect("request");
        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_and_metrics_tokens_are_not_interchangeable() {
        let metrics_token = "metrics-only-secret";
        let (app, _temp) =
            app_with_config(Some(ADMIN_TOKEN), Some(metrics_token), ServerRole::All).await;

        let admin_with_metrics_token = app
            .clone()
            .oneshot(request(Method::GET, "/api/v1/status", Some(metrics_token)))
            .await
            .expect("admin response");
        assert_eq!(admin_with_metrics_token.status(), StatusCode::UNAUTHORIZED);

        let metrics_with_admin_token = app
            .oneshot(request(Method::GET, "/metrics", Some(ADMIN_TOKEN)))
            .await
            .expect("metrics response");
        assert_eq!(metrics_with_admin_token.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_api_is_available_on_each_runtime_role() {
        for role in [ServerRole::All, ServerRole::Api, ServerRole::Transfer] {
            let (app, _temp) = app_with_config(Some(ADMIN_TOKEN), None, role).await;
            let response = app
                .oneshot(request(Method::GET, "/api/v1/status", Some(ADMIN_TOKEN)))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK, "role={role:?}");
            let body = json_body(response).await;
            assert_eq!(body["server_role"], role.as_str());
        }
    }

    #[tokio::test]
    async fn readiness_failure_is_reported_as_degraded_without_internal_details() {
        let (app, temp) = app(Some(ADMIN_TOKEN)).await;
        tokio::fs::remove_file(temp.path().join("metadata.sqlite3"))
            .await
            .expect("remove metadata database");
        tokio::fs::create_dir(temp.path().join("metadata.sqlite3"))
            .await
            .expect("replace metadata database with directory");

        let response = app
            .oneshot(request(Method::GET, "/api/v1/status", Some(ADMIN_TOKEN)))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["state"], "degraded");
        assert_eq!(body["durable_storage_state"], "degraded");
        let encoded = body.to_string();
        assert!(!encoded.contains("sqlite"));
        assert!(!encoded.contains("metadata.sqlite3"));
    }

    #[tokio::test]
    async fn admin_status_reports_current_runtime_without_secret_material() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        let response = app
            .oneshot(request(Method::GET, "/api/v1/status", Some(ADMIN_TOKEN)))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["state"], "ready");
        assert_eq!(body["durable_storage_state"], "ready");
        assert_eq!(body["cache_state"], "ready");
        assert_eq!(body["server_role"], "all");
        assert_eq!(body["server_frontends"], serde_json::json!(["xet"]));
        assert_eq!(body["plugin_registry"], "unsupported");
        assert!(!body.to_string().contains(ADMIN_TOKEN));
    }

    #[tokio::test]
    async fn storage_reports_authoritative_physical_usage_and_honest_ratio_availability() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        let response = app
            .oneshot(request(Method::GET, "/api/v1/storage", Some(ADMIN_TOKEN)))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["authoritative"]["objects"], 0);
        assert_eq!(body["authoritative"]["object_bytes"], 0);
        assert_eq!(body["authoritative"]["chunks"], 0);
        assert_eq!(body["authoritative"]["files"], 0);
        assert_eq!(body["deduplication_ratio_state"], "unsupported");
        assert!(body["deduplication_ratio"].is_null());
    }

    #[tokio::test]
    async fn externally_owned_operations_never_claim_in_process_progress() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        for path in ["/api/v1/gc", "/api/v1/integrity"] {
            let response = app
                .clone()
                .oneshot(request(Method::GET, path, Some(ADMIN_TOKEN)))
                .await
                .expect("response");
            let body = json_body(response).await;
            assert_eq!(body["state"], "external", "{path}");
            assert_eq!(body["execution"], "external", "{path}");
        }
        for path in ["/api/v1/tasks", "/api/v1/replication"] {
            let response = app
                .clone()
                .oneshot(request(Method::GET, path, Some(ADMIN_TOKEN)))
                .await
                .expect("response");
            let body = json_body(response).await;
            assert_eq!(
                body.get("scheduler").or_else(|| body.get("coordinator")),
                Some(&Value::String("external".to_owned())),
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn admin_routes_coexist_with_hub_catch_all_routes() {
        let temp = TempDir::new().expect("temp dir");
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().expect("bind address"),
            "http://127.0.0.1:8080".to_owned(),
            temp.path().to_path_buf(),
            NonZeroUsize::new(65_536).expect("chunk size"),
        )
        .with_server_frontends([ServerFrontend::Hub])
        .expect("frontends")
        .with_deployment_mode(DeploymentMode::Insecure)
        .with_admin_read_token(ADMIN_TOKEN.as_bytes().to_vec())
        .expect("admin token");
        let app = router(config).await.expect("router");

        let response = app
            .oneshot(request(Method::GET, "/api/v1/status", Some(ADMIN_TOKEN)))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(json_body(response).await["api_version"], "v1");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_admin_polling_remains_bounded_and_successful() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        let mut requests = tokio::task::JoinSet::new();
        for index in 0..128 {
            let app = app.clone();
            let path = ADMIN_PATHS[index % ADMIN_PATHS.len()];
            requests.spawn(async move {
                app.oneshot(request(Method::GET, path, Some(ADMIN_TOKEN)))
                    .await
                    .map(|response| response.status())
            });
        }
        while let Some(response) = requests.join_next().await {
            let status = response.expect("poll task").expect("poll response");
            assert!(
                status == StatusCode::OK || status == StatusCode::SERVICE_UNAVAILABLE,
                "unexpected bounded-poll status {status}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn polling_and_cancelled_polls_do_not_mutate_durable_inventory() {
        let temp = TempDir::new().expect("temp dir");
        let make_config = || {
            ServerConfig::new(
                "127.0.0.1:0".parse().expect("bind address"),
                "http://127.0.0.1:8080".to_owned(),
                temp.path().to_path_buf(),
                NonZeroUsize::new(65_536).expect("chunk size"),
            )
            .with_server_frontends([ServerFrontend::Xet])
            .expect("frontends")
            .with_admin_read_token(ADMIN_TOKEN.as_bytes().to_vec())
            .expect("admin token")
        };
        let app = router(make_config()).await.expect("first router");
        let before = json_body(
            app.clone()
                .oneshot(request(Method::GET, "/api/v1/storage", Some(ADMIN_TOKEN)))
                .await
                .expect("initial inventory"),
        )
        .await["authoritative"]
            .clone();

        let mut polls = tokio::task::JoinSet::new();
        for index in 0..256 {
            let app = app.clone();
            let path = ADMIN_PATHS[index % ADMIN_PATHS.len()];
            polls.spawn(async move {
                app.oneshot(request(Method::GET, path, Some(ADMIN_TOKEN)))
                    .await
            });
        }
        for _ in 0..64 {
            polls.abort_all();
            tokio::task::yield_now().await;
        }
        while polls.join_next().await.is_some() {}

        drop(app);
        let restarted = router(make_config()).await.expect("restarted router");
        let after = json_body(
            restarted
                .oneshot(request(Method::GET, "/api/v1/storage", Some(ADMIN_TOKEN)))
                .await
                .expect("inventory after restart"),
        )
        .await["authoritative"]
            .clone();
        assert_eq!(
            after, before,
            "read-only polling must not mutate durable state"
        );
    }

    #[tokio::test]
    async fn collection_filters_are_bounded_typed_and_not_reflected() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        let matching = app
            .clone()
            .oneshot(request(
                Method::GET,
                "/api/v1/nodes?limit=1&state=ready&prefix=current",
                Some(ADMIN_TOKEN),
            ))
            .await
            .expect("matching response");
        assert_eq!(matching.status(), StatusCode::OK);
        let body = json_body(matching).await;
        assert_eq!(body["nodes"].as_array().map(Vec::len), Some(1));
        assert_eq!(body["page"]["limit"], 1);
        assert_eq!(body["page"]["returned"], 1);

        let injection = "%27%20OR%201%3D1%20--%3Cscript%3E";
        let filtered = app
            .clone()
            .oneshot(request(
                Method::GET,
                &format!("/api/v1/nodes?prefix={injection}"),
                Some(ADMIN_TOKEN),
            ))
            .await
            .expect("filtered response");
        assert_eq!(filtered.status(), StatusCode::OK);
        let body = json_body(filtered).await;
        assert_eq!(body["nodes"].as_array().map(Vec::len), Some(0));
        assert!(!body.to_string().contains("<script>"));

        let unsupported_filter = app
            .oneshot(request(
                Method::GET,
                "/api/v1/nodes?capability=storage.read",
                Some(ADMIN_TOKEN),
            ))
            .await
            .expect("unsupported-filter response");
        assert_eq!(unsupported_filter.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn nodes_entry_reports_real_identity_and_backend_fields() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        let response = app
            .oneshot(request(Method::GET, "/api/v1/nodes", Some(ADMIN_TOKEN)))
            .await
            .expect("nodes response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        let nodes = body["nodes"].as_array().expect("nodes array");
        assert_eq!(nodes.len(), 1, "expected exactly one current-process node");
        let entry = &nodes[0];
        assert_eq!(entry["scope"], "current_process");
        assert_eq!(entry["state"], "ready");
        assert!(
            !entry["bind_addr"].as_str().unwrap_or("").is_empty(),
            "bind_addr must be non-empty"
        );
        assert!(
            !entry["bin_version"].as_str().unwrap_or("").is_empty(),
            "bin_version must be non-empty"
        );
        assert!(
            !entry["metadata_backend"].as_str().unwrap_or("").is_empty(),
            "metadata_backend must be non-empty"
        );
        assert!(
            !entry["object_backend"].as_str().unwrap_or("").is_empty(),
            "object_backend must be non-empty"
        );
        assert!(
            !entry["cache_backend"].as_str().unwrap_or("").is_empty(),
            "cache_backend must be non-empty"
        );
        let encoded = body.to_string();
        assert!(!encoded.contains("/tmp/"), "must not leak temp-dir path");
        assert!(
            !encoded.contains("owner/"),
            "must not contain repository owner prefix"
        );
        assert_eq!(
            body["discovery"], "unsupported",
            "discovery must be unsupported"
        );
    }

    #[tokio::test]
    async fn query_validation_happens_after_concealment_and_authentication() {
        let malformed_path = "/api/v1/nodes?limit=invalid&limit=2";
        let (disabled, _temp) = app(None).await;
        let disabled_response = disabled
            .oneshot(request(Method::GET, malformed_path, Some(ADMIN_TOKEN)))
            .await
            .expect("disabled response");
        assert_eq!(disabled_response.status(), StatusCode::NOT_FOUND);

        let (enabled, _temp) = app(Some(ADMIN_TOKEN)).await;
        let unauthorized = enabled
            .clone()
            .oneshot(request(Method::GET, malformed_path, None))
            .await
            .expect("unauthorized response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let authorized = enabled
            .oneshot(request(Method::GET, malformed_path, Some(ADMIN_TOKEN)))
            .await
            .expect("authorized response");
        assert_eq!(authorized.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn malformed_oversized_and_polluted_queries_fail_closed() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        for path in [
            "/api/v1/nodes?limit=0",
            "/api/v1/nodes?limit=1001",
            "/api/v1/nodes?state=unknown",
            "/api/v1/nodes?unknown=value",
            "/api/v1/nodes?prefix=%ZZ",
            "/api/v1/nodes?prefix=%FF",
            "/api/v1/nodes?prefix=%0d%0aInjected%3Ayes",
            "/api/v1/nodes?cursor=not-base64",
            "/api/v1/status?limit=1",
        ] {
            let response = app
                .clone()
                .oneshot(request(Method::GET, path, Some(ADMIN_TOKEN)))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
        }

        let oversized = format!("/api/v1/nodes?prefix={}", "a".repeat(MAX_ADMIN_QUERY_BYTES));
        let response = app
            .oneshot(request(Method::GET, &oversized, Some(ADMIN_TOKEN)))
            .await
            .expect("oversized response");
        assert_eq!(response.status(), StatusCode::URI_TOO_LONG);
    }

    #[tokio::test]
    async fn security_headers_cover_success_auth_failure_and_disabled_responses() {
        for (configured, token, expected) in [
            (Some(ADMIN_TOKEN), Some(ADMIN_TOKEN), StatusCode::OK),
            (Some(ADMIN_TOKEN), Some("wrong"), StatusCode::UNAUTHORIZED),
            (None, Some(ADMIN_TOKEN), StatusCode::NOT_FOUND),
        ] {
            let (app, _temp) = app(configured).await;
            let response = app
                .oneshot(request(Method::GET, "/api/v1/status", token))
                .await
                .expect("response");
            assert_eq!(response.status(), expected);
            assert_eq!(response.headers().get(CACHE_CONTROL), Some(&NO_STORE));
            assert_eq!(
                response.headers().get(X_CONTENT_TYPE_OPTIONS),
                Some(&NOSNIFF)
            );
            assert_eq!(
                response.headers().get(CONTENT_SECURITY_POLICY),
                Some(&API_CONTENT_SECURITY_POLICY)
            );
            assert!(
                response
                    .headers()
                    .get("access-control-allow-credentials")
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn method_confusion_never_reaches_a_mutation_handler() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        for method in [
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::CONNECT,
            Method::TRACE,
        ] {
            let response = app
                .clone()
                .oneshot(request(method.clone(), "/api/v1/status", Some(ADMIN_TOKEN)))
                .await
                .expect("response");
            assert_eq!(
                response.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "{method}"
            );
        }

        let preflight = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/v1/status")
            .header("origin", "https://dashboard.example")
            .header("access-control-request-method", "GET")
            .body(Body::empty())
            .expect("preflight request");
        let response = app.oneshot(preflight).await.expect("preflight response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get("access-control-allow-credentials")
                .is_none()
        );
    }

    #[test]
    fn keyset_cursor_pages_stably_and_is_bound_to_filters() {
        let query = AdminCollectionQuery {
            limit: Some(AdminPageLimit(2)),
            state: Some(OperationalState::External),
            ..AdminCollectionQuery::default()
        };
        let tasks = vec![
            AdminTask {
                id: "task-c".to_owned(),
                state: OperationalState::External,
            },
            AdminTask {
                id: "task-a".to_owned(),
                state: OperationalState::External,
            },
            AdminTask {
                id: "task-b".to_owned(),
                state: OperationalState::External,
            },
        ];
        let (first, first_page) = paginate(
            tasks.clone(),
            &query,
            |task| task.id.as_str(),
            |task| task.state,
            |_task, _capability| false,
        )
        .expect("first page");
        assert_eq!(
            first
                .iter()
                .map(|task| task.id.as_str())
                .collect::<Vec<_>>(),
            ["task-a", "task-b"]
        );
        let mut second_query = query;
        second_query.cursor = first_page
            .next_cursor
            .map(AdminCursorToken::new)
            .transpose()
            .expect("valid generated cursor");
        let (second, second_page) = paginate(
            tasks,
            &second_query,
            |task| task.id.as_str(),
            |task| task.state,
            |_task, _capability| false,
        )
        .expect("second page");
        assert_eq!(
            second
                .iter()
                .map(|task| task.id.as_str())
                .collect::<Vec<_>>(),
            ["task-c"]
        );
        assert!(second_page.next_cursor.is_none());

        second_query.prefix = Some(AdminPrefix::new("different".to_owned()).expect("valid prefix"));
        assert!(matches!(
            paginate(
                vec![AdminTask {
                    id: "task-c".to_owned(),
                    state: OperationalState::External,
                }],
                &second_query,
                |task| task.id.as_str(),
                |task| task.state,
                |_task, _capability| false,
            ),
            Err(ServerError::InvalidAdminQuery)
        ));

        let invalid_newtype_cursor = URL_SAFE_NO_PAD.encode(
            br#"{"version":1,"after":"task-c","state":null,"prefix":"\u0000","capability":null}"#,
        );
        assert!(matches!(
            decode_cursor(&invalid_newtype_cursor, &AdminCollectionQuery::default()),
            Err(ServerError::InvalidAdminQuery)
        ));
    }

    #[tokio::test]
    async fn tasks_endpoint_lists_active_resumable_sessions() {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let temp = TempDir::new().expect("temp dir");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&database_url)
            .await
            .expect("connect");
        crate::apply_database_migrations(&pool)
            .await
            .expect("migrations");
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().expect("bind"),
            "http://127.0.0.1:8080".to_owned(),
            temp.path().to_path_buf(),
            NonZeroUsize::new(65_536).expect("chunk size"),
        )
        .with_server_frontends([ServerFrontend::Xet])
        .expect("frontends")
        .with_index_postgres_url(database_url)
        .expect("postgres url")
        .with_admin_read_token(ADMIN_TOKEN.as_bytes().to_vec())
        .expect("admin token");
        let app = router(config).await.expect("router");

        // Idempotency: clear rows from any prior run before seeding, since the
        // seeded session ids embed only the process id and persist in the DB.
        sqlx::query("DELETE FROM shardline_resumable_sessions WHERE session_id LIKE $1 || '%'")
            .bind("admin-task-")
            .execute(&pool)
            .await
            .expect("cleanup sessions");

        // Insert two active sessions and one terminal session.
        let expiry = chrono::Utc::now() + chrono::Duration::hours(1);
        let pid = std::process::id();
        let id_active_1 = format!("admin-task-active-1-{pid}");
        let id_active_2 = format!("admin-task-active-2-{pid}");
        let id_terminal = format!("admin-task-terminal-{pid}");
        for (sid, state) in [
            (&id_active_1, "active"),
            (&id_active_2, "active"),
            (&id_terminal, "completed"),
        ] {
            sqlx::query(
                "INSERT INTO shardline_resumable_sessions \
                 (session_id, protocol, scope_namespace, target_key, attributes_json, \
                  state, generation, fence_epoch, expires_at) \
                 VALUES ($1, 's3_multipart', 'owner/repo', 'key.bin', '{}', $2, 1, 1, $3)",
            )
            .bind(sid)
            .bind(state)
            .bind(expiry)
            .execute(&pool)
            .await
            .expect("insert session");
        }

        let response = app
            .oneshot(request(Method::GET, "/api/v1/tasks", Some(ADMIN_TOKEN)))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["scheduler"], "external");
        let tasks = body["tasks"].as_array().expect("tasks array");
        let ids: Vec<&str> = tasks.iter().map(|t| t["id"].as_str().unwrap()).collect();
        assert!(
            ids.contains(&id_active_1.as_str()),
            "active-1 must be present: {ids:?}"
        );
        assert!(
            ids.contains(&id_active_2.as_str()),
            "active-2 must be present: {ids:?}"
        );
        assert!(
            !ids.contains(&id_terminal.as_str()),
            "terminal session must not be surfaced: {ids:?}"
        );
        for task in tasks {
            assert_eq!(task["state"], "ready");
        }
        assert_eq!(body["page"]["limit"], DEFAULT_PAGE_LIMIT);
    }

    #[tokio::test]
    async fn tasks_endpoint_filters_and_paginates() {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let temp = TempDir::new().expect("temp dir");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&database_url)
            .await
            .expect("connect");
        crate::apply_database_migrations(&pool)
            .await
            .expect("migrations");
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().expect("bind"),
            "http://127.0.0.1:8080".to_owned(),
            temp.path().to_path_buf(),
            NonZeroUsize::new(65_536).expect("chunk size"),
        )
        .with_server_frontends([ServerFrontend::Xet])
        .expect("frontends")
        .with_index_postgres_url(database_url)
        .expect("postgres url")
        .with_admin_read_token(ADMIN_TOKEN.as_bytes().to_vec())
        .expect("admin token");
        let app = router(config).await.expect("router");

        // Idempotency: clear rows from any prior run before seeding.
        sqlx::query("DELETE FROM shardline_resumable_sessions WHERE session_id LIKE $1 || '%'")
            .bind("admin-paginate-")
            .execute(&pool)
            .await
            .expect("cleanup sessions");

        let expiry = chrono::Utc::now() + chrono::Duration::hours(1);
        let pid = std::process::id();
        for i in 0..4u32 {
            let sid = format!("admin-paginate-{i}-{pid}");
            sqlx::query(
                "INSERT INTO shardline_resumable_sessions \
                 (session_id, protocol, scope_namespace, target_key, attributes_json, \
                  state, generation, fence_epoch, expires_at) \
                 VALUES ($1, 's3_multipart', 'owner/repo', 'key.bin', '{}', 'active', 1, 1, $2)",
            )
            .bind(&sid)
            .bind(expiry)
            .execute(&pool)
            .await
            .expect("insert session");
        }
        // Also insert a terminal session with the same prefix to ensure it is filtered.
        let terminal_id = format!("admin-paginate-terminal-{pid}");
        sqlx::query(
            "INSERT INTO shardline_resumable_sessions \
             (session_id, protocol, scope_namespace, target_key, attributes_json, \
              state, generation, fence_epoch, expires_at) \
             VALUES ($1, 's3_multipart', 'owner/repo', 'key.bin', '{}', 'completed', 1, 1, $2)",
        )
        .bind(&terminal_id)
        .bind(expiry)
        .execute(&pool)
        .await
        .expect("insert terminal session");

        // Page 1: limit=2 with prefix filter.
        let url = "/api/v1/tasks?limit=2&prefix=admin-paginate-".to_owned();
        let response = app
            .clone()
            .oneshot(request(Method::GET, &url, Some(ADMIN_TOKEN)))
            .await
            .expect("page 1");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        let tasks = body["tasks"].as_array().expect("tasks array");
        assert_eq!(tasks.len(), 2, "page 1 must return 2 tasks");
        for task in tasks {
            assert_eq!(task["state"], "ready");
            assert!(
                task["id"].as_str().unwrap().starts_with("admin-paginate-"),
                "task id must match prefix: {}",
                task["id"]
            );
        }
        let next_cursor = body["page"]["next_cursor"]
            .as_str()
            .expect("next_cursor present");
        assert_eq!(body["page"]["returned"], 2);

        // Page 2 via cursor.
        let url2 = format!("/api/v1/tasks?limit=2&prefix=admin-paginate-&cursor={next_cursor}",);
        let response2 = app
            .clone()
            .oneshot(request(Method::GET, &url2, Some(ADMIN_TOKEN)))
            .await
            .expect("page 2");
        assert_eq!(response2.status(), StatusCode::OK);
        let body2 = json_body(response2).await;
        let tasks2 = body2["tasks"].as_array().expect("tasks array");
        assert!(tasks2.len() <= 2, "page 2 must return at most 2 tasks");
        assert!(
            !tasks2.is_empty(),
            "page 2 must not be empty with 4 active sessions"
        );

        // State filter: query for "degraded" should yield zero results
        // (no session maps to degraded).
        let state_url = "/api/v1/tasks?state=degraded&prefix=admin-paginate-".to_owned();
        let state_response = app
            .clone()
            .oneshot(request(Method::GET, &state_url, Some(ADMIN_TOKEN)))
            .await
            .expect("state filter");
        assert_eq!(state_response.status(), StatusCode::OK);
        let state_body = json_body(state_response).await;
        assert_eq!(state_body["tasks"].as_array().map(Vec::len), Some(0));
    }

    #[tokio::test]
    async fn tasks_endpoint_pagination_unaffected_by_terminal_interleaving() {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let temp = TempDir::new().expect("temp dir");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&database_url)
            .await
            .expect("connect");
        crate::apply_database_migrations(&pool)
            .await
            .expect("migrations");
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().expect("bind"),
            "http://127.0.0.1:8080".to_owned(),
            temp.path().to_path_buf(),
            NonZeroUsize::new(65_536).expect("chunk size"),
        )
        .with_server_frontends([ServerFrontend::Xet])
        .expect("frontends")
        .with_index_postgres_url(database_url)
        .expect("postgres url")
        .with_admin_read_token(ADMIN_TOKEN.as_bytes().to_vec())
        .expect("admin token");
        let app = router(config).await.expect("router");

        // Idempotency: clear rows from any prior run before seeding.
        sqlx::query("DELETE FROM shardline_resumable_sessions WHERE session_id LIKE $1 || '%'")
            .bind("admin-interleave-")
            .execute(&pool)
            .await
            .expect("cleanup sessions");

        let expiry = chrono::Utc::now() + chrono::Duration::hours(1);
        let pid = std::process::id();
        // Terminal sessions interleave between active sessions in keyset order
        // so a naive in-memory filter over an unfiltered SQL window would
        // under-return and falsely signal end-of-list.
        let states: [(&str, &str); 5] = [
            ("admin-interleave-1", "active"),
            ("admin-interleave-2", "completed"),
            ("admin-interleave-3", "active"),
            ("admin-interleave-4", "completed"),
            ("admin-interleave-5", "active"),
        ];
        for (id, state) in states {
            let sid = format!("{id}-{pid}");
            sqlx::query(
                "INSERT INTO shardline_resumable_sessions \
                 (session_id, protocol, scope_namespace, target_key, attributes_json, \
                  state, generation, fence_epoch, expires_at) \
                 VALUES ($1, 's3_multipart', 'owner/repo', 'key.bin', '{}', $2, 1, 1, $3)",
            )
            .bind(&sid)
            .bind(state)
            .bind(expiry)
            .execute(&pool)
            .await
            .expect("insert session");
        }

        // Page through with limit=1, following cursors to the end.
        let mut collected_ids: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        let mut page_count = 0;
        loop {
            let url = cursor.as_ref().map_or_else(
                || "/api/v1/tasks?limit=1&prefix=admin-interleave-".to_owned(),
                |c| format!("/api/v1/tasks?limit=1&prefix=admin-interleave-&cursor={c}"),
            );
            let response = app
                .clone()
                .oneshot(request(Method::GET, &url, Some(ADMIN_TOKEN)))
                .await
                .expect("page response");
            assert_eq!(response.status(), StatusCode::OK);
            let body = json_body(response).await;
            let tasks = body["tasks"].as_array().expect("tasks array");
            assert!(tasks.len() <= 1, "limit=1 page must return at most 1 task");
            page_count += 1;
            for task in tasks {
                collected_ids.push(task["id"].as_str().expect("task id").to_owned());
            }
            match body["page"]["next_cursor"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => break,
            }
        }

        assert_eq!(page_count, 3, "three active sessions across three pages");
        assert_eq!(collected_ids.len(), 3, "only active sessions surfaced");
        for (id, _) in states.iter().filter(|(_, state)| *state == "active") {
            assert!(
                collected_ids
                    .iter()
                    .any(|collected| { collected == &format!("{id}-{pid}") }),
                "active session {id}-{pid} must be surfaced"
            );
        }
        for (id, _) in states.iter().filter(|(_, state)| *state != "active") {
            assert!(
                !collected_ids
                    .iter()
                    .any(|collected| collected == &format!("{id}-{pid}")),
                "terminal session {id}-{pid} must not be surfaced"
            );
        }
    }
}
