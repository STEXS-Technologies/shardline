use axum::{
    Router,
    routing::{delete, get, post, put},
};

use super::HubState;
use crate::git::{info_refs, receive_pack, upload_pack};

/// Builds the Hub API router with [`HubState`] as the shared state.
///
/// Xet token routes use explicit Hub repo-type paths so they can coexist with
/// the native Xet frontend's provider-scoped paths.
pub fn router(register_xet_token_routes: bool) -> Router<HubState> {
    router_with_dataset_query(
        register_xet_token_routes,
        dataset_query_enabled_from_environment(),
    )
}

/// Builds the Hub router with an explicit native dataset-query feature gate.
/// Existing deployments keep the feature enabled by default; callers that do
/// not want analytics can omit the route while retaining all legacy previews.
pub fn router_with_dataset_query(
    register_xet_token_routes: bool,
    dataset_query_enabled: bool,
) -> Router<HubState> {
    let mut r = Router::new()
        .route("/health", get(super::health))
        .route("/api/agent-harnesses", get(super::agent_harnesses))
        .route("/api/whoami-v2", get(super::whoami));
    if register_xet_token_routes {
        for repo_type in ["models", "datasets", "spaces"] {
            r = r
                .route(
                    &format!("/api/{repo_type}/{{ns}}/{{repo}}/xet-read-token/{{rev}}"),
                    get(super::xet_read_token),
                )
                .route(
                    &format!("/api/{repo_type}/{{ns}}/{{repo}}/xet-write-token/{{rev}}"),
                    get(super::xet_write_token),
                );
        }
    }
    r =
        r.route("/api/repos/create", post(super::repo_create))
            .route(
                "/api/{type}/{ns}/{repo}/branch/{name}",
                post(super::branch_create).delete(super::branch_delete),
            )
            .route(
                "/api/{type}/{ns}/{repo}/tag/{name}",
                post(super::tag_create).delete(super::tag_delete),
            )
            .route("/api/repos/delete", delete(super::repo_delete_compat))
            .route("/api/repos", get(super::repo_list))
            .route("/api/{type}/search", get(super::repo_search))
            .route(
                "/api/{type}/{ns}/{repo}",
                post(super::repo_create_type)
                    .get(super::repo_info)
                    .delete(super::repo_delete),
            )
            .route(
                "/api/{type}/{ns}/{repo}/revision/{rev}",
                get(super::repo_revision_info),
            )
            .route(
                "/api/{type}/{ns}/{repo}/modelcard",
                get(super::repo_modelcard),
            )
            .route("/api/validate-yaml", post(super::validate_yaml))
            .route(
                "/api/{type}/{ns}/{repo}/revisions",
                get(super::repo_revisions),
            )
            .route("/api/{type}/{ns}/{repo}/refs", get(super::repo_refs))
            .route(
                "/api/{type}/{ns}/{repo}/preupload/{rev}",
                post(super::preupload),
            )
            .route("/api/{type}/{ns}/{repo}/commit/{rev}", post(super::commit))
            .route(
                "/api/{type}/{ns}/{repo}/commits/{rev}",
                get(super::repo_commits),
            )
            .route(
                "/api/{type}/{ns}/{repo}/tree/{rev}",
                get(super::file_tree_at_root),
            )
            .route(
                "/api/{type}/{ns}/{repo}/tree/{rev}/{*path}",
                get(super::file_tree),
            )
            .route("/objects/batch", post(super::lfs_batch))
            .route(
                "/{ns}/{repo}/info/lfs/objects/batch",
                post(super::lfs_batch_git),
            )
            .route(
                "/{repo_type}/{ns}/{repo}/info/lfs/objects/batch",
                post(super::lfs_batch_git_typed),
            )
            .route(
                "/lfs/objects/{oid}",
                put(super::lfs_upload).get(super::lfs_download).route_layer(
                    axum::middleware::from_fn(super::normalize_lfs_authorization),
                ),
            )
            .route(
                "/lfs/objects/{oid}/verify",
                post(super::lfs_verify).route_layer(axum::middleware::from_fn(
                    super::normalize_lfs_authorization,
                )),
            )
            // Dataset viewer endpoints
            .route(
                "/api/datasets/{ns}/{repo}/parquet",
                get(super::dataset_parquet),
            )
            .route(
                "/api/datasets/{ns}/{repo}/first-rows",
                get(super::dataset_first_rows),
            )
            .route(
                "/api/datasets/{ns}/{repo}/viewer/{split}",
                get(super::dataset_viewer),
            )
            // Webhook endpoints
            .route(
                "/api/{type}/{ns}/{repo}/webhooks",
                post(super::webhook_create).get(super::webhook_list),
            )
            .route(
                "/api/{type}/{ns}/{repo}/webhooks/{webhook_id}",
                delete(super::webhook_delete),
            )
            // Root-level Git Smart HTTP and file-resolve endpoints. These own the
            // root path space and are matched before the S3 fallback router (which
            // serves everything a registered route does not claim).
            .route(
                "/{type}/{ns}/{repo}/resolve/{rev}/{*path}",
                get(super::resolve_file).head(super::resolve_file_head),
            )
            .route(
                "/{ns}/{repo}/resolve/{rev}/{*path}",
                get(super::resolve_model_file).head(super::resolve_model_file_head),
            )
            .route("/{type}/{ns}/{repo}/info/refs", get(info_refs))
            .route("/{type}/{ns}/{repo}/HEAD", get(super::git_head))
            .route("/{type}/{ns}/{repo}/git-upload-pack", post(upload_pack))
            .route("/{type}/{ns}/{repo}/git-receive-pack", post(receive_pack));
    if dataset_query_enabled {
        r = r.route(
            "/api/datasets/{ns}/{repo}/query",
            post(super::dataset_query),
        );
    }
    r
}

pub(crate) fn dataset_query_enabled_from_environment() -> bool {
    std::env::var("SHARDLINE_ENABLE_DATASET_QUERY").map_or(true, |value| {
        !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        )
    })
}
