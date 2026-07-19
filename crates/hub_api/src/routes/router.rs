use axum::{
    Router,
    routing::{delete, get, post, put},
};

use super::HubState;
use crate::git::{info_refs, receive_pack, upload_pack};

/// Builds the Hub API router with [`HubState`] as the shared state.
///
/// When `register_xet_token_routes` is `false`, the xet-read-token and
/// xet-write-token routes are omitted to avoid conflicts with the Xet
/// protocol frontend when both are enabled simultaneously.
pub fn router(register_xet_token_routes: bool) -> Router<HubState> {
    let mut r = Router::new()
        .route("/health", get(super::health))
        .route("/api/whoami-v2", get(super::whoami));
    if register_xet_token_routes {
        r = r
            .route(
                "/api/{type}/{ns}/{repo}/xet-read-token/{rev}",
                get(super::xet_read_token),
            )
            .route(
                "/api/{type}/{ns}/{repo}/xet-write-token/{rev}",
                get(super::xet_write_token),
            );
    }
    r = r
        .route("/api/repos/create", post(super::repo_create))
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
        .route(
            "/api/{type}/{ns}/{repo}/revisions",
            get(super::repo_revisions),
        )
        .route(
            "/api/{type}/{ns}/{repo}/preupload/{rev}",
            post(super::preupload),
        )
        .route("/api/{type}/{ns}/{repo}/commit/{rev}", post(super::commit))
        .route(
            "/api/{type}/{ns}/{repo}/tree/{rev}",
            get(super::file_tree_at_root),
        )
        .route(
            "/api/{type}/{ns}/{repo}/tree/{rev}/{*path}",
            get(super::file_tree),
        )
        .route(
            "/{type}/{ns}/{repo}/resolve/{rev}/{*path}",
            get(super::resolve_file),
        )
        .route(
            "/{ns}/{repo}/resolve/{rev}/{*path}",
            get(super::resolve_model_file),
        )
        .route("/objects/batch", post(super::lfs_batch))
        .route("/lfs/objects/{oid}", put(super::lfs_upload))
        .route("/lfs/objects/{oid}", get(super::lfs_download))
        // Git Smart HTTP endpoints
        .route("/{type}/{ns}/{repo}/info/refs", get(info_refs))
        .route("/{type}/{ns}/{repo}/HEAD", get(super::git_head))
        .route(
            "/{type}/{ns}/{repo}/git-upload-pack",
            post(upload_pack),
        )
        .route(
            "/{type}/{ns}/{repo}/git-receive-pack",
            post(receive_pack),
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
        );
    r
}
