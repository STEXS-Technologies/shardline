//! Standard model URLs omit the repository-type prefix.
use crate::{error::HubApiError, routes::HubState};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
};

pub(crate) async fn head(
    state: State<HubState>,
    capability: crate::routes::HubRepository<false, false>,
    Path((namespace, repository)): Path<(String, String)>,
) -> Result<String, HubApiError> {
    crate::routes::git_head(
        state,
        capability,
        Path(("models".to_owned(), namespace, repository)),
    )
    .await
}

pub(crate) async fn info_refs(
    state: State<HubState>,
    Path((namespace, repository)): Path<(String, String)>,
    query: Query<super::InfoRefsQuery>,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    super::info_refs(
        state,
        Path(("models".to_owned(), namespace, repository)),
        query,
        headers,
    )
    .await
}

pub(crate) async fn upload_pack(
    state: State<HubState>,
    Path((namespace, repository)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, HubApiError> {
    super::upload_pack(
        state,
        Path(("models".to_owned(), namespace, repository)),
        headers,
        body,
    )
    .await
}

pub(crate) async fn receive_pack(
    state: State<HubState>,
    Path((namespace, repository)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, HubApiError> {
    super::receive_pack(
        state,
        Path(("models".to_owned(), namespace, repository)),
        headers,
        body,
    )
    .await
}
