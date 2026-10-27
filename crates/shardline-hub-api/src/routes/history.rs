//! SDK commit-history projection from immutable repository-scoped Hub revisions.

use std::collections::HashSet;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, header::LINK},
};

use super::{HubRepository, HubState};
use crate::error::HubApiError;

#[derive(serde::Deserialize)]
pub(crate) struct HistoryQuery {
    limit: Option<usize>,
}

pub(crate) async fn repo_commits(
    State(state): State<HubState>,
    _repo: HubRepository,
    Path((kind, ns, repo, revision_name)): Path<(String, String, String, String)>,
    Query(query): Query<HistoryQuery>,
) -> Result<(HeaderMap, Json<Vec<serde_json::Value>>), HubApiError> {
    let limit = query.limit.unwrap_or(100);
    if !(1..=100).contains(&limit) {
        return Err(HubApiError::BadRequest(
            "history limit must be between 1 and 100".to_owned(),
        ));
    }
    let repo_id = format!("{ns}/{repo}");
    state
        .store
        .get_repo(&repo_id)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    let head = crate::git::smart_http::resolve_commit_id(
        &state,
        &repo_id,
        &revision_name,
        _repo.capability(),
    )?
    .ok_or(HubApiError::RevisionNotFound)?;
    crate::revision_access::authorize_read(
        &state,
        &repo_id,
        &revision_name,
        &head,
        _repo.capability(),
    )
    .await?;
    let snapshot = crate::revision_access::snapshot_commit(&state, &repo_id, _repo.capability())?;
    if snapshot.as_deref().is_some_and(|allowed| allowed != head) {
        return Err(HubApiError::Forbidden);
    }
    let mut visited = HashSet::new();
    let mut result = Vec::new();
    let mut next = Some(head);
    while let Some(sha) = next.take() {
        if !visited.insert(sha.clone()) {
            return Err(HubApiError::CasError(
                "cyclic repository history".to_owned(),
            ));
        }
        let revision = state
            .store
            .get_revision(&repo_id, &sha)
            .map_err(|error| HubApiError::CasError(error.to_string()))?
            .ok_or_else(|| HubApiError::CasError("incomplete repository history".to_owned()))?;
        let seconds = i64::try_from(revision.created_at_unix_seconds)
            .map_err(|e| HubApiError::CasError(e.to_string()))?;
        let date = chrono::DateTime::from_timestamp(seconds, 0)
            .ok_or_else(|| HubApiError::CasError("invalid commit timestamp".to_owned()))?
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();
        let message = revision.message.as_deref().unwrap_or_default();
        let (title, description) = message.split_once('\n').unwrap_or((message, ""));
        result.push(serde_json::json!({
            "id": crate::git::smart_http::public_commit_id(&state, &repo_id, &sha, _repo.capability()).await?,
            // Legacy Hub metadata has no author field. Preserve unknown
            // authorship rather than attribute old commits to the requester.
            "authors": [],
            "date": date,
            "title": title,
            "message": description,
        }));
        next = if snapshot.is_some() {
            None
        } else {
            revision.parent_sha
        };
        if next.as_ref().is_some_and(|parent| visited.contains(parent)) {
            return Err(HubApiError::CasError(
                "cyclic repository history".to_owned(),
            ));
        }
        if result.len() == limit {
            break;
        }
    }
    let mut headers = HeaderMap::new();
    if let Some(parent) = next {
        // Continue from immutable ancestry, never resolve the moving branch
        // again. Every page still passes normal revision authorization.
        let public =
            crate::git::smart_http::public_commit_id(&state, &repo_id, &parent, _repo.capability())
                .await?;
        let mut url = url::Url::parse(&state.public_base_url).map_err(|_error| {
            HubApiError::CasError("invalid configured public Hub URL".to_owned())
        })?;
        url.set_query(None);
        url.set_fragment(None);
        url.path_segments_mut()
            .map_err(|()| HubApiError::CasError("invalid history pagination URL".to_owned()))?
            .pop_if_empty()
            .extend(["api", &kind, &ns, &repo, "commits", &public]);
        url.query_pairs_mut()
            .append_pair("limit", &limit.to_string());
        headers.insert(
            LINK,
            HeaderValue::from_str(&format!("<{url}>; rel=\"next\"")).map_err(|_error| {
                HubApiError::CasError("invalid history pagination URL".to_owned())
            })?,
        );
    }
    Ok((headers, Json(result)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn history_continuation_rechecks_branch_access_after_deletion_and_replacement() {
        use axum::{body::Body, http::Request};
        use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
        use shardline_server_core::{AuthProvider, LocalHmacProvider};
        use tower::ServiceExt;

        let (_root, mut state) = crate::routes::tests::make_test_state();
        let repo = "alice/history";
        state
            .store
            .create_repo(shardline_index::hub::HubRepoType::Model, repo, true)
            .unwrap();
        let initial = state.store.resolve_revision(repo, "main").unwrap().unwrap();
        let ancestor = "a".repeat(64);
        let head = "b".repeat(64);
        for (parent, next) in [(&initial, &ancestor), (&ancestor, &head)] {
            state
                .store
                .create_revision(repo, Some(parent), next, "feature", "private history")
                .unwrap();
        }
        let provider = LocalHmacProvider::new(b"history-test-signing-key-32-bytes!").unwrap();
        let scope = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "alice",
            "history",
            Some("feature"),
        )
        .unwrap();
        let claims =
            TokenClaims::new("local", "reader", TokenScope::Read, scope, u64::MAX).unwrap();
        let token = provider.mint_token(&claims).unwrap();
        state.auth = Some(crate::auth::HubAuth::new(Box::new(provider)));
        let app = crate::routes::router::router(false).with_state(state.clone());
        let request = |uri: &str| {
            Request::builder()
                .uri(uri)
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        let first = app
            .clone()
            .oneshot(request("/api/models/alice/history/commits/feature?limit=1"))
            .await
            .unwrap();
        assert_eq!(first.status(), axum::http::StatusCode::OK);
        let link = first.headers()[LINK].to_str().unwrap();
        let continuation = url::Url::parse(
            link.strip_prefix('<')
                .unwrap()
                .strip_suffix(">; rel=\"next\"")
                .unwrap(),
        )
        .unwrap();
        let target = format!("{}?{}", continuation.path(), continuation.query().unwrap());
        assert_eq!(
            app.clone()
                .oneshot(request(&target))
                .await
                .unwrap()
                .status(),
            axum::http::StatusCode::OK
        );
        state.store.delete_ref(repo, "feature", &head).unwrap();
        assert_eq!(
            app.clone()
                .oneshot(request(&target))
                .await
                .unwrap()
                .status(),
            axum::http::StatusCode::FORBIDDEN
        );
        // A replacement branch sharing only the initial commit must not restore
        // access to the removed branch's immutable history through an old link.
        state
            .store
            .create_revision(
                repo,
                Some(&initial),
                &"c".repeat(64),
                "feature",
                "replacement",
            )
            .unwrap();
        assert_eq!(
            app.oneshot(request(&target)).await.unwrap().status(),
            axum::http::StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn default_history_pages_preserve_large_ancestry_while_branch_advances() {
        let (_root, state) = crate::routes::tests::make_test_state();
        let repo = "alice/history";
        state
            .store
            .create_repo(shardline_index::hub::HubRepoType::Model, repo, true)
            .unwrap();
        let mut head = state.store.resolve_revision(repo, "main").unwrap().unwrap();
        for index in 0..205 {
            let next = format!("{index:064x}");
            state
                .store
                .create_revision(repo, Some(&head), &next, "main", &format!("commit {index}"))
                .unwrap();
            head = next;
        }
        let mut revision = "main".to_owned();
        let mut titles = Vec::new();
        let mut identities = HashSet::new();
        let mut page_sizes = Vec::new();
        loop {
            let (headers, Json(page)) = repo_commits(
                State(state.clone()),
                crate::routes::helpers::test_repo::<false, true>(&state, &HeaderMap::new()),
                Path(("models".into(), "alice".into(), "history".into(), revision)),
                Query(HistoryQuery { limit: None }),
            )
            .await
            .unwrap();
            page_sizes.push(page.len());
            for entry in page {
                assert!(identities.insert(entry["id"].as_str().unwrap().to_owned()));
                titles.push(entry["title"].as_str().unwrap().to_owned());
            }
            let Some(link) = headers.get(LINK) else {
                break;
            };
            let link = link.to_str().unwrap();
            let continuation = url::Url::parse(
                link.strip_prefix('<')
                    .unwrap()
                    .strip_suffix(">; rel=\"next\"")
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(
                continuation.origin().ascii_serialization(),
                state.public_base_url
            );
            assert_eq!(continuation.query(), Some("limit=100"));
            revision = continuation
                .path_segments()
                .unwrap()
                .next_back()
                .unwrap()
                .to_owned();
            assert_ne!(revision, "main");
            // Publish another head after each response, before its continuation.
            let next = format!("{:064x}", 1000 + page_sizes.len());
            state
                .store
                .create_revision(repo, Some(&head), &next, "main", "new concurrent head")
                .unwrap();
            head = next;
        }
        assert_eq!(page_sizes, [100, 100, 6]);
        let expected: Vec<_> = (0..205)
            .rev()
            .map(|index| format!("commit {index}"))
            .collect();
        assert_eq!(&titles[..205], expected);
        assert_eq!(titles.len(), 206); // Includes the repository's initial commit.
        assert!(!titles.iter().any(|title| title == "new concurrent head"));
    }
}
