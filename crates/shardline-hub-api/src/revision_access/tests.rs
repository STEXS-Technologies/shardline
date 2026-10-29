use super::*;
use shardline_index::{
    LocalIndexStore,
    hub::{BoxedHubStore, HubRepoType},
};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthProvider, LocalHmacProvider, ServerObjectStore};

fn capability(owner: &str, revision: Option<&str>) -> AuthorizedRepository {
    let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, owner, "model", revision).unwrap();
    let claims = TokenClaims::new("local", "subject", TokenScope::Read, scope, u64::MAX).unwrap();
    let token = provider.mint_token(&claims).unwrap();
    AuthorizedRepository::verify_and_authorize(&provider, &token, TokenScope::Read).unwrap()
}

fn state(root: &std::path::Path) -> HubState {
    HubState {
        store: BoxedHubStore::from_store(LocalIndexStore::new(root.join("metadata")).unwrap()),
        object_store: ServerObjectStore::local(root.join("objects")).unwrap(),
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
        public_base_url: "http://127.0.0.1:8080".to_owned(),
    }
}

#[test]
fn write_targets_are_exact_and_never_inherit_read_history_or_other_ref_rights() {
    let main = capability("alice", Some("main"));
    for target in ["main", "refs/heads/main"] {
        authorize_write("alice/model", target, &main).unwrap();
    }
    for target in ["private-side", "refs/tags/main", "HEAD", &"a".repeat(64)] {
        assert!(matches!(
            authorize_write("alice/model", target, &main),
            Err(HubApiError::Forbidden)
        ));
    }
    assert!(matches!(
        authorize_write("bob/model", "main", &main),
        Err(HubApiError::Forbidden)
    ));
    authorize_write("alice/model", "private-side", &capability("alice", None)).unwrap();
}

#[tokio::test]
async fn branch_reads_allow_immutable_history_but_never_sibling_names_or_commits() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let repo = "alice/model";
    state
        .store
        .create_repo(HubRepoType::Model, repo, true)
        .unwrap();
    let initial = state.store.resolve_revision(repo, "main").unwrap().unwrap();
    let ancestor = "a".repeat(64);
    let head = "b".repeat(64);
    let sibling = "c".repeat(64);
    for (parent, commit, branch) in [
        (initial.as_str(), ancestor.as_str(), "main"),
        (ancestor.as_str(), head.as_str(), "main"),
        (initial.as_str(), sibling.as_str(), "private-side"),
    ] {
        state.store.store_files(commit, &[]).unwrap();
        state
            .store
            .create_revision(repo, Some(parent), commit, branch, "test")
            .unwrap();
    }
    let auth = capability("alice", Some("main"));
    for (requested, commit) in [
        ("main", head.as_str()),
        ("refs/heads/main", head.as_str()),
        (head.as_str(), head.as_str()),
        (ancestor.as_str(), ancestor.as_str()),
    ] {
        authorize_read(&state, repo, requested, commit, &auth)
            .await
            .unwrap();
    }
    // Even a sibling ref with an identical head must not become accessible.
    for (requested, commit) in [
        ("private-side", sibling.as_str()),
        ("same-head", head.as_str()),
        (sibling.as_str(), sibling.as_str()),
    ] {
        assert!(matches!(
            authorize_read(&state, repo, requested, commit, &auth).await,
            Err(HubApiError::Forbidden)
        ));
    }
    let pinned = capability("alice", Some(&head));
    state
        .store
        .create_revision_if_absent(repo, Some(&head), &head, "refs/tags/release", "tag")
        .unwrap();
    for tag in ["release", "refs/tags/release"] {
        let auth = capability("alice", Some(tag));
        assert_eq!(
            visible_refs(&state, repo, &auth).unwrap().unwrap(),
            std::collections::HashSet::from(["refs/tags/release".to_owned()])
        );
        authorize_read(&state, repo, "refs/tags/release", &head, &auth)
            .await
            .unwrap();
        authorize_read(&state, repo, "release", &head, &auth)
            .await
            .unwrap();
        assert_eq!(
            visible_commits(&state, repo, &auth).await.unwrap().unwrap(),
            std::collections::HashSet::from([head.clone()])
        );
    }
    state
        .store
        .create_revision_if_absent(repo, Some(&head), &head, "release", "branch")
        .unwrap();
    assert!(matches!(
        authorize_read(
            &state,
            repo,
            "release",
            &head,
            &capability("alice", Some("refs/tags/release"))
        )
        .await,
        Err(HubApiError::Forbidden)
    ));
    assert_eq!(
        visible_refs(&state, repo, &auth).unwrap().unwrap(),
        std::collections::HashSet::from(["main".to_owned()])
    );
    assert!(
        visible_refs(&state, repo, &pinned)
            .unwrap()
            .unwrap()
            .is_empty()
    );
    let visible = visible_commits(&state, repo, &auth).await.unwrap().unwrap();
    assert_eq!(
        visible,
        std::collections::HashSet::from([initial, ancestor.clone(), head.clone()])
    );
    assert_eq!(
        visible_commits(&state, repo, &pinned)
            .await
            .unwrap()
            .unwrap(),
        std::collections::HashSet::from([head.clone()])
    );
    assert!(
        visible_commits(&state, repo, &capability("alice", None))
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        visible_commits(&state, repo, &capability("bob", None)).await,
        Err(HubApiError::Forbidden)
    ));
    authorize_read(&state, repo, &head, &head, &pinned)
        .await
        .unwrap();
    for requested in [ancestor.as_str(), "main"] {
        assert!(matches!(
            authorize_read(&state, repo, requested, &ancestor, &pinned).await,
            Err(HubApiError::Forbidden)
        ));
    }
    let tag = capability("alice", Some("refs/tags/release"));
    assert!(
        !branch_contains_commit(&state, repo, "refs/tags/release", &ancestor, &tag)
            .await
            .unwrap()
    );
    assert!(matches!(
        authorize_read(&state, repo, "main", &head, &capability("bob", None)).await,
        Err(HubApiError::Forbidden)
    ));
    authorize_read(
        &state,
        repo,
        "private-side",
        &sibling,
        &capability("alice", None),
    )
    .await
    .unwrap();
}
