use axum::body::Bytes;
use shardline_protocol::RepositoryProvider;

use super::*;

#[tokio::test]
async fn postgres_registration_preserves_scope_capacity_and_revision_deletion() {
    let Some(fixture) = crate::test_support::postgres_fixture::PostgresFixture::new().await else {
        return;
    };
    fixture.migrate().await;
    let backend = &fixture.backend;
    let scope =
        RepositoryScope::new(RepositoryProvider::Generic, "owner", "tree", Some("main")).unwrap();
    let id = "a".repeat(64);
    fixture
        .postgres()
        .upload_file(&id, Bytes::from_static(b"tree payload"), Some(&scope))
        .await
        .unwrap();
    let key = TreeKey::from_scope(&scope).unwrap();
    let repo = RepoKey::from_scope(&scope);
    let one = NonZeroUsize::MIN;
    let two = NonZeroUsize::new(2).unwrap();
    let wrong_scope =
        RepositoryScope::new(RepositoryProvider::Generic, "other", "tree", Some("main")).unwrap();
    assert!(matches!(
        backend
            .register_tree_path(&key, "file", &id, Some(&wrong_scope), one, two)
            .await,
        Err(ServerError::UnregisteredFile(_))
    ));
    assert!(
        backend
            .list_revisions(&repo, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    let first = backend
        .register_tree_path(&key, "file", &id, Some(&scope), one, two)
        .await
        .unwrap();
    assert!(first.created);
    assert_eq!(first.entry.size_bytes, 12);
    let refreshed = backend
        .register_tree_path(&key, "file", &id, Some(&scope), one, two)
        .await
        .unwrap();
    assert!(!refreshed.created);
    backend
        .register_tree_path(&key, "second", &id, Some(&scope), one, two)
        .await
        .unwrap();
    // Preserve the existing rule that even a refresh is rejected at the tree cap.
    assert!(matches!(
        backend
            .register_tree_path(&key, "file", &id, Some(&scope), one, two)
            .await,
        Err(ServerError::TooManyRevisions)
    ));
    assert_eq!(
        backend
            .scan_tree_raw(&key, "", None, 10)
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(
        backend
            .resolve_tree_path(&key, "file")
            .await
            .unwrap()
            .is_some()
    );

    let branch =
        RepositoryScope::new(RepositoryProvider::Generic, "owner", "tree", Some("branch")).unwrap();
    fixture
        .postgres()
        .upload_file(&id, Bytes::from_static(b"tree payload"), Some(&branch))
        .await
        .unwrap();
    assert!(matches!(
        backend
            .register_tree_path(
                &TreeKey::from_scope(&branch).unwrap(),
                "file",
                &id,
                Some(&branch),
                one,
                two
            )
            .await,
        Err(ServerError::TooManyRevisions)
    ));
    assert_eq!(
        backend.list_revisions(&repo, None, 10).await.unwrap().len(),
        1
    );
    assert!(backend.delete_revision(&repo, "main").await.unwrap());
    assert!(!backend.delete_revision(&repo, "main").await.unwrap());
    assert!(
        backend
            .scan_tree_raw(&key, "", None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    fixture.finish().await;
}
