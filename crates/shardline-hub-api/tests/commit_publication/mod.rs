use super::*;

// Hold the body after the handler has resolved its parent, then advance that
// branch through another request. This exercises the actual HTTP race rather
// than relying on scheduler timing.
async fn stalled_publication(
    root: &std::path::Path,
    store: BoxedHubStore,
    conditional: bool,
    immutable_url: bool,
    deleted: bool,
) {
    let repo_name = format!("owner/publication-{conditional}-{immutable_url}-{deleted}");
    let repo = repo_name.as_str();
    let parent = seed(root, &store, repo, 1);
    if deleted {
        store
            .create_revision_if_absent(repo, Some(&parent), &parent, "feature", "branch")
            .unwrap();
    }
    let initial_history = store.list_revisions(repo).unwrap().len();
    let app = router(root, store.clone());
    let mut header = serde_json::json!({"message":"stalled"});
    if conditional {
        header
            .as_object_mut()
            .unwrap()
            .insert("parentCommit".to_owned(), parent.clone().into());
    }
    let payload = format!("{}\n{}", serde_json::json!({"header":header}), add("first"));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let stream = futures_util::stream::once(async move {
        started_tx.send(()).unwrap();
        release_rx.await.unwrap();
        Ok::<_, std::io::Error>(payload)
    });
    let revision = if immutable_url {
        parent.as_str()
    } else if deleted {
        "feature"
    } else {
        "main"
    };
    let request = Request::builder()
        .method("POST")
        .uri(format!("/api/models/{repo}/commit/{revision}"))
        .header("content-type", "application/x-ndjson")
        .body(Body::from_stream(stream))
        .unwrap();
    let pending = tokio::spawn(app.clone().oneshot(request));
    tokio::time::timeout(std::time::Duration::from_secs(10), started_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        commit(&app, repo, vec![add("second")]).await,
        StatusCode::OK
    );
    let after_second = snapshot(&store, repo);
    if deleted {
        store.delete_ref(repo, "feature", &parent).unwrap();
    }
    release_tx.send(()).unwrap();
    let response = tokio::time::timeout(std::time::Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    if deleted {
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(snapshot(&store, repo), after_second);
        assert!(
            store
                .resolve_revision(repo, "refs/heads/feature")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.list_revisions(repo).unwrap().len(),
            initial_history.saturating_add(1)
        );
    } else if conditional || immutable_url {
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(snapshot(&store, repo), after_second);
    } else {
        assert_eq!(response.status(), StatusCode::OK);
        let head = store.resolve_revision(repo, "main").unwrap().unwrap();
        let files = store.get_files(&head).unwrap();
        for path in ["f000000", "first", "second"] {
            assert!(files.iter().any(|file| file.path == path));
        }
        assert_eq!(
            store.list_revisions(repo).unwrap().len(),
            initial_history.saturating_add(2)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publication_preserves_unconditional_and_conditional_semantics() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    contract(root.path(), store).await;
}

pub(super) async fn contract(root: &std::path::Path, store: BoxedHubStore) {
    stalled_publication(root, store.clone(), false, false, false).await;
    stalled_publication(root, store.clone(), true, false, false).await;
    stalled_publication(root, store.clone(), false, true, false).await;
    stalled_publication(root, store.clone(), false, false, true).await;
    concurrent_distinct_files(root, store).await;
}

async fn concurrent_distinct_files(root: &std::path::Path, store: BoxedHubStore) {
    const WRITERS: usize = 32;
    let repo = "owner/concurrent-distinct-files";
    seed(root, &store, repo, 1);
    let initial_history = store.list_revisions(repo).unwrap().len();
    let app = router(root, store.clone());
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(WRITERS));
    let mut uploads = tokio::task::JoinSet::new();
    for index in 0..WRITERS {
        let app = app.clone();
        let barrier = barrier.clone();
        uploads.spawn(async move {
            barrier.wait().await;
            commit(&app, repo, vec![add(&format!("writer-{index}"))]).await
        });
    }
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while let Some(status) = uploads.join_next().await {
            assert_eq!(status.unwrap(), StatusCode::OK);
        }
    })
    .await
    .unwrap();
    let head = store.resolve_revision(repo, "main").unwrap().unwrap();
    let files = store.get_files(&head).unwrap();
    assert_eq!(files.len(), WRITERS.saturating_add(1));
    assert!(files.iter().any(|file| file.path == "f000000"));
    for index in 0..WRITERS {
        assert!(
            files
                .iter()
                .any(|file| file.path == format!("writer-{index}"))
        );
    }
    assert_eq!(
        store.list_revisions(repo).unwrap().len(),
        initial_history.saturating_add(WRITERS)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and SHARDLINE_BIN for disposable PostgreSQL migrations"]
async fn postgres_publication_conflicts_and_deleted_branches() {
    let services = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .start()
            .unwrap()
            .expect("Docker is required")
    })
    .await
    .unwrap();
    let database = services.postgres_url().unwrap();
    let binary = std::env::var_os("SHARDLINE_BIN").expect("SHARDLINE_BIN is required");
    let migrated = shardline_test_support::native_client_output(
        std::process::Command::new(binary)
            .args(["db", "migrate", "up"])
            .env("SHARDLINE_INDEX_POSTGRES_URL", &database),
        std::time::Duration::from_secs(120),
    )
    .unwrap();
    assert!(
        migrated.status.success(),
        "disposable PostgreSQL migration failed"
    );
    let pool = sqlx::PgPool::connect(&database).await.unwrap();
    let store = BoxedHubStore::from_store(shardline_index::PostgresIndexStore::new(pool.clone()));
    let root = tempfile::TempDir::new().unwrap();
    contract(root.path(), store.clone()).await;
    concurrent_head_reads(store.clone()).await;
    // This database belongs exclusively to the disposable fixture. Real
    // corruption must still fail after eliminating mixed-snapshot false alarms.
    let changed = sqlx::query("UPDATE shardline_reliability_events SET event_json = '{}'::jsonb WHERE operation_kind = 'MetadataCommit'")
        .execute(&pool).await.unwrap();
    assert!(changed.rows_affected() > 0);
    let repo = "owner/read-snapshot";
    assert!(store.get_repo(repo).is_err());
    assert!(store.list_refs_bounded(repo, 2).is_err());
    assert!(store.resolve_revision(repo, "main").is_err());
    assert!(store.resolve_revision(repo, "refs/heads/main").is_err());
    pool.close().await;
}

// Calls may observe different committed heads, but no individual read may mix
// its metadata snapshot with evidence from a later publication.
pub(super) async fn concurrent_head_reads(store: BoxedHubStore) {
    let repo = "owner/read-snapshot";
    store.create_repo(HubRepoType::Model, repo, false).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let writer_store = store.clone();
    let writer_barrier = barrier.clone();
    let writer = tokio::task::spawn_blocking(move || {
        let mut parent = writer_store
            .resolve_revision(repo, "main")
            .unwrap()
            .unwrap();
        writer_barrier.wait();
        for index in 1..=200 {
            let next = format!("{index:064x}");
            writer_store
                .create_revision(repo, Some(&parent), &next, "main", "snapshot race")
                .unwrap();
            parent = next;
        }
        parent
    });
    let reader = tokio::task::spawn_blocking(move || {
        barrier.wait();
        for _ in 0..400 {
            assert!(store.get_repo(repo).unwrap().is_some());
            let refs = store.list_refs_bounded(repo, 2).unwrap();
            assert_eq!(refs.len(), 1);
            assert!(store.resolve_revision(repo, "main").unwrap().is_some());
            assert!(
                store
                    .resolve_revision(repo, "refs/heads/main")
                    .unwrap()
                    .is_some()
            );
        }
        store
    });
    let (writer, reader) = tokio::join!(writer, reader);
    assert_eq!(
        writer.unwrap(),
        reader
            .unwrap()
            .resolve_revision(repo, "main")
            .unwrap()
            .unwrap()
    );
}
