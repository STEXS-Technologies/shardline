//! Tree-conflict and immutable-history publication contracts.
use super::*;

async fn contract(app: Router, store: BoxedHubStore) {
    for (index, paths) in [
        vec!["x", "x/a"],
        vec!["x/a", "x"],
        vec!["x", "x-foo", "x/a"],
        vec!["σ", "σ/a"],
    ]
    .into_iter()
    .enumerate()
    {
        let repo = format!("owner/same-{index}");
        store.create_repo(HubRepoType::Model, &repo, false).unwrap();
        let before = snapshot(&store, &repo);
        assert_eq!(
            commit(&app, &repo, paths.into_iter().map(add).collect()).await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            snapshot(&store, &repo),
            before,
            "rejection must preserve current ref, tree and immutable history"
        );
    }
    for (index, (parent, new)) in [("x", "x/a"), ("x/a", "x")].into_iter().enumerate() {
        let repo = format!("owner/parent-{index}");
        store.create_repo(HubRepoType::Model, &repo, false).unwrap();
        assert_eq!(commit(&app, &repo, vec![add(parent)]).await, StatusCode::OK);
        let before = snapshot(&store, &repo);
        assert_eq!(
            commit(&app, &repo, vec![add("x-foo"), add(new)]).await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(snapshot(&store, &repo), before);
    }
    let repo = "owner/replacement";
    store.create_repo(HubRepoType::Model, repo, false).unwrap();
    assert_eq!(
        commit(&app, repo, vec![add("x"), add("xy/a")]).await,
        StatusCode::OK,
        "similar prefix is valid"
    );
    let original = store.resolve_revision(repo, "main").unwrap().unwrap();
    // Validate the final tree, permitting an intermediate conflict removed by
    // a later instruction in the same acknowledged commit.
    assert_eq!(
        commit(&app, repo, vec![add("x/a"), add("x/b"), delete("x")]).await,
        StatusCode::OK
    );
    assert_eq!(
        commit(&app, repo, vec![add("x"), delete("x/a"), delete("x/b")]).await,
        StatusCode::OK
    );
    let head = store.resolve_revision(repo, "main").unwrap().unwrap();
    let paths: Vec<_> = store
        .get_files(&head)
        .unwrap()
        .into_iter()
        .map(|f| f.path)
        .collect();
    assert_eq!(paths, vec!["x", "xy/a"]);
    assert_eq!(
        store.get_files(&original).unwrap().len(),
        2,
        "valid replacement must retain earlier immutable tree"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_hub_path_conflicts_are_rejected_before_revision_publication() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    contract(router(root.path(), store.clone()), store).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_hub_path_conflicts_are_rejected_before_revision_publication() {
    let Some(url) =
        shardline_test_support::postgres_fixture_url("SHARDLINE_TREE_HIERARCHY_TEST_DATABASE_URL")
    else {
        return;
    };
    let root = tempfile::TempDir::new().unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let store = BoxedHubStore::from_store(shardline_index::PostgresIndexStore::new(pool));
    contract(router(root.path(), store.clone()), store).await;
}
