#![allow(clippy::unwrap_used, clippy::expect_used)]
use axum::{Router, body::Body, http::Request};
use shardline_hub_api::{auth::HubAuth, hub_routes, routes::HubState};
use shardline_index::{
    LocalIndexStore, PostgresIndexStore,
    hub::{BoxedHubStore, HubRepoType},
};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthProvider, ServerObjectStore, auth::LocalHmacProvider};
use std::path::Path;
use tower::ServiceExt;
const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

fn app(store: BoxedHubStore, root: &Path) -> Router {
    hub_routes(
        HubState {
            store,
            object_store: ServerObjectStore::local(root.join("objects")).unwrap(),
            auth: Some(HubAuth::new(Box::new(LocalHmacProvider::new(KEY).unwrap()))),
            http_client: None,
            webhook_secret_cipher: None,
            public_base_url: "http://localhost".to_owned(),
        },
        false,
    )
}
fn token(owner: &str, repo: &str) -> String {
    LocalHmacProvider::new(KEY)
        .unwrap()
        .mint_token(
            &TokenClaims::new(
                "shardline",
                "test",
                TokenScope::Read,
                RepositoryScope::new(RepositoryProvider::Generic, owner, repo, None).unwrap(),
                u64::MAX,
            )
            .unwrap(),
        )
        .unwrap()
}
async fn ids(app: &Router, query: &str, bearer: &str) -> Vec<String> {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/models/search?{query}"))
                .header("Authorization", format!("Bearer {bearer}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    parsed
        .get("repos")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.get("id").unwrap().as_str().unwrap().to_owned())
        .collect()
}
fn fixtures(store: &BoxedHubStore) {
    for (name, private) in [
        ("search/a-hidden", true),
        ("search/b-old", false),
        ("search/c-tie", false),
        ("search/z-new", false),
        ("search/own-private", true),
        ("group-a/a", false),
        ("group-b/b", false),
        ("under_score/model", false),
        ("Case/model", false),
        ("case/other", false),
    ] {
        store
            .create_repo(HubRepoType::Model, name, private)
            .unwrap();
    }
}
async fn contract(app: Router) {
    let caller = token("caller", "own");
    assert_eq!(
        ids(&app, "q=search/&limit=1", &caller).await,
        vec!["search/b-old"]
    );
    assert_eq!(
        ids(&app, "q=search/&limit=1&sort=lastModified", &caller).await,
        vec!["search/z-new"]
    );
    assert_eq!(
        ids(
            &app,
            "q=search/&limit=1&sort=lastModified&direction=asc",
            &caller
        )
        .await,
        vec!["search/b-old"]
    );
    assert_eq!(
        ids(
            &app,
            "q=search/&limit=2&sort=lastModified&direction=asc",
            &caller
        )
        .await,
        vec!["search/b-old", "search/c-tie"]
    );
    assert_eq!(
        ids(&app, "q=search/&limit=2&sort=lastModified", &caller).await,
        vec!["search/z-new", "search/b-old"]
    );
    assert_eq!(
        ids(&app, "q=group&author=group-b&limit=1", &caller).await,
        vec!["group-b/b"]
    );
    assert_eq!(
        ids(&app, "q=search/&author=search&limit=20", &caller).await,
        vec!["search/b-old", "search/c-tie", "search/z-new"]
    );
    assert_eq!(
        ids(&app, "q=search/&limit=1&sort=likes&direction=asc", &caller).await,
        vec!["search/z-new"]
    );
    assert_eq!(
        ids(
            &app,
            "q=search/&limit=1&sort=downloads&direction=desc",
            &caller
        )
        .await,
        vec!["search/b-old"]
    );
    assert_eq!(
        ids(
            &app,
            "q=search/&limit=1&sort=unknown&direction=asc",
            &caller
        )
        .await,
        vec!["search/z-new"]
    );
    assert_eq!(
        ids(&app, "q=under_score&limit=1", &caller).await,
        vec!["under_score/model"]
    );
    assert!(
        ids(&app, "q=under%25score&limit=1", &caller)
            .await
            .is_empty()
    );
    assert!(
        ids(
            &app,
            "q=search/a-hidden&limit=1",
            &token("search", "own-private")
        )
        .await
        .is_empty()
    );
    assert_eq!(
        ids(
            &app,
            "q=search/own-private&limit=1",
            &token("search", "own-private")
        )
        .await,
        vec!["search/own-private"]
    );
    assert_eq!(
        ids(&app, "q=Case&limit=10", &caller).await,
        vec!["Case/model"]
    );
    assert_eq!(
        ids(&app, "q=case&limit=10", &caller).await,
        vec!["case/other"]
    );
    assert!(ids(&app, "q=search/&limit=0", &caller).await.is_empty());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_search_filters_and_sorts_before_limit() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    fixtures(&store);
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    connection.execute("UPDATE shardline_hub_repos SET updated_at_unix_seconds = CASE WHEN repo_id = 'search/z-new' THEN 200 ELSE 100 END",[]).unwrap();
    drop(connection);
    contract(app(store, root.path())).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_search_filters_and_sorts_before_limit() {
    let Ok(url) = std::env::var("SHARDLINE_HUB_SEARCH_TEST_DATABASE_URL") else {
        return;
    };
    let root = tempfile::TempDir::new().unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let store = BoxedHubStore::from_store(PostgresIndexStore::new(pool.clone()));
    fixtures(&store);
    sqlx::query("UPDATE shardline_hub_repos SET updated_at_unix_seconds = CASE WHEN repo_id = 'search/z-new' THEN 200 ELSE 100 END").execute(&pool).await.unwrap();
    contract(app(store, root.path())).await;
}
