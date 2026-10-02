#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use shardline_hub_api::{hub_routes, routes::HubState};
use shardline_index::{
    LocalIndexStore, PostgresIndexStore,
    hub::{BoxedHubStore, EMPTY_HUB_REVISION, HubFileEntry, HubRepoType, HubTreePageOptions},
};
use shardline_server_core::ServerObjectStore;
use tower::ServiceExt;
fn app(root: &std::path::Path, store: BoxedHubStore) -> Router {
    hub_routes(
        HubState {
            store,
            object_store: ServerObjectStore::local(root.join("objects")).unwrap(),
            auth: None,
            http_client: None,
            webhook_secret_cipher: None,
            public_base_url: "http://localhost".to_owned(),
        },
        false,
    )
}
async fn read(app: &Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    assert!(
        !response.headers().contains_key("link"),
        "existing JSON array contract must not introduce pagination headers"
    );
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
async fn equivalence(root: &std::path::Path, store: BoxedHubStore) {
    let repo = format!(
        "pages/equivalence-{}",
        root.file_name().unwrap().to_str().unwrap()
    );
    store.create_repo(HubRepoType::Model, &repo, false).unwrap();
    let files: Vec<_> = [
        "A",
        "a.txt",
        "z",
        "é.txt",
        "東京.bin",
        "d/A",
        "d/é",
        "d/z/sub/file",
        "d_/_",
        "d%/x",
        "dd/not-d",
        "empty/",
        "",
    ]
    .into_iter()
    .enumerate()
    .map(|(number, path)| HubFileEntry {
        path: path.to_owned(),
        size: u64::try_from(number).unwrap(),
        sha: format!("sha-{number}"),
        is_lfs: number % 2 == 0,
    })
    .collect();
    let sha = format!(
        "tree-page-equivalence-{}",
        root.file_name().unwrap().to_str().unwrap()
    );
    store.store_files(&sha, &files).unwrap();
    store
        .create_revision(&repo, None, &sha, "main", "pages")
        .unwrap();
    let app = app(root, store.clone());
    for path in ["", "d", "d_", "d%", "empty", "missing", "東京"] {
        for recursive in [false, true] {
            let urlpath = if path.is_empty() {
                String::new()
            } else {
                format!("/{path}")
            };
            let base = format!("/api/models/{repo}/tree/main{urlpath}");
            let base = url::Url::parse(&format!("http://localhost{base}"))
                .unwrap()
                .path()
                .to_owned();
            let (full_status, full) = read(&app, &format!("{base}?recursive={recursive}")).await;
            assert_eq!(full_status, StatusCode::OK);
            let expected = full.as_array().unwrap();
            for limit in [0, 1, 2, 7, 100_001, usize::MAX] {
                for cursor in std::iter::once(None)
                    .chain(
                        expected
                            .iter()
                            .map(|entry| Some(entry["path"].as_str().unwrap())),
                    )
                    .chain(std::iter::once(Some("unknown")))
                {
                    let mut url = url::Url::parse(&format!("http://localhost{base}")).unwrap();
                    url.query_pairs_mut()
                        .append_pair("recursive", &recursive.to_string())
                        .append_pair("limit", &limit.to_string());
                    if let Some(cursor) = cursor {
                        url.query_pairs_mut().append_pair("cursor", cursor);
                    }
                    let uri = format!("{}?{}", url.path(), url.query().unwrap());
                    let (status, page) = read(&app, &uri).await;
                    assert_eq!(status, StatusCode::OK, "{uri}");
                    let start = cursor.map_or(0, |cursor| {
                        expected
                            .iter()
                            .position(|entry| entry["path"].as_str() == Some(cursor))
                            .map_or(expected.len(), |position| position + 1)
                    });
                    let wanted: Vec<_> = expected.iter().skip(start).take(limit).cloned().collect();
                    assert_eq!(page, serde_json::json!(wanted), "{uri}");
                }
            }
            let (_, ignored_cursor) = read(
                &app,
                &format!("{base}?recursive={recursive}&cursor=unknown"),
            )
            .await;
            assert_eq!(ignored_cursor, full);
            let (status, nul_cursor) = read(
                &app,
                &format!("{base}?recursive={recursive}&limit=1&cursor=%00"),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(nul_cursor, serde_json::json!([]));
        }
    }
    for recursive in [false, true] {
        let (status, nul_path) = read(
            &app,
            &format!("/api/models/{repo}/tree/main/%00?recursive={recursive}&limit=1"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(nul_path, serde_json::json!([]));
    }
    let empty = store
        .get_tree_page(
            EMPTY_HUB_REVISION,
            &HubTreePageOptions {
                path: String::new(),
                recursive: true,
                cursor: None,
                limit: 1,
            },
        )
        .unwrap();
    assert!(empty.entries.is_empty());
    assert!(
        store
            .get_tree_page(
                "1234567890abcdef",
                &HubTreePageOptions {
                    path: String::new(),
                    recursive: true,
                    cursor: None,
                    limit: 1
                }
            )
            .is_err()
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_tree_pages_match_complete_http_contract() {
    let root = tempfile::TempDir::new().unwrap();
    equivalence(
        root.path(),
        BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap()),
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_tree_pages_match_complete_http_contract() {
    let Ok(url) = std::env::var("SHARDLINE_TREE_PAGE_TEST_DATABASE_URL") else {
        return;
    };
    let root = tempfile::TempDir::new().unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    equivalence(
        root.path(),
        BoxedHubStore::from_store(PostgresIndexStore::new(pool)),
    )
    .await;
}

async fn ceiling_http(root: &std::path::Path, store: BoxedHubStore) {
    let repo = format!(
        "pages/ceiling-{}",
        root.file_name().unwrap().to_str().unwrap()
    );
    let sha = ceiling_sha(root);
    store.create_repo(HubRepoType::Model, &repo, false).unwrap();
    store
        .create_revision(&repo, None, &sha, "main", "ceiling")
        .unwrap();
    let app = app(root, store.clone());
    let uri = format!("/api/models/{repo}/tree/main?recursive=true");
    let (full_status, full) = read(&app, &uri).await;
    assert_eq!(full_status, StatusCode::OK);
    assert_eq!(
        full.as_array().unwrap().len(),
        shardline_index::hub::HUB_TREE_READ_CEILING
    );
    for limit in [0, 1, 100_001, usize::MAX] {
        let (status, page) = read(&app, &format!("{uri}&limit={limit}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            page.as_array().unwrap().len(),
            limit.min(shardline_index::hub::HUB_TREE_READ_CEILING)
        );
    }
    store
        .store_files(
            &sha,
            &[HubFileEntry {
                path: "extra".to_owned(),
                size: 1,
                sha: "extra".to_owned(),
                is_lfs: false,
            }],
        )
        .unwrap();
    for suffix in [
        String::new(),
        "&limit=0".to_owned(),
        "&limit=1".to_owned(),
        format!("&limit={}", usize::MAX),
        "&limit=0&cursor=%00".to_owned(),
    ] {
        let (status, _error) = read(&app, &format!("{uri}{suffix}")).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }
    assert!(
        store
            .get_tree_page(
                &sha,
                &HubTreePageOptions {
                    path: "missing".to_owned(),
                    recursive: true,
                    cursor: Some("unknown".to_owned()),
                    limit: 0
                }
            )
            .is_err(),
        "globalceiling still applies to empty subtrees and zero limit"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_tree_pages_preserve_whole_commit_ceiling() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    let files: Vec<_> = (0..shardline_index::hub::HUB_TREE_READ_CEILING)
        .map(|number| HubFileEntry {
            path: format!("{number:06}"),
            size: 1,
            sha: "sha".to_owned(),
            is_lfs: false,
        })
        .collect();
    store
        .store_files(&ceiling_sha(root.path()), &files)
        .unwrap();
    ceiling_http(root.path(), store).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_tree_pages_preserve_whole_commit_ceiling() {
    let Ok(url) = std::env::var("SHARDLINE_TREE_PAGE_TEST_DATABASE_URL") else {
        return;
    };
    let root = tempfile::TempDir::new().unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::query(
        "INSERT INTO shardline_hub_file_entries(commit_sha,path,size,sha,is_lfs)
        SELECT $1,lpad(number::text,6,'0'),1,'sha',false FROM generate_series(0,99999)number",
    )
    .bind(ceiling_sha(root.path()))
    .execute(&pool)
    .await
    .unwrap();
    ceiling_http(
        root.path(),
        BoxedHubStore::from_store(PostgresIndexStore::new(pool)),
    )
    .await;
}

fn ceiling_sha(root: &std::path::Path) -> String {
    format!(
        "tree-page-ceiling-{}",
        root.file_name().unwrap().to_str().unwrap()
    )
}
