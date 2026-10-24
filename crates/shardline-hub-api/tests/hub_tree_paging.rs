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

fn persist_payload(root: &std::path::Path, content: &[u8]) -> String {
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
    let hash = blake3::hash(content);
    let sha = hash.to_hex().to_string();
    ServerObjectStore::local(root.join("objects"))
        .unwrap()
        .put_if_absent(
            &ObjectKey::parse(&format!("protocols/lfs/global/objects/{sha}")).unwrap(),
            ObjectBody::from_slice(content),
            &ObjectIntegrity::new(
                shardline_protocol::ShardlineHash::from_bytes(*hash.as_bytes()),
                u64::try_from(content.len()).unwrap(),
            ),
        )
        .unwrap();
    sha
}
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
    expanded_pages(root, store.clone()).await;
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
        "empty/value",
        "root-file",
    ]
    .into_iter()
    .enumerate()
    .map(|(number, path)| HubFileEntry {
        path: path.to_owned(),
        size: u64::try_from(number).unwrap(),
        sha: if number % 2 == 0 {
            use sha2::Digest;
            hex::encode(sha2::Sha256::digest(vec![b'x'; number]))
        } else {
            persist_payload(root, &vec![b'x'; number])
        },
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
    // Send the raw root alias: URL parsers can normalize away `/.` and hide
    // server-side regressions. Exercise nonempty trees on both metadata stores.
    for query in [
        "",
        "?limit=2",
        "?recursive=true&limit=2",
        "?expand=true&limit=50",
    ] {
        let canonical = read(&app, &format!("/api/models/{repo}/tree/main{query}")).await;
        let alias = read(&app, &format!("/api/models/{repo}/tree/main/.{query}")).await;
        assert_eq!(canonical.0, StatusCode::OK);
        assert_eq!(alias, canonical, "root alias differs for {query}");
    }
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
            if matches!(path, "missing" | "東京") {
                assert_eq!(full_status, StatusCode::NOT_FOUND);
                for limit in [0, 1] {
                    let (status, _) = read(&app, &format!("{base}?limit={limit}")).await;
                    assert_eq!(status, StatusCode::NOT_FOUND);
                }
                continue;
            }
            assert_eq!(full_status, StatusCode::OK);
            let expected = full.as_array().unwrap();
            if path.is_empty() && recursive {
                for directory in ["d", "d/z", "d/z/sub", "d_", "d%", "dd", "empty"] {
                    assert!(
                        expected.iter().any(|entry| {
                            entry["path"] == directory && entry["type"] == "directory"
                        }),
                        "missing recursive directory {directory}"
                    );
                }
            }
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
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(nul_path["error"], "not found");
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

async fn expanded_pages(root: &std::path::Path, store: BoxedHubStore) {
    let repo = format!(
        "pages/expanded-{}",
        root.file_name().unwrap().to_str().unwrap()
    );
    store.create_repo(HubRepoType::Model, &repo, false).unwrap();
    // Many logical bytes share one blob. Cached metadata must not be charged
    // as if every expanded file required a fresh whole-payload read.
    let payload = vec![b'x'; 2 * 1024 * 1024];
    let digest = persist_payload(root, &payload);
    let files: Vec<_> = (0..53)
        .map(|index| HubFileEntry {
            path: format!("nested %/file-{index:03}.txt"),
            size: payload.len() as u64,
            sha: digest.clone(),
            is_lfs: false,
        })
        .collect();
    let revision = format!("expanded-{}", root.file_name().unwrap().to_str().unwrap());
    store.store_files(&revision, &files).unwrap();
    store
        .create_revision(&repo, None, &revision, "main", "original tree")
        .unwrap();
    let app = app(root, store.clone());
    let base = format!("/api/models/{repo}/tree/main/nested%20%25?expand=true&recursive=true");
    let (zero_status, zero_page) = read(&app, &format!("{base}&limit=0")).await;
    assert_eq!(zero_status, StatusCode::OK);
    assert_eq!(zero_page, serde_json::json!([]));
    let (missing_status, _) = read(
        &app,
        &format!("/api/models/{repo}/tree/main/missing?expand=true&limit=0"),
    )
    .await;
    assert_eq!(missing_status, StatusCode::NOT_FOUND);
    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&base)
                .header("Host", "attacker.invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let link = first
        .headers()
        .get("link")
        .expect("expanded tree needs a next page")
        .to_str()
        .unwrap()
        .to_owned();
    let bytes = axum::body::to_bytes(first.into_body(), 65536)
        .await
        .unwrap();
    let first_entries: Vec<serde_json::Value> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(first_entries.len(), 50);
    for entry in &first_entries {
        assert_eq!(entry["lastCommit"]["title"], "original tree");
        assert!(
            entry.get("securityFileStatus").is_none(),
            "unperformed scans must not be reported"
        );
    }
    let next = link.strip_prefix('<').unwrap().split_once('>').unwrap().0;
    let url = url::Url::parse(next).unwrap();
    assert_eq!(url.host_str(), Some("localhost"));
    assert!(link.ends_with("; rel=\"next\""));
    assert!(
        !url.path().contains("/tree/main/"),
        "pagination must pin its snapshot"
    );
    let replacement = format!("replacement-{revision}");
    store.store_files(&replacement, &[]).unwrap();
    store
        .create_revision(&repo, Some(&revision), &replacement, "main", "replacement")
        .unwrap();
    let second = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("{}?{}", url.path(), url.query().unwrap()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    assert!(!second.headers().contains_key("link"));
    let second_bytes = axum::body::to_bytes(second.into_body(), 65536)
        .await
        .unwrap();
    let second_entries: Vec<serde_json::Value> = serde_json::from_slice(&second_bytes).unwrap();
    assert_eq!(second_entries.len(), 3);
    let actual: Vec<_> = first_entries
        .iter()
        .chain(&second_entries)
        .map(|entry| entry["path"].as_str().unwrap())
        .collect();
    assert_eq!(
        actual,
        files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>()
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
    let Some(url) =
        shardline_test_support::postgres_fixture_url("SHARDLINE_TREE_PAGE_TEST_DATABASE_URL")
    else {
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
    persist_payload(root, b"x");
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
            sha: blake3::hash(b"x").to_hex().to_string(),
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
    let Some(url) =
        shardline_test_support::postgres_fixture_url("SHARDLINE_TREE_PAGE_TEST_DATABASE_URL")
    else {
        return;
    };
    let root = tempfile::TempDir::new().unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::query(
        "INSERT INTO shardline_hub_file_entries(commit_sha,path,size,sha,is_lfs)
        SELECT $1,lpad(number::text,6,'0'),1,$2,false FROM generate_series(0,99999)number",
    )
    .bind(ceiling_sha(root.path()))
    .bind(blake3::hash(b"x").to_hex().to_string())
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
