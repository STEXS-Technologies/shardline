use super::*;

#[tokio::test]
async fn default_history_page_stops_at_one_hundred_commits() {
    let root = tempfile::tempdir().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_owned()).unwrap());
    store
        .create_repo(HubRepoType::Model, "alice/pages", true)
        .unwrap();
    let mut parent = None;
    for index in 0..101 {
        let sha = format!("{index:064x}");
        store.store_files(&sha, &[]).unwrap();
        store
            .create_revision(
                "alice/pages",
                parent.as_deref(),
                &sha,
                "main",
                &index.to_string(),
            )
            .unwrap();
        parent = Some(sha);
    }
    let app = hub_routes(
        HubState {
            store,
            object_store: ServerObjectStore::local(root.path().join("objects")).unwrap(),
            auth: None,
            public_base_url: "http://localhost".to_owned(),
            http_client: None,
            webhook_secret_cipher: None,
        },
        false,
    );
    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/models/alice/pages/commits/main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let link = first.headers().get("link").unwrap().to_str().unwrap();
    let next = url::Url::parse(link.split_once('>').unwrap().0.trim_start_matches('<')).unwrap();
    let rows: Vec<serde_json::Value> = serde_json::from_slice(
        &axum::body::to_bytes(first.into_body(), 65536)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(rows.len(), 100);
    let last = app
        .oneshot(
            Request::builder()
                .uri(format!("{}?{}", next.path(), next.query().unwrap()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(last.status(), StatusCode::OK);
    assert!(!last.headers().contains_key("link"));
    let remaining: Vec<serde_json::Value> =
        serde_json::from_slice(&axum::body::to_bytes(last.into_body(), 65536).await.unwrap())
            .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining.first().unwrap().get("title").unwrap(), "0");
}

#[tokio::test]
async fn history_pages_keep_immutable_ancestry_and_authorization() {
    let root = tempfile::tempdir().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_owned()).unwrap());
    store
        .create_repo(HubRepoType::Model, "alice/pages", true)
        .unwrap();
    let parent = "a".repeat(64);
    let head = "b".repeat(64);
    for sha in [&parent, &head] {
        store.store_files(sha, &[]).unwrap();
    }
    store
        .create_revision("alice/pages", None, &parent, "main", "parent")
        .unwrap();
    store
        .create_revision("alice/pages", Some(&parent), &head, "main", "head")
        .unwrap();
    let state = HubState {
        store: store.clone(),
        object_store: ServerObjectStore::local(root.path().join("objects")).unwrap(),
        auth: Some(HubAuth::new(Box::new(
            LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap(),
        ))),
        public_base_url: "http://localhost".to_owned(),
        http_client: None,
        webhook_secret_cipher: None,
    };
    let app = hub_routes(state, false);
    let request = |path: &str, scope: Option<&str>| {
        Request::builder()
            .uri(path)
            .header("Authorization", format!("Bearer {}", token("pages", scope)))
            .body(Body::empty())
            .unwrap()
    };
    let response = app
        .clone()
        .oneshot(request(
            "/api/models/alice/pages/commits/main?limit=1",
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let link = response.headers()["link"].to_str().unwrap().to_owned();
    let next = url::Url::parse(link.split_once('>').unwrap().0.trim_start_matches('<')).unwrap();
    assert_eq!(
        next.origin(),
        url::Url::parse("http://localhost").unwrap().origin()
    );
    let rows: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows.get(0).unwrap().get("title").unwrap(), "head");
    let newer = "c".repeat(64);
    store.store_files(&newer, &[]).unwrap();
    store
        .create_revision("alice/pages", Some(&head), &newer, "main", "newer")
        .unwrap();
    let path = format!("{}?{}", next.path(), next.query().unwrap());
    let next_response = app
        .clone()
        .oneshot(request(&path, Some("main")))
        .await
        .unwrap();
    assert_eq!(next_response.status(), StatusCode::OK);
    assert!(!next_response.headers().contains_key("link"));
    let next_rows: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(next_response.into_body(), 65536)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(next_rows.get(0).unwrap().get("title").unwrap(), "parent");
    let denied = app
        .clone()
        .oneshot(request(&path, Some(&head)))
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    for limit in ["0", "101"] {
        let invalid = app
            .clone()
            .oneshot(request(
                &format!("/api/models/alice/pages/commits/main?limit={limit}"),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    }
}
