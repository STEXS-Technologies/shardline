//! Native S3 listing pagination through an actual loopback HTTP server.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use base64::Engine;
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, ServerFrontend, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use std::num::NonZeroUsize;

fn tags(xml: &str, name: &str) -> Vec<String> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    xml.split(&open)
        .skip(1)
        .map(|part| part.split_once(&close).unwrap().0.to_owned())
        .collect()
}

fn entries(xml: &str) -> Vec<String> {
    let mut entries = tags(xml, "Key");
    for block in xml.split("<CommonPrefixes>").skip(1) {
        entries.extend(tags(
            block.split_once("</CommonPrefixes>").unwrap().0,
            "Prefix",
        ));
    }
    entries.sort();
    entries
}

struct ListingClient {
    client: reqwest::Client,
    bucket: String,
    token: String,
}

impl ListingClient {
    async fn list(&self, params: &[(&str, &str)]) -> String {
        let response = self
            .client
            .get(&self.bucket)
            .bearer_auth(&self.token)
            .query(params)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let xml = response.text().await.unwrap();
        assert_eq!(status, 200, "{xml}");
        xml
    }

    async fn walk(&self, v2: bool, prefix: &str, delimiter: Option<&str>) -> Vec<String> {
        let mut seen = Vec::new();
        let mut cursor = None::<String>;
        // This fixture has fewer than twenty entries. A repeated cursor must fail,
        // rather than hang the integration test indefinitely.
        for _ in 0..20 {
            let mut params = vec![("max-keys", "1"), ("prefix", prefix)];
            if v2 {
                params.push(("list-type", "2"));
            }
            if let Some(delimiter) = delimiter {
                params.push(("delimiter", delimiter));
            }
            if let Some(cursor) = cursor.as_deref() {
                params.push((if v2 { "continuation-token" } else { "marker" }, cursor));
            }
            let xml = self.list(&params).await;
            let page = entries(&xml);
            assert!(page.len() <= 1, "logical entries must obey max-keys: {xml}");
            for entry in page {
                assert!(!seen.contains(&entry), "duplicate entry {entry}: {xml}");
                seen.push(entry);
            }
            if xml.contains("<IsTruncated>false</IsTruncated>") {
                return seen;
            }
            let next = tags(
                &xml,
                if v2 {
                    "NextContinuationToken"
                } else {
                    "NextMarker"
                },
            )
            .into_iter()
            .next()
            .expect("truncated page needs cursor");
            assert_ne!(cursor.as_ref(), Some(&next), "cursor must advance");
            cursor = Some(next);
        }
        panic!("listing failed to terminate");
    }
}

async fn check_listing(database_url: Option<&str>) {
    let root = tempfile::tempdir().unwrap();
    let signing_key = b"0123456789abcdef0123456789abcdef";
    let mut config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".into(),
        root.path().into(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends([ServerFrontend::S3])
    .unwrap()
    .with_token_signing_key(signing_key.to_vec())
    .unwrap()
    .with_reconstruction_cache_disabled();
    if let Some(url) = database_url {
        let pool = sqlx::PgPool::connect(url).await.unwrap();
        shardline_server::apply_database_migrations(&pool)
            .await
            .unwrap();
        pool.close().await;
        config = config.with_index_postgres_url(url.to_owned()).unwrap();
    }
    let name = format!(
        "listing-{}",
        root.path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .trim_start_matches('.')
            .to_ascii_lowercase()
    );
    let repo = RepositoryScope::new(RepositoryProvider::Generic, "audit", &name, None).unwrap();
    let token = LocalHmacProvider::new(signing_key)
        .unwrap()
        .mint_token(
            &TokenClaims::new("shardline", "audit", TokenScope::Write, repo, u64::MAX).unwrap(),
        )
        .unwrap();
    let router = app::router(config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bucket = format!("http://{}/audit.{name}", listener.local_addr().unwrap());
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _shutdown_result = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    let listing = ListingClient {
        client: reqwest::Client::new(),
        bucket,
        token,
    };
    let created_bucket = listing
        .client
        .put(&listing.bucket)
        .bearer_auth(&listing.token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        created_bucket.status(),
        200,
        "create bucket: {}",
        created_bucket.text().await.unwrap()
    );
    let keys = [
        "a-",
        "a/one",
        "a/two",
        "a0",
        "plain",
        "prefix",
        "prefix/child",
        "é",
        "é・one",
        "é・two",
        "é0",
    ];
    for key in keys {
        let mut url = reqwest::Url::parse(&listing.bucket).unwrap();
        url.path_segments_mut().unwrap().extend(key.split('/'));
        let response = listing
            .client
            .put(url)
            .bearer_auth(&listing.token)
            .body(format!("payload:{key}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            200,
            "upload {key}: {}",
            response.text().await.unwrap()
        );
    }
    for v2 in [false, true] {
        assert_eq!(
            listing.walk(v2, "", Some("/")).await,
            [
                "a-", "a/", "a0", "plain", "prefix", "prefix/", "é", "é0", "é・one", "é・two"
            ]
        );
        let mut sorted_keys = keys.to_vec();
        sorted_keys.sort();
        assert_eq!(listing.walk(v2, "", None).await, sorted_keys);
        assert_eq!(listing.walk(v2, "é", Some("・")).await, ["é", "é0", "é・"]);
        assert_eq!(
            listing.walk(v2, "prefix", Some("/")).await,
            ["prefix", "prefix/"]
        );
        let cursor_name = if v2 { "start-after" } else { "marker" };
        let mut params = vec![("delimiter", "/"), (cursor_name, "a/one")];
        if v2 {
            params.push(("list-type", "2"));
        }
        let xml = listing.list(&params).await;
        let actual = entries(&xml);
        assert!(
            !actual.contains(&"a/".to_owned()),
            "commonprefix <= marker must be excluded: {xml}"
        );
        assert!(
            actual.contains(&"a0".to_owned()),
            "exact successor must survive: {xml}"
        );
        let mut plain_params = vec![(cursor_name, "a/one"), ("max-keys", "1")];
        if v2 {
            plain_params.push(("list-type", "2"));
        }
        assert_eq!(entries(&listing.list(&plain_params).await), ["a/two"]);
        let mut unicode_params = vec![
            (cursor_name, "é・one"),
            ("prefix", "é"),
            ("delimiter", "・"),
        ];
        if v2 {
            unicode_params.push(("list-type", "2"));
        }
        assert!(
            entries(&listing.list(&unicode_params).await).is_empty(),
            "Unicode commonprefix sorts before the explicit cursor"
        );
        for bad in ["0", "invalid", "-1", "184467440737095516160"] {
            let mut invalid_params = vec![("max-keys", bad)];
            if v2 {
                invalid_params.push(("list-type", "2"));
            }
            let response = listing
                .client
                .get(&listing.bucket)
                .bearer_auth(&listing.token)
                .query(&invalid_params)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 400, "invalid max-keys {bad}");
        }
        let mut capped_params = vec![("max-keys", "1001")];
        if v2 {
            capped_params.push(("list-type", "2"));
        }
        let capped_xml = listing.list(&capped_params).await;
        assert_eq!(entries(&capped_xml).len(), keys.len());
        if !v2 {
            assert!(
                capped_xml.contains("<MaxKeys>1000</MaxKeys>"),
                "{capped_xml}"
            );
        }
    }
    let legacy_token = base64::engine::general_purpose::STANDARD.encode("a/one");
    let xml = listing
        .list(&[
            ("list-type", "2"),
            ("delimiter", "/"),
            ("continuation-token", &legacy_token),
            ("max-keys", "1"),
        ])
        .await;
    assert_eq!(
        entries(&xml),
        ["a0"],
        "legacy raw cursor inside group: {xml}"
    );
    // Boundary groups require a Unicode scalar successor, including the gap
    // between D7FF and E000 and a group with no finite upper successor.
    for boundary_key in [
        "g\u{d7ff}one",
        "g\u{d7ff}two",
        "g\u{e000}",
        "m\u{10ffff}one",
        "m\u{10ffff}two",
        "\u{10ffff}",
        "\u{10ffff}one",
        "\u{10ffff}two",
        "\u{10ffff}\u{10ffff}one",
        "\u{10ffff}\u{10ffff}two",
    ] {
        let mut boundary_url = reqwest::Url::parse(&listing.bucket).unwrap();
        boundary_url.path_segments_mut().unwrap().push(boundary_key);
        let boundary_response = listing
            .client
            .put(boundary_url)
            .bearer_auth(&listing.token)
            .body("boundary payload")
            .send()
            .await
            .unwrap();
        assert_eq!(
            boundary_response.status(),
            200,
            "boundary upload {boundary_key}"
        );
    }
    for boundary_v2 in [false, true] {
        assert_eq!(
            listing.walk(boundary_v2, "g", Some("\u{d7ff}")).await,
            ["g\u{d7ff}", "g\u{e000}"]
        );
        assert_eq!(
            listing.walk(boundary_v2, "m", Some("\u{10ffff}")).await,
            ["m\u{10ffff}"]
        );
        assert_eq!(
            listing.walk(boundary_v2, "\u{10ffff}", None).await,
            [
                "\u{10ffff}",
                "\u{10ffff}one",
                "\u{10ffff}two",
                "\u{10ffff}\u{10ffff}one",
                "\u{10ffff}\u{10ffff}two"
            ]
        );
        assert_eq!(
            listing
                .walk(boundary_v2, "\u{10ffff}", Some("\u{10ffff}"))
                .await,
            [
                "\u{10ffff}",
                "\u{10ffff}one",
                "\u{10ffff}two",
                "\u{10ffff}\u{10ffff}"
            ]
        );
        let boundary_cursor_name = if boundary_v2 { "start-after" } else { "marker" };
        for (boundary_cursor, expected) in [
            ("a/one", vec!["a/two"]),
            ("0", vec!["a/one", "a/two"]),
            ("z", vec![]),
        ] {
            let mut boundary_params = vec![
                ("prefix", "a/"),
                ("delimiter", "/"),
                (boundary_cursor_name, boundary_cursor),
            ];
            if boundary_v2 {
                boundary_params.push(("list-type", "2"));
            }
            let boundary_xml = listing.list(&boundary_params).await;
            assert_eq!(
                entries(&boundary_xml),
                expected,
                "cursor {boundary_cursor} must respect requested prefix: {boundary_xml}"
            );
        }
    }
    let _shutdown_result = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sqlite_native_s3_listing_walks_logical_entries() {
    check_listing(None).await;
}

#[tokio::test]
async fn postgres_native_s3_listing_walks_logical_entries() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping PostgreSQL listing test: DATABASE_URL is not set");
        return;
    };
    check_listing(Some(&url)).await;
}
