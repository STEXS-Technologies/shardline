//! Official CAS file-existence probes must remain scoped metadata reads.

use reqwest::{Client, StatusCode};

pub(super) async fn qualify(base: &str, token: &str, file: &str, length: u64) {
    let client = Client::new();
    let endpoint = format!("{base}/v1/files/{file}");
    let response = client
        .head(&endpoint)
        .bearer_auth(token)
        .header("Range", "bytes=999999-")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-length"], length.to_string());
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    assert!(response.bytes().await.unwrap().is_empty());

    let hidden = super::bearer_token(
        "other-user",
        super::TokenScope::Read,
        super::RepositoryProvider::GitHub,
        "other-team",
        "assets",
        Some("main"),
    )
    .unwrap();
    let response = client
        .head(&endpoint)
        .bearer_auth(hidden)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = client.head(&endpoint).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    for (hash, expected) in [
        ("0".repeat(64), StatusCode::NOT_FOUND),
        ("A".repeat(64), StatusCode::BAD_REQUEST),
        ("bad-hash".to_owned(), StatusCode::BAD_REQUEST),
    ] {
        let response = client
            .head(format!("{base}/v1/files/{hash}"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert!(response.bytes().await.unwrap().is_empty());
    }
    let response = client
        .get(endpoint)
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
}
