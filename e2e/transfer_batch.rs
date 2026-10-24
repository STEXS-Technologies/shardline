//! Real HTTP qualification for the upstream JSON batch reconstruction contract.

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

pub(super) async fn qualify(base: &str, token: &str, file: &str) {
    let client = Client::new();
    let endpoint = format!("{base}/v1/reconstructions");
    let key = json!({"prefix":"default", "hash":file});
    let response = client
        .post(&endpoint)
        .bearer_auth(token)
        .json(&json!([key.clone(), key.clone()]))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["files"].as_object().unwrap().len(), 1);
    assert_eq!(body["files"][file].as_array().unwrap().len(), 2);
    assert_eq!(body["fetch_info"].as_object().unwrap().len(), 2);

    let missing = json!({"prefix":"default", "hash":"0".repeat(64)});
    for keys in [json!([missing.clone()]), json!([key.clone(), missing])] {
        let response = client
            .post(&endpoint)
            .bearer_auth(token)
            .json(&keys)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "POST must not report partial success when a file is missing"
        );
        let body: Value = response.json().await.unwrap();
        assert!(
            body.get("fetch_info").is_none(),
            "failed batches must not expose capabilities"
        );
    }
    let response = client
        .post(&endpoint)
        .json(&json!([key.clone()]))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let hidden_token = super::bearer_token(
        "other-user",
        super::TokenScope::Read,
        super::RepositoryProvider::GitHub,
        "other-team",
        "assets",
        Some("main"),
    )
    .unwrap();
    let response = client
        .post(&endpoint)
        .bearer_auth(hidden_token)
        .json(&json!([key.clone()]))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // The pre-existing query form is intentionally a partial-result API.
    let response = client
        .get(&endpoint)
        .bearer_auth(token)
        .query(&[("file_id", file), ("file_id", &"0".repeat(64))])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["files"].as_object().unwrap().len(), 1);
    assert!(body["files"].get(file).is_some());

    for (keys, expected) in [
        (
            json!([{"prefix":"other", "hash":file}]),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!([{"prefix":"default", "hash":"bad-hash"}]),
            StatusCode::BAD_REQUEST,
        ),
        (
            Value::Array(vec![
                key;
                shardline_server::app::MAX_BATCH_RECONSTRUCTION_FILE_IDS
                    + 1
            ]),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
    ] {
        let response = client
            .post(&endpoint)
            .bearer_auth(token)
            .json(&keys)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
    for body in ["not-json", "{}", r#"[{"prefix":"default"}]"#] {
        let response = client
            .post(&endpoint)
            .bearer_auth(token)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    let response = client
        .post(&endpoint)
        .bearer_auth(token)
        .header("content-type", "application/json")
        .body(" ".repeat(shardline_server::app::MAX_BATCH_RECONSTRUCTION_REQUEST_BYTES + 1))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
