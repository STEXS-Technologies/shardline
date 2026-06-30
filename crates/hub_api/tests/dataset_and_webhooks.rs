#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string
)]

//! Integration tests for dataset viewer and webhook endpoints.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use shardline_hub_api::routes::HubState;
use shardline_index::hub::{BoxedHubStore, HubFileEntry, HubRepoType};
use shardline_index::LocalIndexStore;
use std::sync::{Mutex, Once, OnceLock};
use tempfile::TempDir;
use tower::ServiceExt;

use common::{app, setup};

// ---- Dataset viewer tests ----

#[tokio::test]
async fn dataset_parquet_lists_data_files() {
    setup();
    let store = shardline_hub_api::state::get_for_test().store.clone();

    // Create a dataset repo
    store
        .create_repo(HubRepoType::Dataset, "team/dataset", false)
        .unwrap();

    // Create a revision with parquet and CSV files
    let files = vec![
        HubFileEntry {
            path: "default/train/data.parquet".to_owned(),
            size: 1024,
            sha: "sha_parquet".to_owned(),
            is_lfs: false,
            inline_content: None,
        },
        HubFileEntry {
            path: "default/test/data.csv".to_owned(),
            size: 512,
            sha: "sha_csv".to_owned(),
            is_lfs: false,
            inline_content: None,
        },
        HubFileEntry {
            path: "README.md".to_owned(),
            size: 100,
            sha: "sha_readme".to_owned(),
            is_lfs: false,
            inline_content: Some(b"# Dataset".to_vec()),
        },
    ];
    store.store_files("commit1", &files).unwrap();
    store
        .create_revision("team/dataset", None, "commit1", "main", "init")
        .unwrap();

    let response = app()
        .oneshot(
            Request::builder()
                .uri("/api/datasets/team/dataset/parquet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let files = json["files"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    let paths: Vec<&str> = files.iter().map(|f| f["path"].as_str().unwrap()).collect();
    assert!(paths.contains(&"default/train/data.parquet"));
    assert!(paths.contains(&"default/test/data.csv"));
}

#[tokio::test]
async fn dataset_first_rows_returns_jsonl_data() {
    setup();
    let store = shardline_hub_api::state::get_for_test().store.clone();

    store
        .create_repo(HubRepoType::Dataset, "team/jsonl-dataset", false)
        .unwrap();

    let jsonl_content = "{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n{\"id\":3,\"name\":\"charlie\"}\n";
    let files = vec![HubFileEntry {
        path: "data.jsonl".to_owned(),
        size: jsonl_content.len() as u64,
        sha: "sha_jsonl".to_owned(),
        is_lfs: false,
        inline_content: Some(jsonl_content.as_bytes().to_vec()),
    }];
    store.store_files("commit_jsonl", &files).unwrap();
    store
        .create_revision("team/jsonl-dataset", None, "commit_jsonl", "main", "init")
        .unwrap();

    let response = app()
        .oneshot(
            Request::builder()
                .uri("/api/datasets/team/jsonl-dataset/first-rows?split=train&limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["columns"]["id"], 1);
    assert_eq!(rows[0]["columns"]["name"], "alice");
    assert_eq!(rows[1]["columns"]["id"], 2);
    assert_eq!(rows[1]["columns"]["name"], "bob");
}

#[tokio::test]
async fn dataset_first_rows_returns_csv_data() {
    setup();
    let store = shardline_hub_api::state::get_for_test().store.clone();

    store
        .create_repo(HubRepoType::Dataset, "team/csv-dataset", false)
        .unwrap();

    let csv_content = "id,name,value\n1,alice,100\n2,bob,200\n";
    let files = vec![HubFileEntry {
        path: "data.csv".to_owned(),
        size: csv_content.len() as u64,
        sha: "sha_csv2".to_owned(),
        is_lfs: false,
        inline_content: Some(csv_content.as_bytes().to_vec()),
    }];
    store.store_files("commit_csv", &files).unwrap();
    store
        .create_revision("team/csv-dataset", None, "commit_csv", "main", "init")
        .unwrap();

    let response = app()
        .oneshot(
            Request::builder()
                .uri("/api/datasets/team/csv-dataset/first-rows")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["columns"]["id"], 1);
    assert_eq!(rows[0]["columns"]["name"], "alice");
    assert_eq!(rows[0]["columns"]["value"], 100);
}

#[tokio::test]
async fn dataset_viewer_returns_paginated_rows() {
    setup();
    let store = shardline_hub_api::state::get_for_test().store.clone();

    store
        .create_repo(HubRepoType::Dataset, "team/paginated", false)
        .unwrap();

    let mut jsonl = String::new();
    for i in 0..10 {
        jsonl.push_str(&format!("{{\"index\":{i}}}\n"));
    }
    let files = vec![HubFileEntry {
        path: "data.jsonl".to_owned(),
        size: jsonl.len() as u64,
        sha: "sha_paginated".to_owned(),
        is_lfs: false,
        inline_content: Some(jsonl.into_bytes()),
    }];
    store.store_files("commit_paginated", &files).unwrap();
    store
        .create_revision("team/paginated", None, "commit_paginated", "main", "init")
        .unwrap();

    let response = app()
        .oneshot(
            Request::builder()
                .uri("/api/datasets/team/paginated/viewer/train?offset=3&length=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["columns"]["index"], 3);
    assert_eq!(rows[1]["columns"]["index"], 4);
}

#[tokio::test]
async fn dataset_parquet_rejects_non_dataset_repo() {
    setup();
    let store = shardline_hub_api::state::get_for_test().store.clone();

    store
        .create_repo(HubRepoType::Model, "team/model", false)
        .unwrap();
    store
        .create_revision("team/model", None, "sha1", "main", "init")
        .unwrap();

    let response = app()
        .oneshot(
            Request::builder()
                .uri("/api/datasets/team/model/parquet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ---- Webhook tests ----

#[tokio::test]
async fn webhook_crud_lifecycle() {
    setup();
    let store = shardline_hub_api::state::get_for_test().store.clone();

    store
        .create_repo(HubRepoType::Model, "team/webhook-model", false)
        .unwrap();
    store
        .create_revision("team/webhook-model", None, "sha1", "main", "init")
        .unwrap();

    // Create webhook
    let create_body = serde_json::json!({
        "url": "https://example.com/hook",
        "events": ["push", "delete"],
        "secret": "my-secret"
    });
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/team/webhook-model/webhooks")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = collect_body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let webhook_id = json["id"].as_str().unwrap().to_owned();
    assert_eq!(json["url"], "https://example.com/hook");
    assert_eq!(json["active"], true);

    // List webhooks
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/api/models/team/webhook-model/webhooks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let webhooks = json["webhooks"].as_array().unwrap();
    assert_eq!(webhooks.len(), 1);
    assert_eq!(webhooks[0]["id"], webhook_id);

    // Delete webhook
    let response = app()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!(
                    "/api/models/team/webhook-model/webhooks/{webhook_id}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // List webhooks (should be empty)
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/api/models/team/webhook-model/webhooks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let webhooks = json["webhooks"].as_array().unwrap();
    assert!(webhooks.is_empty());
}

#[tokio::test]
async fn webhook_create_rejects_nonexistent_repo() {
    setup();

    let create_body = serde_json::json!({
        "url": "https://example.com/hook",
        "events": ["push"]
    });
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/team/nonexistent/webhooks")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

async fn collect_body_bytes(response: axum::response::Response) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes()
        .to_vec()
}
