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

#[path = "support/common.rs"]
mod common;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Schema};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serial_test::serial;
use shardline_index::hub::{HubFileEntry, HubRepoType};
use shardline_protocol::ShardlineHash;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::ServiceExt;

use common::{app, setup, state};

// ---- Dataset viewer tests ----

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataset_parquet_lists_data_files() {
    setup();
    let store = common::state().store.clone();

    // Create a dataset repo
    store
        .create_repo(HubRepoType::Dataset, "team/dataset", false)
        .unwrap();

    // Create a revision with parquet and CSV files
    let files = vec![
        HubFileEntry {
            path: "default/train/data.parquet".to_owned(),
            size: 1024,
            sha: "1010101010101010101010101010101010101010101010101010101010101010".to_owned(),
            is_lfs: false,
        },
        HubFileEntry {
            path: "default/test/data.csv".to_owned(),
            size: 512,
            sha: "1111111111111111111111111111111111111111111111111111111111111111".to_owned(),
            is_lfs: false,
        },
        HubFileEntry {
            path: "README.md".to_owned(),
            size: 100,
            sha: "1212121212121212121212121212121212121212121212121212121212121212".to_owned(),
            is_lfs: false,
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

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataset_first_rows_returns_jsonl_data() {
    setup();
    let store = common::state().store.clone();

    store
        .create_repo(HubRepoType::Dataset, "team/jsonl-dataset", false)
        .unwrap();

    let jsonl_content: String = (1..=20_000)
        .map(|id| format!("{{\"id\":{id},\"name\":\"name-{id}\"}}\n"))
        .collect();
    let files = vec![HubFileEntry {
        path: "data.jsonl".to_owned(),
        size: jsonl_content.len() as u64,
        sha: "1313131313131313131313131313131313131313131313131313131313131313".to_owned(),
        is_lfs: false,
    }];
    store.store_files("commit_jsonl", &files).unwrap();
    // Pre-populate ObjectStore
    let key = ObjectKey::parse("protocols/lfs/global/objects/1313131313131313131313131313131313131313131313131313131313131313").unwrap();
    let body = ObjectBody::from_slice(jsonl_content.as_bytes());
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(
            *blake3::hash(jsonl_content.as_bytes()).as_bytes(),
        ),
        jsonl_content.len() as u64,
    );
    state()
        .object_store
        .put_if_absent(&key, body, &integrity)
        .unwrap();
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
    assert_eq!(rows[0]["columns"]["name"], "name-1");
    assert_eq!(rows[1]["columns"]["id"], 2);
    assert_eq!(rows[1]["columns"]["name"], "name-2");

    let response = app()
        .oneshot(
            Request::builder()
                .uri("/api/datasets/team/jsonl-dataset/viewer/train?offset=19999&length=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["rows"].as_array().unwrap().len(), 1);
    assert_eq!(json["rows"][0]["columns"]["id"], 20_000);
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataset_first_rows_reads_parquet_with_bounded_range_reader() {
    setup();
    let store = common::state().store.clone();
    store
        .create_repo(HubRepoType::Dataset, "team/parquet-dataset", false)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (1..=1000).collect();
    let names: Vec<String> = ids.iter().map(|id| format!("name-{id}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
        ],
    )
    .unwrap();
    let mut parquet = Vec::new();
    {
        let cursor = std::io::Cursor::new(&mut parquet);
        let properties = WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();
        let mut writer = ArrowWriter::try_new(cursor, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    let sha = "1717171717171717171717171717171717171717171717171717171717171717";
    let files = vec![HubFileEntry {
        path: "data/train/data.parquet".into(),
        size: parquet.len() as u64,
        sha: sha.into(),
        is_lfs: false,
    }];
    let revision = "a111111111111111111111111111111111111111";
    store.store_files(revision, &files).unwrap();
    store
        .create_revision("team/parquet-dataset", None, revision, "main", "init")
        .unwrap();
    let key = ObjectKey::parse(&format!("protocols/lfs/global/objects/{sha}")).unwrap();
    common::state()
        .object_store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(&parquet),
            &ObjectIntegrity::new(
                ShardlineHash::from_bytes(*blake3::hash(&parquet).as_bytes()),
                parquet.len() as u64,
            ),
        )
        .unwrap();
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/api/datasets/team/parquet-dataset/first-rows?limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = collect_body_bytes(response).await;
    assert_eq!(status, StatusCode::OK, "body={body:?}");
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["columns"], serde_json::json!(["id", "name"]));
    assert_eq!(json["rows"].as_array().unwrap().len(), 2);
    assert_eq!(json["rows"][0]["columns"]["name"], "name-1");

    let unknown_column = serde_json::json!({
        "repository": "team/parquet-dataset", "revision": revision, "file_sha": sha,
        "config": "default", "split": "train", "columns": ["does_not_exist"], "limit": 1
    });
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/datasets/team/parquet-dataset/query")
                .header("content-type", "application/json")
                .body(Body::from(unknown_column.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let unknown_order = serde_json::json!({
        "repository": "team/parquet-dataset", "revision": revision, "file_sha": sha,
        "config": "default", "split": "train",
        "order_by": [{"column": "does_not_exist", "descending": true}], "limit": 1
    });
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/datasets/team/parquet-dataset/query")
                .header("content-type", "application/json")
                .body(Body::from(unknown_order.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Advance the branch. Structured queries must still be able to read the
    // explicitly pinned immutable revision rather than silently switching to
    // the new head.
    let newer_revision = "c222222222222222222222222222222222222222";
    store.store_files(newer_revision, &files).unwrap();
    store
        .create_revision(
            "team/parquet-dataset",
            Some(revision),
            newer_revision,
            "main",
            "advance",
        )
        .unwrap();

    let query = serde_json::json!({
        "repository": "team/parquet-dataset",
        "revision": revision,
        "file_sha": sha,
        "config": "default",
        "split": "train",
        "columns": ["name"],
        "limit": 1
    });
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/datasets/team/parquet-dataset/query")
                .header("content-type", "application/json")
                .body(Body::from(query.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body_bytes(response).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["columns"], serde_json::json!(["name"]));
    assert_eq!(json["rows"][0]["columns"]["name"], "name-1");

    let filtered = serde_json::json!({
        "repository": "team/parquet-dataset", "revision": revision, "file_sha": sha,
        "config": "default", "split": "train", "predicates": [{"column": "id", "op": "gt", "value": 1}], "limit": 10
    });
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/datasets/team/parquet-dataset/query")
                .header("content-type", "application/json")
                .body(Body::from(filtered.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json: serde_json::Value =
        serde_json::from_slice(&collect_body_bytes(response).await).unwrap();
    assert_eq!(json["rows"].as_array().unwrap().len(), 10);

    let aggregate = serde_json::json!({
        "repository": "team/parquet-dataset", "revision": revision, "file_sha": sha,
        "config": "default", "split": "train", "aggregates": [{"function": "count", "alias": "rows"}], "limit": 10
    });
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/datasets/team/parquet-dataset/query")
                .header("content-type", "application/json")
                .body(Body::from(aggregate.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json: serde_json::Value =
        serde_json::from_slice(&collect_body_bytes(response).await).unwrap();
    assert_eq!(json["rows"][0]["columns"]["rows"], 1000);

    let ordered = serde_json::json!({
        "repository": "team/parquet-dataset", "revision": revision, "file_sha": sha,
        "config": "default", "split": "train", "order_by": [{"column": "id", "descending": true}], "limit": 1
    });
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/datasets/team/parquet-dataset/query")
                .header("content-type", "application/json")
                .body(Body::from(ordered.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json: serde_json::Value =
        serde_json::from_slice(&collect_body_bytes(response).await).unwrap();
    assert_eq!(json["rows"][0]["columns"]["id"], 1000);

    let forged = serde_json::json!({
        "repository": "team/parquet-dataset", "revision": revision,
        "file_sha": "1818181818181818181818181818181818181818181818181818181818181818",
        "config": "default", "split": "train", "limit": 1
    });
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/datasets/team/parquet-dataset/query")
                .header("content-type", "application/json")
                .body(Body::from(forged.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = collect_body_bytes(response).await;
    assert!(!String::from_utf8_lossy(&body).contains("181818"));
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataset_first_rows_preserves_nested_and_null_parquet_values() {
    setup();
    let store = common::state().store.clone();
    let repo = "team/nested-null-parquet";
    let revision = "d3333333333333333333333333333333333333";
    let sha = "1718181818181818181818181818181818181818181818181818181818181818";
    store
        .create_repo(HubRepoType::Dataset, repo, false)
        .unwrap();
    let nested_field = Arc::new(Field::new("score", DataType::Int64, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new(
            "meta",
            DataType::Struct(vec![nested_field.clone()].into()),
            true,
        ),
    ]));
    let nested = StructArray::new(
        vec![nested_field].into(),
        vec![Arc::new(Int64Array::from(vec![Some(7), None])) as ArrayRef],
        None,
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("ok"), None])) as ArrayRef,
            Arc::new(nested) as ArrayRef,
        ],
    )
    .unwrap();
    let mut parquet = Vec::new();
    {
        let cursor = std::io::Cursor::new(&mut parquet);
        let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    store
        .store_files(
            revision,
            &[HubFileEntry {
                path: "data/train/data.parquet".into(),
                size: parquet.len() as u64,
                sha: sha.into(),
                is_lfs: false,
            }],
        )
        .unwrap();
    store
        .create_revision(repo, None, revision, "main", "nested null")
        .unwrap();
    let key = ObjectKey::parse(&format!("protocols/lfs/global/objects/{sha}")).unwrap();
    common::state()
        .object_store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(&parquet),
            &ObjectIntegrity::new(
                ShardlineHash::from_bytes(*blake3::hash(&parquet).as_bytes()),
                parquet.len() as u64,
            ),
        )
        .unwrap();

    let response = app()
        .oneshot(
            Request::builder()
                .uri(format!("/api/datasets/{repo}/first-rows?limit=2"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json: serde_json::Value =
        serde_json::from_slice(&collect_body_bytes(response).await).unwrap();
    assert_eq!(json["rows"].as_array().unwrap().len(), 2);
    assert_eq!(json["rows"][0]["columns"]["label"], "ok");
    assert!(json["rows"][1]["columns"]["label"].is_null());
    assert_eq!(json["rows"][0]["columns"]["meta"]["score"], 7);
    assert!(json["rows"][1]["columns"]["meta"]["score"].is_null());
}

/// Exercises the native query endpoint over a real TCP connection.  This is
/// deliberately separate from the `oneshot` coverage above: it catches
/// release-only codec/configuration regressions (for example, a server built
/// without Snappy support) and validates the production HTTP body path.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataset_query_http_e2e_reads_compressed_multiple_row_groups() {
    setup();
    let repo = format!("team/http-e2e-{}", std::process::id());
    let revision = format!("b{:039}", std::process::id());
    let sha = format!("{:0>64}", "18");
    let store = common::state().store.clone();
    store
        .create_repo(HubRepoType::Dataset, &repo, false)
        .unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((1..=100).collect::<Vec<_>>())) as ArrayRef],
    )
    .unwrap();
    let mut parquet = Vec::new();
    {
        let cursor = std::io::Cursor::new(&mut parquet);
        let properties = WriterProperties::builder()
            .set_max_row_group_size(10)
            .build();
        let mut writer = ArrowWriter::try_new(cursor, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    let files = vec![HubFileEntry {
        path: "data/train/data.parquet".into(),
        size: parquet.len() as u64,
        sha: sha.clone(),
        is_lfs: false,
    }];
    store.store_files(&revision, &files).unwrap();
    store
        .create_revision(&repo, None, &revision, "main", "http e2e")
        .unwrap();
    let key = ObjectKey::parse(&format!("protocols/lfs/global/objects/{sha}")).unwrap();
    common::state()
        .object_store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(&parquet),
            &ObjectIntegrity::new(
                ShardlineHash::from_bytes(*blake3::hash(&parquet).as_bytes()),
                parquet.len() as u64,
            ),
        )
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app())
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{address}/api/datasets/{repo}/query"))
        .json(&serde_json::json!({
            "repository": repo,
            "revision": revision,
            "file_sha": sha,
            "config": "default",
            "split": "train",
            "columns": ["id"],
            "limit": 25
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["rows"].as_array().unwrap().len(), 25);
    assert_eq!(body["rows"][0]["columns"]["id"], 1);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataset_query_rejects_body_repository_mismatch() {
    setup();
    let store = common::state().store.clone();
    store
        .create_repo(HubRepoType::Dataset, "team/query-boundary", false)
        .unwrap();
    let revision = "b222222222222222222222222222222222222222";
    let sha = "1818181818181818181818181818181818181818181818181818181818181818";
    store
        .store_files(
            revision,
            &[HubFileEntry {
                path: "data.parquet".into(),
                size: 0,
                sha: sha.into(),
                is_lfs: false,
            }],
        )
        .unwrap();
    store
        .create_revision("team/query-boundary", None, revision, "main", "init")
        .unwrap();
    let query = serde_json::json!({
        "repository": "team/another-repository",
        "revision": revision,
        "file_sha": sha,
        "config": "default",
        "split": "train",
        "limit": 1
    });
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/datasets/team/query-boundary/query")
                .header("content-type", "application/json")
                .body(Body::from(query.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataset_query_redacts_malformed_parquet_errors() {
    setup();
    let store = common::state().store.clone();
    store
        .create_repo(HubRepoType::Dataset, "team/malformed-query", false)
        .unwrap();
    let revision = "d333333333333333333333333333333333333333";
    let sha = "1919191919191919191919191919191919191919191919191919191919191919";
    let content = b"not parquet: provider-secret-token";
    store
        .store_files(
            revision,
            &[HubFileEntry {
                path: "data.parquet".into(),
                size: content.len() as u64,
                sha: sha.into(),
                is_lfs: false,
            }],
        )
        .unwrap();
    store
        .create_revision("team/malformed-query", None, revision, "main", "init")
        .unwrap();
    let key = ObjectKey::parse(&format!("protocols/lfs/global/objects/{sha}")).unwrap();
    common::state()
        .object_store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(content),
            &ObjectIntegrity::new(
                ShardlineHash::from_bytes(*blake3::hash(content).as_bytes()),
                content.len() as u64,
            ),
        )
        .unwrap();
    let request = serde_json::json!({
        "repository": "team/malformed-query",
        "revision": revision,
        "file_sha": sha,
        "config": "default",
        "split": "train",
        "limit": 1
    });
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/datasets/team/malformed-query/query")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = String::from_utf8_lossy(&collect_body_bytes(response).await).into_owned();
    assert!(body.contains("invalid parquet input"));
    assert!(!body.contains("provider-secret-token"));
    assert!(!body.contains(sha));
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataset_query_can_be_disabled_without_disabling_legacy_routes() {
    setup();
    common::state()
        .store
        .create_repo(HubRepoType::Dataset, "team/disabled", false)
        .unwrap();
    common::state()
        .store
        .store_files("e444444444444444444444444444444444444444", &[])
        .unwrap();
    common::state()
        .store
        .create_revision(
            "team/disabled",
            None,
            "e444444444444444444444444444444444444444",
            "main",
            "init",
        )
        .unwrap();
    let app =
        shardline_hub_api::hub_routes_with_dataset_query(common::state().clone(), true, false);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/datasets/team/disabled/query")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response =
        shardline_hub_api::hub_routes_with_dataset_query(common::state().clone(), true, false)
            .oneshot(
                Request::builder()
                    .uri("/api/datasets/team/disabled/first-rows")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataset_first_rows_returns_csv_data() {
    setup();
    let store = common::state().store.clone();

    store
        .create_repo(HubRepoType::Dataset, "team/csv-dataset", false)
        .unwrap();

    let csv_content = "id,name,value\n1,alice,100\n2,bob,200\n";
    let files = vec![HubFileEntry {
        path: "data.csv".to_owned(),
        size: csv_content.len() as u64,
        sha: "1414141414141414141414141414141414141414141414141414141414141414".to_owned(),
        is_lfs: false,
    }];
    store.store_files("commit_csv", &files).unwrap();
    // Pre-populate ObjectStore
    let key = ObjectKey::parse("protocols/lfs/global/objects/1414141414141414141414141414141414141414141414141414141414141414").unwrap();
    let body = ObjectBody::from_slice(csv_content.as_bytes());
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(
            *blake3::hash(csv_content.as_bytes()).as_bytes(),
        ),
        csv_content.len() as u64,
    );
    state()
        .object_store
        .put_if_absent(&key, body, &integrity)
        .unwrap();
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

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataset_viewer_returns_paginated_rows() {
    setup();
    let store = common::state().store.clone();

    store
        .create_repo(HubRepoType::Dataset, "team/paginated", false)
        .unwrap();

    let mut jsonl = String::new();
    for i in 0..10 {
        jsonl.push_str(&format!("{{\"index\":{i}}}\n"));
    }
    let jsonl_bytes = jsonl.into_bytes();
    let files = vec![HubFileEntry {
        path: "data.jsonl".to_owned(),
        size: jsonl_bytes.len() as u64,
        sha: "1515151515151515151515151515151515151515151515151515151515151515".to_owned(),
        is_lfs: false,
    }];
    store.store_files("commit_paginated", &files).unwrap();
    // Pre-populate ObjectStore
    let key = ObjectKey::parse("protocols/lfs/global/objects/1515151515151515151515151515151515151515151515151515151515151515").unwrap();
    let body = ObjectBody::from_slice(&jsonl_bytes);
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(&jsonl_bytes).as_bytes()),
        jsonl_bytes.len() as u64,
    );
    state()
        .object_store
        .put_if_absent(&key, body, &integrity)
        .unwrap();
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

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataset_parquet_rejects_non_dataset_repo() {
    setup();
    let store = common::state().store.clone();

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

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhook_crud_lifecycle() {
    setup();
    let store = common::state().store.clone();

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

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
