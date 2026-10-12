//! Request-count and local timing drill, not a cloud or whole-GC benchmark.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::IntoResponse,
};
use shardline_index::PostgresIndexStore;
use shardline_protocol::SecretString;
use shardline_server_core::{ServerObjectStore, remote_delete::delete_with_postgres_guard};
use shardline_storage::{ObjectKey, S3ObjectStoreConfig};
use sqlx::postgres::PgPoolOptions;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

#[derive(Debug, Default, Clone)]
struct Requests {
    user_deletes: BTreeMap<String, usize>,
    receipt_puts: usize,
    receipt_deletes: usize,
    unexpected: usize,
    receipt_bytes: usize,
}

#[derive(Default)]
struct Provider {
    users: BTreeSet<String>,
    receipts: BTreeMap<String, Bytes>,
    requests: Requests,
}

async fn request(
    State(state): State<Arc<Mutex<Provider>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let mut provider = state.lock().unwrap();
    let path = uri.path().to_owned();
    // The ordinary evidence client uses S3's bulk-delete endpoint, even for
    // one receipt. User data uses the dedicated single-attempt DELETE client.
    if method == Method::POST && uri.query() == Some("delete") {
        let xml = std::str::from_utf8(&body).unwrap();
        let key = xml
            .split_once("<Key>")
            .unwrap()
            .1
            .split_once("</Key>")
            .unwrap()
            .0;
        assert!(key.starts_with("_shardline_tasks/v1/delete-replies/"));
        assert_eq!(xml.matches("<Key>").count(), 1);
        assert!(
            provider
                .receipts
                .remove(&format!("/assets/{key}"))
                .is_some()
        );
        provider.requests.receipt_deletes =
            provider.requests.receipt_deletes.checked_add(1).unwrap();
        return (
            StatusCode::OK,
            [("etag", "\"fixture\"")],
            format!("<DeleteResult><Deleted><Key>{key}</Key></Deleted></DeleteResult>"),
        );
    }
    let receipt = path.starts_with("/assets/_shardline_tasks/v1/delete-replies/");
    let status = if receipt && method == Method::PUT {
        provider.requests.receipt_puts = provider.requests.receipt_puts.checked_add(1).unwrap();
        provider.requests.receipt_bytes = provider
            .requests
            .receipt_bytes
            .checked_add(body.len())
            .unwrap();
        if headers
            .get("if-none-match")
            .and_then(|value| value.to_str().ok())
            != Some("*")
        {
            provider.requests.unexpected = provider.requests.unexpected.checked_add(1).unwrap();
            StatusCode::BAD_REQUEST
        } else if let std::collections::btree_map::Entry::Vacant(entry) =
            provider.receipts.entry(path)
        {
            entry.insert(body);
            StatusCode::OK
        } else {
            StatusCode::PRECONDITION_FAILED
        }
    } else if receipt && method == Method::DELETE {
        provider.requests.receipt_deletes =
            provider.requests.receipt_deletes.checked_add(1).unwrap();
        provider.receipts.remove(&path);
        StatusCode::NO_CONTENT
    } else if method == Method::DELETE && provider.users.remove(&path) {
        let count = provider.requests.user_deletes.entry(path).or_default();
        *count = count.checked_add(1).unwrap();
        StatusCode::NO_CONTENT
    } else {
        provider.requests.unexpected = provider.requests.unexpected.checked_add(1).unwrap();
        StatusCode::BAD_REQUEST
    };
    (status, [("etag", "\"fixture\"")], String::new())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "cost drill requires an isolated migrated DATABASE_URL"]
async fn guarded_delete_batch_request_cost() {
    let url = std::env::var("DATABASE_URL").expect("isolated migrated DATABASE_URL");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let schema = format!(
        "delete_cost_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let tables: Vec<String> = sqlx::query_scalar("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'shardline_%'")
        .fetch_all(&admin).await.unwrap();
    assert!(!tables.is_empty(), "fixture must have released migrations");
    for table in tables {
        let table = format!("\"{}\"", table.replace('"', "\"\""));
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {schema}.{table} (LIKE public.{table} INCLUDING ALL)"
        )))
        .execute(&admin)
        .await
        .unwrap();
    }
    let session_schema = schema.clone();
    let connected = Arc::new(AtomicBool::new(false));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .after_connect(move |connection, _metadata| {
            let first = !connected.swap(true, Ordering::AcqRel);
            let work_schema = session_schema.clone();
            Box::pin(async move {
                if !first {
                    return Err(sqlx::Error::Protocol(
                        "cost fixture refuses replacement work sessions".to_owned(),
                    ));
                }
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "SET search_path TO {work_schema},public"
                )))
                .execute(connection)
                .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .unwrap();
    let index = PostgresIndexStore::new(pool.clone());
    index.initialize_remote_delete_guard().await.unwrap();
    // Same exclusive session lock as ordinary GC, retained by this one pool.
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(0x5348_4152_4447_4301_i64)
        .execute(&pool)
        .await
        .unwrap();

    let provider = Arc::new(Mutex::new(Provider::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new().fallback(request).with_state(provider.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let objects = ServerObjectStore::s3(
        S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
            .with_endpoint(Some(format!("http://{address}")))
            .with_credentials(
                Some(SecretString::new("fixture".to_owned())),
                Some(SecretString::new("fixture".to_owned())),
                None,
            )
            .with_allow_http(true),
    )
    .unwrap();
    let ServerObjectStore::S3(s3) = &objects else {
        panic!("S3 fixture expected");
    };

    for size in [1_usize, 64, 1024] {
        for saved_reply in [false, true] {
            let keys: Vec<_> = (0..size)
                .map(|number| {
                    ObjectKey::parse(&format!("cost/{saved_reply}/{size}/{number}")).unwrap()
                })
                .collect();
            {
                let mut provider = provider.lock().unwrap();
                assert!(provider.users.is_empty());
                assert!(
                    provider.receipts.is_empty(),
                    "remaining: {:?}; requests: {:?}",
                    provider.receipts.keys().collect::<Vec<_>>(),
                    provider.requests
                );
                provider.requests = Requests::default();
                provider
                    .users
                    .extend(keys.iter().map(|key| format!("/assets/{}", key.as_str())));
            }
            let start = Instant::now();
            let mut micros = Vec::with_capacity(size);
            for key in &keys {
                let each = Instant::now();
                if saved_reply {
                    delete_with_postgres_guard(&index, &objects, key)
                        .await
                        .unwrap();
                } else {
                    // A guarded baseline without saved-reply signing/publication.
                    let permit = index.begin_remote_delete(key).await.unwrap();
                    s3.delete_if_present_single_attempt(key).await.unwrap();
                    assert!(
                        index
                            .finish_acknowledged_remote_delete(&permit)
                            .await
                            .unwrap()
                    );
                }
                micros.push(each.elapsed().as_micros());
            }
            let elapsed = start.elapsed();
            micros.sort_unstable();
            let requests = {
                let provider = provider.lock().unwrap();
                assert!(provider.users.is_empty());
                assert!(
                    provider.receipts.is_empty(),
                    "remaining: {:?}; requests: {:?}",
                    provider.receipts.keys().collect::<Vec<_>>(),
                    provider.requests
                );
                provider.requests.clone()
            };
            assert_eq!(requests.unexpected, 0, "unexpected or repeated request");
            assert_eq!(requests.user_deletes.len(), size);
            assert!(requests.user_deletes.values().all(|count| *count == 1));
            assert_eq!(requests.receipt_puts, if saved_reply { size } else { 0 });
            assert_eq!(requests.receipt_deletes, if saved_reply { size } else { 0 });
            println!(
                "delete_cost {}",
                serde_json::json!({
                    "objects": size, "saved_reply": saved_reply, "elapsed_us": elapsed.as_micros(),
                    "p50_us": micros.get(size / 2).unwrap(), "p95_us": micros.get((size.saturating_mul(95) / 100).min(size.saturating_sub(1))).unwrap(),
                    "user_delete_requests": requests.user_deletes.len(), "receipt_put_requests": requests.receipt_puts,
                    "receipt_cleanup_requests": requests.receipt_deletes, "receipt_bytes": requests.receipt_bytes,
                    "unexpected_requests": requests.unexpected,
                })
            );
            assert!(
                index
                    .pending_remote_deletes(0, 100)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_empty()
            );
        }
    }
    pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
