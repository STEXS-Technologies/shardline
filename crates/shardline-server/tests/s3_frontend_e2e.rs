//! Client-shaped end-to-end tests for the S3 **frontend** protocol surface.
//!
//! This file is deliberately distinct from `tests/s3_e2e_http.rs`, which
//! exercises the S3 *storage* adapter via LocalStack. Here we drive the S3
//! frontend the way real clients (pyarrow, Polars / object_store, S3A, DuckDB)
//! do: a real in-process `app::router` on a random port, minted bearer tokens
//! presented either as `Authorization: Bearer <token>` or as the SigV4
//! `Credential=<token>/…` access-key form (the signature is not verified).
//!
//! The harness is **hermetic**: SQLite metadata + a local object store, no
//! Docker / Postgres / LocalStack.

#![allow(
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::let_underscore_must_use,
    clippy::shadow_unrelated,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::string_add,
    clippy::format_push_string,
    clippy::option_if_let_else,
    clippy::or_fun_call,
    clippy::needless_borrows_for_generic_args,
    clippy::unnecessary_map_or
)]

use std::{num::NonZeroUsize, time::Duration};

use reqwest::header::{self, HeaderValue};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, ServerFrontend, ServerRole, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use tempfile::TempDir;
use tokio::net::TcpListener;

/// Test signing key matching the server's `with_token_signing_key`.
const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

/// The bucket name for a token scope `{owner}.{name}`.
fn bucket(owner: &str, name: &str) -> String {
    format!("{owner}.{name}")
}

/// Mints a bearer token scoped to `owner.name` with the test signing key.
fn mint_token(scope: TokenScope, owner: &str, name: &str) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, owner, name, None).unwrap();
    let claims = TokenClaims::new("shardline", "s3-e2e", scope, repo, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
}

// ---------------------------------------------------------------------------
// Test server harness — real HTTP server on a random port.
// ---------------------------------------------------------------------------

struct TestServer {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    base_url: String,
    _tmp: TempDir,
}

impl TestServer {
    /// Starts a full shardline server with the S3 frontend, backed by SQLite
    /// metadata + a local object store (hermetic — no Docker).
    async fn start() -> Self {
        let tmp = TempDir::new().unwrap();
        let chunk_size = NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_server_role(ServerRole::All)
        .with_server_frontends(vec![ServerFrontend::S3])
        .unwrap()
        .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
        .unwrap()
        .with_reconstruction_cache_disabled();

        config.validate_runtime_requirements().unwrap();
        let app = app::router(config).await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.ok();
                })
                .await
                .ok();
        });

        // Give the server a moment to start.
        tokio::time::sleep(Duration::from_millis(100)).await;

        Self {
            shutdown: Some(shutdown_tx),
            base_url,
            _tmp: tmp,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

// ---------------------------------------------------------------------------
// S3 client helpers — the way a real S3 SDK would drive the frontend.
// ---------------------------------------------------------------------------

/// How the client presents its credential: the SigV4 access-key bridge or a
/// plain bearer token.
#[derive(Clone, Copy)]
enum AuthStyle {
    SigV4,
    Bearer,
}

/// A minted credential for one `{owner}.{name}` scope.
struct Auth {
    token: String,
    style: AuthStyle,
}

impl Auth {
    fn write(owner: &str, name: &str, style: AuthStyle) -> Self {
        Self {
            token: mint_token(TokenScope::Write, owner, name),
            style,
        }
    }

    fn read(owner: &str, name: &str, style: AuthStyle) -> Self {
        Self {
            token: mint_token(TokenScope::Read, owner, name),
            style,
        }
    }

    fn header_value(&self) -> HeaderValue {
        let value = match self.style {
            AuthStyle::Bearer => format!("Bearer {}", self.token),
            AuthStyle::SigV4 => format!(
                "AWS4-HMAC-SHA256 Credential={}/20260813/us-east-1/s3/aws4_request, \
                 SignedHeaders=host;x-amz-date, Signature=abc123",
                self.token
            ),
        };
        HeaderValue::from_str(&value).unwrap()
    }
}

/// A thin wrapper over reqwest for the S3 frontend routes.
struct S3Client {
    http: reqwest::Client,
    base_url: String,
}

impl S3Client {
    fn new(server: &TestServer) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: server.base_url.clone(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn put(&self, bucket: &str, key: &str, body: Vec<u8>, auth: &Auth) -> reqwest::Response {
        self.http
            .put(self.url(&format!("/{bucket}/{key}")))
            .header(header::AUTHORIZATION, auth.header_value())
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .send()
            .await
            .unwrap()
    }

    async fn get(&self, bucket: &str, key: &str, auth: &Auth) -> reqwest::Response {
        self.http
            .get(self.url(&format!("/{bucket}/{key}")))
            .header(header::AUTHORIZATION, auth.header_value())
            .send()
            .await
            .unwrap()
    }

    async fn get_range(
        &self,
        bucket: &str,
        key: &str,
        range: &str,
        auth: &Auth,
    ) -> reqwest::Response {
        self.http
            .get(self.url(&format!("/{bucket}/{key}")))
            .header(header::AUTHORIZATION, auth.header_value())
            .header(header::RANGE, range)
            .send()
            .await
            .unwrap()
    }

    async fn head(&self, bucket: &str, key: &str, auth: &Auth) -> reqwest::Response {
        self.http
            .head(self.url(&format!("/{bucket}/{key}")))
            .header(header::AUTHORIZATION, auth.header_value())
            .send()
            .await
            .unwrap()
    }

    async fn head_bucket(&self, bucket: &str, auth: &Auth) -> reqwest::Response {
        self.http
            .head(self.url(&format!("/{bucket}")))
            .header(header::AUTHORIZATION, auth.header_value())
            .send()
            .await
            .unwrap()
    }

    async fn create_bucket(&self, bucket: &str, auth: &Auth) -> reqwest::Response {
        self.http
            .put(self.url(&format!("/{bucket}")))
            .header(header::AUTHORIZATION, auth.header_value())
            .send()
            .await
            .unwrap()
    }

    async fn get_bucket_location(&self, bucket: &str, auth: &Auth) -> reqwest::Response {
        self.http
            .get(self.url(&format!("/{bucket}?location")))
            .header(header::AUTHORIZATION, auth.header_value())
            .send()
            .await
            .unwrap()
    }

    async fn list(&self, bucket: &str, query: &str, auth: &Auth) -> reqwest::Response {
        self.http
            .get(self.url(&format!("/{bucket}?{query}")))
            .header(header::AUTHORIZATION, auth.header_value())
            .send()
            .await
            .unwrap()
    }

    async fn create_multipart(&self, bucket: &str, key: &str, auth: &Auth) -> String {
        let response = self
            .http
            .post(self.url(&format!("/{bucket}/{key}?uploads")))
            .header(header::AUTHORIZATION, auth.header_value())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200, "CreateMultipartUpload");
        let xml = response.text().await.unwrap();
        extract_tag(&xml, "UploadId")
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: Vec<u8>,
        auth: &Auth,
    ) -> reqwest::Response {
        self.http
            .put(self.url(&format!(
                "/{bucket}/{key}?partNumber={part_number}&uploadId={upload_id}"
            )))
            .header(header::AUTHORIZATION, auth.header_value())
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .send()
            .await
            .unwrap()
    }

    async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_numbers: &[u32],
        auth: &Auth,
    ) -> reqwest::Response {
        let body = complete_body(upload_id, part_numbers);
        self.http
            .post(self.url(&format!("/{bucket}/{key}?uploadId={upload_id}")))
            .header(header::AUTHORIZATION, auth.header_value())
            .header(header::CONTENT_TYPE, "application/xml")
            .body(body)
            .send()
            .await
            .unwrap()
    }
}

/// Extracts the text content of the first `<tag>…</tag>` occurrence.
fn extract_tag(xml: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open).unwrap() + open.len();
    let end = xml.find(&close).unwrap();
    xml[start..end].to_owned()
}

/// Builds a minimal `CompleteMultipartUpload` request body.
fn complete_body(upload_id: &str, part_numbers: &[u32]) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    for part in part_numbers {
        xml.push_str(&format!(
            "  <Part><PartNumber>{part}</PartNumber><ETag>\"{upload_id}-{part}\"</ETag></Part>\n"
        ));
    }
    xml.push_str("</CompleteMultipartUpload>\n");
    xml
}

/// Counts occurrences of a substring (used for `<Contents>`/`<CommonPrefixes>`).
fn count_occurrences(haystack: &str, needle: &str) -> usize {
    haystack.matches(needle).count()
}

// ===========================================================================
// pyarrow-style: SigV4 PUT/GET/Range + multipart of a large object
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_pyarrow_put_get_range_and_multipart() {
    let server = TestServer::start().await;
    let client = S3Client::new(&server);
    let bucket_name = bucket("acme", "models");
    let auth = Auth::write("acme", "models", AuthStyle::SigV4);

    // ── Single PUT → GET roundtrip ──────────────────────────────────────────
    let content = b"pyarrow-model-bytes-0123456789";
    let put = client
        .put(&bucket_name, "data/model.pt", content.to_vec(), &auth)
        .await;
    assert_eq!(put.status(), 200, "PutObject");
    let put_etag = put
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        put_etag.starts_with('"') && put_etag.ends_with('"'),
        "etag must be quoted: {put_etag}"
    );

    let get = client.get(&bucket_name, "data/model.pt", &auth).await;
    assert_eq!(get.status(), 200, "GetObject");
    assert_eq!(
        get.headers()
            .get(header::CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap(),
        content.len().to_string()
    );
    assert_eq!(get.bytes().await.unwrap().as_ref(), content);

    // ── Range: closed `bytes=a-b` → 206 + Content-Range ─────────────────────
    let ranged = client
        .get_range(&bucket_name, "data/model.pt", "bytes=4-9", &auth)
        .await;
    assert_eq!(ranged.status(), 206, "GetObject Range");
    assert_eq!(
        ranged
            .headers()
            .get(header::CONTENT_RANGE)
            .unwrap()
            .to_str()
            .unwrap(),
        format!("bytes 4-9/{}", content.len())
    );
    assert_eq!(ranged.bytes().await.unwrap().as_ref(), &content[4..10]);

    // ── Range: suffix `bytes=-N` ────────────────────────────────────────────
    let suffix = client
        .get_range(&bucket_name, "data/model.pt", "bytes=-5", &auth)
        .await;
    assert_eq!(suffix.status(), 206, "GetObject suffix Range");
    assert_eq!(
        suffix.bytes().await.unwrap().as_ref(),
        &content[content.len() - 5..]
    );

    // ── Range: unsatisfiable → 416 InvalidRange XML ─────────────────────────
    let unsat = client
        .get_range(&bucket_name, "data/model.pt", "bytes=999999-", &auth)
        .await;
    assert_eq!(unsat.status(), 416, "unsatisfiable range");
    let xml = unsat.text().await.unwrap();
    assert!(xml.contains("<Code>InvalidRange</Code>"), "{xml}");

    // ── Multipart of a large object → assembled bytes + ETag identity ───────
    // Parts 1 and 2 are non-final, so S3's 5 MiB minimum applies; the final
    // part 3 may be small.
    let part1 = vec![0x11_u8; 5 * 1024 * 1024];
    let part2 = vec![0x22_u8; 5 * 1024 * 1024];
    let part3 = vec![0x33_u8; 256 * 1024];
    let assembled = [part1.clone(), part2.clone(), part3.clone()].concat();

    let upload_id = client
        .create_multipart(&bucket_name, "data/big.pt", &auth)
        .await;
    for (number, part) in [(1_u32, &part1), (2, &part2), (3, &part3)] {
        let part_response = client
            .upload_part(
                &bucket_name,
                "data/big.pt",
                &upload_id,
                number,
                part.clone(),
                &auth,
            )
            .await;
        assert_eq!(part_response.status(), 200, "UploadPart {number}");
        assert!(
            part_response.headers().contains_key(header::ETAG),
            "UploadPart must return a per-part etag"
        );
    }
    let complete = client
        .complete_multipart(&bucket_name, "data/big.pt", &upload_id, &[1, 2, 3], &auth)
        .await;
    assert_eq!(complete.status(), 200, "CompleteMultipartUpload");
    let complete_xml = complete.text().await.unwrap();
    assert!(
        complete_xml.contains("<CompleteMultipartUploadResult"),
        "{complete_xml}"
    );
    let multipart_etag = extract_tag(&complete_xml, "ETag");
    assert!(multipart_etag.starts_with('"'), "{complete_xml}");

    let get_big = client.get(&bucket_name, "data/big.pt", &auth).await;
    assert_eq!(get_big.status(), 200, "GetObject (multipart)");
    assert_eq!(
        get_big.bytes().await.unwrap().as_ref(),
        assembled.as_slice()
    );

    // Whole-object dedup: a single PUT of the same bytes shares the ETag.
    let single = client
        .put(&bucket_name, "data/big-single.pt", assembled, &auth)
        .await;
    assert_eq!(single.status(), 200);
    let single_etag = single
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        multipart_etag, single_etag,
        "multipart and single-PUT of the same bytes must share the BLAKE3 etag"
    );
}

// ===========================================================================
// Polars / object_store-style: HeadObject before GET (Bearer auth)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_object_store_head_before_get() {
    let server = TestServer::start().await;
    let client = S3Client::new(&server);
    let bucket_name = bucket("lake", "warehouse");
    let auth = Auth::write("lake", "warehouse", AuthStyle::Bearer);

    let content = b"parquet-part-00000";
    let put = client
        .put(
            &bucket_name,
            "tables/events/part-00000.parquet",
            content.to_vec(),
            &auth,
        )
        .await;
    assert_eq!(put.status(), 200, "PutObject (Bearer)");
    let put_etag = put
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    // HeadObject: size + ETag + Last-Modified before the GET.
    let head = client
        .head(&bucket_name, "tables/events/part-00000.parquet", &auth)
        .await;
    assert_eq!(head.status(), 200, "HeadObject");
    assert_eq!(
        head.headers()
            .get(header::CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap(),
        content.len().to_string()
    );
    assert_eq!(
        head.headers().get(header::ETAG).unwrap().to_str().unwrap(),
        put_etag
    );
    assert!(
        head.headers().contains_key(header::LAST_MODIFIED),
        "HeadObject must serve Last-Modified"
    );

    let get = client
        .get(&bucket_name, "tables/events/part-00000.parquet", &auth)
        .await;
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), content);

    // HeadObject on a missing key → 404 NoSuchKey (HEAD carries no body).
    let missing = client
        .head(&bucket_name, "tables/events/part-99999.parquet", &auth)
        .await;
    assert_eq!(missing.status(), 404, "HeadObject missing");

    // The same miss over GET carries the NoSuchKey XML envelope.
    let get_missing = client
        .get(&bucket_name, "tables/events/part-99999.parquet", &auth)
        .await;
    assert_eq!(get_missing.status(), 404, "GetObject missing");
    let xml = get_missing.text().await.unwrap();
    assert!(xml.contains("<Code>NoSuchKey</Code>"), "{xml}");

    // Multipart put → HeadObject reflects the assembled size and the
    // complete-etag.
    let part1 = vec![0xAA_u8; 5 * 1024 * 1024]; // non-final → 5 MiB minimum
    let part2 = vec![0xBB_u8; 64 * 1024]; // final part may be small
    let upload_id = client
        .create_multipart(&bucket_name, "tables/events/part-00001.parquet", &auth)
        .await;
    for (number, part) in [(1_u32, &part1), (2, &part2)] {
        let part_response = client
            .upload_part(
                &bucket_name,
                "tables/events/part-00001.parquet",
                &upload_id,
                number,
                part.clone(),
                &auth,
            )
            .await;
        assert_eq!(part_response.status(), 200);
    }
    let complete = client
        .complete_multipart(
            &bucket_name,
            "tables/events/part-00001.parquet",
            &upload_id,
            &[1, 2],
            &auth,
        )
        .await;
    assert_eq!(complete.status(), 200);
    let complete_etag = extract_tag(&complete.text().await.unwrap(), "ETag");

    let head_big = client
        .head(&bucket_name, "tables/events/part-00001.parquet", &auth)
        .await;
    assert_eq!(head_big.status(), 200);
    assert_eq!(
        head_big
            .headers()
            .get(header::CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap(),
        (part1.len() + part2.len()).to_string()
    );
    assert_eq!(
        head_big
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap(),
        complete_etag
    );
}

// ===========================================================================
// S3A-style: HeadBucket / CreateBucket / GetBucketLocation probes
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_s3a_bucket_probes() {
    let server = TestServer::start().await;
    let client = S3Client::new(&server);
    let bucket_name = bucket("acme", "models");
    let auth = Auth::write("acme", "models", AuthStyle::SigV4);
    let read_auth = Auth::read("acme", "models", AuthStyle::SigV4);

    // HeadBucket connect probe → 200 (read scope suffices).
    let head = client.head_bucket(&bucket_name, &read_auth).await;
    assert_eq!(head.status(), 200, "HeadBucket");

    // CreateBucket missing-bucket probe → 200 no-op (write scope).
    let create = client.create_bucket(&bucket_name, &auth).await;
    assert_eq!(create.status(), 200, "CreateBucket");

    // GetBucketLocation region probe → XML stub (read scope).
    let location = client.get_bucket_location(&bucket_name, &read_auth).await;
    assert_eq!(location.status(), 200, "GetBucketLocation");
    let xml = location.text().await.unwrap();
    assert!(
        xml.contains("<LocationConstraint") && xml.contains("us-east-1"),
        "{xml}"
    );

    // Mismatched bucket → 403 AccessDenied (HEAD carries no body; the
    // CreateBucket variant over PUT carries the XML envelope).
    let wrong = bucket("other", "models");
    let head_wrong = client.head_bucket(&wrong, &auth).await;
    assert_eq!(head_wrong.status(), 403, "HeadBucket mismatched scope");

    let create_wrong = client.create_bucket(&wrong, &auth).await;
    assert_eq!(create_wrong.status(), 403, "CreateBucket mismatched scope");
    let xml = create_wrong.text().await.unwrap();
    assert!(xml.contains("<Code>AccessDenied</Code>"), "{xml}");

    // Undecodable bucket → 404 NoSuchBucket (HEAD carries no body; the PUT
    // variant over the object path carries the XML envelope).
    let head_undecodable = client.head_bucket("notabucket", &auth).await;
    assert_eq!(head_undecodable.status(), 404, "HeadBucket undecodable");

    let put_undecodable = client.put("notabucket", "k", Vec::new(), &auth).await;
    assert_eq!(
        put_undecodable.status(),
        404,
        "PutObject undecodable bucket"
    );
    let xml = put_undecodable.text().await.unwrap();
    assert!(xml.contains("<Code>NoSuchBucket</Code>"), "{xml}");
}

// ===========================================================================
// DuckDB-style: ListObjectsV2 with prefix / delimiter / pagination
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_duckdb_listing_prefix_and_delimiter() {
    let server = TestServer::start().await;
    let client = S3Client::new(&server);
    let bucket_name = bucket("duck", "lake");
    let auth = Auth::write("duck", "lake", AuthStyle::SigV4);
    let read_auth = Auth::read("duck", "lake", AuthStyle::SigV4);

    client
        .put(&bucket_name, "dir/a.txt", b"1".to_vec(), &auth)
        .await;
    client
        .put(&bucket_name, "dir/sub/b.txt", b"2".to_vec(), &auth)
        .await;
    client
        .put(&bucket_name, "root.txt", b"3".to_vec(), &auth)
        .await;

    // DuckDB glob shape: `prefix=dir/` returns only keys under dir/.
    let prefixed = client
        .list(&bucket_name, "list-type=2&prefix=dir%2F", &read_auth)
        .await;
    assert_eq!(prefixed.status(), 200, "ListObjectsV2 prefix");
    let xml = prefixed.text().await.unwrap();
    assert!(xml.contains("<Key>dir/a.txt</Key>"), "{xml}");
    assert!(xml.contains("<Key>dir/sub/b.txt</Key>"), "{xml}");
    assert!(!xml.contains("<Key>root.txt</Key>"), "{xml}");
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");

    // Directory shape: `delimiter=/` → root Contents + dir/ CommonPrefixes.
    let delimited = client
        .list(&bucket_name, "list-type=2&delimiter=%2F", &read_auth)
        .await;
    assert_eq!(delimited.status(), 200, "ListObjectsV2 delimiter");
    let xml = delimited.text().await.unwrap();
    assert!(xml.contains("<Key>root.txt</Key>"), "{xml}");
    assert!(!xml.contains("<Key>dir/a.txt</Key>"), "{xml}");
    assert!(!xml.contains("<Key>dir/sub/b.txt</Key>"), "{xml}");
    assert!(
        xml.contains("<CommonPrefixes>") && xml.contains("<Prefix>dir/</Prefix>"),
        "{xml}"
    );
    assert_eq!(count_occurrences(&xml, "<Contents>"), 1);
    assert_eq!(count_occurrences(&xml, "<CommonPrefixes>"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_duckdb_listing_pagination_and_empty() {
    let server = TestServer::start().await;
    let client = S3Client::new(&server);
    let bucket_name = bucket("duck", "lake");
    let auth = Auth::write("duck", "lake", AuthStyle::SigV4);
    let read_auth = Auth::read("duck", "lake", AuthStyle::SigV4);

    // Empty bucket → empty ListBucketResult.
    let empty = client.list(&bucket_name, "list-type=2", &read_auth).await;
    assert_eq!(empty.status(), 200);
    let xml = empty.text().await.unwrap();
    assert!(xml.contains("<ListBucketResult"), "{xml}");
    assert_eq!(count_occurrences(&xml, "<Contents>"), 0);
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");

    // Pagination: max-keys=1 walks the whole bucket via continuation tokens.
    for key in ["a.txt", "b.txt", "c.txt"] {
        client.put(&bucket_name, key, b"x".to_vec(), &auth).await;
    }

    let first = client
        .list(&bucket_name, "list-type=2&max-keys=1", &read_auth)
        .await;
    assert_eq!(first.status(), 200);
    let first_xml = first.text().await.unwrap();
    assert!(first_xml.contains("<Key>a.txt</Key>"), "{first_xml}");
    assert!(
        first_xml.contains("<IsTruncated>true</IsTruncated>"),
        "{first_xml}"
    );
    assert!(first_xml.contains("<NextContinuationToken>"), "{first_xml}");
    let token1 = extract_tag(&first_xml, "NextContinuationToken");

    let second = client
        .list(
            &bucket_name,
            &format!("list-type=2&max-keys=1&continuation-token={token1}"),
            &read_auth,
        )
        .await;
    assert_eq!(second.status(), 200);
    let second_xml = second.text().await.unwrap();
    assert!(second_xml.contains("<Key>b.txt</Key>"), "{second_xml}");
    assert!(
        second_xml.contains("<IsTruncated>true</IsTruncated>"),
        "{second_xml}"
    );
    let token2 = extract_tag(&second_xml, "NextContinuationToken");

    let third = client
        .list(
            &bucket_name,
            &format!("list-type=2&max-keys=1&continuation-token={token2}"),
            &read_auth,
        )
        .await;
    assert_eq!(third.status(), 200);
    let third_xml = third.text().await.unwrap();
    assert!(third_xml.contains("<Key>c.txt</Key>"), "{third_xml}");
    assert!(
        third_xml.contains("<IsTruncated>false</IsTruncated>"),
        "{third_xml}"
    );
}

// ===========================================================================
// Auth forms: the SigV4 access-key bridge and the Bearer fallback both work
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_auth_forms_bearer_and_sigv4() {
    let server = TestServer::start().await;
    let client = S3Client::new(&server);
    let bucket_name = bucket("auth", "forms");
    let sigv4 = Auth::write("auth", "forms", AuthStyle::SigV4);
    let bearer = Auth::write("auth", "forms", AuthStyle::Bearer);

    // SigV4 form: the access key IS the bearer token (signature not verified).
    let content = b"via-sigv4";
    let put_sigv4 = client
        .put(&bucket_name, "k-sigv4", content.to_vec(), &sigv4)
        .await;
    assert_eq!(put_sigv4.status(), 200, "PutObject (SigV4)");
    let get_sigv4 = client.get(&bucket_name, "k-sigv4", &sigv4).await;
    assert_eq!(get_sigv4.status(), 200);
    assert_eq!(get_sigv4.bytes().await.unwrap().as_ref(), content);

    // Bearer fallback: same result.
    let put_bearer = client
        .put(&bucket_name, "k-bearer", content.to_vec(), &bearer)
        .await;
    assert_eq!(put_bearer.status(), 200, "PutObject (Bearer)");
    let get_bearer = client.get(&bucket_name, "k-bearer", &bearer).await;
    assert_eq!(get_bearer.status(), 200);
    assert_eq!(get_bearer.bytes().await.unwrap().as_ref(), content);

    // Missing credentials → 403 AccessDenied XML.
    let no_auth = reqwest::Client::new()
        .get(client.url(&format!("/{bucket_name}/k-sigv4")))
        .send()
        .await
        .unwrap();
    assert_eq!(no_auth.status(), 403, "missing credentials");
    let xml = no_auth.text().await.unwrap();
    assert!(xml.contains("<Code>AccessDenied</Code>"), "{xml}");
}
