//! Official Hub/Xet client recovery from a genuinely rejected expired URL.
//!
//! The test proxy expires the first advertised download grant, emulating a
//! reconstruction response delayed beyond its lifetime. Client code and its
//! URL provider are unmodified; the real server must reject the stale grant.

use super::*;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{StatusCode, header},
    response::Response,
    routing::any,
};
use hmac::{Hmac, Mac};
use shardline_protocol::{TokenClaims, TokenSigner};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct ExpiryProxy {
    upstream: String,
    transfer_upstream: Option<String>,
    client: reqwest::Client,
    armed: AtomicBool,
    injected: AtomicUsize,
    rejected: AtomicUsize,
    refreshed: AtomicUsize,
    recovered: AtomicUsize,
    bearer_recovered: AtomicUsize,
}

#[derive(Clone, Copy)]
enum DownloadClient {
    HuggingFace,
    Sdx,
}

struct ProxyTask(JoinHandle<()>);

impl Drop for ProxyTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_huggingface_cli_recovers_after_expired_xorb_url() -> Result<(), TestError> {
    expiry_recovery(None, false, DownloadClient::HuggingFace).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker PostgreSQL and MinIO"]
async fn native_huggingface_cli_recovers_after_expired_xorb_url_remote() -> Result<(), TestError> {
    assert!(
        command_available("hf"),
        "remote expiry qualification requires hf"
    );
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("remote expiry qualification requires Docker")?;
    expiry_recovery(Some(services), false, DownloadClient::HuggingFace).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_huggingface_cli_recovers_after_expired_xorb_url_split_roles()
-> Result<(), TestError> {
    expiry_recovery(None, true, DownloadClient::HuggingFace).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker PostgreSQL and MinIO"]
async fn native_huggingface_cli_recovers_after_expired_xorb_url_remote_split_roles()
-> Result<(), TestError> {
    assert!(
        command_available("hf"),
        "remote expiry qualification requires hf"
    );
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("remote expiry qualification requires Docker")?;
    expiry_recovery(Some(services), true, DownloadClient::HuggingFace).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sdx_reads_expired_xorb_url_with_bearer_split_roles() -> Result<(), TestError> {
    expiry_recovery(None, true, DownloadClient::Sdx).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker PostgreSQL and MinIO"]
async fn native_sdx_reads_expired_xorb_url_with_bearer_remote_split_roles() -> Result<(), TestError>
{
    assert!(command_available("hf"), "sdx fixture upload requires hf");
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("remote sdx qualification requires Docker")?;
    expiry_recovery(Some(services), true, DownloadClient::Sdx).await
}

async fn expiry_recovery(
    services: Option<shardline_test_support::DockerLocalStack>,
    split: bool,
    client: DownloadClient,
) -> Result<(), TestError> {
    if !command_available("hf") {
        return Ok(());
    }
    assert!(
        python_package_available("hf_xet"),
        "expiry qualification requires hf_xet"
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let public = format!("http://{}", listener.local_addr()?);
    let mut runtime =
        start_runtime_with_roles(&[ServerFrontend::Hub], services, Some(&public), split).await?;
    let proxy = Arc::new(ExpiryProxy {
        upstream: runtime.base_url.clone(),
        transfer_upstream: runtime.transfer.as_ref().map(|(url, _)| url.clone()),
        client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        armed: AtomicBool::new(false),
        injected: AtomicUsize::new(0),
        rejected: AtomicUsize::new(0),
        refreshed: AtomicUsize::new(0),
        recovered: AtomicUsize::new(0),
        bearer_recovered: AtomicUsize::new(0),
    });
    let router = Router::new()
        .fallback(any(forward))
        .with_state(proxy.clone());
    let _proxy_task = ProxyTask(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    runtime.base_url = public;
    let created = reqwest::Client::new()
        .post(format!("{}/api/repos/create", runtime.base_url))
        .bearer_auth(&runtime.model_token)
        .json(&serde_json::json!({"type":"model", "name":"team/cli-model"}))
        .send()
        .await?;
    assert_eq!(created.status(), StatusCode::CREATED);
    let working = tempfile::tempdir()?;
    let source = working.path().join("expiry.bin");
    let payload: Vec<u8> = (0..9 * 1024 * 1024)
        .map(|index| ((index * 31 + index / 251) % 256) as u8)
        .collect();
    write(&source, &payload)?;
    let upload_home = tempfile::tempdir()?;
    run_hf(
        &runtime,
        upload_home.path(),
        [
            "upload",
            "team/cli-model",
            path_as_str(&source)?,
            "expiry.bin",
            "--token",
            &runtime.model_token,
        ],
    )?;
    let download_home = tempfile::tempdir()?;
    let destination = working.path().join("download");
    if matches!(client, DownloadClient::Sdx) {
        let metadata = reqwest::Client::new()
            .head(format!(
                "{}/team/cli-model/resolve/main/expiry.bin",
                runtime.base_url
            ))
            .bearer_auth(&runtime.model_token)
            .send()
            .await?
            .error_for_status()?;
        let file_id = metadata
            .headers()
            .get("x-xet-hash")
            .ok_or("missing native identity")?
            .to_str()?
            .to_owned();
        let auth = sdx::Auth::new(
            &runtime.base_url,
            sdx::RepositoryId {
                provider: "github".to_owned(),
                owner: "team".to_owned(),
                repo: "cli-model".to_owned(),
                revision: "main".to_owned(),
            },
        )?
        .with_api_key("test-api-key-16bytes".to_owned())
        .with_subject("github-user-1".to_owned());
        let host = runtime
            .base_url
            .strip_prefix("http://")
            .ok_or("fixture must use HTTP")?;
        let sdk = sdx::XetClientBuilder::new()
            .endpoint(format!("xet://{host}/github/team/cli-model/main"))
            .auth(auth)
            .with_buffer_semaphore(32 * 1024 * 1024)
            .build()?;
        proxy.armed.store(true, Ordering::SeqCst);
        let actual = sdk.download_bytes(&file_id).await?;
        assert_eq!(actual.as_ref(), payload.as_slice());
        assert!(proxy.injected.load(Ordering::SeqCst) > 0);
        assert!(
            proxy.bearer_recovered.load(Ordering::SeqCst) > 0,
            "sdx must reauthorize its expired range URL with a scoped bearer"
        );
        return Ok(());
    }
    proxy.armed.store(true, Ordering::SeqCst);
    run_hf(
        &runtime,
        download_home.path(),
        [
            "download",
            "team/cli-model",
            "expiry.bin",
            "--local-dir",
            path_as_str(&destination)?,
            "--token",
            &runtime.model_token,
        ],
    )?;
    assert_eq!(read(destination.join("expiry.bin"))?, payload);
    assert!(
        proxy.injected.load(Ordering::SeqCst) > 0,
        "client must receive an expired native fetch URL"
    );
    assert!(
        proxy.rejected.load(Ordering::SeqCst) > 0,
        "real server must reject the expired URL without bearer headers"
    );
    assert!(
        proxy.refreshed.load(Ordering::SeqCst) > 0,
        "stock client must refresh reconstruction"
    );
    assert!(
        proxy.recovered.load(Ordering::SeqCst) > 0,
        "stock client must consume a fresh headerless native range"
    );
    Ok(())
}

async fn forward(State(state): State<Arc<ExpiryProxy>>, request: Request) -> Response {
    match forward_checked(&state, request).await {
        Ok(response) => response,
        // Do not log URL capabilities or credentials when fault setup fails.
        Err(error) => {
            drop(error);
            Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Body::from("expiry test proxy failed"))
                .unwrap()
        }
    }
}

async fn forward_checked(state: &ExpiryProxy, request: Request) -> Result<Response, TestError> {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path();
    let reconstruction = path.contains("/reconstructions/") || path.ends_with("/reconstructions");
    let head = parts.method == axum::http::Method::HEAD;
    let transfer = path.starts_with("/transfer/xorb/");
    let headerless = !parts.headers.contains_key(header::AUTHORIZATION);
    let expired_grant = parts.uri.query().is_some_and(|query| {
        url::form_urlencoded::parse(query.as_bytes()).any(|(name, token)| {
            name == "xorb_read_capability"
                && capability_signer()
                    .ok()
                    .and_then(|signer| signer.verify_at(&token, 0).ok())
                    .is_some_and(|claims| claims.expires_at_unix_seconds() == 1)
        })
    });
    let origin = if path.starts_with("/transfer/")
        || path.starts_with("/v1/chunks/")
        || path.starts_with("/v1/xorbs/")
    {
        state.transfer_upstream.as_ref().unwrap_or(&state.upstream)
    } else {
        &state.upstream
    };
    let upstream = format!("{origin}{}", parts.uri);
    let body = to_bytes(body, 128 * 1024 * 1024).await?;
    let mut headers = parts.headers;
    headers.remove(header::HOST);
    headers.remove(header::CONNECTION);
    headers.remove(header::CONTENT_LENGTH);
    let response = state
        .client
        .request(parts.method, upstream)
        .headers(headers)
        .header(header::ACCEPT_ENCODING, "identity")
        .body(body)
        .send()
        .await?;
    let status = response.status();
    let mut headers = response.headers().clone();
    let mut body = response.bytes().await?.to_vec();
    if transfer && headerless && status == StatusCode::FORBIDDEN {
        state.rejected.fetch_add(1, Ordering::SeqCst);
    }
    if transfer
        && !headerless
        && expired_grant
        && status == StatusCode::PARTIAL_CONTENT
        && state.injected.load(Ordering::SeqCst) > 0
    {
        state.bearer_recovered.fetch_add(1, Ordering::SeqCst);
    }
    if transfer
        && headerless
        && status == StatusCode::PARTIAL_CONTENT
        && state.rejected.load(Ordering::SeqCst) > 0
    {
        state.recovered.fetch_add(1, Ordering::SeqCst);
    }
    if reconstruction && status == StatusCode::OK {
        if state.armed.swap(false, Ordering::SeqCst) {
            let mut plan: serde_json::Value = serde_json::from_slice(&body)?;
            let changed = expire_urls(&mut plan)?;
            if changed == 0 {
                return Err("no signed reconstruction URL to expire".into());
            }
            body = serde_json::to_vec(&plan)?;
            state.injected.fetch_add(changed, Ordering::SeqCst);
        } else if state.rejected.load(Ordering::SeqCst) > 0 {
            state.refreshed.fetch_add(1, Ordering::SeqCst);
        }
    }
    if !head {
        headers.remove(header::CONTENT_LENGTH);
    }
    headers.remove(header::TRANSFER_ENCODING);
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(response)
}

fn capability_signer() -> Result<TokenSigner, TestError> {
    let mut derivation = Hmac::<Sha256>::new_from_slice(b"test-signing-key-32-bytes-long!!")?;
    derivation.update(b"shardline:xorb-range-read-capability:v1");
    Ok(TokenSigner::new(&derivation.finalize().into_bytes())?)
}

fn expire_urls(value: &mut serde_json::Value) -> Result<usize, TestError> {
    let mut changed = 0;
    match value {
        serde_json::Value::Object(fields) => {
            for (name, value) in fields {
                if name == "url" {
                    if let Some(raw) = value.as_str() {
                        let mut url = reqwest::Url::parse(raw)?;
                        let signer = capability_signer()?;
                        let mut pairs = Vec::new();
                        for (name, token) in url.query_pairs() {
                            let token = if name == "xorb_read_capability" {
                                let claims = signer.verify_now(&token)?;
                                changed += 1;
                                signer.sign(&TokenClaims::new(
                                    claims.issuer(),
                                    claims.subject(),
                                    claims.scope(),
                                    claims.repository().clone(),
                                    1,
                                )?)?
                            } else {
                                token.into_owned()
                            };
                            pairs.push((name.into_owned(), token));
                        }
                        url.set_query(None);
                        if !pairs.is_empty() {
                            url.query_pairs_mut().extend_pairs(pairs);
                        }
                        *value = serde_json::Value::String(url.into());
                    }
                } else {
                    changed += expire_urls(value)?;
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                changed += expire_urls(value)?;
            }
        }
        _ => {}
    }
    Ok(changed)
}
