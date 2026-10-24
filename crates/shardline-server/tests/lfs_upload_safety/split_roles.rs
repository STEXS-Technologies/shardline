//! Exercise cancellation through a streaming API/transfer front door.
use super::*;
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    response::Response,
    routing::any,
};
use shardline_server::ServerRole;

#[derive(Clone)]
struct Upstreams {
    api: String,
    transfer: String,
    client: Client,
}

async fn forward(State(state): State<Upstreams>, request: Request) -> Result<Response, StatusCode> {
    let path = request.uri().path();
    let transfer = path.starts_with("/lfs/objects/")
        || (path.starts_with("/v1/lfs/objects/") && path != "/v1/lfs/objects/batch");
    let upstream = if transfer {
        &state.transfer
    } else {
        &state.api
    };
    let target = format!(
        "{upstream}{}",
        request.uri().path_and_query().map_or("/", |p| p.as_str())
    );
    let (mut parts, body) = request.into_parts();
    for name in ["host", "connection", "transfer-encoding"] {
        parts.headers.remove(name);
    }
    let response = state
        .client
        .request(parts.method, target)
        .headers(parts.headers)
        .body(reqwest::Body::wrap_stream(body.into_data_stream()))
        .send()
        .await
        .map_err(|_error| StatusCode::BAD_GATEWAY)?;
    let status = response.status();
    let mut headers = response.headers().clone();
    headers.remove("connection");
    headers.remove("transfer-encoding");
    let mut result = Response::new(Body::from_stream(response.bytes_stream()));
    *result.status_mut() = status;
    *result.headers_mut() = headers;
    Ok(result)
}

#[tokio::test]
async fn split_lfs_disconnect_retry_preserves_bytes_and_scope() {
    exercise(None, None).await;
}

pub(super) async fn exercise(database: Option<&str>, storage: Option<S3ObjectStoreConfig>) {
    for (frontend, prefix) in [
        (ServerFrontend::Lfs, "/v1/lfs/objects"),
        (ServerFrontend::Hub, "/lfs/objects"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let transfer_root = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public = format!("http://{}", listener.local_addr().unwrap());
        let api = start_routed(
            root.path(),
            frontend,
            true,
            database,
            storage.clone(),
            None,
            Some((ServerRole::Api, &public)),
        )
        .await;
        let transfer = start_routed(
            if database.is_some() {
                transfer_root.path()
            } else {
                root.path()
            },
            frontend,
            true,
            database,
            storage.clone(),
            None,
            Some((ServerRole::Transfer, &public)),
        )
        .await;
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap();
        let router = Router::new().fallback(any(forward)).with_state(Upstreams {
            api: api.base.clone(),
            transfer: transfer.base.clone(),
            client: client.clone(),
        });
        let proxy = tokio::spawn(async move { axum::serve(listener, router).await });
        let credential = token("writer", "split-safety", TokenScope::Write);
        let payload: Vec<u8> = (0..131_072).map(|n| (n % 251) as u8).collect();
        let oid = hex::encode(Sha256::digest(&payload));
        let batch_path = if frontend == ServerFrontend::Hub {
            "/split-safety/locks.git/info/lfs/objects/batch"
        } else {
            "/v1/lfs/objects/batch"
        };
        let batch = client.post(format!("{public}{batch_path}"))
            .bearer_auth(&credential)
            .json(&json!({"operation":"upload", "transfers":["basic"], "objects":[{"oid":oid,"size":payload.len()}]}))
            .send().await.unwrap();
        assert_eq!(batch.status(), StatusCode::OK);
        let batch: Value = batch.json().await.unwrap();
        let action = batch["objects"][0]["actions"]["upload"]["href"]
            .as_str()
            .expect("absolute upload action");
        assert!(action.starts_with(&format!("{public}{prefix}/{oid}")));
        let path = format!("{prefix}/{oid}");
        let address = public.strip_prefix("http://").unwrap();
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket.write_all(format!("PUT {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {credential}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", payload.len()).as_bytes()).await.unwrap();
        socket.write_all(&payload[..65_536]).await.unwrap();
        socket.shutdown().await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            socket.read_to_end(&mut response),
        )
        .await
        .expect("split upload did not terminate")
        .unwrap();
        let url = format!("{public}{path}");
        assert_eq!(
            client
                .get(&url)
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert!(
            client
                .put(action)
                .bearer_auth(&credential)
                .body(payload.clone())
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        let downloaded = client
            .get(&url)
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(downloaded.status(), StatusCode::OK);
        assert_eq!(
            downloaded.bytes().await.unwrap().as_ref(),
            payload.as_slice()
        );
        let other = token("other", "other-split", TokenScope::Read);
        assert!(
            !client
                .get(&url)
                .bearer_auth(other)
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        proxy.abort();
        let _stopped = proxy.await;
        transfer.stop().await;
        api.stop().await;
    }
}
