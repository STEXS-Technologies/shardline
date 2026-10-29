//! Streaming test front door for the documented API/transfer path split.
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{StatusCode, header},
    response::Response,
    routing::any,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{net::TcpListener, task::JoinHandle};

pub(super) type RangeObservations = Arc<Mutex<Vec<(String, StatusCode)>>>;

#[derive(Clone)]
struct Upstreams {
    api: String,
    transfer: String,
    client: reqwest::Client,
    ranges: Option<RangeObservations>,
    rejected_lfs: Option<Arc<AtomicUsize>>,
    lost_commit: Option<Arc<AtomicUsize>>,
}

pub(super) struct Proxy(JoinHandle<Result<(), std::io::Error>>);

impl Drop for Proxy {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) fn start(listener: TcpListener, api: &str, transfer: &str) -> Proxy {
    start_inner(listener, api, transfer, None, None, None)
}

pub(super) fn start_rejecting_lfs(listener: TcpListener, api: &str) -> (Proxy, Arc<AtomicUsize>) {
    let rejected = Arc::new(AtomicUsize::new(0));
    (
        start_inner(listener, api, api, None, Some(rejected.clone()), None),
        rejected,
    )
}

pub(super) fn start_observing_ranges(
    listener: TcpListener,
    api: &str,
    transfer: &str,
) -> (Proxy, RangeObservations) {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let proxy = start_inner(
        listener,
        api,
        transfer,
        Some(observations.clone()),
        None,
        None,
    );
    (proxy, observations)
}

pub(super) fn start_losing_commit_reply(
    listener: TcpListener,
    api: &str,
) -> (Proxy, Arc<AtomicUsize>) {
    let lost = Arc::new(AtomicUsize::new(0));
    (
        start_inner(listener, api, api, None, None, Some(lost.clone())),
        lost,
    )
}

fn start_inner(
    listener: TcpListener,
    api: &str,
    transfer: &str,
    ranges: Option<RangeObservations>,
    rejected_lfs: Option<Arc<AtomicUsize>>,
    lost_commit: Option<Arc<AtomicUsize>>,
) -> Proxy {
    let upstreams = Upstreams {
        api: api.to_owned(),
        transfer: transfer.to_owned(),
        ranges,
        rejected_lfs,
        lost_commit,
        client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("test proxy client"),
    };
    let router = Router::new().fallback(any(forward)).with_state(upstreams);
    Proxy(tokio::spawn(
        async move { axum::serve(listener, router).await },
    ))
}

async fn forward(State(state): State<Upstreams>, request: Request) -> Result<Response, StatusCode> {
    let path = request.uri().path();
    let commit = request.method() == reqwest::Method::POST && path.contains("/commit/");
    if path.ends_with("/info/lfs/objects/batch")
        && let Some(rejected) = &state.rejected_lfs
    {
        rejected.fetch_add(1, Ordering::SeqCst);
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
            .body(Body::from(
                r#"{"message":"injected LFS negotiation rejection"}"#,
            ))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);
    }
    let observed_range = request
        .headers()
        .contains_key(header::RANGE)
        .then(|| path.to_owned());
    let upstream = if path.starts_with("/v1/chunks/")
        || path.starts_with("/v1/xorbs/")
        || path.starts_with("/transfer/xorb/")
    {
        &state.transfer
    } else {
        &state.api
    };
    let target = format!(
        "{upstream}{}",
        request
            .uri()
            .path_and_query()
            .map_or("/", |value| value.as_str())
    );
    let (mut parts, body) = request.into_parts();
    // Forward credentials exactly as supplied; never inject test credentials.
    for name in [header::HOST, header::CONNECTION, header::TRANSFER_ENCODING] {
        parts.headers.remove(name);
    }
    let mut response = state
        .client
        .request(parts.method, target)
        .headers(parts.headers)
        .body(reqwest::Body::wrap_stream(body.into_data_stream()))
        .send()
        .await
        .map_err(|_error| StatusCode::BAD_GATEWAY)?;
    let status = response.status();
    if let (Some(observations), Some(path)) = (&state.ranges, observed_range) {
        // Retain only paths and status codes, never credentials or URL queries.
        observations
            .lock()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .push((path, status));
    }
    let mut headers = response.headers().clone();
    headers.remove(header::CONNECTION);
    headers.remove(header::TRANSFER_ENCODING);
    if commit
        && status == StatusCode::OK
        && let Some(lost) = &state.lost_commit
    {
        // Consume and validate only the bounded control reply. Uploaded files
        // and downloads continue to stream without buffering in this proxy.
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?
        {
            if bytes.len().saturating_add(chunk.len()) > 65536 {
                return Err(StatusCode::BAD_GATEWAY);
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| StatusCode::BAD_GATEWAY)?;
        if value["commitOid"].as_str().is_none() {
            return Err(StatusCode::BAD_GATEWAY);
        }
        let body = if lost.fetch_add(1, Ordering::SeqCst) == 0 {
            Body::from_stream(futures_util::stream::once(async {
                Err::<bytes::Bytes, _>(std::io::Error::other("injected lost commit reply"))
            }))
        } else {
            Body::from(bytes)
        };
        let mut forwarded = Response::new(body);
        *forwarded.status_mut() = status;
        *forwarded.headers_mut() = headers;
        return Ok(forwarded);
    }
    let mut forwarded = Response::new(Body::from_stream(response.bytes_stream()));
    *forwarded.status_mut() = status;
    *forwarded.headers_mut() = headers;
    Ok(forwarded)
}
