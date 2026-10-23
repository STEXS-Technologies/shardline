//! Opt-in signing identities; legacy token clients keep their existing path.
use std::{
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderValue, Method, header::AUTHORIZATION},
    middleware::{self, Next},
    response::Response,
};
use futures_util::{StreamExt, stream};
use sha2::{Digest, Sha256};
use shardline_s3_adapter::{
    S3Error, SigV4Credential, extract_access_key, extract_access_key_from_query, verify_sigv4,
};

use crate::{S3SigningIdentity, ServerConfig, app::AppState};

#[derive(Clone)]
struct SigningState {
    identities: Arc<Vec<S3SigningIdentity>>,
    control_body_limit: usize,
    root_dir: PathBuf,
    max_upload_bytes: u64,
}

pub(crate) fn protect(
    router: Router<Arc<AppState>>,
    config: &ServerConfig,
) -> Router<Arc<AppState>> {
    if config.s3_signing_identities.is_empty() {
        return router;
    }
    router.route_layer(middleware::from_fn_with_state(
        SigningState {
            identities: Arc::new(config.s3_signing_identities.clone()),
            control_body_limit: config.max_request_body_bytes().get(),
            root_dir: config.root_dir().to_path_buf(),
            max_upload_bytes: config.s3_max_part_bytes().get(),
        },
        authenticate,
    ))
}

async fn authenticate(
    State(state): State<SigningState>,
    request: Request,
    next: Next,
) -> Result<Response, S3Error> {
    if is_post_object_form(&request) {
        let request = super::post_object::authenticate_and_spool(
            request,
            &state.identities,
            &state.root_dir,
            state.max_upload_bytes,
        )
        .await?;
        return Ok(next.run(request).await);
    }
    let query = super::parse_s3_query(request.uri())?;
    let header_key = extract_access_key(request.headers());
    let query_key = extract_access_key_from_query(&query);
    let identity = state.identities.iter().find(|identity| {
        Some(identity.access_key.as_str()) == header_key
            || Some(identity.access_key.as_str()) == query_key
    });
    let Some(identity) = identity else {
        return Ok(next.run(request).await);
    };
    let now = i64::try_from(shardline_protocol::unix_now_seconds_lossy())
        .map_err(|_overflow| S3Error::internal())?;
    let verified = verify_sigv4(
        request.method(),
        request.uri(),
        request.headers(),
        &SigV4Credential {
            access_key: &identity.access_key,
            secret_key: identity.secret_key.expose_secret(),
            session_token: identity
                .session_token
                .as_ref()
                .map(shardline_protocol::SecretString::expose_secret),
        },
        &identity.region,
        now,
    )?;
    let streamed = request.method() == Method::PUT
        && request.uri().path().trim_start_matches('/').contains('/')
        && !request.headers().contains_key("x-amz-copy-source");
    let (mut parts, body) = request.into_parts();
    let invalid_payload = Arc::new(Mutex::new(None::<S3Error>));
    let body = if let Some(verifier) = verified.streaming_verifier() {
        if !streamed {
            return Err(S3Error::access_denied());
        }
        let stream = stream::try_unfold(
            (
                body.into_data_stream(),
                verifier,
                Arc::clone(&invalid_payload),
            ),
            |(mut body, mut verifier, invalid_payload)| async move {
                match body.next().await {
                    Some(Ok(bytes)) => {
                        verifier
                            .update(&bytes)
                            .map_err(|error| payload_failure(&invalid_payload, error))?;
                        Ok(Some((bytes, (body, verifier, invalid_payload))))
                    }
                    Some(Err(error)) => Err(io::Error::other(error)),
                    None => {
                        verifier
                            .finish()
                            .map_err(|error| payload_failure(&invalid_payload, error))?;
                        Ok(None)
                    }
                }
            },
        );
        Body::from_stream(stream)
    } else if verified.payload_sha256.is_some() {
        if streamed {
            // The existing object/part ingestion paths drain EOF before their
            // publication transaction. An error here leaves only private
            // staging bytes, exactly like an interrupted upload.
            let stream = stream::try_unfold(
                (
                    body.into_data_stream(),
                    Sha256::new(),
                    Arc::clone(&invalid_payload),
                ),
                move |(mut body, mut hasher, invalid_payload)| {
                    let verified = verified.clone();
                    async move {
                        match body.next().await {
                            Some(Ok(bytes)) => {
                                hasher.update(&bytes);
                                Ok(Some((bytes, (body, hasher, invalid_payload))))
                            }
                            Some(Err(error)) => Err(io::Error::other(error)),
                            None if verified.matches_payload(&hasher.finalize().into()) => Ok(None),
                            None => {
                                Err(payload_failure(&invalid_payload, S3Error::access_denied()))
                            }
                        }
                    }
                },
            );
            Body::from_stream(stream)
        } else {
            // Control handlers may ignore their body. Verify it before they
            // can mutate metadata, even for COPY, abort, and bucket probes.
            let bytes = to_bytes(body, state.control_body_limit)
                .await
                .map_err(|_body_error| S3Error::invalid_argument("Invalid signed request body"))?;
            if !verified.matches_payload(&Sha256::digest(&bytes).into()) {
                return Err(S3Error::access_denied());
            }
            Body::from(bytes)
        }
    } else {
        body
    };
    parts.headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", identity.token.expose_secret()))
            .map_err(|_invalid_token| S3Error::access_denied())?,
    );
    let response = next.run(Request::from_parts(parts, body)).await;
    // Ingestion must observe the body error before publishing. Preserve the
    // authentication failure classification after its staging cleanup, rather
    // than exposing the wrapped I/O error as a retryable internal failure.
    let failure = invalid_payload
        .lock()
        .map_err(|_poisoned| S3Error::internal())?
        .take();
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(response)
}

fn is_post_object_form(request: &Request) -> bool {
    request.method() == Method::POST
        && request.uri().query().is_none()
        && request
            .uri()
            .path()
            .strip_prefix('/')
            .is_some_and(|bucket| !bucket.is_empty() && !bucket.contains('/'))
        && request
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .get(.."multipart/form-data".len())
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case("multipart/form-data"))
            })
}

fn payload_failure(failure: &Mutex<Option<S3Error>>, error: S3Error) -> io::Error {
    if let Ok(mut stored) = failure.lock() {
        *stored = Some(error);
    }
    io::Error::new(io::ErrorKind::InvalidData, "S3 payload verification failed")
}
