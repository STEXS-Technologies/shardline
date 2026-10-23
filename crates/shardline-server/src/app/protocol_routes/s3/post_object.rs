//! Bounded parsing and authentication for S3 browser form uploads.

use std::{io, path::Path, time::SystemTime};

use axum::{
    body::Body,
    extract::Request,
    http::{HeaderMap, HeaderName, HeaderValue, header::AUTHORIZATION},
};
use bytes::Bytes;
use futures_util::stream;
use shardline_s3_adapter::{
    S3Error, SigV4Credential, SigV4PostPolicyAuth, verify_sigv4_post_policy,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::S3SigningIdentity;

const MAX_FORM_FIELDS: usize = 64;
const MAX_FORM_FIELD_BYTES: usize = 20 * 1024;
const MAX_FORM_OVERHEAD_BYTES: u64 = 1024 * 1024;
const BODY_CHUNK_BYTES: usize = 64 * 1024;

/// Authenticated, validated fields retained for the bucket-level handler.
#[derive(Clone)]
pub(crate) struct S3PostObjectForm {
    pub(super) key: String,
    pub(super) user_metadata: Vec<(String, String)>,
    pub(super) payload_headers: HeaderMap,
    pub(super) success_status: u16,
    pub(super) success_redirect: Option<String>,
}

/// Authenticates a SigV4 POST policy, bounds and spools its file part, and
/// replaces the multipart request body with a stream of the file bytes.
///
/// The file part must be last, as required by S3 POST forms. It is spooled to
/// an anonymous temporary file under the server root rather than held in
/// memory. No object-store or index write occurs until the policy and all form
/// conditions pass.
pub(super) async fn authenticate_and_spool(
    request: Request,
    identities: &[S3SigningIdentity],
    root: &Path,
    max_file_bytes: u64,
) -> Result<Request, S3Error> {
    if request.headers().contains_key(AUTHORIZATION) || request.uri().query().is_some() {
        return Err(S3Error::access_denied());
    }
    let bucket = bucket_from_path(request.uri().path())?;
    let content_type = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| S3Error::invalid_argument("Missing multipart form content type"))?;
    let boundary = multer::parse_boundary(content_type)
        .map_err(|_error| S3Error::invalid_argument("Invalid multipart form boundary"))?;
    let (mut request_parts, body) = request.into_parts();
    let whole_stream_limit = max_file_bytes.saturating_add(MAX_FORM_OVERHEAD_BYTES);
    let constraints = multer::Constraints::new().size_limit(
        multer::SizeLimit::new()
            .whole_stream(whole_stream_limit)
            .per_field(max_file_bytes),
    );
    let mut multipart =
        multer::Multipart::with_constraints(body.into_data_stream(), boundary, constraints);
    let mut fields = Vec::<(String, String)>::new();
    let mut total_field_bytes = 0usize;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|_error| S3Error::invalid_argument("Invalid S3 form fields"))?
    {
        let name = field
            .name()
            .ok_or_else(|| S3Error::invalid_argument("Missing multipart form field name"))?
            .to_owned();
        if name == "file" {
            if fields.len() >= MAX_FORM_FIELDS {
                return Err(S3Error::invalid_argument("Too many S3 form fields"));
            }
            if fields.is_empty() {
                return Err(S3Error::invalid_argument("Invalid S3 file form field"));
            }
            let submitted_file_name = field.file_name().unwrap_or_default();
            let submitted_file_name = submitted_file_name
                .rsplit(['/', '\\'])
                .next()
                .unwrap_or_default()
                .to_owned();
            let (identity, policy) = authenticate_policy_fields(&fields, identities)?;
            let fields = expand_filename(&fields, &submitted_file_name);
            policy.validate_form_fields(&fields, &bucket)?;
            let policy_max_file_bytes = policy.maximum_file_size()?.unwrap_or(max_file_bytes);
            let effective_max_file_bytes = max_file_bytes.min(policy_max_file_bytes);
            let mut output =
                tokio::fs::File::from_std(tempfile::tempfile_in(root).map_err(io_to_s3)?);
            let mut file_size = 0u64;
            while let Some(chunk) = field
                .chunk()
                .await
                .map_err(|_error| S3Error::invalid_argument("Invalid uploaded file part"))?
            {
                file_size = file_size
                    .checked_add(u64::try_from(chunk.len()).map_err(|_error| S3Error::internal())?)
                    .ok_or_else(S3Error::internal)?;
                if file_size > effective_max_file_bytes {
                    if file_size > max_file_bytes {
                        return Err(entity_too_large());
                    }
                    return Err(S3Error::access_denied());
                }
                output.write_all(&chunk).await.map_err(io_to_s3)?;
            }
            output.flush().await.map_err(io_to_s3)?;
            output
                .seek(std::io::SeekFrom::Start(0))
                .await
                .map_err(io_to_s3)?;
            drop(field);
            policy.validate_form(&fields, &bucket, file_size)?;
            if multipart
                .next_field()
                .await
                .map_err(|_error| S3Error::invalid_argument("Invalid trailing multipart form"))?
                .is_some()
            {
                return Err(S3Error::invalid_argument("The S3 file field must be last"));
            }
            let verified = build_form(&fields)?;
            let stream = stream::try_unfold(output, |mut input| async move {
                let mut buffer = vec![0u8; BODY_CHUNK_BYTES];
                let read = input.read(&mut buffer).await?;
                if read == 0 {
                    return Ok::<Option<(Bytes, tokio::fs::File)>, io::Error>(None);
                }
                buffer.truncate(read);
                Ok(Some((Bytes::from(buffer), input)))
            });
            request_parts.headers = axum::http::HeaderMap::new();
            request_parts.headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", identity.token.expose_secret()))
                    .map_err(|_error| S3Error::access_denied())?,
            );
            request_parts.extensions.insert(verified);
            return Ok(Request::from_parts(
                request_parts,
                Body::from_stream(stream),
            ));
        }

        if fields.len() >= MAX_FORM_FIELDS {
            return Err(S3Error::invalid_argument("Too many S3 form fields"));
        }
        if !supported_field(&name) {
            return Err(S3Error::not_implemented());
        }
        let mut value = Vec::new();
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|_error| S3Error::invalid_argument("Invalid S3 form field"))?
        {
            total_field_bytes = total_field_bytes
                .checked_add(chunk.len())
                .ok_or_else(S3Error::internal)?;
            if total_field_bytes > MAX_FORM_FIELD_BYTES {
                return Err(S3Error::invalid_argument("S3 form fields exceed 20 KiB"));
            }
            value.extend_from_slice(&chunk);
        }
        let value = String::from_utf8(value)
            .map_err(|_error| S3Error::invalid_argument("S3 form fields must be UTF-8"))?;
        if fields
            .iter()
            .any(|(existing, _)| existing.eq_ignore_ascii_case(&name))
        {
            return Err(S3Error::invalid_argument("Repeated S3 form field"));
        }
        fields.push((name, value));
    }

    Err(S3Error::invalid_argument("Missing S3 file form field"))
}

fn authenticate_policy_fields<'identity>(
    fields: &[(String, String)],
    identities: &'identity [S3SigningIdentity],
) -> Result<
    (
        &'identity S3SigningIdentity,
        shardline_s3_adapter::VerifiedSigV4PostPolicy,
    ),
    S3Error,
> {
    let find = |name: &str| {
        let mut matches = fields
            .iter()
            .filter(|(field, _)| field.eq_ignore_ascii_case(name));
        let value = matches.next().map(|(_, value)| value.as_str());
        if matches.next().is_some() {
            return Err(S3Error::access_denied());
        }
        value.ok_or_else(S3Error::access_denied)
    };
    if find("x-amz-algorithm")? != "AWS4-HMAC-SHA256" {
        return Err(S3Error::access_denied());
    }
    let credential_scope = find("x-amz-credential")?;
    let access_key = credential_scope
        .split('/')
        .next()
        .ok_or_else(S3Error::access_denied)?;
    let identity = identities
        .iter()
        .find(|identity| identity.access_key == access_key)
        .ok_or_else(S3Error::access_denied)?;
    let policy = find("policy")?;
    let date = find("x-amz-date")?;
    let signature = find("x-amz-signature")?;
    let security_token = fields
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-amz-security-token"))
        .map(|(_, value)| value.as_str());
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_error| S3Error::internal())?
            .as_secs(),
    )
    .map_err(|_error| S3Error::internal())?;
    let verified = verify_sigv4_post_policy(
        policy,
        &SigV4PostPolicyAuth {
            credential_scope,
            date,
            security_token,
            signature,
        },
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
    Ok((identity, verified))
}

fn build_form(fields: &[(String, String)]) -> Result<S3PostObjectForm, S3Error> {
    let get = |name: &str| {
        fields
            .iter()
            .find(|(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    };
    let key = get("key")
        .filter(|key| !key.is_empty())
        .ok_or_else(S3Error::access_denied)?
        .to_owned();
    if get("x-amz-storage-class")
        .is_some_and(|storage_class| !storage_class.eq_ignore_ascii_case("STANDARD"))
    {
        return Err(S3Error::not_implemented());
    }
    let success_status = match get("success_action_status") {
        None => 204,
        Some("200") => 200,
        Some("201") => 201,
        Some("204") => 204,
        Some(_) => 204,
    };
    let success_redirect = get("success_action_redirect")
        .or_else(|| get("redirect"))
        .and_then(valid_redirect_url)
        .map(str::to_owned);
    let mut payload_headers = HeaderMap::new();
    for (name, value) in fields {
        let name = name.to_ascii_lowercase();
        if name == "x-amz-meta-" {
            return Err(S3Error::invalid_argument("Invalid S3 user metadata name"));
        }
        if super::object_headers::is_property_header(&name) {
            super::object_headers::attach_form_field(&mut payload_headers, &name, value)?;
        } else if name.starts_with("x-amz-meta-")
            || name == "x-amz-tagging"
            || matches!(
                name.as_str(),
                "content-md5"
                    | "x-amz-checksum-crc32"
                    | "x-amz-checksum-crc32c"
                    | "x-amz-checksum-sha1"
                    | "x-amz-checksum-sha256"
            )
        {
            let header = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_error| S3Error::invalid_argument("Invalid S3 form field name"))?;
            let value = HeaderValue::from_str(value)
                .map_err(|_error| S3Error::invalid_argument("Invalid S3 form field value"))?;
            payload_headers.append(header, value);
        }
    }
    if payload_headers.keys().any(|name| {
        name.as_str()
            .strip_prefix("x-amz-meta-")
            .is_some_and(super::checksum::is_internal_metadata)
    }) {
        return Err(S3Error::invalid_argument("Reserved S3 user metadata name"));
    }
    let mut user_metadata = super::object::capture_user_metadata(&payload_headers)?;
    super::object_headers::capture_headers(&payload_headers, &mut user_metadata)?;
    if let Some(tags) = super::tagging::parse_header(&payload_headers)? {
        super::tagging::replace_metadata(&mut user_metadata, &tags)?;
    }
    Ok(S3PostObjectForm {
        key,
        user_metadata,
        payload_headers,
        success_status,
        success_redirect,
    })
}

fn supported_field(name: &str) -> bool {
    let lower_name = name.to_ascii_lowercase();
    lower_name.starts_with("x-ignore-")
        || super::object_headers::is_property_header(&lower_name)
        || matches!(
            lower_name.as_str(),
            "key"
                | "policy"
                | "x-amz-signature"
                | "x-amz-algorithm"
                | "x-amz-credential"
                | "x-amz-date"
                | "x-amz-security-token"
                | "success_action_status"
                | "success_action_redirect"
                | "redirect"
                | "bucket"
                | "content-md5"
                | "x-amz-checksum-crc32"
                | "x-amz-checksum-crc32c"
                | "x-amz-checksum-sha1"
                | "x-amz-checksum-sha256"
                | "x-amz-tagging"
                | "x-amz-storage-class"
        )
        || name.to_ascii_lowercase().starts_with("x-amz-meta-")
}

/// S3 ignores redirect values it cannot interpret. Restrict redirects to
/// absolute HTTP(S) URLs so a form cannot turn a successful upload response
/// into a `javascript:`, `data:`, or credential-bearing redirect.
fn valid_redirect_url(value: &str) -> Option<&str> {
    let parsed = url::Url::parse(value).ok()?;
    matches!(parsed.scheme(), "http" | "https")
        .then_some(value)
        .filter(|_| {
            parsed.host_str().is_some()
                && parsed.username().is_empty()
                && parsed.password().is_none()
        })
}

fn expand_filename(fields: &[(String, String)], filename: &str) -> Vec<(String, String)> {
    fields
        .iter()
        .map(|(name, value)| {
            if name.eq_ignore_ascii_case("key") {
                (name.clone(), value.replace("${filename}", filename))
            } else {
                (name.clone(), value.clone())
            }
        })
        .collect()
}

fn bucket_from_path(path: &str) -> Result<String, S3Error> {
    let Some(bucket) = path.strip_prefix('/') else {
        return Err(S3Error::access_denied());
    };
    if bucket.is_empty() || bucket.contains('/') {
        return Err(S3Error::access_denied());
    }
    let bucket = percent_encoding::percent_decode_str(bucket)
        .decode_utf8()
        .map(|bucket| bucket.into_owned())
        .map_err(|_error| S3Error::access_denied())?;
    if bucket.is_empty() || bucket.contains('/') || bucket.contains('\\') {
        return Err(S3Error::access_denied());
    }
    Ok(bucket)
}

fn entity_too_large() -> S3Error {
    S3Error {
        code: "EntityTooLarge",
        message: "Your proposed upload exceeds the maximum allowed object size".to_owned(),
        status: axum::http::StatusCode::PAYLOAD_TOO_LARGE,
    }
}

fn io_to_s3(error: io::Error) -> S3Error {
    S3Error::from(crate::ServerError::Io(error))
}
