//! Server-side part copies reuse pinned reads and durable part publication.
use super::*;

pub(super) async fn upload_part_copy(
    state: &Arc<AppState>,
    capability: &AuthorizedRepository,
    destination: &S3ObjectContext<'_>,
    part_number: u32,
    upload_id: &str,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let mut sources = headers.get_all(COPY_SOURCE).iter();
    let source = sources.next().ok_or_else(S3Error::internal)?;
    if sources.next().is_some() {
        return Err(S3Error::invalid_argument("Repeated copy source header"));
    }
    let source = parse_copy_source(
        source
            .to_str()
            .map_err(|_invalid_header| S3Error::invalid_argument("Invalid copy source header"))?,
    )
    .map_err(|_invalid_source| S3Error::invalid_argument("Invalid copy source header"))?;
    require_s3_bucket_binding(capability.repository(), &source.bucket)?;
    let context = require_s3_object_context(capability, &source.key)?;
    let snapshot = match state
        .backend
        .s3_object_read_snapshot(&context.scope_namespace, &context.key, &context.object_key)
        .await
    {
        Ok(snapshot) => snapshot,
        Err(ServerError::NotFound) => return Err(S3Error::no_such_key(&source.key)),
        Err(error) => return Err(error.into()),
    };
    check_copy_source_preconditions(headers, &snapshot)?;
    let mut ranges = headers.get_all("x-amz-copy-source-range").iter();
    let range =
        if let Some(value) = ranges.next() {
            if ranges.next().is_some() {
                return Err(S3Error::invalid_argument("Repeated copy source range"));
            }
            let value = value.to_str().map_err(|_invalid_header| {
                S3Error::invalid_argument("Invalid copy source range")
            })?;
            // Copy ranges require explicit inclusive bounds; GetObject suffix and
            // open-ended ranges must not silently acquire different semantics.
            let bounds = value
                .strip_prefix("bytes=")
                .and_then(|value| value.split_once('-'))
                .ok_or_else(|| S3Error::invalid_argument("Invalid copy source range"))?;
            let start = bounds.0.parse::<u64>().map_err(|_invalid_number| {
                S3Error::invalid_argument("Invalid copy source range")
            })?;
            let end = bounds.1.parse::<u64>().map_err(|_invalid_number| {
                S3Error::invalid_argument("Invalid copy source range")
            })?;
            if start > end || end >= snapshot.total_bytes {
                return Err(S3Error::invalid_range());
            }
            Some(parse_s3_range(Some(value), snapshot.total_bytes)?)
        } else {
            None
        };
    let length = range
        .map_or(Some(snapshot.total_bytes), |range| range.len())
        .ok_or(ServerError::Overflow)?;
    if length > state.config.s3_max_part_bytes().get() {
        return Err(multipart::entity_too_large());
    }
    #[cfg(test)]
    super::copy_part_test_hooks::pause_after_snapshot(&context.object_key).await;
    let stream = state
        .backend
        .read_object_stream_pinned(
            &context.object_key,
            snapshot.total_bytes,
            range,
            snapshot.record_content_hash.as_deref(),
        )
        .await?;
    // Request-body framing and Content-MD5 describe the caller's empty COPY
    // request, not the source bytes. Only the verified source stream is passed
    // into the existing quota/checksum/publication machinery.
    let part = multipart::s3_upload_part(
        state,
        destination,
        part_number,
        upload_id,
        &HeaderMap::new(),
        Body::from_stream(bounded_byte_stream(stream, length)),
    )
    .await?;
    let etag = part
        .headers()
        .get(ETAG)
        .ok_or_else(S3Error::internal)?
        .to_str()
        .map_err(|_invalid_etag| S3Error::internal())?;
    let now = i64::try_from(shardline_protocol::unix_now_seconds_lossy())
        .map_err(|_overflow| S3Error::internal())?;
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CopyPartResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><ETag>{}</ETag><LastModified>{}</LastModified></CopyPartResult>",
        shardline_s3_adapter::xml_escape(etag),
        format_iso8601(now)
    );
    Ok((StatusCode::OK, [(CONTENT_TYPE, s3_xml_content_type())], xml).into_response())
}
