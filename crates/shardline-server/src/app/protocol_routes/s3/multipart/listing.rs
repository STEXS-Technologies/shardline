//! Read-only multipart discovery over the existing durable session state.
use super::*;
use shardline_s3_adapter::{QueryMap, xml_escape};
use std::fmt::Write;

mod anchor;
mod page;

async fn marker_time(
    state: &Arc<AppState>,
    scope: &str,
    bucket: &str,
    key: &str,
    id: &str,
) -> Result<Option<Option<u64>>, S3Error> {
    if key.is_empty() || id.is_empty() || shardline_s3_adapter::validate_upload_id(id).is_err() {
        return Ok(None);
    }
    if durable_sessions_enabled(state) {
        if let Some(session) = state.backend.resumable_session_by_id(id).await?
            && session.protocol() == ResumableSessionProtocol::S3Multipart
            && session.scope_namespace() == scope
            && session.target_key() == key
        {
            let attributes: DurableS3SessionAttributes =
                serde_json::from_str(session.attributes_json())
                    .map_err(|_invalid_attributes| S3Error::internal())?;
            if attributes.bucket == bucket {
                return Ok(Some(attributes.initiated_at));
            }
        }
    } else {
        match shardline_s3_adapter::read_session(
            state.config.root_dir(),
            id,
            state.config.s3_upload_session_ttl_seconds(),
        )
        .await
        {
            Ok(session)
                if session.scope_namespace == scope
                    && session.bucket == bucket
                    && session.key == key =>
            {
                return Ok(Some(Some(session.created_at_unix_seconds)));
            }
            Ok(_) => {}
            Err(shardline_s3_adapter::S3SessionError::NotFound) => {
                return anchor::read(state, scope, bucket, key, id).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(None)
}

fn number(query: &QueryMap, name: &str, default: u32, maximum: u32) -> Result<u32, S3Error> {
    let mut values = query.iter().filter(|(key, _)| key == name);
    let value = values.next().map_or(Ok(default), |(_, value)| {
        value.parse::<u32>().map_err(|_invalid_number| {
            S3Error::invalid_argument("Invalid multipart listing parameter")
        })
    })?;
    if values.next().is_some() || value > maximum {
        return Err(S3Error::invalid_argument(
            "Invalid multipart listing parameter",
        ));
    }
    Ok(value)
}

fn text<'query>(query: &'query QueryMap, name: &str) -> Result<&'query str, S3Error> {
    let mut values = query.iter().filter(|(key, _)| key == name);
    let value = values.next().map_or("", |(_, value)| value.as_str());
    if values.next().is_some() {
        return Err(S3Error::invalid_argument(
            "Repeated multipart listing parameter",
        ));
    }
    Ok(value)
}

pub(crate) async fn s3_list_uploads(
    state: &Arc<AppState>,
    auth: &shardline_server_core::AuthorizedRepository,
    bucket: &str,
    query: &QueryMap,
) -> Result<Response, S3Error> {
    let scope = crate::protocol_support::scope_namespace(auth.namespace());
    let prefix = text(query, "prefix")?;
    let delimiter = text(query, "delimiter")?;
    let marker = text(query, "key-marker")?;
    let upload_marker = text(query, "upload-id-marker")?;
    let encoding = text(query, "encoding-type")?;
    if !matches!(encoding, "" | "url") {
        return Err(S3Error::invalid_argument("Invalid encoding type"));
    }
    let limit = number(query, "max-uploads", 1000, 1000)? as usize;
    if limit == 0 {
        return Err(S3Error::invalid_argument("max-uploads must be positive"));
    }
    let anchor = marker_time(state, &scope, bucket, marker, upload_marker).await?;
    let mut selected =
        page::UploadPage::new(prefix, delimiter, marker, upload_marker, anchor, limit);
    if durable_sessions_enabled(state) {
        let now = state.backend.postgres_now().await?;
        let mut after = None;
        loop {
            let page = state
                .backend
                .list_scoped_inflight_resumable_sessions(
                    &scope,
                    ResumableSessionProtocol::S3Multipart,
                    after.as_deref(),
                    64,
                )
                .await?;
            if page.is_empty() {
                break;
            }
            for session in &page {
                let attributes: DurableS3SessionAttributes =
                    serde_json::from_str(session.attributes_json())
                        .map_err(|_invalid_attributes| S3Error::internal())?;
                if attributes.bucket == bucket && session.expires_at() > now {
                    selected.push(
                        session.target_key().to_owned(),
                        session.session_id().to_owned(),
                        attributes.initiated_at,
                    );
                }
            }
            after = page.last().map(|session| session.session_id().to_owned());
            if page.len() < 64 {
                break;
            }
        }
    } else {
        shardline_s3_adapter::visit_bucket_sessions(
            state.config.root_dir(),
            &scope,
            bucket,
            state.config.s3_upload_session_ttl_seconds(),
            |session| {
                selected.push(
                    session.key,
                    session.upload_id,
                    Some(session.created_at_unix_seconds),
                )
            },
        )
        .await?;
    }
    let (truncated, entries) = selected.finish();
    if truncated && let Some(last) = entries.last() {
        anchor::retain(state, &scope, bucket, last).await?;
    }
    let encode = |value: &str| {
        if encoding == "url" {
            value
                .bytes()
                .map(|byte| {
                    if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
                        char::from(byte).to_string()
                    } else {
                        format!("%{byte:02X}")
                    }
                })
                .collect::<String>()
        } else {
            value.to_owned()
        }
    };
    let (next_key, next_id) = entries
        .last()
        .map_or(("", ""), |entry| (entry.key.as_str(), entry.id.as_str()));
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket><KeyMarker>{}</KeyMarker><UploadIdMarker>{}</UploadIdMarker><NextKeyMarker>{}</NextKeyMarker><NextUploadIdMarker>{}</NextUploadIdMarker><Prefix>{}</Prefix><Delimiter>{}</Delimiter><MaxUploads>{limit}</MaxUploads><IsTruncated>{truncated}</IsTruncated>",
        xml_escape(bucket),
        xml_escape(&encode(marker)),
        xml_escape(upload_marker),
        xml_escape(&encode(next_key)),
        xml_escape(next_id),
        xml_escape(&encode(prefix)),
        xml_escape(&encode(delimiter))
    );
    if encoding == "url" {
        xml.push_str("<EncodingType>url</EncodingType>");
    }
    for page::UploadEntry {
        key,
        id,
        initiated,
        group,
    } in entries
    {
        if group {
            write!(
                xml,
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                xml_escape(&encode(&key))
            )
            .map_err(|_format| S3Error::internal())?;
        } else {
            write!(
                xml,
                "<Upload><Key>{}</Key><UploadId>{}</UploadId><StorageClass>STANDARD</StorageClass>",
                xml_escape(&encode(&key)),
                xml_escape(&id)
            )
            .map_err(|_format| S3Error::internal())?;
            if let Some(time) = initiated
                .and_then(|seconds| i64::try_from(seconds).ok())
                .and_then(|seconds| chrono::DateTime::from_timestamp(seconds, 0))
            {
                write!(
                    xml,
                    "<Initiated>{}</Initiated>",
                    time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                )
                .map_err(|_format| S3Error::internal())?;
            }
            xml.push_str("</Upload>");
        }
    }
    xml.push_str("</ListMultipartUploadsResult>");
    Ok((
        [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
        xml,
    )
        .into_response())
}

pub(crate) async fn s3_list_parts(
    state: &Arc<AppState>,
    context: &S3ObjectContext<'_>,
    upload_id: &str,
    query: &QueryMap,
) -> Result<Response, S3Error> {
    let marker = number(query, "part-number-marker", 0, 10000)?;
    let limit = number(query, "max-parts", 1000, 1000)? as usize;
    let checksum_config;
    let mut parts = Vec::new();
    if durable_sessions_enabled(state) {
        let (session, snapshot) = state
            .backend
            .resumable_session_snapshot(upload_id)
            .await?
            .ok_or_else(S3Error::no_such_upload)?;
        let attributes: DurableS3SessionAttributes =
            serde_json::from_str(session.attributes_json())
                .map_err(|_invalid_attributes| S3Error::internal())?;
        if session.protocol() != ResumableSessionProtocol::S3Multipart
            || !matches!(
                session.state(),
                ResumableSessionState::Active | ResumableSessionState::Completing
            )
            || session.scope_namespace() != context.scope_namespace
            || session.target_key() != context.key
            || attributes.bucket != context.bucket
            || session.expires_at() <= state.backend.postgres_now().await?
        {
            return Err(S3Error::no_such_upload());
        }
        checksum_config = checksum::multipart_checksum_config(&attributes.user_metadata);
        let checksum_algorithm = checksum_config.map(|config| config.algorithm);
        for part in snapshot {
            let number = u32::try_from(part.part_number().get())
                .map_err(|_invalid_part_number| S3Error::internal())?;
            if number > marker {
                let checksum_value = checksum_algorithm.and_then(|algorithm| {
                    attributes
                        .part_checksums
                        .get(&number)
                        .filter(|part_checksum| {
                            part_checksum.algorithm == algorithm.s3_name()
                                && part_checksum.generation == part.generation().get()
                        })
                        .map(|part_checksum| part_checksum.checksum.clone())
                });
                let checksum_value = if let Some(algorithm) = checksum_algorithm {
                    match checksum_value {
                        Some(value) => Some(value),
                        None => {
                            let mut reader = s3_resumable_parts_reader(
                                &state.backend.object_store(),
                                std::slice::from_ref(&part),
                            )
                            .await?
                            .ok_or_else(S3Error::internal)?;
                            Some(
                                checksum::checksum_reader(
                                    algorithm,
                                    &mut reader,
                                    part.size_bytes(),
                                )
                                .await?,
                            )
                        }
                    }
                } else {
                    None
                };
                parts.push((
                    number,
                    part.size_bytes(),
                    part.etag().ok_or_else(S3Error::internal)?.to_owned(),
                    checksum_value,
                ));
            }
        }
    } else {
        let root = state.config.root_dir();
        let _sessions = lock_upload_sessions(root).await?;
        let part_lock = acquire_session_part_lock_for_root(root, upload_id);
        let _guard = part_lock.lock().await;
        let _parts = lock_session_parts(root, upload_id).await?;
        let session = read_session_locked(
            root,
            upload_id,
            state.config.s3_upload_session_ttl_seconds(),
        )
        .await?;
        drop(_sessions);
        if session.bucket != context.bucket
            || session.key != context.key
            || session.scope_namespace != context.scope_namespace
        {
            return Err(S3Error::no_such_upload());
        }
        checksum_config = checksum::multipart_checksum_config(&session.user_metadata);
        let checksum_algorithm = checksum_config.map(|config| config.algorithm);
        for (number, part) in session
            .parts
            .range((
                std::ops::Bound::Excluded(marker),
                std::ops::Bound::Unbounded,
            ))
            .take(limit.saturating_add(1))
        {
            let etag = match &part.etag {
                Some(etag) => etag.clone(),
                None => {
                    local_part_md5_etag(
                        &stored_part_file_path(root, upload_id, *number, part)?,
                        part.size_bytes,
                    )
                    .await?
                }
            };
            let checksum_value = if let Some(algorithm) = checksum_algorithm {
                match checksum::part_checksum_from_metadata(
                    &session.user_metadata,
                    *number,
                    &part.file_name,
                    algorithm,
                )? {
                    Some(value) => Some(value),
                    None => {
                        let path = stored_part_file_path(root, upload_id, *number, part)?;
                        if let Some((_, digest)) = part
                            .file_name
                            .rsplit_once('-')
                            .filter(|(_, digest)| digest.len() == 64)
                        {
                            validate_local_part_file(&path, part.size_bytes, digest).await?;
                        }
                        let file = tokio::fs::File::open(&path).await.map_err(io_to_s3)?;
                        let mut reader =
                            RequestBodyReader::from_reader_chain(vec![file], 64 * 1024);
                        Some(
                            checksum::checksum_reader(algorithm, &mut reader, part.size_bytes)
                                .await?,
                        )
                    }
                }
            } else {
                None
            };
            parts.push((*number, part.size_bytes, etag, checksum_value));
        }
    }
    let checksum_algorithm = checksum_config.map(|config| config.algorithm);
    parts.sort_by_key(|part| part.0);
    let truncated = parts.len() > limit;
    parts.truncate(limit);
    let next = parts.last().map_or(marker, |part| part.0);
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId><PartNumberMarker>{marker}</PartNumberMarker><NextPartNumberMarker>{next}</NextPartNumberMarker><MaxParts>{limit}</MaxParts><IsTruncated>{truncated}</IsTruncated><StorageClass>STANDARD</StorageClass>",
        xml_escape(&context.bucket),
        xml_escape(&context.key),
        xml_escape(upload_id)
    );
    if let Some(config) = checksum_config {
        write!(
            xml,
            "<ChecksumAlgorithm>{}</ChecksumAlgorithm><ChecksumType>{}</ChecksumType>",
            config.algorithm.s3_name(),
            config.checksum_type.s3_name()
        )
        .map_err(|_| S3Error::internal())?;
    }
    for (number, size, etag, checksum_value) in parts {
        if let Some(checksum_value) = checksum_value {
            let algorithm = checksum_algorithm.ok_or_else(S3Error::internal)?;
            write!(
                xml,
                "<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag><Size>{size}</Size><{}>{}</{}></Part>",
                xml_escape(&etag),
                algorithm.xml_field(),
                xml_escape(&checksum_value),
                algorithm.xml_field()
            )
            .map_err(|_| S3Error::internal())?;
        } else {
            write!(
                xml,
                "<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag><Size>{size}</Size></Part>",
                xml_escape(&etag)
            )
            .map_err(|_| S3Error::internal())?;
        }
    }
    xml.push_str("</ListPartsResult>");
    Ok((
        [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
        xml,
    )
        .into_response())
}
