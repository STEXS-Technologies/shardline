//! Attributes from one verified committed snapshot; never infer multipart data.
use super::*;
use shardline_s3_adapter::xml_escape;
use std::fmt::Write as _;

pub(super) async fn get(
    state: &Arc<AppState>,
    context: &S3ObjectContext<'_>,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let mut values = headers.get_all("x-amz-object-attributes").iter();
    let value = values
        .next()
        .ok_or_else(|| S3Error::invalid_argument("Missing x-amz-object-attributes"))?;
    if values.next().is_some() {
        return Err(S3Error::invalid_argument(
            "Repeated x-amz-object-attributes",
        ));
    }
    let value = value
        .to_str()
        .map_err(|_invalid_header| S3Error::invalid_argument("Invalid x-amz-object-attributes"))?;
    let mut selected = std::collections::BTreeSet::new();
    for attribute in value.split(',').map(str::trim) {
        match attribute {
            "ETag" | "ObjectSize" | "StorageClass" | "Checksum" | "ObjectParts" => {}
            _ => return Err(S3Error::invalid_argument("Invalid object attribute")),
        }
        if !selected.insert(attribute) {
            return Err(S3Error::invalid_argument("Repeated object attribute"));
        }
    }
    let max_parts = parse_optional_u32_header(headers, "x-amz-max-parts")?;
    let part_number_marker = parse_optional_u32_header(headers, "x-amz-part-number-marker")?;
    if (max_parts.is_some() || part_number_marker.is_some()) && !selected.contains("ObjectParts") {
        return Err(S3Error::invalid_argument(
            "Part pagination headers require ObjectParts",
        ));
    }
    if max_parts.is_some_and(|value| value > 1000) {
        return Err(S3Error::invalid_argument("Invalid x-amz-max-parts"));
    }
    for unsupported in [
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-server-side-encryption-customer-key",
        "x-amz-server-side-encryption-customer-key-md5",
        "x-amz-request-payer",
        "x-amz-expected-bucket-owner",
    ] {
        if headers.contains_key(unsupported) {
            return Err(S3Error::not_implemented());
        }
    }
    let snapshot = match state
        .backend
        .s3_object_read_snapshot(&context.scope_namespace, &context.key, &context.object_key)
        .await
    {
        Ok(snapshot) => snapshot,
        Err(ServerError::NotFound) => return Err(S3Error::no_such_key(&context.key)),
        Err(error) => return Err(error.into()),
    };
    let known_part_count = snapshot.multipart_part_count;
    let part_count = if selected.contains("ObjectParts") {
        match known_part_count {
            Some(count) => Some(count),
            None => return Err(S3Error::not_implemented()),
        }
    } else {
        None
    };
    let checksum_manifest = if selected.contains("Checksum") || selected.contains("ObjectParts") {
        match known_part_count {
            Some(count) => checksum::read_multipart_manifest(
                &snapshot.user_metadata,
                count,
                snapshot.total_bytes,
            )?,
            None => None,
        }
    } else {
        None
    };
    if let Some(response) = check_read_precondition(
        snapshot.etag.as_deref(),
        snapshot.updated_at_unix_seconds,
        headers,
        &context.key,
    )? {
        return Ok(response);
    }
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    if selected.contains("ETag") {
        if let Some(etag) = &snapshot.etag {
            write!(xml, "<ETag>{}</ETag>", xml_escape(etag))
                .map_err(|_format_error| S3Error::internal())?;
        } else {
            // Unindexed legacy records have no S3 ETag; a BLAKE3 content root
            // cannot substitute for the MD5-based S3 validator.
            return Err(S3Error::not_implemented());
        }
    }
    if selected.contains("Checksum")
        && let Some(manifest) = &checksum_manifest
    {
        write!(
            xml,
            "<Checksum><{}>{}</{}><ChecksumType>COMPOSITE</ChecksumType></Checksum>",
            manifest.algorithm.xml_field(),
            xml_escape(&manifest.checksum_value),
            manifest.algorithm.xml_field()
        )
        .map_err(|_format_error| S3Error::internal())?;
    }
    // Never present Shardline's internal content commitment as an S3 checksum.
    // Standard is S3's default storage class and S3 omits this response field
    // for Standard objects.
    if selected.contains("ObjectSize") {
        write!(xml, "<ObjectSize>{}</ObjectSize>", snapshot.total_bytes)
            .map_err(|_format_error| S3Error::internal())?;
    }
    if let Some(part_count) = part_count.filter(|count| *count > 0) {
        let max_parts = max_parts.unwrap_or(1000);
        let part_number_marker = part_number_marker.unwrap_or(0);
        let checksum_field = checksum_manifest
            .as_ref()
            .map(|manifest| manifest.algorithm.xml_field());
        let page = checksum_manifest.as_ref().map(|manifest| {
            let mut parts = manifest
                .parts
                .iter()
                .filter(|part| part.part_number > part_number_marker);
            let page: Vec<_> = parts.by_ref().take(max_parts as usize).collect();
            let is_truncated = parts.next().is_some();
            (page, is_truncated)
        });
        let (page, is_truncated) = page.map_or_else(
            || (Vec::new(), false),
            |(parts, truncated)| (parts, truncated),
        );
        write!(
            xml,
            "<ObjectParts><IsTruncated>{is_truncated}</IsTruncated><MaxParts>{max_parts}</MaxParts>"
        )
        .map_err(|_format_error| S3Error::internal())?;
        if is_truncated && let Some(last) = page.last() {
            write!(
                xml,
                "<NextPartNumberMarker>{}</NextPartNumberMarker>",
                last.part_number
            )
            .map_err(|_format_error| S3Error::internal())?;
        }
        write!(
            xml,
            "<PartNumberMarker>{part_number_marker}</PartNumberMarker>"
        )
        .map_err(|_format_error| S3Error::internal())?;
        for part in page {
            let Some(checksum_field) = checksum_field else {
                return Err(S3Error::internal());
            };
            write!(
                xml,
                "<Part><PartNumber>{}</PartNumber><Size>{}</Size><{}>{}</{}></Part>",
                part.part_number,
                part.size_bytes,
                checksum_field,
                xml_escape(&part.checksum_value),
                checksum_field
            )
            .map_err(|_format_error| S3Error::internal())?;
        }
        write!(xml, "<PartsCount>{part_count}</PartsCount></ObjectParts>")
            .map_err(|_format_error| S3Error::internal())?;
    }
    xml.push_str("</GetObjectAttributesResponse>");
    let mut response =
        (StatusCode::OK, [(CONTENT_TYPE, s3_xml_content_type())], xml).into_response();
    response.headers_mut().insert(
        LAST_MODIFIED,
        HeaderValue::from_str(&format_http_date(
            snapshot.updated_at_unix_seconds.unwrap_or(0),
        ))
        .map_err(|_invalid_date| S3Error::internal())?,
    );
    Ok(response)
}

fn parse_optional_u32_header(
    headers: &HeaderMap,
    name: &'static str,
) -> Result<Option<u32>, S3Error> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(S3Error::invalid_argument("Repeated part pagination header"));
    }
    let value = value
        .to_str()
        .map_err(|_invalid_header| S3Error::invalid_argument("Invalid part pagination header"))?;
    value
        .parse::<u32>()
        .map(Some)
        .map_err(|_invalid_integer| S3Error::invalid_argument("Invalid part pagination header"))
}
