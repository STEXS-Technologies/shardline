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
    // Completed legacy uploads do not retain authoritative per-part metadata.
    // Do not manufacture boundaries or claim a multipart object has no parts.
    if selected.contains("ObjectParts") {
        return Err(S3Error::not_implemented());
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
    // No S3 checksum metadata is persisted yet. Omit unavailable Checksum
    // rather than presenting the internal content commitment as an S3 checksum.
    if selected.contains("StorageClass") {
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
    }
    if selected.contains("ObjectSize") {
        write!(xml, "<ObjectSize>{}</ObjectSize>", snapshot.total_bytes)
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
