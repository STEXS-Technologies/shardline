//! Read validators use the same committed timestamp as the returned metadata.
use super::*;
use axum::http::header::{IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_UNMODIFIED_SINCE};

pub(super) fn timestamp(value: &str) -> Option<i64> {
    if let Ok(date) = httpdate::parse_http_date(value) {
        let date: chrono::DateTime<chrono::Utc> = date.into();
        return Some(date.timestamp());
    }
    // httpdate deliberately excludes pre-epoch dates. Accept their canonical
    // IMF-fixdate spelling without relaxing validation of ordinary dates.
    let format = "%a, %d %b %Y %H:%M:%S GMT";
    let date = chrono::NaiveDateTime::parse_from_str(value, format).ok()?;
    let timestamp = date.and_utc().timestamp();
    (timestamp < 0 && date.format(format).to_string() == value).then_some(timestamp)
}

fn date(headers: &HeaderMap, name: HeaderName) -> Option<i64> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    // HTTP date conditions are ignored if the value is not one valid date.
    if values.next().is_some() {
        return None;
    }
    timestamp(value.to_str().ok()?)
}

pub(super) fn require_unmodified(
    modified: Option<i64>,
    headers: &HeaderMap,
) -> Result<(), S3Error> {
    // An entity-tag validator takes precedence over its corresponding date.
    if !headers.contains_key(IF_MATCH)
        && let (Some(modified), Some(since)) = (modified, date(headers, IF_UNMODIFIED_SINCE))
        && modified > since
    {
        return Err(S3Error::precondition_failed());
    }
    Ok(())
}

pub(super) fn modified_response(
    etag: Option<&str>,
    modified: Option<i64>,
    headers: &HeaderMap,
) -> Result<Option<Response>, S3Error> {
    if !headers.contains_key(IF_NONE_MATCH)
        && let (Some(modified), Some(since)) = (modified, date(headers, IF_MODIFIED_SINCE))
        && modified <= since
    {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        response.headers_mut().insert(
            LAST_MODIFIED,
            HeaderValue::from_str(&format_http_date(modified))
                .map_err(|_invalid_date| S3Error::internal())?,
        );
        if let Some(etag) = etag {
            response.headers_mut().insert(
                ETAG,
                HeaderValue::from_str(&etag_header(etag))
                    .map_err(|_invalid_etag| S3Error::internal())?,
            );
        }
        return Ok(Some(response));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    #[test]
    fn pre_epoch_dates_preserve_calendar_validation() {
        assert_eq!(
            super::timestamp("Fri, 01 Jan 1960 00:00:00 GMT"),
            Some(-315619200)
        );
        for invalid in [
            "Thu, 01 Jan 1960 00:00:00 GMT",
            "Fri, 32 Jan 1960 00:00:00 GMT",
            "Fri, 01 Jan 1960 24:00:00 GMT",
            "Fri, 01 Jan 1960 00:00:00 UTC",
            "Fri, 01 Jan 1960 00:00:00 GMT extra",
        ] {
            assert_eq!(super::timestamp(invalid), None, "{invalid}");
        }
    }
}
