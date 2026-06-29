use std::{fmt::Write, num::NonZeroUsize};

use axum::http::{
    HeaderMap,
    header::{AUTHORIZATION, HeaderValue},
};
use shardline_index::ProviderRepositoryState;
use shardline_protocol::RepositoryProvider;

use super::{
    MAX_BATCH_RECONSTRUCTION_FILE_IDS, MAX_BATCH_RECONSTRUCTION_QUERY_BYTES,
    MAX_PROVIDER_BASIC_AUTH_HEADER_BYTES, MAX_PROVIDER_NAME_BYTES, MAX_PROVIDER_SUBJECT_BYTES,
    MAX_PROVIDER_WEBHOOK_BODY_BYTES, bounded_api_body_limit, extract_provider_subject,
    latest_lifecycle_signal_at, parse_batch_reconstruction_query,
    reconciled_provider_repository_state, validate_provider_name_path,
};
use crate::ServerError;

#[test]
fn provider_subject_extraction_rejects_oversized_query_subject() {
    let oversized = "s".repeat(MAX_PROVIDER_SUBJECT_BYTES + 1);
    let result = extract_provider_subject(&HeaderMap::new(), Some(&oversized));

    assert!(matches!(
        result,
        Err(ServerError::InvalidProviderTokenRequest)
    ));
}

#[test]
fn provider_subject_extraction_rejects_oversized_basic_auth_header_before_decode() {
    let oversized = "a".repeat(MAX_PROVIDER_BASIC_AUTH_HEADER_BYTES + 1);
    let header_value = HeaderValue::from_str(&format!("Basic {oversized}"));
    assert!(header_value.is_ok());
    let Ok(header_value) = header_value else {
        return;
    };
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, header_value);

    let result = extract_provider_subject(&headers, None);

    assert!(matches!(
        result,
        Err(ServerError::InvalidAuthorizationHeader)
    ));
}

#[test]
fn provider_api_body_limit_uses_stricter_configured_or_endpoint_ceiling() {
    let tighter = NonZeroUsize::new(32).unwrap_or(NonZeroUsize::MIN);
    let looser =
        NonZeroUsize::new(MAX_PROVIDER_WEBHOOK_BODY_BYTES + 1).unwrap_or(NonZeroUsize::MIN);

    assert_eq!(
        bounded_api_body_limit(tighter, MAX_PROVIDER_WEBHOOK_BODY_BYTES),
        tighter.get()
    );
    assert_eq!(
        bounded_api_body_limit(looser, MAX_PROVIDER_WEBHOOK_BODY_BYTES),
        MAX_PROVIDER_WEBHOOK_BODY_BYTES
    );
}

#[test]
fn provider_repository_reconciliation_marks_pending_lifecycle_signals() {
    let state = ProviderRepositoryState::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        Some(10),
        Some(12),
        Some("refs/heads/main".to_owned()),
    )
    .with_reconciliation(Some(11), None, None);

    assert_eq!(latest_lifecycle_signal_at(&state), Some(12));
    let reconciled = reconciled_provider_repository_state(&state, 20);

    assert_eq!(
        reconciled.last_cache_invalidated_at_unix_seconds(),
        Some(20)
    );
    assert_eq!(
        reconciled.last_authorization_rechecked_at_unix_seconds(),
        Some(20)
    );
    assert_eq!(reconciled.last_drift_checked_at_unix_seconds(), Some(20));
}

#[test]
fn batch_reconstruction_parser_deduplicates_file_ids() {
    let parsed = parse_batch_reconstruction_query(
        "file_id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&file_id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&ignored=value",
    );

    assert!(parsed.is_ok());
    let Ok(parsed) = parsed else {
        return;
    };
    assert_eq!(
        parsed,
        vec!["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()]
    );
}

#[test]
fn batch_reconstruction_parser_rejects_excessive_file_ids() {
    let mut query = String::new();
    for index in 0..=MAX_BATCH_RECONSTRUCTION_FILE_IDS {
        if !query.is_empty() {
            query.push('&');
        }
        query.push_str("file_id=");
        let written = write!(&mut query, "{index:064x}");
        assert!(written.is_ok());
    }

    let parsed = parse_batch_reconstruction_query(&query);

    assert!(matches!(
        parsed,
        Err(ServerError::TooManyBatchReconstructionFileIds)
    ));
}

#[test]
fn batch_reconstruction_parser_rejects_oversized_query_before_scanning() {
    let mut query = String::from("ignored=");
    query.push_str(&"a".repeat(MAX_BATCH_RECONSTRUCTION_QUERY_BYTES + 1));

    let parsed = parse_batch_reconstruction_query(&query);

    assert!(matches!(parsed, Err(ServerError::RequestQueryTooLarge)));
}

#[test]
fn provider_path_name_rejects_empty_or_oversized_values() {
    let empty = validate_provider_name_path("");
    let oversized = validate_provider_name_path(&"p".repeat(MAX_PROVIDER_NAME_BYTES + 1));
    let valid = validate_provider_name_path("github");

    assert!(matches!(
        empty,
        Err(ServerError::InvalidProviderTokenRequest)
    ));
    assert!(matches!(
        oversized,
        Err(ServerError::InvalidProviderTokenRequest)
    ));
    assert!(valid.is_ok());
}
