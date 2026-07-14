use std::num::NonZeroUsize;

use axum::body::Bytes;

#[cfg(test)]
use crate::config::default_upload_max_in_flight_chunks;
use crate::{
    ServerError, model::UploadFileResponse, object_store::ServerObjectStore,
    upload_ingest::FileUploadIngestor, validation::validate_identifier,
};

/// Runs the upload ingestion pipeline without persisting chunk bytes or metadata.
///
/// This is intended for throughput benchmarking of request processing, chunking, and
/// hashing without object-store or index-store costs.
///
/// # Errors
///
/// Returns [`ServerError`] when the identifier is invalid, arithmetic overflows, or the
/// optional expected SHA-256 digest does not match the uploaded bytes.
#[cfg(test)]
pub async fn ingest_without_storage(
    chunk_size: NonZeroUsize,
    file_id: &str,
    body: Bytes,
    expected_sha256: Option<&str>,
) -> Result<UploadFileResponse, ServerError> {
    ingest_without_storage_with_parallelism(
        chunk_size,
        default_upload_max_in_flight_chunks(),
        file_id,
        body,
        expected_sha256,
    )
    .await
}

/// Runs the upload ingestion pipeline without storage using explicit chunk parallelism.
///
/// # Errors
///
/// Returns [`ServerError`] when the identifier is invalid, arithmetic overflows, or the
/// optional expected SHA-256 digest does not match the uploaded bytes.
pub async fn ingest_without_storage_with_parallelism(
    chunk_size: NonZeroUsize,
    max_in_flight_chunks: NonZeroUsize,
    file_id: &str,
    body: Bytes,
    expected_sha256: Option<&str>,
) -> Result<UploadFileResponse, ServerError> {
    validate_identifier(file_id)?;
    let object_store = ServerObjectStore::blackhole();
    let mut ingestor = FileUploadIngestor::new_with_parallelism(
        chunk_size,
        expected_sha256.is_some(),
        max_in_flight_chunks,
    );
    ingestor.ingest_body_chunk(&object_store, &body).await?;
    let (_record, response) = ingestor
        .finish(&object_store, file_id, None, expected_sha256)
        .await?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use axum::body::Bytes;

    use super::ingest_without_storage;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_without_storage_counts_chunk_processing() {
        let chunk_size = NonZeroUsize::new(4);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };

        let response = ingest_without_storage(
            chunk_size,
            "asset.bin",
            Bytes::from_static(b"abcdefgh"),
            None,
        )
        .await;
        assert!(response.is_ok());
        let Ok(response) = response else {
            return;
        };

        assert_eq!(response.total_bytes, 8);
        assert_eq!(response.inserted_chunks, 2);
        assert_eq!(response.reused_chunks, 0);
        assert_eq!(response.stored_bytes, 8);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_without_storage_does_not_report_reuse_between_uploads() {
        let chunk_size = NonZeroUsize::new(4);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };

        let first = ingest_without_storage(
            chunk_size,
            "first.bin",
            Bytes::from_static(b"abcdefgh"),
            None,
        )
        .await;
        assert!(first.is_ok());
        let second = ingest_without_storage(
            chunk_size,
            "second.bin",
            Bytes::from_static(b"abcdefgh"),
            None,
        )
        .await;
        assert!(second.is_ok());
        let Ok(second) = second else {
            return;
        };

        assert_eq!(second.inserted_chunks, 2);
        assert_eq!(second.reused_chunks, 0);
        assert_eq!(second.stored_bytes, 8);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_without_storage_rejects_invalid_file_id() {
        let chunk_size = NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN);
        let result = ingest_without_storage(
            chunk_size,
            "../invalid",
            Bytes::from_static(b"test"),
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_without_storage_with_expected_sha256_verifies_digest() {
        use sha2::{Digest, Sha256};
        let chunk_size = NonZeroUsize::new(4096).unwrap_or(NonZeroUsize::MIN);
        let body = Bytes::from_static(b"hello ingest bench");
        let expected = format!("{:x}", Sha256::digest(&body));
        let result = ingest_without_storage(
            chunk_size,
            "test-file.bin",
            body.clone(),
            Some(expected.as_str()),
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_without_storage_rejects_mismatched_expected_sha256() {
        let chunk_size = NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN);
        let body = Bytes::from_static(b"hello world");
        let result = ingest_without_storage(
            chunk_size,
            "test-file.bin",
            body,
            Some("0000000000000000000000000000000000000000000000000000000000000000"),
        )
        .await;
        assert!(matches!(result, Err(crate::ServerError::ExpectedBodyHashMismatch)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_without_storage_with_parallelism_uses_custom_parallelism() {
        use super::ingest_without_storage_with_parallelism;
        let chunk_size = NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN);
        let max_in_flight = NonZeroUsize::new(1).unwrap_or(NonZeroUsize::MIN);
        let result = ingest_without_storage_with_parallelism(
            chunk_size,
            max_in_flight,
            "asset.bin",
            Bytes::from_static(b"abcdef"),
            None,
        )
        .await;
        assert!(result.is_ok());
        let Ok(response) = result else { return };
        assert_eq!(response.total_bytes, 6);
    }
}
