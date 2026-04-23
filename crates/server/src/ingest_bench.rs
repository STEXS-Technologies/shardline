use std::num::NonZeroUsize;

use axum::body::Bytes;

use crate::{
    ServerError, config::default_upload_max_in_flight_chunks, model::UploadFileResponse,
    object_store::ServerObjectStore, upload_ingest::FileUploadIngestor,
    validation::validate_identifier,
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
}
