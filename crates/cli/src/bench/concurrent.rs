use std::num::NonZeroUsize;

use shardline_server::{BenchmarkBackend, ServerError, ingest_without_storage_with_parallelism};
use tokio::task::JoinSet;

use super::{
    BenchRuntimeError, ConcurrentIngestUploadCase, ConcurrentUploadCase,
    TimedConcurrentIngestUpload, capture_process_cpu_micros, checked_add_u64, duration_micros,
    host_utilization_per_mille, namespaced_file_id, ratio_per_mille,
};

pub(crate) async fn run_concurrent_latest_downloads(
    backend: &BenchmarkBackend,
    file_id: &str,
    expected: &[u8],
    concurrency: u32,
) -> Result<(u64, u64), BenchRuntimeError> {
    let started = std::time::Instant::now();
    let mut tasks = JoinSet::new();
    for _worker in 0..concurrency {
        let backend = backend.clone();
        let file_id = file_id.to_owned();
        tasks.spawn(async move { backend.download_file(&file_id, None, None).await });
    }

    let mut downloaded_bytes = 0_u64;
    while let Some(joined) = tasks.join_next().await {
        let bytes = joined
            .map_err(BenchRuntimeError::from)?
            .map_err(BenchRuntimeError::from)?;
        if bytes.as_slice() != expected {
            return Err(BenchRuntimeError::ConcurrentLatestDownloadMismatch);
        }
        downloaded_bytes = checked_add_u64(downloaded_bytes, u64::try_from(bytes.len())?)?;
    }

    Ok((duration_micros(started.elapsed())?, downloaded_bytes))
}

pub(crate) async fn run_concurrent_uploads(
    backend: &BenchmarkBackend,
    namespace: &str,
    cases: &[ConcurrentUploadCase],
) -> Result<(u64, u64, u64, u64, u64), BenchRuntimeError> {
    let started = std::time::Instant::now();
    let mut tasks = JoinSet::new();
    for case in cases {
        let backend = backend.clone();
        let file_id = namespaced_file_id(namespace, &case.file_id);
        let expected_bytes = case.expected_bytes.clone();
        tasks.spawn(async move {
            let response = backend
                .upload_file(&file_id, expected_bytes.clone(), None)
                .await?;
            Ok::<_, ServerError>((file_id, expected_bytes, response))
        });
    }

    let mut uploaded_bytes = 0_u64;
    let mut newly_stored_bytes = 0_u64;
    let mut inserted_chunks = 0_u64;
    let mut reused_chunks = 0_u64;
    let mut completed = Vec::with_capacity(cases.len());
    while let Some(joined) = tasks.join_next().await {
        let (file_id, expected_bytes, response) = joined
            .map_err(BenchRuntimeError::from)?
            .map_err(BenchRuntimeError::from)?;
        uploaded_bytes = checked_add_u64(uploaded_bytes, response.total_bytes)?;
        newly_stored_bytes = checked_add_u64(newly_stored_bytes, response.stored_bytes)?;
        inserted_chunks = checked_add_u64(inserted_chunks, response.inserted_chunks)?;
        reused_chunks = checked_add_u64(reused_chunks, response.reused_chunks)?;
        completed.push((file_id, expected_bytes));
    }
    let upload_micros = duration_micros(started.elapsed())?;

    for (file_id, expected_bytes) in completed {
        let bytes = backend.download_file(&file_id, None, None).await?;
        if bytes.as_slice() != expected_bytes.as_ref() {
            return Err(BenchRuntimeError::ConcurrentUploadVerificationMismatch);
        }
    }

    Ok((
        upload_micros,
        uploaded_bytes,
        newly_stored_bytes,
        inserted_chunks,
        reused_chunks,
    ))
}

pub(crate) async fn run_concurrent_ingest_uploads(
    chunk_size: NonZeroUsize,
    upload_max_in_flight_chunks: NonZeroUsize,
    cases: &[ConcurrentIngestUploadCase],
    available_parallelism: u64,
) -> Result<TimedConcurrentIngestUpload, BenchRuntimeError> {
    let started = std::time::Instant::now();
    let process_cpu_started = capture_process_cpu_micros();
    let mut tasks = JoinSet::new();
    for case in cases {
        let file_id = case.file_id.clone();
        let body = case.body.clone();
        tasks.spawn(async move {
            ingest_without_storage_with_parallelism(
                chunk_size,
                upload_max_in_flight_chunks,
                &file_id,
                body,
                None,
            )
            .await
        });
    }

    let mut uploaded_bytes = 0_u64;
    let mut inserted_chunks = 0_u64;
    while let Some(joined) = tasks.join_next().await {
        let response = joined
            .map_err(BenchRuntimeError::from)?
            .map_err(BenchRuntimeError::from)?;
        uploaded_bytes = checked_add_u64(uploaded_bytes, response.total_bytes)?;
        inserted_chunks = checked_add_u64(inserted_chunks, response.inserted_chunks)?;
    }
    let elapsed_micros = duration_micros(started.elapsed())?;
    let process_cpu_micros = capture_process_cpu_micros().saturating_sub(process_cpu_started);

    Ok(TimedConcurrentIngestUpload {
        elapsed_micros,
        uploaded_bytes,
        inserted_chunks,
        process_cpu_micros,
        process_cpu_cores_per_mille: ratio_per_mille(process_cpu_micros, elapsed_micros),
        process_host_utilization_per_mille: host_utilization_per_mille(
            process_cpu_micros,
            elapsed_micros,
            available_parallelism,
        ),
    })
}
