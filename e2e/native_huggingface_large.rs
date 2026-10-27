//! Explicit large-file qualification; default smoke tests remain lightweight.

use super::*;
use std::io::{BufReader, BufWriter, Read, Write};

fn write_unique_fixture(path: &Path, size_mib: u64) -> Result<String, TestError> {
    let mut file = BufWriter::new(std::fs::File::create(path)?);
    let mut block = vec![0_u8; 1024 * 1024];
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut digest = Sha256::new();
    for _ in 0..size_mib {
        for word in block.as_chunks_mut::<8>().0 {
            state ^= state.wrapping_shl(13);
            state ^= state.wrapping_shr(7);
            state ^= state.wrapping_shl(17);
            word.copy_from_slice(&state.to_le_bytes());
        }
        file.write_all(&block)?;
        digest.update(&block);
    }
    file.flush()?;
    Ok(hex::encode(digest.finalize()))
}

fn peak_rss_kib() -> Result<u64, TestError> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
        .ok_or_else(|| "Linux peak RSS is missing".into())
}

#[derive(Clone, Copy)]
struct ProcessIo {
    read_bytes: u64,
    write_bytes: u64,
    written_characters: u64,
}

impl ProcessIo {
    fn read() -> Result<Self, TestError> {
        // Linux includes waited-for children in /proc/self/io. Stock clients
        // are subprocesses of this test, so these are combined fixture/client
        // counters, unlike the separately running server in paired benchmarks.
        let counters = std::fs::read_to_string("/proc/self/io")?;
        let value = |name: &str| -> Result<u64, TestError> {
            counters
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .ok_or_else(|| format!("missing process I/O counter {name}"))?
                .trim()
                .parse()
                .map_err(Into::into)
        };
        Ok(Self {
            read_bytes: value("read_bytes:")?,
            write_bytes: value("write_bytes:")?,
            written_characters: value("wchar:")?,
        })
    }

    fn report_since(self, before: Self, stage: &str) {
        eprintln!(
            "{stage}: server/test plus waited-for child process-accounted read_bytes={}, write_bytes={}, wchar={}; includes reaped stock clients, not isolated server/scratch writes or device wear",
            self.read_bytes.saturating_sub(before.read_bytes),
            self.write_bytes.saturating_sub(before.write_bytes),
            self.written_characters
                .saturating_sub(before.written_characters)
        );
    }
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit 1 GiB stock-client and server memory qualification"]
async fn native_huggingface_one_gib_upload_and_download_remain_bounded() -> Result<(), TestError> {
    qualify_large_transfer(start_hub_runtime().await?).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit 1 GiB stock-client qualification with Docker PostgreSQL and MinIO"]
async fn native_huggingface_one_gib_with_postgres_and_minio() -> Result<(), TestError> {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("large remote qualification requires Docker")?;
    let runtime = start_runtime_with_services(&[ServerFrontend::Hub], Some(services)).await?;
    qualify_large_transfer(runtime).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit 1 GiB stock-client qualification with separate API and transfer roles"]
async fn native_huggingface_one_gib_with_split_roles() -> Result<(), TestError> {
    qualify_split_large_transfer(None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit 1 GiB split-role qualification with Docker PostgreSQL and MinIO"]
async fn native_huggingface_one_gib_with_split_roles_postgres_and_minio() -> Result<(), TestError> {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("large split-role qualification requires Docker")?;
    assert!(
        services.s3_raw_config(None).is_some(),
        "remote split-role qualification requires S3 objects"
    );
    qualify_split_large_transfer(Some(services)).await
}

async fn qualify_split_large_transfer(
    services: Option<shardline_test_support::DockerLocalStack>,
) -> Result<(), TestError> {
    qualify_split_large_transfer_with_size(services, 1024).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit 2 GiB split-role qualification with Docker PostgreSQL and MinIO"]
async fn native_huggingface_two_gib_with_split_roles_postgres_and_minio() -> Result<(), TestError> {
    // Cross both the existing 1 GiB fixture and the signed 32-bit byte limit.
    // Keep the same integrity, cancellation and absolute RSS checks below.
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("large split-role qualification requires Docker")?;
    assert!(
        services.s3_raw_config(None).is_some(),
        "remote split-role qualification requires S3 objects"
    );
    qualify_split_large_transfer_with_size(Some(services), 2048).await
}

async fn qualify_split_large_transfer_with_size(
    services: Option<shardline_test_support::DockerLocalStack>,
    size_mib: u64,
) -> Result<(), TestError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let public = format!("http://{}", listener.local_addr()?);
    let mut runtime =
        start_runtime_with_roles(&[ServerFrontend::Hub], services, Some(&public), true).await?;
    let transfer = &runtime.transfer.as_ref().ok_or("missing transfer role")?.0;
    let (_proxy, observations) =
        native_huggingface_proxy::start_observing_ranges(listener, &runtime.base_url, transfer);
    runtime.base_url = public;
    qualify_large_transfer_with_size(runtime, size_mib).await?;
    assert!(
        observations
            .lock()
            .map_err(|_| "range observations poisoned")?
            .iter()
            .any(|(path, status)| path.starts_with("/transfer/xorb/")
                && *status == reqwest::StatusCode::PARTIAL_CONTENT),
        "cold stock-client download must traverse the Shardline transfer role"
    );
    Ok(())
}

async fn qualify_large_transfer(runtime: HubRuntime) -> Result<(), TestError> {
    qualify_large_transfer_with_size(runtime, 1024).await
}

async fn qualify_large_transfer_with_size(
    runtime: HubRuntime,
    size_mib: u64,
) -> Result<(), TestError> {
    let expected_length = size_mib
        .checked_mul(1024 * 1024)
        .ok_or("fixture size overflow")?;
    let logical_mib = f64::from(u32::try_from(size_mib)?);
    let _subscriber = tracing_subscriber::fmt()
        .with_env_filter("shardline_server=warn,shardline_server::app::operational=debug,shardline_xet_adapter=debug")
        .with_test_writer()
        .try_init();
    assert!(command_available("hf"), "hf is required");
    assert!(python_package_available("hf_xet"), "hf_xet is required");
    let home = tempfile::tempdir()?;
    let working = tempfile::tempdir()?;
    let source = working.path().join("large.bin");
    let digest = write_unique_fixture(&source, size_mib)?;
    let before_rss = peak_rss_kib()?;
    let before_io = ProcessIo::read()?;
    let shards = read_metric_counter(&runtime, "shardline_shards_total").await?;
    let xorbs = read_metric_counter(&runtime, "shardline_xorbs_total").await?;
    let basic = read_metric_counter(&runtime, "shardline_hub_api_file_uploads_total").await?;
    let upload_started = std::time::Instant::now();
    run_hf(
        &runtime,
        home.path(),
        [
            "upload",
            "team/cli-model",
            path_as_str(&source)?,
            "large.bin",
            "--token",
            &runtime.model_token,
        ],
    )?;
    let upload_seconds = upload_started.elapsed().as_secs_f64();
    eprintln!(
        "{size_mib} MiB stock-client upload: {upload_seconds:.3} s, {:.3} MiB/s; includes client processing and Hub commit completion",
        logical_mib / upload_seconds
    );
    eprintln!("{size_mib} MiB upload peak RSS: {} KiB", peak_rss_kib()?);
    let uploaded_io = ProcessIo::read()?;
    uploaded_io.report_since(before_io, &format!("{size_mib} MiB upload"));
    assert!(read_metric_counter(&runtime, "shardline_shards_total").await? > shards);
    assert!(read_metric_counter(&runtime, "shardline_xorbs_total").await? > xorbs);
    assert_eq!(
        read_metric_counter(&runtime, "shardline_hub_api_file_uploads_total").await?,
        basic
    );
    let download = working.path().join("download");
    let cold_home = tempfile::tempdir()?;
    let native_reads = native_download_counter(&runtime).await?;
    let download_started = std::time::Instant::now();
    run_hf(
        &runtime,
        cold_home.path(),
        [
            "download",
            "team/cli-model",
            "large.bin",
            "--local-dir",
            path_as_str(&download)?,
            "--force-download",
            "--token",
            &runtime.model_token,
        ],
    )?;
    let download_seconds = download_started.elapsed().as_secs_f64();
    eprintln!(
        "{size_mib} MiB cold stock-client download: {download_seconds:.3} s, {:.3} MiB/s; includes client reconstruction and completed file output",
        logical_mib / download_seconds
    );
    assert!(
        native_download_counter(&runtime).await? > native_reads,
        "cold stock-client download must use the native Xorb transfer path"
    );
    let downloaded = download.join("large.bin");
    eprintln!(
        "{size_mib} MiB cold download peak RSS: {} KiB",
        peak_rss_kib()?
    );
    let downloaded_io = ProcessIo::read()?;
    downloaded_io.report_since(uploaded_io, &format!("{size_mib} MiB cold native download"));
    assert_eq!(std::fs::metadata(&downloaded)?.len(), expected_length);
    let mut original = BufReader::new(std::fs::File::open(&source)?);
    let mut restored = BufReader::new(std::fs::File::open(&downloaded)?);
    let mut expected = vec![0_u8; 1024 * 1024];
    let mut actual = vec![0_u8; 1024 * 1024];
    loop {
        let count = original.read(&mut expected)?;
        if count == 0 {
            break;
        }
        restored.read_exact(actual.get_mut(..count).ok_or("comparison buffer")?)?;
        assert_eq!(expected.get(..count), actual.get(..count));
    }
    // Stop consuming a large response before its digest can complete, then
    // require subsequent readers to finish with verified bytes. This exercises
    // client abandonment at real file size, in addition to the focused
    // single-permit cancellation tests that prove exact admission cleanup.
    cancel_large_http_download(&runtime, &digest, expected_length).await?;
    // Two slow readers exercise overlapping verified spools and response
    // ownership. Each verifies the complete body, rather than header latency.
    let slow_readers_io = ProcessIo::read()?;
    tokio::try_join!(
        verify_slow_http_download(&runtime, &digest, expected_length),
        verify_slow_http_download(&runtime, &digest, expected_length),
    )?;
    ProcessIo::read()?.report_since(
        slow_readers_io,
        &format!("two concurrent {size_mib} MiB slow HTTP readers"),
    );
    let peak = peak_rss_kib()?;
    let increase = peak.saturating_sub(before_rss);
    eprintln!(
        "{size_mib} MiB native Hub transfer with two slow HTTP readers: server/test peak RSS {peak} KiB, increase {increase} KiB; client RSS excluded"
    );
    assert!(
        peak < 384 * 1024,
        "server/test peak memory exceeded the qualification ceiling: {peak} KiB"
    );
    Ok(())
}

async fn native_download_counter(runtime: &HubRuntime) -> Result<u64, TestError> {
    let base = runtime
        .transfer
        .as_ref()
        .map_or(runtime.base_url.as_str(), |(url, _)| url.as_str());
    read_metric_counter_at(base, "shardline_xet_xorb_downloads_total").await
}

async fn verify_slow_http_download(
    runtime: &HubRuntime,
    digest: &str,
    expected_length: u64,
) -> Result<(), TestError> {
    // Also exercise the ordinary HTTP path that previously buffered whole files.
    let mut response = large_http_response(runtime, digest, expected_length).await?;
    let mut observed = Sha256::new();
    let mut length = 0_u64;
    while let Some(frame) = response.chunk().await? {
        length = length
            .checked_add(u64::try_from(frame.len())?)
            .ok_or("download length overflow")?;
        observed.update(&frame);
        // Deliberately apply consumer backpressure without buffering the body.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert_eq!(length, expected_length);
    assert_eq!(hex::encode(observed.finalize()), digest);
    Ok(())
}

async fn cancel_large_http_download(
    runtime: &HubRuntime,
    digest: &str,
    expected_length: u64,
) -> Result<(), TestError> {
    let mut response = large_http_response(runtime, digest, expected_length).await?;
    let frame = response
        .chunk()
        .await?
        .ok_or("missing large response body")?;
    assert!(!frame.is_empty());
    assert!(u64::try_from(frame.len())? < expected_length);
    drop(response);
    Ok(())
}

async fn large_http_response(
    runtime: &HubRuntime,
    digest: &str,
    expected_length: u64,
) -> Result<reqwest::Response, TestError> {
    let response = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(180))
        .redirect(reqwest::redirect::Policy::none())
        .build()?
        .get(format!("{}/v1/hub/xet-files/{digest}", runtime.base_url))
        .bearer_auth(&runtime.model_token)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(response.content_length(), Some(expected_length));
    Ok(response)
}
