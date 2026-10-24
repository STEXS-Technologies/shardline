//! Explicit large-file qualification; default smoke tests remain lightweight.

use super::*;
use std::io::{BufReader, BufWriter, Read, Write};

fn write_unique_fixture(path: &Path) -> Result<String, TestError> {
    let mut file = BufWriter::new(std::fs::File::create(path)?);
    let mut block = vec![0_u8; 1024 * 1024];
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut digest = Sha256::new();
    for _ in 0..1024 {
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
            "{stage}: server/test process-accounted read_bytes={}, write_bytes={}, wchar={}; these are not isolated scratch writes or device wear",
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

async fn qualify_large_transfer(runtime: HubRuntime) -> Result<(), TestError> {
    let _subscriber = tracing_subscriber::fmt()
        .with_env_filter("shardline_server=warn,shardline_server::app::operational=debug,shardline_xet_adapter=debug")
        .with_test_writer()
        .try_init();
    assert!(command_available("hf"), "hf is required");
    assert!(python_package_available("hf_xet"), "hf_xet is required");
    let home = tempfile::tempdir()?;
    let working = tempfile::tempdir()?;
    let source = working.path().join("one-gib.bin");
    let digest = write_unique_fixture(&source)?;
    let before_rss = peak_rss_kib()?;
    let before_io = ProcessIo::read()?;
    let shards = read_metric_counter(&runtime, "shardline_shards_total").await?;
    let xorbs = read_metric_counter(&runtime, "shardline_xorbs_total").await?;
    let basic = read_metric_counter(&runtime, "shardline_hub_api_file_uploads_total").await?;
    run_hf(
        &runtime,
        home.path(),
        [
            "upload",
            "team/cli-model",
            path_as_str(&source)?,
            "one-gib.bin",
            "--token",
            &runtime.model_token,
        ],
    )?;
    eprintln!("1 GiB upload peak RSS: {} KiB", peak_rss_kib()?);
    let uploaded_io = ProcessIo::read()?;
    uploaded_io.report_since(before_io, "1 GiB upload");
    assert!(read_metric_counter(&runtime, "shardline_shards_total").await? > shards);
    assert!(read_metric_counter(&runtime, "shardline_xorbs_total").await? > xorbs);
    assert_eq!(
        read_metric_counter(&runtime, "shardline_hub_api_file_uploads_total").await?,
        basic
    );
    let download = working.path().join("download");
    let cold_home = tempfile::tempdir()?;
    let native_reads = read_metric_counter(&runtime, "shardline_xet_xorb_downloads_total").await?;
    run_hf(
        &runtime,
        cold_home.path(),
        [
            "download",
            "team/cli-model",
            "one-gib.bin",
            "--local-dir",
            path_as_str(&download)?,
            "--force-download",
            "--token",
            &runtime.model_token,
        ],
    )?;
    assert!(
        read_metric_counter(&runtime, "shardline_xet_xorb_downloads_total").await? > native_reads,
        "cold stock-client download must use the native Xorb transfer path"
    );
    let downloaded = download.join("one-gib.bin");
    eprintln!("1 GiB cold download peak RSS: {} KiB", peak_rss_kib()?);
    let downloaded_io = ProcessIo::read()?;
    downloaded_io.report_since(uploaded_io, "1 GiB cold native download");
    assert_eq!(
        std::fs::metadata(&downloaded)?.len(),
        1024_u64 * 1024 * 1024
    );
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
    // Two slow readers exercise overlapping verified spools and response
    // ownership. Each verifies the complete body, rather than header latency.
    let slow_readers_io = ProcessIo::read()?;
    tokio::try_join!(
        verify_slow_http_download(&runtime, &digest),
        verify_slow_http_download(&runtime, &digest),
    )?;
    ProcessIo::read()?.report_since(slow_readers_io, "two concurrent 1 GiB slow HTTP readers");
    let peak = peak_rss_kib()?;
    let increase = peak.saturating_sub(before_rss);
    eprintln!(
        "1 GiB native Hub transfer with two slow HTTP readers: server/test peak RSS {peak} KiB, increase {increase} KiB; client RSS excluded"
    );
    assert!(
        peak < 384 * 1024,
        "server/test peak memory exceeded the qualification ceiling: {peak} KiB"
    );
    Ok(())
}

async fn verify_slow_http_download(runtime: &HubRuntime, digest: &str) -> Result<(), TestError> {
    // Also exercise the ordinary HTTP path that previously buffered whole files.
    let response = reqwest::Client::new()
        .get(format!("{}/v1/hub/xet-files/{digest}", runtime.base_url))
        .bearer_auth(&runtime.model_token)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(response.content_length(), Some(1024_u64 * 1024 * 1024));
    let mut response = response;
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
    assert_eq!(length, 1024_u64 * 1024 * 1024);
    assert_eq!(hex::encode(observed.finalize()), digest);
    Ok(())
}
