//! Upload a file via Xet protocol and measure CDC dedup.
//! Starts its own server (same as e2e tests), uploads original and modified file.
#![allow(unused_imports)]

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{net::TcpListener, spawn, time::sleep};
use shardline_server::{ServerConfig, serve_with_listener};
use xet_data::processing::{FileUploadSession, Sha256Policy, configurations::TranslatorConfig};

const CHUNK_SIZE: usize = 65_536;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <original-file> <modified-file>", args[0]);
        std::process::exit(1);
    }
    let orig_path = &args[1];
    let mod_path = &args[2];

    let storage = tempfile::tempdir()?;
    let client_workdir = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");

    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(CHUNK_SIZE).ok_or("chunk size")?,
    );
    let server = spawn(async move { serve_with_listener(config, listener).await });

    // Wait for server ready
    let client = reqwest::Client::new();
    for _ in 0..20 {
        if client.get(format!("{base_url}/healthz")).send().await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }

    let translator = Arc::new(TranslatorConfig::test_server_config(
        &base_url,
        client_workdir.path(),
    )?);

    // Upload original
    let orig_bytes = std::fs::read(orig_path)?;
    let file_name = std::path::Path::new(orig_path)
        .file_name().unwrap_or_default().to_string_lossy().to_string();
    eprintln!("Original: {} bytes ({})", orig_bytes.len(), file_name);

    let (orig_info, orig_metrics) = upload(&translator, &file_name, &orig_bytes).await?;
    eprintln!("  chunks: total={}, new={}, deduped={}", 
        orig_metrics.total_chunks, orig_metrics.new_chunks, orig_metrics.deduped_chunks);
    eprintln!("  bytes: total={}, new={}, deduped={}", 
        orig_metrics.total_bytes, orig_metrics.new_bytes, orig_metrics.deduped_bytes);

    // Upload modified
    let mod_bytes = std::fs::read(mod_path)?;
    let mod_name = std::path::Path::new(mod_path)
        .file_name().unwrap_or_default().to_string_lossy().to_string();
    eprintln!("\nModified: {} bytes ({})", mod_bytes.len(), mod_name);

    let (mod_info, mod_metrics) = upload(&translator, &mod_name, &mod_bytes).await?;
    eprintln!("  chunks: total={}, new={}, deduped={}", 
        mod_metrics.total_chunks, mod_metrics.new_chunks, mod_metrics.deduped_chunks);
    eprintln!("  bytes: total={}, new={}, deduped={}", 
        mod_metrics.total_bytes, mod_metrics.new_bytes, mod_metrics.deduped_bytes);

    // Results
    println!("\n=== Xet CDC DEDUP RESULTS ===");
    println!("Original upload:  {} chunks ({} bytes new of {} total)",
        orig_metrics.total_chunks, orig_metrics.new_bytes, orig_metrics.total_bytes);
    println!("Modified upload:  {} chunks total, {} new, {} deduped (saved)",
        mod_metrics.total_chunks, mod_metrics.new_chunks, mod_metrics.deduped_chunks);
    let reuse_pct = if mod_metrics.total_chunks > 0 {
        (mod_metrics.deduped_chunks as f64 / mod_metrics.total_chunks as f64) * 100.0
    } else {
        0.0
    };
    println!("Chunk reuse rate: {:.1}%", reuse_pct);
    println!("Bytes saved via dedup: {} of {} ({:.1}%)",
        mod_metrics.deduped_bytes, mod_metrics.total_bytes,
        if mod_metrics.total_bytes > 0 {
            (mod_metrics.deduped_bytes as f64 / mod_metrics.total_bytes as f64) * 100.0
        } else { 0.0 }
    );

    server.abort();
    Ok(())
}

async fn upload(
    translator: &Arc<TranslatorConfig>,
    name: &str,
    bytes: &[u8],
) -> Result<(xet_data::processing::XetFileInfo, xet_data::deduplication::DeduplicationMetrics), Box<dyn std::error::Error>> {
    // Set up a custom HTTP client that captures error bodies
    let upload_session = FileUploadSession::new(translator.clone()).await
        .map_err(|e| {
            eprintln!("FileUploadSession::new error: {e:#}");
            e
        })?;
    let (_clean_id, mut cleaner) = upload_session.start_clean(
        Some(Arc::<str>::from(name)),
        Some(u64::try_from(bytes.len())?),
        Sha256Policy::Compute,
    )?;
    cleaner.add_data(bytes).await?;
    let (file_info, cleaner_metrics) = cleaner.finish().await?;
    let session_metrics = upload_session.finalize().await?;

    let mut metrics = cleaner_metrics;
    metrics.total_bytes_uploaded = session_metrics.total_bytes_uploaded;
    Ok((file_info, metrics))
}
