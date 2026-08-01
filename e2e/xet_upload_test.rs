//! Upload a file via Xet protocol and report dedup metrics.
//! Usage: xet_upload_test <server-url> <file-path> [file-name]
//!
//! Prints JSON with: file_hash, total_bytes, new_bytes, deduped_chunks, xorb_count
use std::sync::Arc;
use xet_data::processing::{FileUploadSession, Sha256Policy, configurations::TranslatorConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <server-url> <file-path> [file-name]", args[0]);
        std::process::exit(1);
    }
    let base_url = args[1].trim_end_matches('/');
    let file_path = &args[2];
    let file_name = args.get(3).cloned().unwrap_or_else(|| {
        std::path::Path::new(file_path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string()
    });

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let client_workdir = tempfile::tempdir()?;
        let translator = Arc::new(TranslatorConfig::test_server_config(
            base_url,
            client_workdir.path(),
        )?);

        let bytes = std::fs::read(file_path)?;
        eprintln!("File: {file_path} ({name}): {size} bytes",
            name = file_name,
            size = bytes.len()
        );

        let upload_session = FileUploadSession::new(translator).await
            .map_err(|e| { eprintln!("FileUploadSession::new error: {e:#}"); e })?;
        let (_clean_id, mut cleaner) = upload_session.start_clean(
            Some(Arc::<str>::from(file_name.as_str())),
            Some(u64::try_from(bytes.len())?),
            Sha256Policy::Compute,
        )?;
        cleaner.add_data(&bytes).await?;
        let (file_info, cleaner_metrics) = cleaner.finish().await?;
        let session_metrics = upload_session.finalize().await?;

        let result = serde_json::json!({
            "file_hash": file_info.hash().to_string(),
            "total_bytes": cleaner_metrics.total_bytes,
            "total_chunks": cleaner_metrics.total_chunks,
            "deduped_bytes": cleaner_metrics.deduped_bytes,
            "new_bytes": cleaner_metrics.new_bytes,
            "deduped_chunks": cleaner_metrics.deduped_chunks,
            "new_chunks": cleaner_metrics.new_chunks,
            "xorb_bytes_uploaded": session_metrics.xorb_bytes_uploaded + cleaner_metrics.xorb_bytes_uploaded,
            "shard_bytes_uploaded": session_metrics.shard_bytes_uploaded + cleaner_metrics.shard_bytes_uploaded,
            "total_bytes_uploaded": session_metrics.total_bytes_uploaded + cleaner_metrics.total_bytes_uploaded,
        });
        println!("{result}");
        Ok(())
    })
}
