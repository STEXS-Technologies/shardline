mod support;

use std::{env::var, error::Error, path::PathBuf, process::Command};

use support::CliE2eInvariantError;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_reports_sparse_update_metrics() {
    let result = exercise_bench().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "bench e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_reports_focused_scenario_output() {
    let result = exercise_focused_bench().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "focused bench e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_reports_cached_reconstruction_metrics() {
    let result = exercise_cached_bench().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "cached bench e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_reports_configured_backend_target() {
    let result = exercise_configured_bench().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "configured bench e2e failed: {error:?}");
}

async fn exercise_bench() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let root = storage.path().join("bench-root");
    let shardline_bin = shardline_binary()?;

    let output = Command::new(shardline_bin)
        .args([
            "bench",
            "--storage-dir",
            root.to_str()
                .ok_or("benchmark storage path was not valid utf-8")?,
            "--iterations",
            "1",
            "--chunk-size-bytes",
            "4",
            "--base-bytes",
            "12",
            "--mutated-bytes",
            "4",
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "bench failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains("scenario: sparse-update") {
        return Err(CliE2eInvariantError::new("bench did not report scenario").into());
    }
    if !stdout.contains("deployment_target: isolated-local") {
        return Err(CliE2eInvariantError::new("bench did not report deployment target").into());
    }
    if !stdout.contains("metadata_backend: local") {
        return Err(CliE2eInvariantError::new("bench did not report metadata backend").into());
    }
    if !stdout.contains("object_backend: local") {
        return Err(CliE2eInvariantError::new("bench did not report object backend").into());
    }
    if !stdout.contains("inventory_scope: isolated") {
        return Err(CliE2eInvariantError::new("bench did not report inventory scope").into());
    }
    if !stdout.contains("scenario: concurrent-latest-download") {
        return Err(
            CliE2eInvariantError::new("bench did not report concurrent download scenario").into(),
        );
    }
    if !stdout.contains("scenario: concurrent-upload") {
        return Err(
            CliE2eInvariantError::new("bench did not report concurrent upload scenario").into(),
        );
    }
    if !stdout.contains("scenario: cross-repository-upload") {
        return Err(CliE2eInvariantError::new(
            "bench did not report cross-repository upload scenario",
        )
        .into());
    }
    if !stdout.contains("scenario: cached-latest-reconstruction") {
        return Err(CliE2eInvariantError::new(
            "bench did not report cached reconstruction scenario",
        )
        .into());
    }
    if !stdout.contains("concurrency: 4") {
        return Err(CliE2eInvariantError::new("bench did not report default concurrency").into());
    }
    if !stdout.contains("total_initial_inserted_chunks: 3") {
        return Err(
            CliE2eInvariantError::new("bench did not report initial inserted chunks").into(),
        );
    }
    if !stdout.contains("total_sparse_update_inserted_chunks: 1") {
        return Err(CliE2eInvariantError::new(
            "bench did not report sparse update inserted chunks",
        )
        .into());
    }
    if !stdout.contains("total_sparse_update_reused_chunks: 2") {
        return Err(
            CliE2eInvariantError::new("bench did not report sparse update reused chunks").into(),
        );
    }
    if !stdout.contains("total_newly_stored_bytes: 48") {
        return Err(
            CliE2eInvariantError::new("bench did not report total newly stored bytes").into(),
        );
    }
    if !stdout.contains("total_concurrent_upload_inserted_chunks: 4") {
        return Err(CliE2eInvariantError::new(
            "bench did not report concurrent upload inserted chunks",
        )
        .into());
    }
    if !stdout.contains("total_concurrent_uploaded_bytes: 48") {
        return Err(
            CliE2eInvariantError::new("bench did not report concurrent uploaded bytes").into(),
        );
    }
    if !stdout.contains("total_cross_repository_inserted_chunks: 1") {
        return Err(CliE2eInvariantError::new(
            "bench did not report cross-repository inserted chunks",
        )
        .into());
    }
    if !stdout.contains("total_cross_repository_reused_chunks: 2") {
        return Err(CliE2eInvariantError::new(
            "bench did not report cross-repository reused chunks",
        )
        .into());
    }
    if !stdout.contains("total_cross_repository_newly_stored_bytes: 4") {
        return Err(CliE2eInvariantError::new(
            "bench did not report cross-repository newly stored bytes",
        )
        .into());
    }

    Ok(())
}

async fn exercise_configured_bench() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let root = storage.path().join("bench-root");
    let shardline_bin = shardline_binary()?;

    let output = Command::new(shardline_bin)
        .args([
            "bench",
            "--deployment-target",
            "configured",
            "--storage-dir",
            root.to_str()
                .ok_or("benchmark storage path was not valid utf-8")?,
            "--iterations",
            "1",
            "--chunk-size-bytes",
            "4",
            "--base-bytes",
            "12",
            "--mutated-bytes",
            "4",
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "configured bench failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains("deployment_target: configured") {
        return Err(CliE2eInvariantError::new("configured bench did not report target").into());
    }
    if !stdout.contains("metadata_backend: local") {
        return Err(
            CliE2eInvariantError::new("configured bench did not report metadata backend").into(),
        );
    }
    if !stdout.contains("object_backend: local") {
        return Err(
            CliE2eInvariantError::new("configured bench did not report object backend").into(),
        );
    }
    if !stdout.contains("inventory_scope: isolated") {
        return Err(
            CliE2eInvariantError::new("configured bench did not report inventory scope").into(),
        );
    }

    Ok(())
}

async fn exercise_cached_bench() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let root = storage.path().join("bench-root");
    let shardline_bin = shardline_binary()?;

    let output = Command::new(shardline_bin)
        .args([
            "bench",
            "--storage-dir",
            root.to_str()
                .ok_or("benchmark storage path was not valid utf-8")?,
            "--scenario",
            "cached-latest-reconstruction",
            "--iterations",
            "1",
            "--chunk-size-bytes",
            "4",
            "--base-bytes",
            "12",
            "--mutated-bytes",
            "4",
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "cached bench failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains("scenario: cached-latest-reconstruction") {
        return Err(
            CliE2eInvariantError::new("cached bench did not report selected scenario").into(),
        );
    }
    if !stdout.contains("cache_hit_iterations: 1") {
        return Err(CliE2eInvariantError::new("cached bench did not report cache hit").into());
    }
    if !stdout.contains("total_cached_reconstruction_response_bytes: ") {
        return Err(CliE2eInvariantError::new(
            "cached bench did not report cached reconstruction payload bytes",
        )
        .into());
    }
    if !stdout.contains("average_cached_latest_reconstruction_hot_micros: ") {
        return Err(
            CliE2eInvariantError::new("cached bench did not report hot cache latency").into(),
        );
    }

    Ok(())
}

async fn exercise_focused_bench() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let root = storage.path().join("bench-root");
    let shardline_bin = shardline_binary()?;

    let output = Command::new(shardline_bin)
        .args([
            "bench",
            "--storage-dir",
            root.to_str()
                .ok_or("benchmark storage path was not valid utf-8")?,
            "--scenario",
            "cross-repository-upload",
            "--iterations",
            "1",
            "--chunk-size-bytes",
            "4",
            "--base-bytes",
            "12",
            "--mutated-bytes",
            "4",
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "focused bench failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains("scenario: cross-repository-upload") {
        return Err(
            CliE2eInvariantError::new("focused bench did not report selected scenario").into(),
        );
    }
    if !stdout.contains("total_cross_repository_reused_chunks: 2") {
        return Err(CliE2eInvariantError::new(
            "focused bench did not report cross-repository reuse",
        )
        .into());
    }
    if !stdout.contains("total_initial_inserted_chunks: 0") {
        return Err(
            CliE2eInvariantError::new("focused bench did not zero untimed setup metrics").into(),
        );
    }

    Ok(())
}

fn shardline_binary() -> Result<PathBuf, Box<dyn Error>> {
    if let Ok(path) = var("CARGO_BIN_EXE_shardline") {
        return Ok(PathBuf::from(path));
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let Some(workspace_root) = manifest_dir.ancestors().nth(2) else {
        return Err(CliE2eInvariantError::new("workspace root could not be resolved").into());
    };
    let binary = workspace_root
        .join("target")
        .join("debug")
        .join("shardline");
    if !binary.is_file() {
        return Err(CliE2eInvariantError::new(format!(
            "shardline binary was not built at {}",
            binary.display()
        ))
        .into());
    }

    Ok(binary)
}
