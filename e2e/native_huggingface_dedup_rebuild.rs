//! Stock cold-cache deduplication after derived metadata loss and rebuild.

use super::*;

pub(super) async fn recover_legacy_descriptors(runtime: &HubRuntime) -> Result<(), TestError> {
    // No client runs during this fixture mutation. Remove only the derived
    // descriptor index; immutable Xorbs, shards and authoritative files remain.
    let removed = if let Some(url) = &runtime.postgres_url {
        let pool = sqlx::PgPool::connect(url).await?;
        let removed = sqlx::query("DELETE FROM shardline_xet_xorbs")
            .execute(&pool)
            .await?
            .rows_affected();
        pool.close().await;
        removed
    } else {
        let connection =
            rusqlite::Connection::open(runtime._storage.path().join("metadata.sqlite3"))?;
        u64::try_from(connection.execute("DELETE FROM shardline_xet_xorbs", [])?)?
    };
    assert!(removed > 0, "fixture must remove actual native descriptors");
    let report = shardline_server::run_index_rebuild(runtime.maintenance_config.clone()).await?;
    assert!(report.is_clean(), "legacy descriptor rebuild must be clean");
    Ok(())
}

async fn qualify(remote: bool, split: bool) -> Result<(), TestError> {
    assert!(
        command_available("hf"),
        "cold-client qualification requires hf"
    );
    assert!(
        python_package_available("hf_xet"),
        "cold-client qualification requires hf_xet"
    );
    let _measurements = MEASUREMENTS.lock().await;
    let services = if remote {
        Some(
            shardline_test_support::DockerLocalStack::builder()
                .with_postgres()
                .with_minio()
                .start()?
                .ok_or("cold-client qualification requires Docker")?,
        )
    } else {
        None
    };
    let ingress = if split {
        Some(TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?)
    } else {
        None
    };
    let public_base = ingress
        .as_ref()
        .map(|listener| {
            listener
                .local_addr()
                .map(|address| format!("http://{address}"))
        })
        .transpose()?;
    let mut runtime = start_runtime_with_roles(
        &[ServerFrontend::Hub],
        services,
        public_base.as_deref(),
        split,
    )
    .await?;
    let _proxy = if let Some(listener) = ingress {
        let transfer = &runtime.transfer.as_ref().ok_or("transfer role missing")?.0;
        let proxy = native_huggingface_proxy::start(listener, &runtime.base_url, transfer);
        runtime.base_url = public_base.ok_or("public ingress missing")?;
        Some(proxy)
    } else {
        None
    };
    reqwest::Client::new()
        .post(format!("{}/api/repos/create", runtime.base_url))
        .bearer_auth(&runtime.model_token)
        .json(&serde_json::json!({"name":"team/cli-model", "type":"model", "private":true}))
        .send()
        .await?
        .error_for_status()?;
    let working = tempfile::tempdir()?;
    verify_cold_client_chunk_dedup_with_rebuild(&runtime, "team/cli-model", working.path(), true)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real hf and hf_xet clients"]
async fn native_huggingface_cold_dedup_after_local_legacy_rebuild() -> Result<(), TestError> {
    qualify(false, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires owned Docker PostgreSQL/MinIO and real hf/hf_xet clients"]
async fn native_huggingface_cold_dedup_after_remote_legacy_rebuild() -> Result<(), TestError> {
    qualify(true, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real hf and hf_xet clients"]
async fn native_huggingface_cold_dedup_after_local_legacy_rebuild_split_roles()
-> Result<(), TestError> {
    qualify(false, true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires owned Docker PostgreSQL/MinIO and real hf/hf_xet clients"]
async fn native_huggingface_cold_dedup_after_remote_legacy_rebuild_split_roles()
-> Result<(), TestError> {
    qualify(true, true).await
}
