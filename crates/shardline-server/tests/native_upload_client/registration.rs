//! The publication following admitted parallel file-identity verification.
use super::*;

const CUTS: [&str; 6] = [
    "created",
    "storing",
    "object",
    "stored",
    "committed",
    "visible",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit 512 MiB stock-client shard registration interruption qualification"]
async fn stock_xet_large_registration_recovers_after_server_sigkill() -> Result {
    for cut in CUTS {
        let root = tempfile::tempdir()?;
        let index = shardline_index::LocalIndexStore::new(root.path().to_owned())?;
        interrupted_client_for_prefix(root.path(), index, &[], cut, None, "shard-v2-").await?;
    }
    Ok(())
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit 512 MiB stock-client shard registration takeover with PostgreSQL and MinIO"]
async fn stock_xet_large_registration_recovers_with_postgres_and_minio() -> Result {
    for cut in CUTS {
        let remote = super::super::native_upload_remote::RemoteFixture::start().await?;
        let root = tempfile::tempdir()?;
        let result = interrupted_client_for_prefix(
            root.path(),
            shardline_index::PostgresIndexStore::new(remote.pool.clone()),
            &remote.environment,
            cut,
            None,
            "shard-v2-",
        )
        .await;
        remote.pool.close().await;
        result?;
    }
    Ok(())
}
