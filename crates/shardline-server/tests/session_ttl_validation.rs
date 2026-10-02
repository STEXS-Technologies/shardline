//! Durable-session TTL validation before backend initialization.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::unreachable)]
use shardline_server::{ServerConfig, ServerConfigError, ServerError, ServerFrontend, app};
use std::num::{NonZeroU64, NonZeroUsize};

fn config(root: &std::path::Path, frontend: ServerFrontend) -> ServerConfig {
    ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://localhost".into(),
        root.into(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends([frontend])
    .unwrap()
    .with_reconstruction_cache_disabled()
}

fn with_ttl(config: ServerConfig, frontend: ServerFrontend, seconds: u64) -> ServerConfig {
    let ttl = NonZeroU64::new(seconds).unwrap();
    match frontend {
        ServerFrontend::Oci => config.with_oci_upload_session_ttl_seconds(ttl),
        ServerFrontend::S3 => config.with_s3_upload_session_ttl_seconds(ttl).unwrap(),
        ServerFrontend::Lfs => config.with_lfs_patch_ttl_seconds(ttl).unwrap(),
        ServerFrontend::Xet | ServerFrontend::BazelHttp | ServerFrontend::Hub => unreachable!(),
    }
}

#[tokio::test]
async fn unrepresentable_durable_ttls_fail_before_backend_initialization() {
    let temp = tempfile::tempdir().unwrap();
    for frontend in [ServerFrontend::Oci, ServerFrontend::S3, ServerFrontend::Lfs] {
        let root = temp.path().join(format!("{frontend:?}"));
        let config = with_ttl(config(&root, frontend), frontend, u64::MAX)
            .with_index_postgres_url("postgres://unused@127.0.0.1:1/unreachable".into())
            .unwrap();
        assert!(matches!(
            config.validate_runtime_requirements(),
            Err(ServerConfigError::SessionTtlOutOfRange {
                seconds: u64::MAX,
                ..
            })
        ));
        let error = app::router(config)
            .await
            .expect_err("invalid TTL must reject router");
        assert!(matches!(
            error,
            ServerError::Config(ServerConfigError::SessionTtlOutOfRange {
                seconds: u64::MAX,
                ..
            })
        ));
        assert!(
            !root.exists(),
            "validation must precede filesystem/backend initialization"
        );
    }
}

#[tokio::test]
async fn local_large_ttls_and_normal_durable_ttls_remain_valid() {
    let temp = tempfile::tempdir().unwrap();
    for frontend in [ServerFrontend::Oci, ServerFrontend::S3, ServerFrontend::Lfs] {
        let local = with_ttl(config(temp.path(), frontend), frontend, u64::MAX);
        assert!(local.validate_runtime_requirements().is_ok());
        assert!(app::router(local).await.is_ok());
        let durable = with_ttl(config(temp.path(), frontend), frontend, 3600)
            .with_index_postgres_url("postgres://unused@127.0.0.1:1/unreachable".into())
            .unwrap();
        assert!(durable.validate_runtime_requirements().is_ok());
    }
    // An unused frontend's TTL does not constrain this server's session storage.
    let unused = config(temp.path(), ServerFrontend::Xet)
        .with_oci_upload_session_ttl_seconds(NonZeroU64::new(u64::MAX).unwrap())
        .with_s3_upload_session_ttl_seconds(NonZeroU64::new(u64::MAX).unwrap())
        .unwrap()
        .with_lfs_patch_ttl_seconds(NonZeroU64::new(u64::MAX).unwrap())
        .unwrap()
        .with_index_postgres_url("postgres://unused@127.0.0.1:1/unreachable".into())
        .unwrap();
    assert!(unused.validate_runtime_requirements().is_ok());
}
