//! Real CAS publication and upstream downloads after equivalent encoded retries.

use super::*;

type DownloadError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable PostgreSQL and MinIO"]
async fn native_xorb_remote_alternate_encoding_round_trip() -> Result {
    for compressed_first in [false, true] {
        let remote = RemoteFixture::start().await?;
        let root = tempfile::tempdir()?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        drop(listener);
        let endpoint = format!("http://{address}");
        let mut server = start_with_environment(root.path(), address, None, &remote.environment)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        ready(&client, &endpoint, &mut server).await?;
        let scope =
            RepositoryScope::new(RepositoryProvider::Generic, "encoding", "roundtrip", None)?;
        let token = LocalHmacProvider::new(KEY)?.mint_token(&TokenClaims::new(
            "shardline",
            "encoding-client",
            TokenScope::Write,
            scope,
            u64::MAX,
        )?)?;
        let (compressed, plain, hash) =
            shardline_server::test_fixtures::alternate_xorb_encodings()?;
        let inputs = if compressed_first {
            [compressed, plain]
        } else {
            [plain, compressed]
        };
        for body in inputs {
            let response = client
                .post(format!("{endpoint}/v1/xorbs/default/{hash}"))
                .bearer_auth(&token)
                .body(body)
                .send()
                .await?;
            assert!(
                response.status().is_success(),
                "encoded upload: {}",
                response.status()
            );
        }
        let data = vec![b'a'; 128 * 1024];
        let (shard, file_hash) =
            shardline_server::test_fixtures::single_file_shard(&[(&data, &hash)]);
        let published = client
            .post(format!("{endpoint}/v1/shards"))
            .bearer_auth(&token)
            .body(shard)
            .send()
            .await?;
        assert!(
            published.status().is_success(),
            "shard publication: {}",
            published.status()
        );
        let downloaded =
            tokio::task::spawn_blocking(move || -> std::result::Result<Vec<u8>, DownloadError> {
                let runtime = xet_runtime::core::XetRuntime::new()?;
                runtime.bridge_sync(async move {
                    let temporary = tempfile::tempdir()?;
                    let mut config =
                        xet_data::processing::configurations::TranslatorConfig::test_server_config(
                            &endpoint,
                            temporary.path(),
                        )?;
                    config.session.auth = xet_client::cas_client::auth::AuthConfig::maybe_new(
                        Some(token),
                        None,
                        None,
                    );
                    let session = xet_data::processing::FileDownloadSession::new(
                        std::sync::Arc::new(config),
                        None,
                    )
                    .await?;
                    let output = temporary.path().join("restored");
                    session
                        .download_file(
                            &xet_data::processing::XetFileInfo::new(file_hash, 128 * 1024),
                            &output,
                        )
                        .await?;
                    Ok::<_, DownloadError>(std::fs::read(output)?)
                })?
            })
            .await?
            .map_err(|error| -> Box<dyn std::error::Error> { error })?;
        assert_eq!(downloaded, data);
        drop(server);
        remote.pool.close().await;
    }
    Ok(())
}
