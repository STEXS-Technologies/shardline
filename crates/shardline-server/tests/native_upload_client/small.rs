//! Real-client coverage of the bounded memory-backed Xorb publication path.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit stock hf_xet small upload and cold-client replay qualification"]
async fn stock_xet_small_upload_preserves_memory_publication_and_cold_replay() -> Result {
    let root = tempfile::tempdir()?;
    let index = shardline_index::LocalIndexStore::new(root.path().to_owned())?;
    small_upload(root.path(), index, &[]).await
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit stock hf_xet memory publication with PostgreSQL/MinIO and replica replacement"]
async fn stock_xet_small_upload_survives_remote_replica_replacement() -> Result {
    let remote = super::super::native_upload_remote::RemoteFixture::start().await?;
    let root = tempfile::tempdir()?;
    let result = small_upload(
        root.path(),
        shardline_index::PostgresIndexStore::new(remote.pool.clone()),
        &remote.environment,
    )
    .await;
    remote.pool.close().await;
    result
}

async fn small_upload<I>(root: &Path, index: I, environment: &[(&str, String)]) -> Result
where
    I: UploadIntentStore,
    I::Error: std::error::Error + 'static,
{
    let working = tempfile::tempdir()?;
    let source = working.path().join("small.bin");
    let mut content = vec![0; 64 * 1024];
    blake3::Hasher::new().finalize_xof().fill(&mut content);
    std::fs::write(&source, &content)?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    let base = format!("http://{address}");
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "small", "retry", None)?;
    let token = LocalHmacProvider::new(KEY)?.mint_token(&TokenClaims::new(
        "shardline",
        "small-native-client",
        TokenScope::Write,
        scope,
        u64::MAX,
    )?)?;
    let mut server = start_with_environment(root, address, None, environment)?;
    ready(&reqwest::Client::new(), &base, &mut server).await?;
    let replacement = if environment.is_empty() {
        None
    } else {
        Some(tempfile::tempdir()?)
    };
    let mut first_hash = None;
    for attempt in ["first", "cold-replay"] {
        if attempt == "cold-replay" {
            server.0.kill()?;
            server.0.wait()?;
            server = start_with_environment(
                replacement
                    .as_ref()
                    .map_or(root, |directory| directory.path()),
                address,
                None,
                environment,
            )?;
            ready(&reqwest::Client::new(), &base, &mut server).await?;
            let home = working.path().join("read-before-retry");
            std::fs::create_dir_all(&home)?;
            let output = shardline_test_support::native_client_output(
                Command::new("python")
                    .args(["-c", DOWNLOAD_AFTER_RESTART])
                    .env(
                        "NATIVE_HASH",
                        first_hash.as_ref().ok_or("missing first upload identity")?,
                    )
                    .env("NATIVE_ENDPOINT", &base)
                    .env("NATIVE_TOKEN", &token)
                    .env("NATIVE_SOURCE", &source)
                    .env("HF_HOME", &home)
                    .env("HF_XET_CACHE", home.join("xet"))
                    .env("NO_PROXY", "127.0.0.1,localhost")
                    .env("no_proxy", "127.0.0.1,localhost"),
                Duration::from_secs(60),
            )?;
            assert!(
                output.status.success(),
                "cold download before retry failed; credential-bearing client output omitted"
            );
            assert_eq!(std::fs::read(home.join("restored.bin"))?, content);
        }
        let home = working.path().join(attempt);
        let mut process = client(&source, &base, &token, &home)?;
        let status = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if let Some(status) = process.0.try_wait()? {
                    return Ok::<_, std::io::Error>(status);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        assert!(
            status.success(),
            "small stock-client transfer failed; credential-bearing client output omitted"
        );
        let result: serde_json::Value =
            serde_json::from_slice(&std::fs::read(home.join("result.json"))?)?;
        assert_eq!(
            result.get("size").and_then(serde_json::Value::as_u64),
            Some(content.len() as u64)
        );
        assert_eq!(std::fs::read(home.join("restored.bin"))?, content);
        let current_hash = result
            .get("hash")
            .and_then(serde_json::Value::as_str)
            .ok_or("stock client omitted content hash")?;
        match &first_hash {
            Some(hash) => assert_eq!(current_hash, hash),
            None => first_hash = Some(current_hash.to_owned()),
        }
    }
    let visible = index.intents_by_state(UploadIntentState::Visible).await?;
    let xorbs: Vec<_> = visible
        .iter()
        .filter(|intent| intent.intent_id().starts_with("xorb-v2-"))
        .collect();
    assert!(!xorbs.is_empty(), "stock client must publish native Xorbs");
    assert!(
        xorbs
            .iter()
            .all(|intent| intent.object_length() <= 256 * 1024),
        "native uploads must exercise memory staging"
    );
    Ok(())
}

const DOWNLOAD_AFTER_RESTART: &str = r#"
import os, pathlib, time
from hf_xet import PyXetDownloadInfo, download_files
source = pathlib.Path(os.environ["NATIVE_SOURCE"])
destination = pathlib.Path(os.environ["HF_HOME"]) / "restored.bin"
info = PyXetDownloadInfo(str(destination), os.environ["NATIVE_HASH"], source.stat().st_size)
download_files([info], os.environ["NATIVE_ENDPOINT"],
               (os.environ["NATIVE_TOKEN"], int(time.time()) + 3600), None, None)
assert destination.read_bytes() == source.read_bytes()
"#;
