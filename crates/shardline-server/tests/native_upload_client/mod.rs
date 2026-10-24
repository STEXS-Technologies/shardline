//! Stock-client recovery after a large native upload loses its server process.

use super::*;
use shardline_index::{UploadIntentState, UploadIntentStore};
use std::io::{BufWriter, Write};

pub(super) fn configure_remote(config: ServerConfig) -> Result<ServerConfig> {
    let Ok(url) = std::env::var("NATIVE_PROCESS_POSTGRES_URL") else {
        return Ok(config);
    };
    let config = config.with_index_postgres_url(url)?;
    let s3 = shardline_storage::S3ObjectStoreConfig::new(
        std::env::var("NATIVE_PROCESS_S3_BUCKET")?,
        std::env::var("NATIVE_PROCESS_S3_REGION")?,
    )
    .with_endpoint(Some(std::env::var("NATIVE_PROCESS_S3_ENDPOINT")?))
    .with_key_prefix(Some(&std::env::var("NATIVE_PROCESS_S3_PREFIX")?))
    .with_credentials(
        Some(shardline_protocol::SecretString::new(std::env::var(
            "NATIVE_PROCESS_S3_ACCESS",
        )?)),
        Some(shardline_protocol::SecretString::new(std::env::var(
            "NATIVE_PROCESS_S3_SECRET",
        )?)),
        None,
    )
    .with_allow_http(true);
    Ok(config.with_object_storage(shardline_server::ObjectStorageAdapter::S3, Some(s3)))
}

const CLIENT: &str = r#"
import hashlib, json, os, pathlib, time
from hf_xet import XetSession

source = pathlib.Path(os.environ["NATIVE_SOURCE"])
session = XetSession()
auth = dict(endpoint=os.environ["NATIVE_ENDPOINT"], token=os.environ["NATIVE_TOKEN"],
            token_expiry_unix_secs=int(time.time()) + 3600)
with session.new_upload_commit(**auth) as commit:
    upload = commit.start_upload_file(str(source))
info = upload.result().xet_info
destination = source.with_suffix(".restored")
group = session.new_file_download_group(**auth)
group.start_download_file(info, str(destination))
group.wait_to_finish()
def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as content:
        for block in iter(lambda: content.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()
assert destination.stat().st_size == source.stat().st_size
assert digest(destination) == digest(source)
pathlib.Path(os.environ["NATIVE_RESULT"]).write_text(json.dumps(
    dict(hash=info.hash, size=info.file_size, sha256=digest(destination))))
"#;

fn fixture(path: &Path) -> Result {
    let mut output = BufWriter::new(std::fs::File::create(path)?);
    let mut block = vec![0_u8; 1024 * 1024];
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    for _ in 0..512 {
        for word in block.as_chunks_mut::<8>().0 {
            state ^= state.wrapping_shl(13);
            state ^= state.wrapping_shr(7);
            state ^= state.wrapping_shl(17);
            word.copy_from_slice(&state.to_le_bytes());
        }
        output.write_all(&block)?;
    }
    output.flush()?;
    Ok(())
}

fn client(source: &Path, base: &str, token: &str, home: &Path) -> Result<Process> {
    std::fs::create_dir_all(home)?;
    Ok(Process(
        Command::new("python")
            .args(["-c", CLIENT])
            .env("NATIVE_SOURCE", source)
            .env("NATIVE_ENDPOINT", base)
            .env("NATIVE_TOKEN", token)
            .env("NATIVE_RESULT", home.join("result.json"))
            .env("HF_HOME", home)
            .env("HF_XET_CACHE", home.join("xet"))
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(std::fs::File::create(
                home.join("client.stderr"),
            )?))
            .spawn()?,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit 512 MiB real hf_xet process interruption qualification"]
async fn stock_xet_large_upload_recovers_after_server_sigkill() -> Result {
    let root = tempfile::tempdir()?;
    let index = shardline_index::LocalIndexStore::new(root.path().to_owned())?;
    interrupted_client(root.path(), index, &[]).await
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit 512 MiB stock-client SIGKILL qualification with PostgreSQL and MinIO"]
async fn stock_xet_large_upload_recovers_with_postgres_and_minio() -> Result {
    let remote = super::native_upload_remote::RemoteFixture::start().await?;
    let root = tempfile::tempdir()?;
    let result = interrupted_client(
        root.path(),
        shardline_index::PostgresIndexStore::new(remote.pool.clone()),
        &remote.environment,
    )
    .await;
    remote.pool.close().await;
    result
}

async fn interrupted_client<I>(root: &Path, index: I, environment: &[(&str, String)]) -> Result
where
    I: UploadIntentStore,
    I::Error: std::error::Error + 'static,
{
    use std::os::unix::process::ExitStatusExt;
    let available = Command::new("python")
        .args(["-c", "from hf_xet import XetSession"])
        .status()?;
    assert!(
        available.success(),
        "stock hf_xet with XetSession is required"
    );
    let working = tempfile::tempdir()?;
    let source = working.path().join("512-mib.bin");
    fixture(&source)?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    let base = format!("http://{address}");
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "large", "retry", None)?;
    let token = LocalHmacProvider::new(KEY)?.mint_token(&TokenClaims::new(
        "shardline",
        "large-native-client",
        TokenScope::Write,
        scope,
        u64::MAX,
    )?)?;
    let http = reqwest::Client::new();
    let mut server = start_with_environment(root, address, None, environment)?;
    ready(&http, &base, &mut server).await?;
    let mut uploading = client(&source, &base, &token, &working.path().join("interrupted"))?;
    let interrupted = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if let Some(status) = uploading.0.try_wait()? {
                let error =
                    std::fs::read_to_string(working.path().join("interrupted/client.stderr"))?;
                return Err(format!("client exited before interruption: {status}: {error}").into());
            }
            if let Some(intent) = index
                .intents_by_state(UploadIntentState::Storing)
                .await?
                .into_iter()
                .find(|intent| intent.intent_id().starts_with("xorb-v2-"))
            {
                return Ok::<_, Box<dyn std::error::Error>>(intent);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    server.0.kill()?;
    assert_eq!(server.0.wait()?.signal(), Some(9));
    uploading.0.kill()?;
    uploading.0.wait()?;
    drop(uploading);
    drop(server);

    // Remote takeover must not depend on the failed replica's local files.
    let takeover_root = if environment.is_empty() {
        None
    } else {
        Some(tempfile::tempdir()?)
    };
    let retry_address = if takeover_root.is_some() {
        let takeover_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        takeover_listener.local_addr()?
    } else {
        address
    };
    let retry_base = format!("http://{retry_address}");
    let mut restarted = start_with_environment(
        takeover_root
            .as_ref()
            .map_or(root, |directory| directory.path()),
        retry_address,
        None,
        environment,
    )?;
    ready(&http, &retry_base, &mut restarted).await?;
    let retry_home = working.path().join("retry");
    let mut retry = client(&source, &retry_base, &token, &retry_home)?;
    let status = tokio::time::timeout(Duration::from_secs(240), async {
        loop {
            if let Some(status) = retry.0.try_wait()? {
                return Ok::<_, std::io::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    assert!(
        status.success(),
        "stock retry failed: {}",
        std::fs::read_to_string(retry_home.join("client.stderr"))?
    );
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(retry_home.join("result.json"))?)?;
    assert_eq!(
        result.get("size").and_then(serde_json::Value::as_u64),
        Some(512 * 1024 * 1024)
    );
    let original = index
        .intent_by_id(interrupted.intent_id())
        .await?
        .ok_or("lost intent history")?;
    assert!(matches!(
        original.state(),
        UploadIntentState::Failed | UploadIntentState::Visible
    ));
    assert!(
        index
            .intents_by_state(UploadIntentState::Visible)
            .await?
            .iter()
            .any(|intent| intent.object_key() == interrupted.object_key()
                && intent.object_hash() == interrupted.object_hash()
                && intent.object_length() == interrupted.object_length()),
        "the exact interrupted immutable object must become visible on retry"
    );
    Ok(())
}
