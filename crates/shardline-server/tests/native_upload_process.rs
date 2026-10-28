//! Real process termination at native-Xorb durability boundaries.
#![cfg(unix)]
#![allow(clippy::panic_in_result_fn)]

use shardline_cas::UploadLifecycleBoundary;
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, serve_with_listener};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use shardline_xet_core::{
    merklehash::{compute_data_hash, xorb_hash},
    xorb_object::{
        CompressionScheme, XorbObject,
        xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};
use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const KEY: &[u8] = b"native-process-test-signing-key-32";
const CHILD_ROOT: &str = "SHARDLINE_TEST_NATIVE_PROCESS_ROOT";

mod native_upload_client;
#[cfg(feature = "docker")]
mod native_upload_remote;

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

fn start(root: &Path, address: SocketAddr, cut: Option<(&str, &str)>) -> Result<Process> {
    start_with_environment(root, address, cut, &[])
}

fn start_with_environment(
    root: &Path,
    address: SocketAddr,
    cut: Option<(&str, &str)>,
    environment: &[(&str, String)],
) -> Result<Process> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["--exact", "native_upload_child", "--ignored", "--nocapture"])
        .env(CHILD_ROOT, root)
        .env("SHARDLINE_TEST_NATIVE_PROCESS_ADDRESS", address.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    // Every child uses the backend explicitly supplied by its owning fixture.
    // Parent-shell settings must not turn a local control into a remote run.
    for name in native_upload_client::REMOTE_FIELDS {
        command.env_remove(name);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    if let Some((boundary, intent)) = cut {
        command
            .env("SHARDLINE_TEST_NATIVE_PROCESS_CUT", boundary)
            .env("SHARDLINE_TEST_NATIVE_PROCESS_INTENT", intent);
    } else {
        command
            .env_remove("SHARDLINE_TEST_NATIVE_PROCESS_CUT")
            .env_remove("SHARDLINE_TEST_NATIVE_PROCESS_INTENT");
    }
    Ok(Process(command.spawn()?))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "subprocess fixture; launched by the parent process tests"]
async fn native_upload_child() -> Result {
    let root = std::env::var_os(CHILD_ROOT).ok_or("subprocess fixture requires its parent")?;
    let root = std::path::PathBuf::from(root);
    let address: SocketAddr = std::env::var("SHARDLINE_TEST_NATIVE_PROCESS_ADDRESS")?.parse()?;
    let pause = if let Ok(cut) = std::env::var("SHARDLINE_TEST_NATIVE_PROCESS_CUT") {
        let boundary = match cut.as_str() {
            "created" => UploadLifecycleBoundary::AfterIntentCreated,
            "storing" => UploadLifecycleBoundary::AfterStoring,
            "object" => UploadLifecycleBoundary::AfterObjectWork,
            "stored" => UploadLifecycleBoundary::AfterStored,
            "committed" => UploadLifecycleBoundary::AfterMetadataCommitted,
            "visible" => UploadLifecycleBoundary::AfterVisible,
            _ => return Err("invalid process cut".into()),
        };
        let intent = std::env::var("SHARDLINE_TEST_NATIVE_PROCESS_INTENT")?;
        Some(
            if intent == "stock-client-xorb" || intent == "stock-client-shard" {
                shardline_cas::arm_upload_process_pause_for_prefix(
                    if intent == "stock-client-xorb" {
                        "xorb-v2-"
                    } else {
                        "shard-v2-"
                    }
                    .to_owned(),
                    boundary,
                    root.join("reached"),
                )
            } else {
                shardline_cas::arm_upload_process_pause(intent, boundary, root.join("reached"))
            },
        )
    } else {
        None
    };
    let listener = tokio::net::TcpListener::bind(address).await?;
    let config = ServerConfig::new(
        address,
        format!("http://{address}"),
        root,
        NonZeroUsize::new(65536).ok_or("chunk size")?,
    )
    .with_token_signing_key(KEY.to_vec())?;
    let config = native_upload_client::configure_remote(config)?;
    let result = serve_with_listener(config, listener).await;
    drop(pause);
    result?;
    Ok(())
}

async fn ready(client: &reqwest::Client, base: &str, child: &mut Process) -> Result {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(status) = child.0.try_wait()? {
                return Err(format!("server exited: {status}").into());
            }
            if let Ok(response) = client.get(format!("{base}/healthz")).send().await
                && response.status().is_success()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_xorb_retry_survives_sigkill_at_every_durable_boundary() -> Result {
    qualify_native_retry(1, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_native_members_survive_sigkill_at_every_durable_boundary() -> Result {
    qualify_native_retry(128, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interleaved_native_members_survive_sigkill_at_every_durable_boundary() -> Result {
    qualify_native_retry(128, true).await
}

async fn qualify_native_retry(repetitions: usize, interleaved: bool) -> Result {
    use shardline_index::UploadIntentStore;
    use std::os::unix::process::ExitStatusExt;
    let content = b"verified native process interruption payload";
    let chunk = compute_data_hash(content);
    let alternate: Vec<_> = content.iter().map(|byte| byte ^ 0x20).collect();
    let alternate_hash = compute_data_hash(&alternate);
    let content_length = u64::try_from(content.len())?;
    let members: Vec<_> = (0..repetitions)
        .map(|index| {
            (
                if interleaved && index % 2 == 1 {
                    alternate_hash
                } else {
                    chunk
                },
                content_length,
            )
        })
        .collect();
    let identity = xorb_hash(&members);
    let hash = shardline_server::test_fixtures::xet_hash_hex(&identity);
    let boundaries = members
        .iter()
        .enumerate()
        .map(|(index, (member_hash, _))| {
            Ok((
                *member_hash,
                content_length
                    .checked_mul(u64::try_from(
                        index.checked_add(1).ok_or("chunk count overflow")?,
                    )?)
                    .ok_or("chunk boundary overflow")?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let payload: Vec<_> = members
        .iter()
        .flat_map(|(member_hash, _)| {
            if *member_hash == alternate_hash {
                alternate.as_slice()
            } else {
                content.as_slice()
            }
        })
        .copied()
        .collect();
    let bytes = serialized_xorb_object_from_components(
        &identity,
        payload,
        boundaries,
        CompressionScheme::None,
    )?
    .serialized_data;
    let intent = format!("xorb-v2-{hash}-{}", bytes.len());
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "process", "retry", None)?;
    let token = LocalHmacProvider::new(KEY)?.mint_token(&TokenClaims::new(
        "shardline",
        "process-test",
        TokenScope::Write,
        scope,
        u64::MAX,
    )?)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    for cut in [
        "created",
        "storing",
        "object",
        "stored",
        "committed",
        "visible",
    ] {
        let root = tempfile::tempdir()?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        drop(listener);
        let base = format!("http://{address}");
        let mut child = start(root.path(), address, Some((cut, &intent)))?;
        ready(&client, &base, &mut child).await?;
        let request = client
            .post(format!("{base}/v1/xorbs/default/{hash}"))
            .bearer_auth(&token)
            .body(bytes.clone());
        let mut upload = tokio::spawn(async move { request.send().await });
        tokio::time::timeout(Duration::from_secs(30), async {
            while !root.path().join("reached").exists() {
                assert!(child.0.try_wait()?.is_none(), "process exited before {cut}");
                if upload.is_finished() {
                    let response = (&mut upload).await??;
                    return Err(format!(
                        "upload returned {} before {cut} pause ({repetitions} members)",
                        response.status()
                    )
                    .into());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await
        .map_err(|error| {
            format!("upload did not reach {cut} ({repetitions} members): {error}")
        })??;
        child.0.kill()?;
        assert_eq!(child.0.wait()?.signal(), Some(9));
        upload.abort();
        let _upload_result = upload.await;
        drop(child);
        let mut restarted = start(root.path(), address, None)?;
        ready(&client, &base, &mut restarted).await?;
        let response = client
            .post(format!("{base}/v1/xorbs/default/{hash}"))
            .bearer_auth(&token)
            .body(bytes.clone())
            .send()
            .await?;
        let status = response.status();
        assert!(
            status.is_success(),
            "retry at {cut}: {status} {}",
            response.text().await?
        );
        let stored = std::fs::read(
            root.path()
                .join(format!("chunks/xorbs/default/{}/{hash}.xorb", &hash[..2])),
        )?;
        assert_eq!(stored, bytes, "persisted Xorb at {cut}");
        // Native ingestion persists the verified container, not a redundant
        // decoded copy of each member. Validate every member and the aggregate
        // commitment after restart; shard publication establishes indexed reads
        // separately and has its own process-recovery qualification.
        assert!(
            XorbObject::validate_xorb_object(&mut std::io::Cursor::new(&stored), &identity,)?
                .is_some(),
            "recovered Xorb integrity at {cut}"
        );
        for member_hash in [chunk, alternate_hash]
            .into_iter()
            .take(if interleaved { 2 } else { 1 })
        {
            let member_hex = shardline_server::test_fixtures::xet_hash_hex(&member_hash);
            assert!(
                !root
                    .path()
                    .join(format!("chunks/{}/{member_hex}", &member_hex[..2],))
                    .exists(),
                "unexpected redundant member copy at {cut}"
            );
        }
        let index = shardline_index::LocalIndexStore::new(root.path().to_owned())?;
        let original = index
            .intent_by_id(&intent)
            .await?
            .ok_or("missing original intent")?;
        assert!(matches!(
            original.state(),
            shardline_index::UploadIntentState::Visible
                | shardline_index::UploadIntentState::Failed
        ));
        drop(restarted);
    }
    Ok(())
}
