//! Stock native uploads remain readable after a committed reply is lost.
use super::*;
use std::sync::atomic::Ordering;

struct Peer(JoinHandle<Result<(), ServerError>>);
impl Drop for Peer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn client(
    runtime: &HubRuntime,
    endpoint: &str,
    directory: &Path,
    phase: &str,
) -> Result<(), TestError> {
    let home = tempfile::tempdir()?;
    let script = r#"
import hashlib, os, pathlib
from huggingface_hub import HfApi, hf_hub_download
api = HfApi(endpoint=os.environ['HF_ENDPOINT'], token=os.environ['HF_TOKEN'])
repo = 'team/cli-model'
root = pathlib.Path(os.environ['FIXTURE'])
def upload(name):
    api.upload_file(repo_id=repo, path_in_repo=name, path_or_fileobj=str(root/name))
def verify(name):
    result = hf_hub_download(repo, name, endpoint=api.endpoint, token=api.token, force_download=True)
    assert hashlib.sha256(pathlib.Path(result).read_bytes()).digest() == hashlib.sha256((root/name).read_bytes()).digest()
phase = os.environ['PHASE']
if phase == 'lost':
    api.create_repo(repo, exist_ok=True)
    try:
        upload('original.bin')
    except Exception:
        pass
    else:
        raise AssertionError('client unexpectedly received the dropped commit reply')
elif phase == 'peer':
    verify('original.bin')
    upload('peer.bin')
elif phase == 'retry':
    before = len(list(api.list_repo_commits(repo)))
    upload('original.bin')
    assert len(list(api.list_repo_commits(repo))) == before + 1
    verify('original.bin')
    verify('peer.bin')
else:
    raise AssertionError('unknown qualification phase')
"#;
    let output = native_output(
        Command::new(hf_python())
            .args(["-c", script])
            .env("HF_ENDPOINT", endpoint)
            .env("HF_TOKEN", &runtime.model_token)
            .env("HF_HOME", home.path())
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env_remove("HF_HUB_DISABLE_XET")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost")
            .env("FIXTURE", directory)
            .env("PHASE", phase),
    )?;
    assert!(
        output.status.success(),
        "native lost-reply phase {phase} failed: {}",
        output.status
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires stock hf_xet and owned PostgreSQL/MinIO"]
async fn native_huggingface_lost_commit_reply_preserves_peer_publication() -> Result<(), TestError>
{
    assert!(command_available("hf"), "native qualification requires hf");
    assert!(
        python_package_available("hf_xet"),
        "native qualification requires hf_xet"
    );
    let _measurements = MEASUREMENTS.lock().await;
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("Docker required")?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let public = format!("http://{}", listener.local_addr()?);
    let runtime =
        start_runtime_with_roles(&[ServerFrontend::Hub], Some(services), Some(&public), false)
            .await?;
    let (_proxy, replies) =
        native_huggingface_proxy::start_losing_commit_reply(listener, &runtime.base_url);
    let peer_listener = TcpListener::bind("127.0.0.1:0").await?;
    let peer_url = format!("http://{}", peer_listener.local_addr()?);
    let config = runtime.maintenance_config.clone();
    let _peer = Peer(tokio::spawn(async move {
        serve_with_listener(config, peer_listener).await
    }));
    wait_for_health(&peer_url).await?;
    let working = tempfile::tempdir()?;
    for (name, seed) in [("original.bin", 73_u64), ("peer.bin", 197_u64)] {
        let mut state = seed;
        let bytes: Vec<u8> = (0..9 * 1024 * 1024)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect();
        write(working.path().join(name), bytes)?;
    }
    let xorbs = read_metric_counter(&runtime, "shardline_xorbs_total").await?;
    let shards = read_metric_counter(&runtime, "shardline_shards_total").await?;
    let regular = read_metric_counter(&runtime, "shardline_hub_api_file_uploads_total").await?;
    client(&runtime, &public, working.path(), "lost")?;
    assert_eq!(
        replies.load(Ordering::SeqCst),
        1,
        "successful commit reply was consumed before disconnection"
    );
    client(&runtime, &peer_url, working.path(), "peer")?;
    client(&runtime, &public, working.path(), "retry")?;
    assert!(read_metric_counter(&runtime, "shardline_xorbs_total").await? >= xorbs + 2);
    assert!(read_metric_counter(&runtime, "shardline_shards_total").await? >= shards + 2);
    assert_eq!(
        read_metric_counter(&runtime, "shardline_hub_api_file_uploads_total").await?,
        regular
    );
    Ok(())
}
