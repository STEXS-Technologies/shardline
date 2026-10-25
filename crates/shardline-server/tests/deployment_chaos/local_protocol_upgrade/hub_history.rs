//! Stock Hub clients against actual predecessor/current processes and persisted history.
use super::*;

const CLIENT: &str = r#"
import io, json, os, pathlib, sys, urllib.request, urllib.error
from huggingface_hub import HfApi, hf_hub_download
stage = 'setup'
def safe_failure(kind, error, traceback):
    print('HUB_HISTORY_STAGE=' + stage, flush=True)
    print('HUB_HISTORY_FAILURE=' + kind.__name__, flush=True)
    print('HUB_HISTORY_STATUS=' + str(getattr(getattr(error, 'response', None), 'status_code', 0)), flush=True)
    for category in ('etag', 'commit', 'force_download', 'metadata', 'revision'):
        if category in str(error).lower():
            print('HUB_HISTORY_CATEGORY=' + category, flush=True)
    cause = error.__cause__
    for depth in range(3):
        if cause is None:
            break
        print('HUB_HISTORY_CAUSE=' + type(cause).__name__, flush=True)
        print('HUB_HISTORY_CAUSE_STATUS=' + str(getattr(getattr(cause, 'response', None), 'status_code', 0)), flush=True)
        cause = cause.__cause__
sys.excepthook = safe_failure
api = HfApi(endpoint=os.environ['HF_ENDPOINT'], token=os.environ['HF_TOKEN'])
state_path = pathlib.Path(os.environ['HUB_HISTORY_STATE'])
phase = os.environ['HUB_HISTORY_PHASE']
state = json.loads(state_path.read_text()) if state_path.exists() else []
def revisions():
    request = urllib.request.Request(os.environ['HF_ENDPOINT'] + '/api/models/drill/drill/revisions',
        headers={'Authorization': 'Bearer ' + os.environ['HF_TOKEN']})
    with urllib.request.urlopen(request, timeout=30) as response:
        return {entry['sha'] for entry in json.load(response)['revisions']}
for index, (public, text, legacy) in enumerate(state):
    stage = 'historical_read_' + str(index)
    revision = legacy if phase == 'rollback' else public
    path = hf_hub_download('drill/drill', 'history.txt', revision=revision,
        endpoint=os.environ['HF_ENDPOINT'], token=os.environ['HF_TOKEN'], force_download=True)
    assert pathlib.Path(path).read_bytes() == text.encode()
    if phase == 'rollback' and public != legacy:
        # New Git IDs are not a capability of the immutable predecessor binary.
        # Qualify its explicit boundary as well as its legacy-ID data reads.
        request = urllib.request.Request(os.environ['HF_ENDPOINT'] + '/drill/drill/resolve/' + public + '/history.txt',
            headers={'Authorization': 'Bearer ' + os.environ['HF_TOKEN']})
        try:
            urllib.request.urlopen(request, timeout=30)
        except urllib.error.HTTPError as error:
            assert error.code == 404
        else:
            raise AssertionError('predecessor public-ID behavior changed')
if state:
    stage = 'head_read'
    path = hf_hub_download('drill/drill', 'history.txt',
        endpoint=os.environ['HF_ENDPOINT'], token=os.environ['HF_TOKEN'], force_download=True)
    assert pathlib.Path(path).read_bytes() == state[-1][1].encode()
if phase != 'verify':
    if phase == 'released':
        stage = 'create'
        api.create_repo('drill/drill', repo_type='model', private=True)
    text = 'acknowledged by ' + phase
    stage = 'prior_revisions'
    previous = revisions()
    stage = 'upload'
    commit = api.upload_file(path_or_fileobj=io.BytesIO(text.encode()),
        path_in_repo='history.txt', repo_id='drill/drill', commit_message=phase)
    stage = 'info'
    # The predecessor omits model_info.sha; retain its acknowledged commit ID.
    revision = commit.oid
    if phase == 'upgraded':
        assert api.model_info('drill/drill').sha == revision
    assert revision and revision not in [entry[0] for entry in state]
    stage = 'legacy_revision'
    added = revisions() - previous
    assert len(added) == 1
    legacy = added.pop()
    state.append([revision, text, legacy])
    state_path.write_text(json.dumps(state))
    stage = 'published_read'
    path = hf_hub_download('drill/drill', 'history.txt', revision=revision,
        endpoint=os.environ['HF_ENDPOINT'], token=os.environ['HF_TOKEN'], force_download=True)
    assert pathlib.Path(path).read_bytes() == text.encode()
if phase == 'upgraded':
    stage = 'native_upload'
    native_source = state_path.parent / 'native.bin'
    native_source.write_bytes(bytes(range(256)) * (9 * 4096))
    native_commit = api.upload_file(path_or_fileobj=native_source,
        path_in_repo='native.bin', repo_id='drill/drill', commit_message='native history')
    state_path.with_suffix('.native').write_text(native_commit.oid)
if phase in ('upgraded', 'verify'):
    stage = 'native_history'
    native_revision = state_path.with_suffix('.native').read_text()
    request = urllib.request.Request(os.environ['HF_ENDPOINT'] + '/drill/drill/resolve/' + native_revision + '/native.bin',
        headers={'Authorization': 'Bearer ' + os.environ['HF_TOKEN']}, method='HEAD')
    with urllib.request.urlopen(request, timeout=30) as response:
        assert response.headers.get('X-Xet-Hash')
        assert int(response.headers['Content-Length']) == 9 * 1024 * 1024
    path = hf_hub_download('drill/drill', 'native.bin', revision=native_revision,
        endpoint=os.environ['HF_ENDPOINT'], token=os.environ['HF_TOKEN'], force_download=True)
    assert pathlib.Path(path).read_bytes() == bytes(range(256)) * (9 * 4096)
print('HUB_HISTORY_OK=' + phase, flush=True)
"#;

fn client_available() -> bool {
    let available = shardline_test_support::native_client_output(
        Command::new("python3").args(["-c", "import huggingface_hub, hf_xet"]),
        Duration::from_secs(10),
    )
    .is_ok_and(|output| output.status.success());
    shardline_test_support::require_native_client("python3 + huggingface_hub + hf_xet", available)
}

async fn client(server: &LocalServer, state: &Path, token: &str, phase: &str) {
    let base = server.base.clone();
    let state = state.to_path_buf();
    let token = token.to_owned();
    let phase = phase.to_owned();
    tokio::task::spawn_blocking(move || {
        let home = TempDir::new().unwrap();
        let output = shardline_test_support::native_client_output(
            Command::new("python3")
                .args(["-c", CLIENT])
                .env("HF_ENDPOINT", base)
                .env("HF_TOKEN", token)
                .env("HF_HOME", home.path())
                .env("HF_HUB_DISABLE_XET", "0")
                .env("HF_HUB_DISABLE_TELEMETRY", "1")
                .env("NO_PROXY", "127.0.0.1,localhost")
                .env("no_proxy", "127.0.0.1,localhost")
                .env("HUB_HISTORY_STATE", state)
                .env("HUB_HISTORY_PHASE", &phase),
            Duration::from_secs(120),
        )
        .unwrap();
        let diagnostic: Vec<_> = String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| {
                line.starts_with("HUB_HISTORY_")
                    && line.len() < 128
                    && line
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'='))
            })
            .map(str::to_owned)
            .collect();
        assert!(
            output.status.success(),
            "stock Hub history phase {phase}: {} {diagnostic:?}",
            output.status
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires stock huggingface_hub and actual predecessor/current binaries"]
async fn released_local_hub_history_survives_upgrade_and_rollback() {
    upgrade(None, &[]).await;
}

#[cfg(feature = "docker")]
#[tokio::test]
#[ignore = "requires stock huggingface_hub, Docker and actual predecessor/current binaries"]
async fn released_remote_hub_history_survives_upgrade_and_rollback() {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()
        .expect("Hub history migration services")
        .expect("Hub history migration requires Docker");
    let postgres = services.postgres_url().expect("PostgreSQL fixture");
    let prefix = services.unique_s3_key_prefix("hub-history-predecessor");
    let s3 = services
        .s3_raw_config(Some(&prefix))
        .expect("MinIO fixture");
    let storage_env = vec![
        ("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3".to_owned()),
        ("SHARDLINE_S3_BUCKET", s3.bucket),
        ("SHARDLINE_S3_REGION", s3.region),
        ("SHARDLINE_S3_ENDPOINT", s3.endpoint.unwrap()),
        ("SHARDLINE_S3_ACCESS_KEY_ID", s3.access_key.unwrap()),
        ("SHARDLINE_S3_SECRET_ACCESS_KEY", s3.secret_key.unwrap()),
        ("SHARDLINE_S3_KEY_PREFIX", prefix),
        ("SHARDLINE_S3_ALLOW_HTTP", "true".to_owned()),
    ];
    upgrade(Some(&postgres), &storage_env).await;
}

async fn upgrade(postgres: Option<&str>, storage_env: &[(&str, String)]) {
    if !client_available() {
        return;
    }
    let Some(old) = resolve_n_minus_one_binary("Hub history upgrade and rollback") else {
        return;
    };
    let current = resolve_shardline_binary().expect("current binary required");
    let mut storage_env = storage_env.to_vec();
    let mut webhook_key = [0_u8; 16];
    getrandom::fill(&mut webhook_key).unwrap();
    storage_env.push(("SHARDLINE_HUB_WEBHOOK_SECRET_KEY", hex::encode(webhook_key)));
    storage_env.push((
        "SHARDLINE_TOKEN_SIGNING_KEY",
        std::str::from_utf8(TEST_SIGNING_KEY).unwrap().to_owned(),
    ));
    let storage_env = &storage_env;
    let root = TempDir::new().unwrap();
    let state_root = TempDir::new().unwrap();
    let state = state_root.path().join("history.json");
    let token = mint_token("drill", "drill", TokenScope::Write);
    migrate_metadata(&old, root.path(), postgres).await;
    let mut predecessor =
        LocalServer::spawn_with_storage(&old, root.path(), postgres, "hub", storage_env);
    predecessor.ready().await;
    client(&predecessor, &state, &token, "released").await;
    drop(predecessor);
    migrate_metadata(&current, root.path(), postgres).await;
    let mut candidate =
        LocalServer::spawn_with_storage(&current, root.path(), postgres, "hub", storage_env);
    candidate.ready().await;
    client(&candidate, &state, &token, "upgraded").await;
    maintenance::gc_before_rollback(&current, root.path(), postgres, storage_env, "hub").await;
    drop(candidate);
    let mut rollback =
        LocalServer::spawn_with_storage(&old, root.path(), postgres, "hub", storage_env);
    rollback.ready().await;
    client(&rollback, &state, &token, "rollback").await;
    drop(rollback);
    let mut restart =
        LocalServer::spawn_with_storage(&current, root.path(), postgres, "hub", storage_env);
    restart.ready().await;
    client(&restart, &state, &token, "verify").await;
    maintenance::gc_before_rollback(&current, root.path(), postgres, storage_env, "hub").await;
    client(&restart, &state, &token, "verify").await;
}
