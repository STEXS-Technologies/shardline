//! Stock SDK upload selection at the inline/native-Xet boundary.
use super::{Command, HubRuntime, TestError, hf_python};

pub(super) fn roundtrip(runtime: &HubRuntime, repo: &str) -> Result<(), TestError> {
    let socket = std::net::TcpListener::bind("127.0.0.1:0")?;
    socket.set_nonblocking(true)?;
    let endpoint = format!("http://{}", socket.local_addr()?);
    let (_proxy, rejected) = super::native_huggingface_proxy::start_rejecting_lfs(
        tokio::net::TcpListener::from_std(socket)?,
        &runtime.base_url,
    );
    let home = tempfile::tempdir()?;
    let script = r#"
import hashlib, os, pathlib, tempfile, urllib.request
from huggingface_hub import HfApi, hf_hub_download
endpoint, token, repo = os.environ["HF_ENDPOINT"], os.environ["HF_TOKEN"], os.environ["HF_REPO"]
api = HfApi(endpoint=endpoint, token=token)
def counter(name):
    text = urllib.request.urlopen(endpoint + "/metrics", timeout=30).read().decode()
    return sum(float(line.split()[-1]) for line in text.splitlines()
        if line.startswith(name + " ") or line.startswith(name + "{"))
for ordinal, size in enumerate([8 * 1024 * 1024 - 1, 8 * 1024 * 1024, 8 * 1024 * 1024 + 1]):
    name = f"selection-boundary-{size}.bin"
    block = bytes((index * 13 + index // 251 + ordinal) % 256 for index in range(65536))
    payload = (block * ((size + len(block) - 1) // len(block)))[:size]
    before = {name: counter(name) for name in ["shardline_xorbs_total", "shardline_shards_total", "shardline_hub_api_file_uploads_total"]}
    api.upload_file(path_or_fileobj=payload, path_in_repo=name, repo_id=repo, commit_message=f"boundary {size}")
    info = api.get_paths_info(repo, [name])[0]
    headers = {"Authorization": "Bearer " + token}
    request = urllib.request.Request(endpoint + "/" + repo + "/resolve/main/" + name, headers=headers, method="HEAD")
    with urllib.request.urlopen(request, timeout=30) as response:
        assert int(response.headers["Content-Length"]) == size
        native = bool(response.headers.get("X-Xet-Hash"))
        if native:
            assert response.headers["X-Xet-Refresh-Route"].endswith("/" + response.headers["X-Repo-Commit"])
    assert native == (size > 8 * 1024 * 1024), (size, native)
    assert (info.lfs is not None) == native
    listed = {entry.path: entry for entry in api.list_repo_tree(repo, recursive=True)}[name]
    assert listed.blob_id == info.blob_id
    if not native:
        assert listed.blob_id == hashlib.sha1(b"blob " + str(size).encode() + b"\0" + payload).hexdigest()
    for metric in ["shardline_xorbs_total", "shardline_shards_total"]:
        after = counter(metric)
        assert (after > before[metric]) if native else (after == before[metric]), (size, metric, before[metric], after)
    assert counter("shardline_hub_api_file_uploads_total") == before["shardline_hub_api_file_uploads_total"]
    with tempfile.TemporaryDirectory() as cache:
        downloaded = hf_hub_download(repo, name, endpoint=endpoint, token=token, cache_dir=cache, force_download=True)
        assert hashlib.sha256(pathlib.Path(downloaded).read_bytes()).digest() == hashlib.sha256(payload).digest()
with tempfile.TemporaryDirectory() as folder, tempfile.TemporaryDirectory() as cache:
    expected = {}
    before = {name: counter(name) for name in ["shardline_xorbs_total", "shardline_shards_total"]}
    for ordinal in range(9):
        name = f"batch-small-{ordinal}.bin"
        payload = bytes([31 + ordinal]) * (6 * 1024 * 1024)
        pathlib.Path(folder, name).write_bytes(payload)
        expected[name] = hashlib.sha256(payload).digest()
    api.upload_folder(folder_path=folder, repo_id=repo, commit_message="multiple inline-sized files")
    assert all(info.lfs is None for info in api.get_paths_info(repo, list(expected)))
    assert all(counter(name) == value for name, value in before.items()), "small folder files must remain regular uploads"
    for name, digest in expected.items():
        downloaded = hf_hub_download(repo, name, endpoint=endpoint, token=token, cache_dir=cache, force_download=True)
        assert hashlib.sha256(pathlib.Path(downloaded).read_bytes()).digest() == digest
# Rejected LFS negotiation is terminal: no regular-body fallback or Hub commit.
from huggingface_hub.utils import HfHubHTTPError
import io
head = api.repo_info(repo).sha
before = {name: counter(name) for name in ["shardline_xorbs_total", "shardline_shards_total", "shardline_hub_api_file_uploads_total"]}
payload = bytes(range(256)) * (36 * 1024 + 1)
failing = HfApi(endpoint=os.environ["HF_FAILING_ENDPOINT"], token=token)
try:
    # Buffered streams use the stock client's documented basic-LFS path;
    # current Hub versions send ordinary byte/file inputs directly through Xet.
    failing.upload_file(path_or_fileobj=io.BytesIO(payload), path_in_repo="negotiation-retry.bin", repo_id=repo)
except HfHubHTTPError as error:
    assert error.response.status_code == 403, error.response.status_code
else:
    raise AssertionError("rejected LFS negotiation unexpectedly succeeded")
assert api.repo_info(repo).sha == head
assert "negotiation-retry.bin" not in api.list_repo_files(repo)
assert all(counter(name) == value for name, value in before.items()), "rejected negotiation persisted data"
api.upload_file(path_or_fileobj=payload, path_in_repo="negotiation-retry.bin", repo_id=repo)
assert counter("shardline_xorbs_total") > before["shardline_xorbs_total"]
assert counter("shardline_shards_total") > before["shardline_shards_total"]
assert counter("shardline_hub_api_file_uploads_total") == before["shardline_hub_api_file_uploads_total"]
with tempfile.TemporaryDirectory() as cache:
    try:
        downloaded = hf_hub_download(repo, "negotiation-retry.bin", endpoint=endpoint, token=token, cache_dir=cache)
    except Exception as error:
        print("download_failure_type=" + type(error).__name__, flush=True)
        raise
    assert pathlib.Path(downloaded).read_bytes() == payload
"#;
    let output = super::native_output(
        Command::new(hf_python())
            .args(["-c", script])
            .env("HF_ENDPOINT", &runtime.base_url)
            .env("HF_FAILING_ENDPOINT", endpoint)
            .env("HF_TOKEN", &runtime.model_token)
            .env("HF_REPO", repo)
            .env("HF_HOME", home.path())
            .env("HF_HUB_DISABLE_XET", "0")
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost"),
    )?;
    if !output.status.success()
        && let Some(path) = std::env::var_os("SHARDLINE_NATIVE_CLIENT_FAILURE_LOG")
    {
        std::fs::write(path, &output.stderr)?;
    }
    assert!(
        output.status.success(),
        "stock SDK preupload boundary failed: {}; download category: {:?}; script line: {:?}",
        output.status,
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| line.strip_prefix("download_failure_type="))
            .find(|name| name
                .chars()
                .all(|value| value.is_ascii_alphanumeric() || value == '_')),
        String::from_utf8_lossy(&output.stderr)
            .lines()
            .filter_map(|line| line.trim().strip_prefix("File \"<string>\", line "))
            .filter_map(|line| line.split(',').next()?.parse::<usize>().ok())
            .next_back()
    );
    assert!(
        rejected.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "stock SDK must reach the rejected LFS batch endpoint"
    );
    Ok(())
}
