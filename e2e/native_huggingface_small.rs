//! Native Xet payloads around the historical inline/LFS resolve boundary.

use super::{Command, HubRuntime, TestError, hf_python};

pub(super) fn roundtrip(runtime: &HubRuntime, repo: &str) -> Result<(), TestError> {
    let home = tempfile::tempdir()?;
    let script = r#"
import hashlib, json, os, pathlib, subprocess, tempfile, urllib.request, urllib.error
import hf_xet
from huggingface_hub import HfApi, hf_hub_download
endpoint, token, repo = os.environ["HF_ENDPOINT"], os.environ["HF_TOKEN"], os.environ["HF_REPO"]
api = HfApi(endpoint=endpoint, token=token)
def counter(name):
    text = urllib.request.urlopen(endpoint + "/metrics", timeout=30).read().decode()
    return sum(float(line.split()[-1]) for line in text.splitlines()
        if line.startswith(name + " ") or line.startswith(name + "{"))
before = {name: counter(name) for name in ["shardline_xorbs_total", "shardline_shards_total", "shardline_hub_api_file_uploads_total"]}
payloads = [bytes((index * 13 + index // 251) % 256 for index in range(size))
    for size in [0, 32 * 1024, 1024 * 1024, 1024 * 1024 + 1]]
session = hf_xet.XetSession()
with session.new_upload_commit(
    token_refresh_url=endpoint + "/api/models/" + repo + "/xet-write-token/main",
    token_refresh_headers={"Authorization": "Bearer " + token},
) as upload:
    handles = [upload.start_upload_bytes(data, sha256=hf_xet.COMPUTE_SHA256) for data in payloads]
lines = [{"key": "header", "value": {"summary": "native small-file boundary", "parentCommit": api.repo_info(repo).sha}}]
for data, handle in zip(payloads, handles):
    sha = hashlib.sha256(data).hexdigest()
    assert handle.result().xet_info.sha256 == sha
    lines.append({"key": "lfsFile", "value": {"path": f"small-native-{len(data)}.bin", "oid": sha, "size": len(data)}})
request = urllib.request.Request(endpoint + "/api/models/" + repo + "/commit/main",
    data="\n".join(json.dumps(line) for line in lines).encode(),
    headers={"Authorization": "Bearer " + token, "Content-Type": "application/x-ndjson"})
with urllib.request.urlopen(request, timeout=30) as response:
    assert response.status == 200
assert counter("shardline_xorbs_total") > before["shardline_xorbs_total"]
assert counter("shardline_shards_total") > before["shardline_shards_total"]
assert counter("shardline_hub_api_file_uploads_total") == before["shardline_hub_api_file_uploads_total"]
for data in payloads:
    name = f"small-native-{len(data)}.bin"
    url = endpoint + "/" + repo + "/resolve/main/" + name
    headers = {"Authorization": "Bearer " + token}
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers, method="HEAD"), timeout=30) as response:
        assert response.status == 200
        assert int(response.headers["Content-Length"]) == len(data)
        assert response.headers["X-Xet-Hash"]
    downloaded = hf_hub_download(repo, name, endpoint=endpoint, token=token, force_download=True)
    assert pathlib.Path(downloaded).read_bytes() == data
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as response:
        assert response.read() == data
    if data:
        with urllib.request.urlopen(urllib.request.Request(url, headers={**headers, "Range": "bytes=7-4095"}), timeout=30) as response:
            assert response.status == 206
            assert response.read() == data[7:4096]
    else:
        try:
            urllib.request.urlopen(urllib.request.Request(url, headers={**headers, "Range": "bytes=0-0"}), timeout=30)
        except urllib.error.HTTPError as error:
            assert error.code == 416
            assert error.headers["Content-Range"] == "bytes */0"
        else:
            raise AssertionError("empty-file byte range was accepted")
tree = {entry.path: entry for entry in api.list_repo_tree(repo, recursive=True)}
for data in payloads:
    name = f"small-native-{len(data)}.bin"
    info = api.get_paths_info(repo, [name])[0]
    assert tree[name].blob_id == info.blob_id, (name, tree[name].blob_id, info.blob_id)
    pointer = f"version https://git-lfs.github.com/spec/v1\noid sha256:{hashlib.sha256(data).hexdigest()}\nsize {len(data)}\n".encode()
    expected = hashlib.sha1(b"blob " + str(len(pointer)).encode() + b"\0" + pointer).hexdigest()
    assert info.blob_id == expected
nested = b"nested metadata identity"
api.upload_file(path_or_fileobj=nested, path_in_repo="metadata-proof/meta.txt", repo_id=repo, commit_message="nested metadata")
info = {entry.path: entry for entry in api.get_paths_info(repo, ["metadata-proof", "metadata-proof/meta.txt"])}
expected = hashlib.sha1(b"blob " + str(len(nested)).encode() + b"\0" + nested).hexdigest()
entries = list(api.list_repo_tree(repo, path_in_repo="metadata-proof"))
observed = [(entry.path, getattr(entry, "blob_id", None)) for entry in entries]
assert observed == [("metadata-proof/meta.txt", expected)], observed
folder = {entry.path: entry for entry in api.list_repo_tree(repo)}["metadata-proof"]
assert folder.tree_id == info["metadata-proof"].tree_id
request = urllib.request.Request(endpoint + "/api/models/" + repo + "/tree/main/metadata-proof?limit=1", headers={"Authorization": "Bearer " + token})
with urllib.request.urlopen(request, timeout=30) as response:
    page = json.load(response)
assert [(entry["path"], entry["oid"]) for entry in page] == [("metadata-proof/meta.txt", expected)]
api.upload_file(path_or_fileobj=b"deeper file", path_in_repo="metadata-proof/sub/file.txt", repo_id=repo)
recursive = {entry.path: entry for entry in api.list_repo_tree(repo, path_in_repo="metadata-proof", recursive=True)}
assert set(recursive) == {"metadata-proof/meta.txt", "metadata-proof/sub", "metadata-proof/sub/file.txt"}, recursive
sub = api.get_paths_info(repo, ["metadata-proof/sub"])[0]
assert recursive["metadata-proof/sub"].tree_id == sub.tree_id
with tempfile.TemporaryDirectory() as checkout:
    subprocess.run(["git", "-c", "http.extraHeader=Authorization: Bearer " + token,
        "clone", "--bare", "--depth", "1", "--no-tags", endpoint + "/" + repo + ".git", checkout],
        check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=45)
    objects = subprocess.run(["git", "-C", checkout, "ls-tree", "-r", "-t", "-z", "HEAD"],
        check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30).stdout
    listing = {entry.path: entry for entry in api.list_repo_tree(repo, recursive=True)}
    for item in objects.rstrip(b"\0").split(b"\0"):
        metadata, path = item.decode().split("\t", 1)
        mode, kind, oid = metadata.split()
        actual = listing[path].tree_id if kind == "tree" else listing[path].blob_id
        assert actual == oid, (path, actual, oid)
from huggingface_hub.errors import RemoteEntryNotFoundError
for missing in ["missing-tree", "metadata-proof/meta.txt"]:
    try:
        list(api.list_repo_tree(repo, path_in_repo=missing, recursive=True))
    except RemoteEntryNotFoundError as error:
        assert error.response.status_code == 404
        assert error.response.headers["X-Error-Code"] == "EntryNotFound"
    else:
        raise AssertionError("non-directory tree request succeeded: " + missing)
    for limit in [0, 1]:
        request = urllib.request.Request(endpoint + "/api/models/" + repo + "/tree/main/" + missing + "?limit=" + str(limit), headers={"Authorization": "Bearer " + token})
        try:
            urllib.request.urlopen(request, timeout=30)
        except urllib.error.HTTPError as error:
            assert error.code == 404
        else:
            raise AssertionError("paged non-directory request succeeded")
"#;
    let output = super::native_output(
        Command::new(hf_python())
            .args(["-c", script])
            .env("HF_ENDPOINT", &runtime.base_url)
            .env("HF_TOKEN", &runtime.model_token)
            .env("HF_REPO", repo)
            .env("HF_HOME", home.path())
            .env("HF_HUB_DISABLE_XET", "0")
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost"),
    )?;
    assert!(
        output.status.success(),
        "native small-file readback failed: {}",
        output.status
    );
    Ok(())
}
