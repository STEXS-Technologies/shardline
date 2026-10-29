//! Official clients read existing Hub objects with a repository-wide read token.

use super::{
    Command, HubRuntime, RepositoryProvider, TestError, TokenScope, bearer_token, hf_python,
};

pub(super) fn roundtrip(runtime: &HubRuntime, repo: &str) -> Result<(), TestError> {
    let (owner, name) = repo.split_once('/').ok_or("invalid fixture repository")?;
    let token = bearer_token(
        "github-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        owner,
        name,
        None,
    )?;
    let writer = bearer_token(
        "github-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        owner,
        name,
        None,
    )?;
    let side_reader = bearer_token(
        "github-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        owner,
        name,
        Some("private-side"),
    )?;
    let snapshot_reader = bearer_token(
        "github-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        owner,
        name,
        Some("refs/tags/metadata-snapshot"),
    )?;
    let home = tempfile::tempdir()?;
    let script = r#"
import hashlib, json, os, pathlib, urllib.request, urllib.error, urllib.parse
from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_download
api = HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
repo = os.environ["HF_REPO"]
api.create_commit(repo_id=repo, commit_message="expanded tree pagination fixture",
    token=os.environ["HF_WRITE_TOKEN"], operations=[
        CommitOperationAdd(path_in_repo=f"paged metadata/file-{index:03}.txt", path_or_fileobj=b"paged metadata")
        for index in range(53)])
info = api.repo_info(repo)
assert len(info.sha) == 40, info.sha
for filename in ["single.txt", "copied-basic-lfs.bin"]:
    path = hf_hub_download(repo, filename, endpoint=api.endpoint, token=api.token)
    actual = pathlib.Path(path).read_bytes()
    if filename == "copied-basic-lfs.bin":
        assert actual == b"basic-lfs-copy-fixture\n" * (450 * 1024)
    else:
        assert actual == b"single upload via hf cli\n", filename
native = hf_hub_download(repo, "large-model.bin", revision=info.sha, endpoint=api.endpoint, token=api.token)
expected = bytes((index * 31 + index // 251) % 256 for index in range(9 * 1024 * 1024))
assert hashlib.sha256(pathlib.Path(native).read_bytes()).digest() == hashlib.sha256(expected).digest()
def request(path, token, method="GET"):
    req = urllib.request.Request(api.endpoint + path, method=method, headers={"Authorization": "Bearer " + token})
    return urllib.request.urlopen(req, timeout=30)
with request("/" + repo + "/resolve/" + info.sha + "/large-model.bin", api.token, "HEAD") as response:
    file_id = response.headers["X-Xet-Hash"]
for source_token in [api.token, os.environ["HF_SCOPED_TOKEN"]]:
    with request("/api/models/" + repo + "/xet-read-token/" + info.sha, source_token) as response:
        current_token = json.load(response)["accessToken"]
    try:
        request("/api/models/" + repo, current_token)
    except urllib.error.HTTPError as error:
        assert error.code == 403, error.code
    else:
        raise AssertionError("CAS credential was accepted as a general Hub credential")
    with request("/v1/reconstructions/" + file_id, current_token) as response:
        reconstruction = json.load(response)
    fetch = next(iter(reconstruction["fetch_info"].values()))[0]
    start, end = fetch["url_range"]["start"], fetch["url_range"]["end"]
    def fetch_range(url, first, last, token=None):
        headers = {"Range": f"bytes={first}-{last}"}
        if token is not None:
            headers["Authorization"] = "Bearer " + token
        return urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30)
    with fetch_range(fetch["url"], start, end) as response:
        assert response.status == 206
        assert len(response.read()) == end - start + 1
    parsed = urllib.parse.urlsplit(fetch["url"])
    unsigned = urllib.parse.urlunsplit(parsed._replace(query=""))
    for url, last, bearer in [(fetch["url"], end + 1, None), (unsigned, end, current_token)]:
        try:
            fetch_range(url, start, last, bearer)
        except urllib.error.HTTPError as error:
            assert error.code == 404, error.code
        else:
            raise AssertionError("commit-scoped read escaped its signed range")
history = api.list_repo_commits(repo)
historical = next(entry.commit_id for entry in reversed(history)
    if api.get_paths_info(repo, ["large-model.bin"], revision=entry.commit_id))
assert historical != info.sha
for history_token in [api.token, os.environ["HF_SCOPED_TOKEN"]]:
    older_native = hf_hub_download(repo, "large-model.bin", revision=historical,
        endpoint=api.endpoint, token=history_token, force_download=True)
    assert hashlib.sha256(pathlib.Path(older_native).read_bytes()).digest() == hashlib.sha256(expected).digest()
# An immutable ID is not permission to enter a sibling branch. First prove
# this branch's native payload downloads successfully with the broader token.
writer = os.environ["HF_WRITE_TOKEN"]
api.create_tag(repo, tag="metadata-snapshot", revision=info.sha, token=writer)
snapshot = HfApi(endpoint=api.endpoint, token=os.environ["HF_SNAPSHOT_TOKEN"])
expanded = list(api.list_repo_tree(repo, revision=info.sha, recursive=True, expand=True))
basic = list(api.list_repo_tree(repo, revision=info.sha, recursive=True))
assert [(entry.path, entry.blob_id if hasattr(entry, "blob_id") else entry.tree_id) for entry in expanded] == [
    (entry.path, entry.blob_id if hasattr(entry, "blob_id") else entry.tree_id) for entry in basic]
assert all(entry.last_commit is not None for entry in expanded), expanded
assert len(expanded) > 50, "stock SDK must follow at least one expanded next-page link"
assert all(getattr(entry, "security", None) is None for entry in expanded)
for revision in ["metadata-snapshot", "refs/tags/metadata-snapshot", info.sha]:
    snapshot_history = snapshot.list_repo_commits(repo, revision=revision)
    assert [entry.commit_id for entry in snapshot_history] == [info.sha], snapshot_history
    paths = snapshot.get_paths_info(repo, ["single.txt"], revision=revision, expand=True)
    assert len(paths) == 1
    broad_paths = api.get_paths_info(repo, ["single.txt"], revision=revision, expand=True)
    assert broad_paths[0].last_commit is not None
    assert broad_paths[0].last_commit.oid != info.sha
    assert paths[0].last_commit is None, paths[0]
    snapshot_tree = list(snapshot.list_repo_tree(repo, revision=revision, recursive=True, expand=True))
    assert [(entry.path, entry.blob_id if hasattr(entry, "blob_id") else entry.tree_id) for entry in snapshot_tree] == [
        (entry.path, entry.blob_id if hasattr(entry, "blob_id") else entry.tree_id) for entry in basic]
    assert all(entry.last_commit is None or entry.last_commit.oid == info.sha for entry in snapshot_tree)
api.create_branch(repo, branch="metadata-snapshot", revision=info.sha, token=writer)
try:
    snapshot.list_repo_commits(repo, revision="metadata-snapshot")
except Exception as error:
    assert getattr(getattr(error, "response", None), "status_code", None) == 403, repr(error)
else:
    raise AssertionError("tag credential entered a same-name branch")
assert len(snapshot.list_repo_commits(repo, revision="refs/tags/metadata-snapshot")) == 1
api.delete_branch(repo, branch="metadata-snapshot", token=writer)
api.create_branch(repo, branch="private-side", revision=historical, token=writer)
side_bytes = b"sibling-only-file" + expected[17:]
side_source = pathlib.Path(os.environ["HF_HOME"]) / "side-file.bin"
side_source.write_bytes(side_bytes)
side_commit = api.upload_file(path_or_fileobj=side_source, path_in_repo="private-side.bin",
    repo_id=repo, revision="private-side", token=writer)
side_id = side_commit.oid
for operation in [
    lambda: api.upload_file(path_or_fileobj=b"unauthorized", path_in_repo="denied.txt",
        repo_id=repo, revision="private-side", token=os.environ["HF_SCOPED_TOKEN"]),
    lambda: api.create_branch(repo, branch="denied-branch", token=os.environ["HF_SCOPED_TOKEN"]),
    lambda: api.delete_branch(repo, branch="private-side", token=os.environ["HF_SCOPED_TOKEN"]),
    lambda: api.create_tag(repo, tag="denied-tag", token=os.environ["HF_SCOPED_TOKEN"]),
]:
    try:
        operation()
    except Exception as error:
        assert getattr(getattr(error, "response", None), "status_code", None) == 403, repr(error)
    else:
        raise AssertionError("main credential mutated another reference")
assert api.repo_info(repo, revision="private-side").sha == side_id
refs_after_denial = api.list_repo_refs(repo)
assert "denied-branch" not in [ref.name for ref in refs_after_denial.branches]
assert "denied-tag" not in [ref.name for ref in refs_after_denial.tags]
for scoped_token, allowed_branch in [
    (os.environ["HF_SCOPED_TOKEN"], "main"),
    (os.environ["HF_SIDE_TOKEN"], "private-side"),
]:
    with request("/api/models/" + repo + "/refs", scoped_token) as response:
        scoped_refs = json.load(response)
    assert [item["name"] for item in scoped_refs["branches"]] == [allowed_branch], scoped_refs
    assert scoped_refs["tags"] == scoped_refs["converts"] == scoped_refs["pullRequests"] == []
    with request("/api/models/" + repo + "/revisions", scoped_token) as response:
        scoped_history = json.load(response)["revisions"]
    assert scoped_history, allowed_branch
    assert all(item["ref_name"] == allowed_branch for item in scoped_history), scoped_history
for suffix in ["", "/modelcard"]:
    try:
        request("/api/models/" + repo + suffix, os.environ["HF_SIDE_TOKEN"])
    except urllib.error.HTTPError as error:
        assert error.code == 403, (suffix, error.code)
    else:
        raise AssertionError("sibling credential read default main metadata")
side_download = hf_hub_download(repo, "private-side.bin", revision=side_id,
    endpoint=api.endpoint, token=api.token, force_download=True)
assert hashlib.sha256(pathlib.Path(side_download).read_bytes()).digest() == hashlib.sha256(side_bytes).digest()
for method in ["HEAD", "GET"]:
    try:
        request("/" + repo + "/resolve/private-side/private-side.bin", os.environ["HF_SCOPED_TOKEN"], method)
    except urllib.error.HTTPError as error:
        assert error.code in (403, 404), error.code
    else:
        raise AssertionError("main credential resolved sibling-only native metadata or bytes")
for suffix in ["revision/private-side", "tree/private-side", "commits/private-side"]:
    try:
        request("/api/models/" + repo + "/" + suffix, os.environ["HF_SCOPED_TOKEN"])
    except urllib.error.HTTPError as error:
        assert error.code in (403, 404), (suffix, error.code)
    else:
        raise AssertionError("main credential read sibling-only metadata: " + suffix)
try:
    api.get_paths_info(repo, ["private-side.bin"], revision="private-side", token=os.environ["HF_SCOPED_TOKEN"])
except Exception as error:
    assert getattr(getattr(error, "response", None), "status_code", None) in (403, 404), repr(error)
else:
    raise AssertionError("main credential read sibling-only paths-info")
try:
    hf_hub_download(repo, "private-side.bin", revision=side_id,
        endpoint=api.endpoint, token=os.environ["HF_SCOPED_TOKEN"], force_download=True)
except Exception as error:
    # A hidden revision may be rejected before CAS exchange (404); a resolved
    # revision is denied at exchange (403). Both preserve branch isolation.
    # The SDK wraps a denied metadata HEAD in ValueError when force_download
    # is set. Require an actual authorization/not-found response in its chain.
    statuses, seen = [], set()
    cause = error
    while cause is not None and id(cause) not in seen and len(seen) < 16:
        seen.add(id(cause))
        statuses.append(getattr(getattr(cause, "response", None), "status_code", None))
        cause = cause.__cause__ or cause.__context__
    assert any(status in (403, 404) for status in statuses), (repr(error), statuses)
else:
    raise AssertionError("main credential downloaded sibling-only native content")
oldest = history[-1].commit_id
with request("/api/models/" + repo + "/xet-read-token/" + oldest, api.token) as response:
    old_token = json.load(response)["accessToken"]
try:
    request("/v1/reconstructions/" + file_id, old_token)
except urllib.error.HTTPError as error:
    assert error.code == 404, error.code
else:
    raise AssertionError("old commit token exposed a file published in a later commit")
"#;
    let output = super::native_output(
        Command::new(hf_python())
            .args(["-c", script])
            .env("HF_ENDPOINT", &runtime.base_url)
            .env("HF_TOKEN", token)
            .env("HF_WRITE_TOKEN", writer)
            .env("HF_SIDE_TOKEN", side_reader)
            .env("HF_SNAPSHOT_TOKEN", snapshot_reader)
            .env("HF_SCOPED_TOKEN", &runtime.model_token)
            .env("HF_REPO", repo)
            .env("HF_HOME", home.path())
            .env("HF_HUB_DISABLE_XET", "0")
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost"),
    )?;
    assert!(
        output.status.success(),
        "stock SDK repository-wide credential read failed: {}",
        output.status
    );
    Ok(())
}
