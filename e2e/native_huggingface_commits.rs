//! Stock SDK commit operations preserve immutable revisions and branch scope.

use super::{
    Command, HubRuntime, RepositoryProvider, TestError, TokenScope, bearer_token, hf_python,
};

pub(super) fn roundtrip(runtime: &HubRuntime, repo: &str, token: &str) -> Result<(), TestError> {
    let (owner, name) = repo.split_once('/').ok_or("invalid fixture repository")?;
    let repository_writer = bearer_token(
        "github-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        owner,
        name,
        None,
    )?;
    let home = tempfile::tempdir()?;
    let script = r#"
import os, json, pathlib, urllib.request, urllib.error
from huggingface_hub import HfApi, CommitOperationAdd, CommitOperationDelete, CommitOperationCopy, hf_hub_download

api = HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
repo = os.environ["HF_REPO"]
def counter(name):
    with urllib.request.urlopen(api.endpoint + "/metrics", timeout=30) as response:
        lines = response.read().decode().splitlines()
    return next(int(line.split()[1]) for line in lines if line.startswith(name + " "))
native_uploads_before = counter("shardline_xorbs_total")
original = api.repo_info(repo).sha
api = HfApi(endpoint=api.endpoint, token=os.environ["HF_REPO_WRITE_TOKEN"])
api.create_branch(repo, branch="commit-operations", revision=original)
created = api.create_commit(repo, revision="commit-operations", commit_message="nested deletion fixture", operations=[
    CommitOperationAdd(path_in_repo="delete-me/a.txt", path_or_fileobj=b"first"),
    CommitOperationAdd(path_in_repo="delete-me/nested/b.txt", path_or_fileobj=b"second"),
    CommitOperationAdd(path_in_repo="delete-me-neighbor.txt", path_or_fileobj=b"neighbor"),
])
deleted = api.create_commit(repo, revision="commit-operations", parent_commit=created.oid,
    commit_message="delete folder and add replacement", operations=[
        CommitOperationDelete(path_in_repo="delete-me", is_folder=True),
        CommitOperationAdd(path_in_repo="delete-me/replacement.txt", path_or_fileobj=b"replacement"),
    ])
paths = set(api.list_repo_files(repo, revision=deleted.oid))
assert "delete-me/a.txt" not in paths, paths
assert "delete-me/nested/b.txt" not in paths, paths
assert {"delete-me/replacement.txt", "delete-me-neighbor.txt"} <= paths, paths
historical = set(api.list_repo_files(repo, revision=created.oid))
assert {"delete-me/a.txt", "delete-me/nested/b.txt", "delete-me-neighbor.txt"} <= historical, historical
assert "delete-me/replacement.txt" not in historical
copied = api.create_commit(repo, revision="commit-operations", parent_commit=deleted.oid,
    commit_message="copy regular and Xet-backed LFS files", operations=[
        CommitOperationCopy(src_path_in_repo="single.txt", path_in_repo="copied/regular.txt"),
        CommitOperationCopy(src_path_in_repo="large-model.bin", path_in_repo="copied/large.bin"),
    ])
entries = {entry.path: entry for entry in api.get_paths_info(repo,
    ["large-model.bin", "copied/large.bin", "single.txt", "copied/regular.txt"], revision=copied.oid)}
assert entries["copied/large.bin"].size == entries["large-model.bin"].size
assert entries["copied/large.bin"].lfs.sha256 == entries["large-model.bin"].lfs.sha256
assert entries["copied/regular.txt"].blob_id == entries["single.txt"].blob_id
moved = api.create_commit(repo, revision="commit-operations", parent_commit=copied.oid,
    commit_message="move copied native LFS file", operations=[
        CommitOperationCopy(src_path_in_repo="copied/large.bin", src_revision=copied.oid, path_in_repo="moved.bin"),
        CommitOperationDelete(path_in_repo="copied/large.bin", is_folder=False),
    ])
assert "copied/large.bin" not in api.list_repo_files(repo, revision=moved.oid)
assert "copied/large.bin" in api.list_repo_files(repo, revision=copied.oid)
# Force the stock SDK paginator through multiple pages of immutable history.
# Compare directories as well as files, including repository-relative cursors
# inside a subtree after its original children have been deleted on a later ref.
from huggingface_hub.utils import paginate
for revision, subtree in [(created.oid, ""), (deleted.oid, ""), (moved.oid, ""), (created.oid, "delete-me")]:
    expected = {entry.path for entry in api.list_repo_tree(repo, path_in_repo=subtree or None,
        revision=revision, recursive=True)}
    url = api.endpoint + "/api/models/" + repo + "/tree/" + revision
    if subtree:
        url += "/" + subtree
    headers = {"Authorization": "Bearer " + api.token}
    request = urllib.request.Request(url + "?recursive=true&limit=1", headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        first_page = json.load(response)
        assert len(first_page) == 1
        assert 'rel="next"' in response.headers.get("Link", ""), response.headers
    actual = list(paginate(url, params={"recursive": "true", "limit": 1}, headers=headers, timeout=30))
    paths = [entry["path"] for entry in actual]
    assert len(paths) == len(set(paths)), paths
    assert set(paths) == expected, (revision, subtree, paths, expected)
# Pause the actual SDK iterator after its first page, then advance the branch.
# Next-page links must retain the first page's immutable snapshot in both modes.
expected = {entry.path for entry in api.list_repo_tree(repo, revision=moved.oid, recursive=True)}
for expanded in [False, True]:
    branch = "pagination-expanded" if expanded else "pagination-plain"
    api.create_branch(repo, branch=branch, revision=moved.oid)
    url = api.endpoint + "/api/models/" + repo + "/tree/" + branch
    pages = iter(paginate(url, params={"recursive": "true", "limit": 1,
        "expand": str(expanded).lower()}, headers=headers, timeout=30))
    first = next(pages)
    advanced = api.create_commit(repo, revision=branch, parent_commit=moved.oid,
        commit_message="advance branch between tree pages", operations=[
            CommitOperationDelete(path_in_repo="delete-me-neighbor.txt", is_folder=False),
            CommitOperationAdd(path_in_repo="concurrent-new.txt", path_or_fileobj=b"new revision"),
        ])
    assert advanced.oid != moved.oid
    paths = [first["path"]] + [entry["path"] for entry in pages]
    assert len(paths) == len(set(paths)), paths
    assert set(paths) == expected, (expanded, paths, expected)
    latest = set(api.list_repo_files(repo, revision=branch))
    assert "concurrent-new.txt" in latest and "delete-me-neighbor.txt" not in latest
    api.delete_branch(repo, branch=branch)
# Omitted sizes are derived only for an existing object in the caller's scope.
# Invalid explicit fields must not be treated as omitted or publish a revision.
for pointer in [
    {"path": "unknown.bin", "oid": "f" * 64},
    {"path": "bad-size.bin", "oid": entries["large-model.bin"].lfs.sha256, "size": "invalid"},
    {"path": "bad-oid.bin", "oid": "not-a-content-identity"},
]:
    body = "\n".join(json.dumps(line) for line in [
        {"key": "header", "value": {"summary": "must not publish", "parentCommit": moved.oid}},
        {"key": "lfsFile", "value": pointer},
    ])
    request = urllib.request.Request(api.endpoint + "/api/models/" + repo + "/commit/commit-operations",
        data=body.encode(), headers={"Authorization": "Bearer " + api.token, "Content-Type": "application/x-ndjson"})
    try:
        urllib.request.urlopen(request, timeout=30)
    except urllib.error.HTTPError as error:
        assert error.code == 400, error.code
    else:
        raise AssertionError("invalid pointer was published")
    assert api.repo_info(repo, revision="commit-operations").sha == moved.oid
assert api.repo_info(repo).sha == original
# Competing stock SDK writers must not both publish from the same parent.
# Each uses an independent client and the server's ordinary commit endpoint.
import concurrent.futures, threading
from huggingface_hub.utils import HfHubHTTPError
api.create_branch(repo, branch="competing-sdk-writers", revision=moved.oid)
ready = threading.Barrier(2)
def publish_competing(label):
    writer = HfApi(endpoint=api.endpoint, token=api.token)
    ready.wait(timeout=30)
    try:
        committed = writer.create_commit(repo, revision="competing-sdk-writers",
            parent_commit=moved.oid, commit_message="competing writer " + label,
            operations=[CommitOperationAdd(path_in_repo="winner-" + label + ".txt",
                path_or_fileobj=label.encode())])
        return label, committed.oid
    except HfHubHTTPError as error:
        assert error.response.status_code == 409, error.response.status_code
        return label, None
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as writers:
    results = list(writers.map(publish_competing, ["left", "right"]))
winners = [(label, oid) for label, oid in results if oid is not None]
assert len(winners) == 1, results
winner, head = winners[0]
assert api.repo_info(repo, revision="competing-sdk-writers").sha == head
current = set(api.list_repo_files(repo, revision=head))
assert {path for path in current if path.startswith("winner-")} == {"winner-" + winner + ".txt"}
assert not any(path.startswith("winner-") for path in api.list_repo_files(repo, revision=moved.oid))
assert api.repo_info(repo).sha == original
api.delete_branch(repo, branch="competing-sdk-writers")
api.delete_branch(repo, branch="commit-operations")
api = HfApi(endpoint=api.endpoint, token=os.environ["HF_TOKEN"])

# The supplied transfer credential is main-scoped. Exercise native byte reads
# on that ref; do not broaden it into another ref's CAS credential for this test.
source = pathlib.Path(hf_hub_download(repo, "large-model.bin", endpoint=api.endpoint, token=api.token)).read_bytes()
main_copy = api.create_commit(repo, parent_commit=original, commit_message="native LFS copy on main", operations=[
    CommitOperationCopy(src_path_in_repo="large-model.bin", path_in_repo="copied-native.bin"),
])
downloaded = hf_hub_download(repo, "copied-native.bin", endpoint=api.endpoint, token=api.token)
assert pathlib.Path(downloaded).read_bytes() == source
main_move = api.create_commit(repo, parent_commit=main_copy.oid, commit_message="native LFS move on main", operations=[
    CommitOperationCopy(src_path_in_repo="copied-native.bin", path_in_repo="moved-native.bin"),
    CommitOperationDelete(path_in_repo="copied-native.bin", is_folder=False),
])
downloaded = hf_hub_download(repo, "moved-native.bin", endpoint=api.endpoint, token=api.token)
assert pathlib.Path(downloaded).read_bytes() == source
assert "copied-native.bin" not in api.list_repo_files(repo)
assert "copied-native.bin" in api.list_repo_files(repo, revision=main_copy.oid)
assert counter("shardline_xorbs_total") == native_uploads_before
"#;
    let output = super::native_output(
        Command::new(hf_python())
            .args(["-c", script])
            .env("HF_ENDPOINT", &runtime.base_url)
            .env("HF_TOKEN", token)
            .env("HF_REPO_WRITE_TOKEN", repository_writer)
            .env("HF_REPO", repo)
            .env("HF_HOME", home.path())
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost"),
    )?;
    assert!(
        output.status.success(),
        "stock Hub commit operations failed: {}; script line: {:?}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
            .lines()
            .filter_map(|line| line.trim().strip_prefix("File \"<string>\", line "))
            .filter_map(|line| line.split(',').next()?.parse::<usize>().ok())
            .next_back()
    );
    let basic_script = r#"
import os, pathlib, urllib.request
from huggingface_hub import HfApi, CommitOperationCopy, hf_hub_download
api = HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
repo = os.environ["HF_REPO"]
def counter(name):
    with urllib.request.urlopen(api.endpoint + "/metrics", timeout=30) as response:
        lines = response.read().decode().splitlines()
    return next(int(line.split()[1]) for line in lines if line.startswith(name + " "))
uploads_before = counter("shardline_hub_api_file_uploads_total")
xorbs_before = counter("shardline_xorbs_total")
content = b"basic-lfs-copy-fixture\n" * (450 * 1024)
api.upload_file(repo_id=repo, path_in_repo="basic-lfs.bin", path_or_fileobj=content)
uploads_after = counter("shardline_hub_api_file_uploads_total")
assert uploads_after > uploads_before
assert counter("shardline_xorbs_total") == xorbs_before
source = api.get_paths_info(repo, ["basic-lfs.bin"])[0]
assert source.lfs is not None, source
api.create_commit(repo, commit_message="copy basic LFS file", operations=[
    CommitOperationCopy(src_path_in_repo="basic-lfs.bin", path_in_repo="copied-basic-lfs.bin"),
])
copied = api.get_paths_info(repo, ["copied-basic-lfs.bin"])[0]
assert copied.size == len(content) and copied.lfs.sha256 == source.lfs.sha256
downloaded = hf_hub_download(repo, "copied-basic-lfs.bin", endpoint=api.endpoint, token=api.token)
assert pathlib.Path(downloaded).read_bytes() == content
assert counter("shardline_hub_api_file_uploads_total") == uploads_after
"#;
    let basic_output = super::native_output(
        Command::new(hf_python())
            .args(["-c", basic_script])
            .env("HF_ENDPOINT", &runtime.base_url)
            .env("HF_TOKEN", token)
            .env("HF_REPO", repo)
            .env("HF_HOME", home.path())
            .env("HF_HUB_DISABLE_XET", "1")
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost"),
    )?;
    assert!(
        basic_output.status.success(),
        "stock basic LFS copy failed: {}",
        basic_output.status
    );
    Ok(())
}
