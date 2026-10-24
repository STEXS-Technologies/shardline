//! Real Hub SDK model-card publication, parsing and immutable metadata reads.

use super::{
    Command, HubRuntime, RepositoryProvider, TestError, TokenScope, bearer_token, hf_python,
};

pub(super) fn roundtrip(runtime: &HubRuntime, repo: &str, token: &str) -> Result<(), TestError> {
    let home = tempfile::tempdir()?;
    let (owner, name) = repo.split_once('/').ok_or("invalid fixture repository")?;
    let writer = bearer_token(
        "github-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        owner,
        name,
        None,
    )?;
    let script = r##"
import json, os, urllib.request, urllib.error
from huggingface_hub import HfApi, ModelCard

endpoint = os.environ["HF_ENDPOINT"]
token = os.environ["HF_TOKEN"]
repo = os.environ["HF_REPO"]
api = HfApi(endpoint=endpoint, token=token)
content = "---\nlanguage:\n- en\nlicense: mit\ntags:\n- shardline-e2e\nmodel-index:\n- name: roundtrip\n  results: []\n---\n# Verified card\n"

# ModelCard.validate() in this pinned upstream client hardcodes huggingface.co.
# Qualify our endpoint explicitly, without patching the client's HTTP transport
# or silently sending a Shardline card/token to the public service.
def validate(value):
    request = urllib.request.Request(
        endpoint + "/api/validate-yaml",
        data=json.dumps({"content": value, "repoType": "model"}).encode(),
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    return urllib.request.urlopen(request, timeout=30)

with validate(content) as response:
    assert response.status == 200
try:
    validate("---\ntags: [unfinished\n---\n")
except urllib.error.HTTPError as error:
    assert error.code == 400, error.code
else:
    raise AssertionError("malformed model card was accepted")

published = api.upload_file(
    repo_id=repo, path_in_repo="README.md", path_or_fileobj=content.encode(),
    commit_message="publish structured model card",
)
loaded = ModelCard.load(repo, token=token)
assert loaded.data.language == ["en"], loaded.data
assert loaded.data.license == "mit", loaded.data
assert loaded.data.tags == ["shardline-e2e"], loaded.data
assert loaded.data.to_dict()["model-index"] == [{"name": "roundtrip", "results": []}], loaded.data
assert "# Verified card" in loaded.text
info = api.model_info(repo, revision=published.oid)
assert info.card_data.to_dict() == loaded.data.to_dict(), info.card_data

replacement = content.replace("license: mit", "license: apache-2.0")
writer = HfApi(endpoint=endpoint, token=os.environ["HF_CARD_WRITER"])
writer.upload_file(repo_id=repo, path_in_repo="README.md", path_or_fileobj=replacement.encode())
assert ModelCard.load(repo, token=token).data.license == "apache-2.0"
assert api.model_info(repo).card_data.license == "apache-2.0"
assert api.model_info(repo, revision="main").card_data.license == "apache-2.0"
assert api.model_info(repo, revision=published.oid).card_data.license == "mit"
request = urllib.request.Request(endpoint + "/api/models/" + repo + "/modelcard",
    headers={"Authorization": "Bearer " + token})
with urllib.request.urlopen(request, timeout=30) as response:
    assert response.read() == replacement.encode()
"##;
    let output = super::native_output(
        Command::new(hf_python())
            .arg("-c")
            .arg(script)
            .env("HF_ENDPOINT", &runtime.base_url)
            .env("HF_TOKEN", token)
            .env("HF_CARD_WRITER", writer)
            .env("HF_REPO", repo)
            .env("HF_HOME", home.path()),
    )?;
    assert!(
        output.status.success(),
        "stock model-card roundtrip failed: {}",
        output.status
    );
    Ok(())
}
