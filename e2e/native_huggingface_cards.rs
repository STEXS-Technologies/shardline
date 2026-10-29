//! Real Hub SDK model-card publication, parsing and immutable metadata reads.

use super::{
    Command, HubRuntime, RepositoryProvider, TestError, TokenScope, bearer_token, hf_python,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires stock huggingface_hub 2.2 endpoint-local ModelCard validation"]
async fn native_huggingface_model_card_push_uses_configured_endpoint() -> Result<(), TestError> {
    assert!(
        super::command_available("hf"),
        "stock Hub client is required"
    );
    let runtime = super::start_hub_runtime().await?;
    stock_push(&runtime)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires stock huggingface_hub 2.2 and disposable PostgreSQL/MinIO"]
async fn native_huggingface_model_card_push_with_postgres_and_minio() -> Result<(), TestError> {
    assert!(
        super::command_available("hf"),
        "stock Hub client is required"
    );
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("model-card qualification requires Docker")?;
    let runtime = super::start_runtime_with_services(
        &[shardline_server::ServerFrontend::Hub],
        Some(services),
    )
    .await?;
    stock_push(&runtime)
}

fn stock_push(runtime: &HubRuntime) -> Result<(), TestError> {
    let home = tempfile::tempdir()?;
    let script = r##"
import os, urllib.request, urllib.error
from huggingface_hub import HfApi, ModelCard, constants
from huggingface_hub.errors import HfHubHTTPError

endpoint = os.environ["HF_ENDPOINT"]
assert constants.ENDPOINT == endpoint
repo = "team/cli-created"
token = os.environ["HF_TOKEN"]
api = HfApi(endpoint=endpoint, token=token)
api.create_repo(repo, private=True)
content = "---\nlanguage:\n- en\nlicense: mit\ntags:\n- shardline-e2e\n---\n# Private stock model card\n"
card = ModelCard(content)
try:
    published = card.push_to_hub(repo_id=repo, token=token,
                                commit_message="publish with stock ModelCard")
except HfHubHTTPError as error:
    print("stock-card-http-status=" + str(error.response.status_code), flush=True)
    raise
info = api.model_info(repo)
assert info.sha == published.oid
assert info.private is True
assert info.card_data.license == "mit"
assert ModelCard.load(repo, token=token).data.language == ["en"]

# This YAML is accepted by the client's parser, but exceeds Shardline's bounded
# nesting budget. Validation must fail before README publication changes state.
deep = "---\ncustom: " + "[" * 40 + "0" + "]" * 40 + "\n---\n# Too deep\n"
invalid = ModelCard(deep)
try:
    invalid.push_to_hub(repo_id=repo, token=token)
except ValueError as error:
    assert "YAML" in str(error), type(error).__name__
else:
    raise AssertionError("excessively nested card was published")
assert api.model_info(repo).sha == published.oid
assert ModelCard.load(repo, token=token).data.license == "mit"

replacement = ModelCard(content.replace("license: mit", "license: apache-2.0"))
updated = replacement.push_to_hub(repo_id=repo, token=token,
                                 parent_commit=published.oid)
assert updated.oid != published.oid
assert api.model_info(repo).card_data.license == "apache-2.0"
assert api.model_info(repo, revision=published.oid).card_data.license == "mit"

# Anonymous validation cannot grant access to the private repository.
request = urllib.request.Request(endpoint + "/api/models/" + repo)
try:
    urllib.request.urlopen(request, timeout=30)
except urllib.error.HTTPError as error:
    assert error.code in (401, 403, 404), error.code
else:
    raise AssertionError("private repository became anonymously readable")
"##;
    let output = super::native_output(
        Command::new(hf_python())
            .arg("-c")
            .arg(script)
            .env("HF_ENDPOINT", &runtime.base_url)
            .env("HF_TOKEN", &runtime.created_token)
            .env("HF_HOME", home.path())
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost"),
    )?;
    let validation_status = String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| {
            line.strip_prefix("stock-card-http-status=")?
                .parse::<u16>()
                .ok()
        });
    assert!(
        output.status.success(),
        "unmodified ModelCard.push_to_hub failed: {}; validation status {validation_status:?}; client output omitted",
        output.status,
    );
    Ok(())
}

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

# ModelCard.validate() in Hub 1.25.1 hardcodes huggingface.co.
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
