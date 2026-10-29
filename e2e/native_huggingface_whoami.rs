//! Identity compatibility when authentication is intentionally unconfigured.

use super::*;

struct ServerTask(JoinHandle<Result<(), ServerError>>);
impl Drop for ServerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_huggingface_permissive_whoami_preserves_anonymous_identity() -> Result<(), TestError>
{
    if !command_available("hf") {
        return Ok(());
    }
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    let base = format!("http://{address}");
    let config = ServerConfig::new(
        address,
        base.clone(),
        storage.path().to_owned(),
        NonZeroUsize::new(65536).ok_or("chunk size")?,
    )
    .with_server_frontends([ServerFrontend::Hub])?
    .with_deployment_mode(shardline_server::DeploymentMode::Insecure);
    let _server = ServerTask(tokio::spawn(async move {
        serve_with_listener(config, listener).await
    }));
    wait_for_health(&base).await?;
    let http = reqwest::Client::new();
    for token in [None, Some("permissive-client-placeholder")] {
        let request = http.get(format!("{base}/api/whoami-v2"));
        let request = if let Some(token) = token {
            request.bearer_auth(token)
        } else {
            request
        };
        let identity: serde_json::Value = request.send().await?.error_for_status()?.json().await?;
        assert_eq!(identity["name"], "anonymous");
        assert_eq!(identity["orgs"], serde_json::json!([]));
        assert_eq!(identity["is_admin"], false);
        assert_eq!(identity["type"], "user");
        assert_eq!(identity["auth"]["identity"]["account"]["name"], "anonymous");
    }
    let home = tempfile::tempdir()?;
    let cli = super::native_output(
        Command::new("hf")
            .args(["auth", "whoami", "--format", "json"])
            .env("HF_ENDPOINT", &base)
            .env("HF_HOME", home.path())
            .env("HF_TOKEN", "permissive-client-placeholder")
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost"),
    )?;
    assert!(
        cli.status.success(),
        "stock hf whoami must accept permissive identity"
    );
    let identity: serde_json::Value = serde_json::from_slice(&cli.stdout)?;
    assert_eq!(identity["user"], "anonymous");
    assert!(identity["orgs"].is_null() || identity["orgs"] == serde_json::json!([]));
    let python = super::native_output(
        Command::new(hf_python())
            .args([
                "-c",
                r#"
import os
from huggingface_hub import HfApi
identity = HfApi(endpoint=os.environ["HF_ENDPOINT"], token="permissive-client-placeholder").whoami()
assert identity["name"] == "anonymous"
assert identity["orgs"] == []
assert identity["is_admin"] is False
assert identity["auth"]["identity"]["account"]["name"] == "anonymous"
"#,
            ])
            .env("HF_ENDPOINT", &base)
            .env("HF_HOME", home.path())
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost"),
    )?;
    assert!(
        python.status.success(),
        "stock HfApi.whoami must accept permissive identity"
    );
    Ok(())
}
