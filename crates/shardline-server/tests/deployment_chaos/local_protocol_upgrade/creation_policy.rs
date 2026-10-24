//! Actual released Hub writer, explicit policy rollout and rollback reads.
use super::*;

fn spawn_hub(binary: &Path, root: &Path) -> LocalServer {
    LocalServer::spawn_with_storage(
        binary,
        root,
        None,
        "hub",
        &[("SHARDLINE_HUB_WEBHOOK_SECRET_KEY", "h".repeat(32))],
    )
}

async fn set_policy(binary: &Path, root: &Path, clear: bool) {
    let binary = binary.to_owned();
    let root = root.to_owned();
    tokio::task::spawn_blocking(move || {
        let input = root.join("creation-policy.json");
        std::fs::write(&input, r#"{"enabled":false}"#).unwrap();
        let mut command = Command::new(binary);
        command
            .env_clear()
            .current_dir(&root)
            .args([
                "admin",
                "repository-policy",
                "--namespace",
                "drill",
                "--root",
            ])
            .arg(&root);
        if clear {
            command.arg("--clear");
        } else {
            command.arg("--policy-file").arg(input);
        }
        assert!(
            command.output().unwrap().status.success(),
            "owner policy command must succeed"
        );
    })
    .await
    .unwrap();
}

async fn create(server: &LocalServer, name: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/api/repos/create", server.base))
        .bearer_auth(mint_token("drill", name, TokenScope::Write))
        .json(
            &serde_json::json!({"type":"model","organization":"drill","name":name,"private":true}),
        )
        .send()
        .await
        .unwrap()
}

async fn assert_private(server: &LocalServer, name: &str) {
    let response = reqwest::Client::new()
        .get(format!("{}/api/models/drill/{name}", server.base))
        .bearer_auth(mint_token("drill", name, TokenScope::Write))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response
            .json::<serde_json::Value>()
            .await
            .unwrap()
            .get("private"),
        Some(&serde_json::json!(true))
    );
}

#[tokio::test]
async fn released_hub_creation_policy_rollout_preserves_reads_and_cleared_rollback() {
    let Some(old) = resolve_n_minus_one_binary("Hub creation policy rollback") else {
        return;
    };
    let current = resolve_shardline_binary().expect("current binary required");
    let root = TempDir::new().unwrap();
    migrate(&old, root.path());
    let mut previous = spawn_hub(&old, root.path());
    previous.ready().await;
    assert_eq!(
        create(&previous, "old").await.status(),
        reqwest::StatusCode::CREATED
    );
    migrate(&current, root.path());
    let mut upgraded = spawn_hub(&current, root.path());
    upgraded.ready().await;
    // Before opt-in, both real binary versions retain creation compatibility.
    assert_eq!(
        create(&previous, "mixed-old").await.status(),
        reqwest::StatusCode::CREATED
    );
    assert_eq!(
        create(&upgraded, "mixed-new").await.status(),
        reqwest::StatusCode::CREATED
    );
    set_policy(&current, root.path(), false).await;
    assert_eq!(
        create(&upgraded, "blocked-new").await.status(),
        reqwest::StatusCode::FORBIDDEN
    );
    assert!(!create(&previous, "blocked-old").await.status().is_success());
    for name in ["old", "mixed-old", "mixed-new"] {
        assert_private(&previous, name).await;
        assert_private(&upgraded, name).await;
    }
    // Clear with the upgraded owner before returning creation to old writers.
    set_policy(&current, root.path(), true).await;
    assert_eq!(
        create(&previous, "rollback").await.status(),
        reqwest::StatusCode::CREATED
    );
    drop(previous);
    drop(upgraded);
    let mut restarted = spawn_hub(&old, root.path());
    restarted.ready().await;
    for name in ["old", "mixed-old", "mixed-new", "rollback"] {
        assert_private(&restarted, name).await;
    }
    assert_eq!(
        create(&restarted, "after-restart").await.status(),
        reqwest::StatusCode::CREATED
    );
}
