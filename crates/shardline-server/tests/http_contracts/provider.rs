//! Provider token HTTP contracts, independent of the deployment's metadata store.

use super::super::TestServer;

const VALID_KEY: &str = "test-api-key-16bytes";

async fn post(server: &TestServer, endpoint: &str, key: Option<&str>) -> reqwest::Response {
    let mut request = reqwest::Client::new()
        .post(server.url(&format!("/v1/providers/generic/{endpoint}")))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "subject": "test-user",
            "owner": "test",
            "repo": "test",
            "revision": null,
            "scope": "Read"
        }));
    if let Some(key) = key {
        request = request.header("x-shardline-provider-key", key);
    }
    request.send().await.unwrap()
}

async fn success_json(response: reqwest::Response) -> serde_json::Value {
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, 200, "provider token request failed: {body}");
    serde_json::from_str(&body).unwrap()
}

pub(crate) async fn issue_token(server: &TestServer) {
    let json = success_json(post(server, "tokens", Some(VALID_KEY)).await).await;
    assert!(!json["token"].as_str().unwrap().is_empty());
    assert_eq!(json["owner"], "test");
    assert_eq!(json["repo"], "test");
    assert_eq!(json["scope"], "Read");
}

pub(crate) async fn deny_token(server: &TestServer, key: Option<&str>, status: u16) {
    assert_eq!(post(server, "tokens", key).await.status(), status);
}

pub(crate) async fn git_lfs_authenticate(server: &TestServer) {
    let json = success_json(post(server, "git-lfs-authenticate", Some(VALID_KEY)).await).await;
    assert!(!json["href"].as_str().unwrap().is_empty());
    assert!(
        !json["header"]["X-Xet-Access-Token"]
            .as_str()
            .unwrap()
            .is_empty()
    );
}

pub(crate) async fn xet_token(server: &TestServer, operation: &str) {
    let response = reqwest::Client::new()
        .get(server.url(&format!(
            "/api/generic/test/test/xet-{operation}-token/main?subject=test-user"
        )))
        .header("x-shardline-provider-key", VALID_KEY)
        .send()
        .await
        .unwrap();
    let json = success_json(response).await;
    assert!(!json["accessToken"].as_str().unwrap().is_empty());
    assert!(!json["casUrl"].as_str().unwrap().is_empty());
}
