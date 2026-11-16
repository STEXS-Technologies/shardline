//! Shared repository client and read-token fixtures for metadata endpoint tests.
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use crate::{Auth, RepositoryId, XetClientBuilder};

pub(crate) const READ_TOKEN: &str = "read-token";
pub(crate) const BOOTSTRAP_KEY: &str = "bootstrap";

pub(crate) fn build_client(server: &MockServer) -> crate::XetClient {
    let auth = Auth::new(
        &server.uri(),
        RepositoryId {
            provider: "github".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: "main".to_owned(),
        },
    )
    .unwrap()
    .with_api_key(BOOTSTRAP_KEY.to_owned())
    .with_subject("user".to_owned());
    let port = server.uri().split(':').next_back().unwrap().to_owned();
    XetClientBuilder::new()
        .endpoint(format!("xet://127.0.0.1:{port}/github/team/assets/main"))
        .auth(auth)
        .build()
        .unwrap()
}

pub(crate) async fn mock_read_token(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/github/team/assets/xet-read-token/main"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "casUrl": server.uri(),
            "exp": 4_000_000_000u64,
            "accessToken": READ_TOKEN,
        })))
        .mount(server)
        .await;
}
