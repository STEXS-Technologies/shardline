//! Revision/branch client (M5b, `docs/SDX_PLAN.md` §4.3): create, list, and
//! delete revisions against the M5a revision-registry endpoints.
//!
//! Exposes [`XetClient::list_revisions`], [`XetClient::create_revision`], and
//! [`XetClient::delete_revision`]. Creating a revision that already exists
//! returns [`SdxError::RevisionExists`]; deletion is idempotent.

use reqwest::Method;
use serde::Deserialize;
use shardline_xet_adapter::{XET_REVISION_ROUTE, XET_REVISIONS_ROUTE};

use crate::{
    client::XetClient,
    error::{SdxError, TransferError},
    tree::MetadataClient,
};

/// A revision record (`{name,createdAt,updatedAt}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Revision {
    /// Revision name.
    pub name: String,
    /// Creation time as Unix seconds.
    pub created_at: u64,
    /// Last update time as Unix seconds.
    pub updated_at: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RevisionJson {
    name: String,
    created_at: u64,
    updated_at: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RevisionsResponse {
    revisions: Vec<RevisionJson>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeleteRevisionResponse {}

impl MetadataClient {
    /// Lists every revision of the repository by walking the keyset-cursor
    /// pages until the server reports no `nextCursor`.
    ///
    /// The server bounds each page (default limit 1000), so a repository with
    /// more revisions than one page would silently truncate without this loop
    /// (F-84).
    async fn list_revisions(&self) -> Result<Vec<Revision>, SdxError> {
        let retry = self.read_retry();
        let token = self.tokens.read_token().await?;
        let route = self.repo_route(XET_REVISIONS_ROUTE);
        let mut revisions = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let query: Vec<(String, String)> = cursor
                .as_deref()
                .map(|c| vec![("cursor".to_owned(), c.to_owned())])
                .unwrap_or_default();
            let url = crate::tree::build_url(&self.api_base, &route, &query);
            let body = self
                .send(&retry, token.token.clone(), Method::GET, url, None)
                .await?;
            let response: RevisionsResponse = serde_json::from_slice(&body)
                .map_err(|error| crate::tree::metadata_parse("list_revisions", &error))?;
            revisions.extend(response.revisions.into_iter().map(|revision| Revision {
                name: revision.name,
                created_at: revision.created_at,
                updated_at: revision.updated_at,
            }));
            match response.next_cursor {
                Some(next) if next != cursor.as_deref().unwrap_or_default() => cursor = Some(next),
                _ => break,
            }
        }
        Ok(revisions)
    }

    async fn create_revision(&self, rev: &str) -> Result<Revision, SdxError> {
        let retry = self.write_retry();
        let token = self.tokens.write_token().await?;
        let route = self
            .repo_route_scope(XET_REVISION_ROUTE)
            .replace("{rev}", &crate::tree::encode_query(rev));
        let url = crate::tree::build_url(&self.api_base, &route, crate::tree::no_query());
        match self
            .send(&retry, token.token, Method::POST, url, None)
            .await
        {
            Ok(body) => {
                let revision: RevisionJson = serde_json::from_slice(&body)
                    .map_err(|error| crate::tree::metadata_parse("create_revision", &error))?;
                Ok(Revision {
                    name: revision.name,
                    created_at: revision.created_at,
                    updated_at: revision.updated_at,
                })
            }
            Err(SdxError::Transfer(TransferError::HttpStatus { status: 409, .. })) => {
                Err(SdxError::RevisionExists(rev.to_owned()))
            }
            Err(error) => Err(error),
        }
    }

    async fn delete_revision(&self, rev: &str) -> Result<(), SdxError> {
        let retry = self.write_retry();
        let token = self.tokens.write_token().await?;
        let route = self
            .repo_route_scope(XET_REVISION_ROUTE)
            .replace("{rev}", &crate::tree::encode_query(rev));
        let url = crate::tree::build_url(&self.api_base, &route, crate::tree::no_query());
        let body = self
            .send(&retry, token.token, Method::DELETE, url, None)
            .await?;
        // Idempotent: the server returns 200 even when `deleted: false`.
        let _: DeleteRevisionResponse = serde_json::from_slice(&body)
            .map_err(|error| crate::tree::metadata_parse("delete_revision", &error))?;
        Ok(())
    }
}

impl XetClient {
    /// Lists all revisions of the client's repository.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the request fails or the response is invalid.
    pub async fn list_revisions(&self) -> Result<Vec<Revision>, SdxError> {
        MetadataClient::from_download(self.download_inner())
            .list_revisions()
            .await
    }

    /// Creates a revision, returning its record.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError::RevisionExists`] when the revision already exists,
    /// or another typed error on failure.
    pub async fn create_revision(&self, rev: &str) -> Result<Revision, SdxError> {
        MetadataClient::from_download(self.download_inner())
            .create_revision(rev)
            .await
    }

    /// Deletes a revision (and cascades its tree rows). Idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the request fails.
    pub async fn delete_revision(&self, rev: &str) -> Result<(), SdxError> {
        MetadataClient::from_download(self.download_inner())
            .delete_revision(rev)
            .await
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::Revision;
    use crate::{Auth, RepositoryId, XetClientBuilder};

    const READ_TOKEN: &str = "read-token";

    async fn build_client(server: &MockServer) -> crate::XetClient {
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
        .with_api_key("bootstrap".to_owned())
        .with_subject("user".to_owned());
        let port = server.uri().split(':').next_back().unwrap().to_owned();
        XetClientBuilder::new()
            .endpoint(format!("xet://127.0.0.1:{port}/github/team/assets/main"))
            .auth(auth)
            .build()
            .unwrap()
    }

    async fn mock_read_token(server: &MockServer) {
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

    fn revision_json(name: &str) -> serde_json::Value {
        json!({"name": name, "createdAt": 1, "updatedAt": 2})
    }

    #[test]
    fn revision_is_comparable() {
        let a = Revision {
            name: "main".to_owned(),
            created_at: 1,
            updated_at: 2,
        };
        let b = a.clone();
        assert_eq!(a, b);
        assert_ne!(
            a,
            Revision {
                name: "other".to_owned(),
                ..a
            }
        );
    }

    /// A response with `nextCursor` set must be followed until the cursor is
    /// exhausted so a repository with more than one page of revisions (server
    /// default limit 1000) is never silently truncated (F-84).
    #[tokio::test]
    async fn list_revisions_follows_next_cursor_until_exhausted() {
        let server = MockServer::start().await;
        mock_read_token(&server).await;
        // Page 2 mounted first so first-match-wins handles it.
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/revisions"))
            .and(query_param("cursor", "two"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "revisions": [revision_json("three"), revision_json("four")],
                "nextCursor": null
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/revisions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "revisions": [revision_json("one"), revision_json("two")],
                "nextCursor": "two"
            })))
            .mount(&server)
            .await;

        let client = build_client(&server).await;
        let revisions = client.list_revisions().await.unwrap();
        let names: Vec<&str> = revisions.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["one", "two", "three", "four"]);
    }

    /// A single page with no cursor returns exactly the page contents.
    #[tokio::test]
    async fn list_revisions_single_page_without_cursor() {
        let server = MockServer::start().await;
        mock_read_token(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/revisions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "revisions": [revision_json("main")],
                "nextCursor": null
            })))
            .mount(&server)
            .await;

        let client = build_client(&server).await;
        let revisions = client.list_revisions().await.unwrap();
        let names: Vec<&str> = revisions.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["main"]);
    }
}
