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
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeleteRevisionResponse {}

impl MetadataClient {
    async fn list_revisions(&self) -> Result<Vec<Revision>, SdxError> {
        let retry = self.read_retry();
        let token = self.tokens.read_token().await?;
        let route = self.repo_route(XET_REVISIONS_ROUTE);
        let url = crate::tree::build_url(&self.api_base, &route, crate::tree::no_query());
        let body = self
            .send(&retry, token.token, Method::GET, url, None)
            .await?;
        let response: RevisionsResponse = serde_json::from_slice(&body)
            .map_err(|error| crate::tree::metadata_parse("list_revisions", &error))?;
        Ok(response
            .revisions
            .into_iter()
            .map(|revision| Revision {
                name: revision.name,
                created_at: revision.created_at,
                updated_at: revision.updated_at,
            })
            .collect())
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
    use super::Revision;

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
}
