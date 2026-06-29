use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::error::HubApiError;
use crate::models::{
    LfsBatchOperation, LfsBatchRequest, LfsBatchResponse, LfsObjectActions, LfsObjectError,
    LfsObjectResponse,
};

/// In-memory LFS object store (stores uploaded objects).
#[derive(Debug, Clone, Default)]
pub struct LfsStore {
    inner: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl LfsStore {
    /// Creates a new empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Handles a batch request.
    ///
    /// # Errors
    ///
    /// Returns [`HubApiError::CasError`] on internal errors.
    pub async fn batch(
        &self,
        request: LfsBatchRequest,
    ) -> Result<LfsBatchResponse, HubApiError> {
        let inner = self.inner.read().await;

        let objects: Vec<LfsObjectResponse> = request
            .objects
            .iter()
            .map(|obj| {
                let exists = inner.contains_key(&obj.oid);
                let actions = match request.operation {
                    LfsBatchOperation::Download => {
                        if exists {
                            Some(LfsObjectActions {
                                download: Some(crate::models::LfsObjectAction {
                                    href: format!("/lfs/objects/{}", obj.oid),
                                    header: None,
                                    ssh: None,
                                }),
                                upload: None,
                                verify: None,
                            })
                        } else {
                            None
                        }
                    }
                    LfsBatchOperation::Upload => {
                        if exists {
                            // Already uploaded
                            Some(LfsObjectActions {
                                download: None,
                                upload: None,
                                verify: Some(crate::models::LfsObjectAction {
                                    href: format!("/lfs/objects/{}", obj.oid),
                                    header: None,
                                    ssh: None,
                                }),
                            })
                        } else {
                            Some(LfsObjectActions {
                                download: None,
                                upload: Some(crate::models::LfsObjectAction {
                                    href: format!("/lfs/objects/{}", obj.oid),
                                    header: None,
                                    ssh: None,
                                }),
                                verify: None,
                            })
                        }
                    }
                    LfsBatchOperation::Verify => {
                        if exists {
                            Some(LfsObjectActions {
                                download: None,
                                upload: None,
                                verify: Some(crate::models::LfsObjectAction {
                                    href: format!("/lfs/objects/{}", obj.oid),
                                    header: None,
                                    ssh: None,
                                }),
                            })
                        } else {
                            Some(LfsObjectActions {
                                download: None,
                                upload: Some(crate::models::LfsObjectAction {
                                    href: format!("/lfs/objects/{}", obj.oid),
                                    header: None,
                                    ssh: None,
                                }),
                                verify: None,
                            })
                        }
                    }
                };

                let error = if !exists && request.operation == LfsBatchOperation::Download {
                    Some(LfsObjectError {
                        code: 404,
                        message: "Object not found".to_owned(),
                    })
                } else {
                    None
                };

                LfsObjectResponse {
                    oid: obj.oid.clone(),
                    size: obj.size,
                    actions,
                    error,
                }
            })
            .collect();

        Ok(LfsBatchResponse {
            transfer: "basic".to_owned(),
            objects,
        })
    }

    /// Stores an LFS object.
    ///
    /// # Errors
    ///
    /// Returns [`HubApiError::Conflict`] if the object already exists with different content.
    pub async fn put_object(
        &self,
        oid: &str,
        data: Vec<u8>,
    ) -> Result<(), HubApiError> {
        let mut inner = self.inner.write().await;
        if let Some(existing) = inner.get(oid) {
            if existing.len() != data.len() {
                return Err(HubApiError::Conflict);
            }
        }
        inner.insert(oid.to_owned(), data);
        Ok(())
    }

    /// Returns an LFS object by OID.
    ///
    /// # Errors
    ///
    /// Returns [`HubApiError::NotFound`] if the object does not exist.
    pub async fn get_object(&self, oid: &str) -> Result<Vec<u8>, HubApiError> {
        let inner = self.inner.read().await;
        inner
            .get(oid)
            .cloned()
            .ok_or(HubApiError::NotFound)
    }
}
