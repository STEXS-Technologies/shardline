//! Bounded metadata reads shared by history inspection and retention.

use super::TaskEvidenceStore;
use crate::ServerError;
use shardline_storage::ObjectPrefix;
use std::collections::BTreeMap;

/// One metadata page, never more than 64 bodies of at most 64 KiB each.
/// Legacy inventory can be unordered; keys outside the page use ordinary reads.
pub(super) struct ManifestReader<'store> {
    store: &'store TaskEvidenceStore,
    prefix: &'store ObjectPrefix,
    cursor: Option<String>,
    payloads: BTreeMap<String, Option<Vec<u8>>>,
}

impl<'store> ManifestReader<'store> {
    pub(super) fn new(
        store: &'store TaskEvidenceStore,
        prefix: &'store ObjectPrefix,
        after: Option<&str>,
    ) -> Self {
        Self {
            store,
            prefix,
            cursor: after.map(str::to_owned),
            payloads: BTreeMap::new(),
        }
    }

    pub(super) fn read(
        &mut self,
        enumerated_key: &str,
        suffix: &str,
    ) -> Result<Option<Vec<u8>>, ServerError> {
        // A malformed nested inventory key must retain the normal canonical
        // lookup behavior rather than supplying a different identity's bytes.
        if enumerated_key != TaskEvidenceStore::key(suffix)?.as_str() {
            return self.store.read(suffix, 65536);
        }
        if self.store.postgres_metadata.is_some() || self.store.local_metadata.is_some() {
            if !self.payloads.contains_key(enumerated_key)
                && self
                    .cursor
                    .as_deref()
                    .is_none_or(|after| enumerated_key > after)
            {
                let page = if let Some(postgres) = &self.store.postgres_metadata {
                    postgres
                        .bounded_page(self.prefix.as_str(), self.cursor.as_deref())?
                        .into_iter()
                        .map(|(key, artifact)| (key, artifact.bytes))
                        .collect()
                } else if let Some(local) = &self.store.local_metadata {
                    local
                        .bounded_task_artifact_page(self.prefix.as_str(), self.cursor.as_deref())?
                } else {
                    Vec::new()
                };
                self.cursor = page.last().map(|(key, _)| key.clone());
                self.payloads = page.into_iter().collect();
            }
            if let Some(payload) = self.payloads.remove(enumerated_key) {
                let bytes = payload.ok_or(ServerError::RequestBodyTooLarge)?;
                if self
                    .store
                    .read_objects(suffix, 65536)?
                    .is_some_and(|legacy| legacy != bytes)
                {
                    return Err(ServerError::InvalidContentHash);
                }
                return Ok(Some(bytes));
            }
        }
        self.store.read(suffix, 65536)
    }
}
