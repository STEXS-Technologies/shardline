//! Recognize control evidence without changing arbitrary legacy payloads.

use shardline_index::RemoteDeleteReceipt;
use shardline_storage::ObjectKey;

use crate::{
    ServerObjectStore,
    task_tracking::{TASK_EVIDENCE_PREFIX, TaskEvidenceStore, TaskRunIdentity},
};

struct RecognizedRun {
    identity: TaskRunIdentity,
    manifest: Vec<u8>,
}

/// Reuses the most recent immutable legacy manifest within one payload copy.
/// Keeping one manifest retains the existing 64 KiB read bound without growing
/// a cache with the source inventory. Failed recognition is never cached.
pub(super) struct EvidenceInventory {
    store: TaskEvidenceStore,
    recognized: Option<RecognizedRun>,
}

impl EvidenceInventory {
    pub(super) fn new(source: &ServerObjectStore) -> Self {
        Self {
            store: TaskEvidenceStore::from_objects(source.clone()),
            recognized: None,
        }
    }

    pub(super) fn is_task_evidence(&mut self, key: &ObjectKey) -> bool {
        let Some(suffix) = key.as_str().strip_prefix(TASK_EVIDENCE_PREFIX) else {
            return false;
        };
        if suffix.starts_with("delete-replies/") {
            return is_saved_delete_reply(&self.store, suffix);
        }
        let id = if let Some(id) = suffix
            .strip_prefix("manifests/")
            .or_else(|| suffix.strip_prefix("retiring/"))
            .or_else(|| suffix.strip_prefix("scope-bindings/"))
        {
            id
        } else if let Some(run) = suffix.strip_prefix("runs/") {
            let Some((id, _rest)) = run.split_once('/') else {
                return false;
            };
            id
        } else {
            return false;
        };
        if id.is_empty() || id.len() > 128 || id.contains('/') {
            return false;
        }
        // List-only callers retain their existing behavior when evidence cannot
        // be read. Recognition never makes source writes or database access mandatory.
        if !self
            .recognized
            .as_ref()
            .is_some_and(|run| run.identity.id == id)
        {
            let bytes = self
                .store
                .read(&format!("manifests/{id}"), 65536)
                .ok()
                .flatten()
                .or_else(|| {
                    self.store
                        .read(&format!("retiring/{id}"), 65536)
                        .ok()
                        .flatten()
                });
            let Some(bytes) = bytes else {
                return false;
            };
            let Ok(identity) = serde_json::from_slice::<TaskRunIdentity>(&bytes) else {
                return false;
            };
            if !(identity.schema == 1
                && identity.id == id
                && identity.scope.definition == identity.definition.fingerprint())
            {
                return false;
            }
            self.recognized = Some(RecognizedRun {
                identity,
                manifest: bytes,
            });
        }
        let Some(run) = &self.recognized else {
            return false;
        };
        if suffix.starts_with("scope-bindings/") {
            matches!(
                self.store.history_scope_binding_match_for_manifest(
                    &run.identity,
                    run.identity.deployment,
                    &run.manifest
                ),
                Ok(Some(_))
            )
        } else {
            true
        }
    }
}

fn is_saved_delete_reply(store: &TaskEvidenceStore, suffix: &str) -> bool {
    let Some((namespace, name)) = suffix
        .strip_prefix("delete-replies/")
        .and_then(|path| path.split_once("/replies/"))
    else {
        return false;
    };
    if namespace.len() != 64 || name.len() != 69 || !name.ends_with(".json") {
        return false;
    }
    let Ok(Some(bytes)) = store.read(suffix, 16 * 1024) else {
        return false;
    };
    let Ok(receipt) = serde_json::from_slice::<RemoteDeleteReceipt>(&bytes) else {
        return false;
    };
    receipt.verified_namespace().ok() == Some(namespace)
        && name == format!("{}.json", blake3::hash(&bytes).to_hex())
}

#[cfg(test)]
mod tests;
