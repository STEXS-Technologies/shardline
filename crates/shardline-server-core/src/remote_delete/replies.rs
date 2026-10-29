//! Bounded saved-reply recovery. This module never deletes a user object.

use shardline_index::{
    PostgresIndexStore, PostgresMetadataStoreError, RemoteDeleteReceipt,
    RemoteDeleteReceiptSettlement,
};
use shardline_protocol::ShardlineHash;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore};

use super::GuardedRemoteDeleteError;
use crate::{BoundedObjectReadError, ServerObjectStore};

#[cfg(test)]
mod tests;

const PREFIX: &str = "_shardline_tasks/v1/delete-replies/";
const PAGE_SIZE: usize = 64;
const MAX_REPLY_BYTES: u64 = 16 * 1024;

fn invalid(detail: &str) -> GuardedRemoteDeleteError {
    PostgresMetadataStoreError::Unsupported(detail.to_owned()).into()
}

fn object_key(value: &str) -> Result<ObjectKey, GuardedRemoteDeleteError> {
    ObjectKey::parse(value).map_err(|_error| invalid("invalid saved reply object key"))
}

fn reply_key(namespace: &str, bytes: &[u8]) -> Result<ObjectKey, GuardedRemoteDeleteError> {
    object_key(&format!(
        "{PREFIX}{namespace}/replies/{}.json",
        blake3::hash(bytes).to_hex()
    ))
}

fn integrity(bytes: &[u8]) -> Result<ObjectIntegrity, GuardedRemoteDeleteError> {
    let length =
        u64::try_from(bytes.len()).map_err(|_error| invalid("saved reply length overflow"))?;
    Ok(ObjectIntegrity::new(
        ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes()),
        length,
    ))
}

pub(super) fn save(
    objects: &ServerObjectStore,
    receipt: &RemoteDeleteReceipt,
) -> Result<ObjectKey, GuardedRemoteDeleteError> {
    let bytes = serde_json::to_vec(receipt)
        .map_err(|_error| invalid("remote delete receipt encoding failed"))?;
    let key = reply_key(receipt.verified_namespace()?, &bytes)?;
    ObjectStore::put_if_absent(
        objects,
        &key,
        ObjectBody::from_slice(&bytes),
        &integrity(&bytes)?,
    )?;
    Ok(key)
}

fn read_bounded(
    objects: &ServerObjectStore,
    key: &ObjectKey,
    maximum: u64,
) -> Result<Option<Vec<u8>>, GuardedRemoteDeleteError> {
    match objects.read_object_if_present(key, maximum) {
        Ok(Some((bytes, _length))) if !bytes.is_empty() => Ok(Some(bytes)),
        Ok(_) | Err(BoundedObjectReadError::ObjectTooLarge) => Ok(None),
        Err(BoundedObjectReadError::Store(error)) => Err(error.into()),
    }
}

/// Result of one bounded saved-reply recovery pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RemoteDeleteReplyRecovery {
    /// Saved replies examined, including invalid evidence retained for inspection.
    pub examined: usize,
    /// Exact pending markers settled using verified observed replies.
    pub settled: usize,
    /// Replies removed because metadata already has no pending delete for them.
    pub retired: usize,
    /// Replies retained because they are invalid, foreign or do not settle.
    pub retained: usize,
}

/// Settles at most 64 saved successful replies for the actual metadata namespace.
/// A durable storage cursor advances past invalid or foreign records and wraps,
/// so they cannot permanently hide later replies. No user-object DELETE, work
/// ownership acquisition, replacement connection or missing-reply inference occurs.
/// The caller must retain its existing maintenance ownership session.
///
/// # Errors
/// Returns database or object-storage failure. Malformed evidence is retained;
/// failure does not authorize replaying a delete or resuming lost work ownership.
pub async fn recover_saved_remote_delete_replies(
    index: &PostgresIndexStore,
    objects: &ServerObjectStore,
) -> Result<RemoteDeleteReplyRecovery, GuardedRemoteDeleteError> {
    let Some(namespace) = index.remote_delete_receipt_namespace().await? else {
        return Ok(RemoteDeleteReplyRecovery::default());
    };
    let prefix_text = format!("{PREFIX}{namespace}/replies/");
    let prefix = ObjectPrefix::parse(&prefix_text)
        .map_err(|_error| invalid("invalid saved reply prefix"))?;
    let cursor_key = object_key(&format!("{PREFIX}{namespace}/cursor"))?;
    let cursor = read_bounded(objects, &cursor_key, 4096)?.and_then(|bytes| {
        String::from_utf8(bytes).ok().and_then(|value| {
            let suffix = value.strip_prefix(&prefix_text)?;
            if suffix.is_empty() || suffix.contains('/') {
                return None;
            }
            ObjectKey::parse(&value).ok()
        })
    });
    let page = objects.list_flat_namespace_page(&prefix, cursor.as_ref(), PAGE_SIZE + 1)?;
    let mut report = RemoteDeleteReplyRecovery::default();
    for metadata in page.iter().take(PAGE_SIZE) {
        report.examined = report.examined.saturating_add(1);
        let receipt = read_bounded(objects, metadata.key(), MAX_REPLY_BYTES)?.and_then(|bytes| {
            let receipt: RemoteDeleteReceipt = serde_json::from_slice(&bytes).ok()?;
            if receipt.verified_namespace().ok()? != namespace
                || reply_key(&namespace, &bytes).ok()?.as_str() != metadata.key().as_str()
            {
                return None;
            }
            Some(receipt)
        });
        if let Some(receipt) = receipt {
            match index.reconcile_remote_delete_receipt(&receipt).await? {
                RemoteDeleteReceiptSettlement::Settled => {
                    report.settled = report.settled.saturating_add(1);
                    ObjectStore::delete_if_present(objects, metadata.key())?;
                    continue;
                }
                RemoteDeleteReceiptSettlement::NoPendingDelete => {
                    report.retired = report.retired.saturating_add(1);
                    ObjectStore::delete_if_present(objects, metadata.key())?;
                    continue;
                }
                RemoteDeleteReceiptSettlement::Unmatched => {}
            }
        }
        report.retained = report.retained.saturating_add(1);
        tracing::warn!(
            receipt_key = metadata.key().as_str(),
            "saved remote delete reply retained; no pending attempt was cleared"
        );
    }
    // Scan progress is never mutation evidence. A malformed cursor resets the
    // scan; advancing it cannot settle any pending marker.
    if page.len() > PAGE_SIZE
        && let Some(last) = page.get(PAGE_SIZE.saturating_sub(1))
    {
        let bytes = last.key().as_str().as_bytes();
        objects.put_overwrite(
            &cursor_key,
            ObjectBody::from_slice(bytes),
            &integrity(bytes)?,
        )?;
    } else if cursor.is_some() {
        ObjectStore::delete_if_present(objects, &cursor_key)?;
    }
    Ok(report)
}
