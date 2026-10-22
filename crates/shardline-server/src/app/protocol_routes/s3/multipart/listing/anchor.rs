//! Retained listing positions contain no upload bytes or execution authority.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalAnchor {
    scope: String,
    bucket: String,
    key: String,
    upload_id: String,
    initiated: Option<u64>,
}

pub(super) async fn retain(
    state: &Arc<AppState>,
    scope: &str,
    bucket: &str,
    entry: &page::UploadEntry,
) -> Result<(), S3Error> {
    if durable_sessions_enabled(state) || entry.group {
        return Ok(());
    }
    let anchor = LocalAnchor {
        scope: scope.to_owned(),
        bucket: bucket.to_owned(),
        key: entry.key.clone(),
        upload_id: entry.id.clone(),
        initiated: entry.initiated,
    };
    let bytes = serde_json::to_vec(&anchor).map_err(|_invalid_anchor| S3Error::internal())?;
    let root = state.config.root_dir().to_owned();
    let now = shardline_protocol::unix_now_seconds_lossy();
    let expires = now
        .checked_add(state.config.s3_upload_session_ttl_seconds().get())
        .ok_or_else(S3Error::internal)?;
    crate::admission::blocking_work(&state.pools.blocking_io, move || {
        shardline_index::LocalIndexStore::open(root).retain_s3_listing_anchor(
            &anchor.upload_id,
            &bytes,
            now,
            expires,
        )?;
        Ok(())
    })
    .await?;
    Ok(())
}

pub(super) async fn read(
    state: &Arc<AppState>,
    scope: &str,
    bucket: &str,
    key: &str,
    id: &str,
) -> Result<Option<Option<u64>>, S3Error> {
    let root = state.config.root_dir().to_owned();
    let upload_id = id.to_owned();
    let bytes = crate::admission::blocking_work(&state.pools.blocking_io, move || {
        Ok(shardline_index::LocalIndexStore::open(root)
            .read_s3_listing_anchor(&upload_id, shardline_protocol::unix_now_seconds_lossy())?)
    })
    .await?;
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    decode_position(&bytes, scope, bucket, key, id)
}

fn decode_position(
    bytes: &[u8],
    scope: &str,
    bucket: &str,
    key: &str,
    id: &str,
) -> Result<Option<Option<u64>>, S3Error> {
    let anchor: LocalAnchor =
        serde_json::from_slice(bytes).map_err(|_invalid_anchor| S3Error::internal())?;
    Ok((anchor.scope == scope
        && anchor.bucket == bucket
        && anchor.key == key
        && anchor.upload_id == id)
        .then_some(anchor.initiated))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_positions_require_every_namespace_and_upload_binding() {
        let bytes = br#"{"scope":"owner/repo","bucket":"repo.locks","key":"asset","upload_id":"old-id","initiated":42}"#;
        assert_eq!(
            decode_position(bytes, "owner/repo", "repo.locks", "asset", "old-id").unwrap(),
            Some(Some(42))
        );
        for (scope, bucket, key, id) in [
            ("other/repo", "repo.locks", "asset", "old-id"),
            ("owner/repo", "other.locks", "asset", "old-id"),
            ("owner/repo", "repo.locks", "other", "old-id"),
            ("owner/repo", "repo.locks", "asset", "other-id"),
        ] {
            assert_eq!(
                decode_position(bytes, scope, bucket, key, id).unwrap(),
                None
            );
        }
        assert!(decode_position(b"{}", "owner/repo", "repo.locks", "asset", "old-id").is_err());
    }
}
