//! Expiring multipart pagination positions; never upload execution authority.
use super::{LocalIndexStore, LocalIndexStoreError, helpers};
use rusqlite::{Connection, OptionalExtension, params};

const MAX_ANCHOR_BYTES: usize = 65536;
type StoredAnchor = (i64, i64, Option<Vec<u8>>, String);

fn exists(connection: &Connection) -> Result<bool, LocalIndexStoreError> {
    Ok(connection.query_row("SELECT 1 FROM sqlite_master WHERE type='table' AND name='shardline_s3_listing_anchors_v1'", [], |_| Ok(())).optional()?.is_some())
}

fn digest(id: &str, expires: i64, bytes: &[u8]) -> Result<String, LocalIndexStoreError> {
    Ok(shardline_reliability::canonical_state_digest(&(
        "shardline.s3.multipart-listing-anchor.v1",
        id,
        expires,
        bytes,
    ))?
    .as_str()
    .to_owned())
}

fn verify(id: &str, expires: i64, bytes: &[u8], stored: &str) -> Result<(), LocalIndexStoreError> {
    if digest(id, expires, bytes)? != stored {
        return Err(LocalIndexStoreError::ReliabilityEventConflict(
            "multipart listing anchor digest mismatch".to_owned(),
        ));
    }
    Ok(())
}

impl LocalIndexStore {
    /// Retains an immutable, bounded pagination position until its expiry.
    /// Identical retries do not extend retention or rewrite payloads. This
    /// optional additive table is ignored by older binaries.
    ///
    /// # Errors
    /// Returns SQLite, size, corruption or conflicting-position failures.
    pub fn retain_s3_listing_anchor(
        &self,
        id: &str,
        bytes: &[u8],
        now: u64,
        expires: u64,
    ) -> Result<(), LocalIndexStoreError> {
        if bytes.len() > MAX_ANCHOR_BYTES {
            return Err(LocalIndexStoreError::MetadataTooLarge {
                observed_bytes: bytes.len() as u64,
                maximum_bytes: MAX_ANCHOR_BYTES as u64,
            });
        }
        let now = helpers::u64_to_i64(now)?;
        let expires = helpers::u64_to_i64(expires)?;
        if expires <= now {
            return Err(std::io::Error::other("invalid listing anchor expiry").into());
        }
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        transaction.execute_batch("CREATE TABLE IF NOT EXISTS shardline_s3_listing_anchors_v1 (
            upload_id TEXT PRIMARY KEY COLLATE BINARY, expires_at INTEGER NOT NULL,
            payload BLOB NOT NULL, digest TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS shardline_s3_listing_anchors_expiry_v1 ON shardline_s3_listing_anchors_v1(expires_at);")?;
        transaction.execute(
            "DELETE FROM shardline_s3_listing_anchors_v1 WHERE upload_id IN (
                 SELECT upload_id FROM shardline_s3_listing_anchors_v1
                 WHERE expires_at <= ?1 ORDER BY expires_at LIMIT 128
             )",
            [now],
        )?;
        let existing: Option<(i64, bool, String)> = transaction.query_row(
            "SELECT expires_at, payload=?2, digest FROM shardline_s3_listing_anchors_v1 WHERE upload_id=?1", params![id, bytes], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).optional()?;
        if let Some((stored_expiry, same, stored_digest)) = existing {
            if !same {
                return Err(LocalIndexStoreError::ReliabilityEventConflict(
                    "multipart listing anchor conflict".to_owned(),
                ));
            }
            verify(id, stored_expiry, bytes, &stored_digest)?;
        } else {
            transaction.execute("INSERT INTO shardline_s3_listing_anchors_v1(upload_id, expires_at, payload, digest) VALUES (?1, ?2, ?3, ?4)", params![id, expires, bytes, digest(id, expires, bytes)?])?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Reads an unexpired, corruption-checked position without creating its table.
    /// Digests detect inconsistency, not a privileged metadata writer's forgery.
    ///
    /// # Errors
    /// Returns database, payload bounds or commitment failures.
    pub fn read_s3_listing_anchor(
        &self,
        id: &str,
        now: u64,
    ) -> Result<Option<Vec<u8>>, LocalIndexStoreError> {
        let connection = self.open_connection()?;
        if !exists(&connection)? {
            return Ok(None);
        }
        let stored: Option<StoredAnchor> = connection.query_row(
            "SELECT expires_at, length(payload), CASE WHEN length(payload)<=?2 THEN payload ELSE NULL END,
                    CASE WHEN length(digest)<=128 THEN digest ELSE '' END
             FROM shardline_s3_listing_anchors_v1 WHERE upload_id=?1",
            params![id, MAX_ANCHOR_BYTES as i64],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).optional()?;
        let Some((expires, size, bytes, stored_digest)) = stored else {
            return Ok(None);
        };
        let bytes = bytes.ok_or(LocalIndexStoreError::MetadataTooLarge {
            observed_bytes: helpers::i64_to_u64(size)?,
            maximum_bytes: MAX_ANCHOR_BYTES as u64,
        })?;
        verify(id, expires, &bytes, &stored_digest)?;
        if expires <= helpers::u64_to_i64(now)? {
            return Ok(None);
        }
        Ok(Some(bytes))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn anchors_survive_reopen_reject_changes_expire_and_prune() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(root.path().to_owned());
        assert!(store.read_s3_listing_anchor("id", 10).unwrap().is_none());
        store
            .retain_s3_listing_anchor("id", b"position", 10, 20)
            .unwrap();
        store
            .retain_s3_listing_anchor("id", b"position", 11, 30)
            .unwrap();
        assert!(
            store
                .retain_s3_listing_anchor("id", b"changed", 11, 30)
                .is_err()
        );
        let reopened = LocalIndexStore::open(root.path().to_owned());
        assert_eq!(
            reopened.read_s3_listing_anchor("id", 19).unwrap(),
            Some(b"position".to_vec())
        );
        assert!(reopened.read_s3_listing_anchor("id", 20).unwrap().is_none());
        reopened
            .retain_s3_listing_anchor("other", b"next", 20, 30)
            .unwrap();
        let connection = reopened.open_connection().unwrap();
        let count: i64 = connection
            .query_row(
                "SELECT count(*) FROM shardline_s3_listing_anchors_v1 WHERE upload_id='id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
        connection
            .execute(
                "UPDATE shardline_s3_listing_anchors_v1 SET payload=x'00' WHERE upload_id='other'",
                [],
            )
            .unwrap();
        assert!(reopened.read_s3_listing_anchor("other", 21).is_err());
    }

    #[test]
    fn readers_bound_corrupt_payloads_and_verify_expiry_before_trusting_it() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(root.path().to_owned());
        store
            .retain_s3_listing_anchor("oversized", b"position", 1, 20)
            .unwrap();
        store
            .retain_s3_listing_anchor("expiry", b"position", 1, 20)
            .unwrap();
        assert!(
            store
                .retain_s3_listing_anchor("rejected", &vec![0; MAX_ANCHOR_BYTES + 1], 1, 20)
                .is_err()
        );
        let reader = LocalIndexStore::open_read_only(root.path().to_owned());
        assert_eq!(
            reader.read_s3_listing_anchor("expiry", 2).unwrap(),
            Some(b"position".to_vec())
        );
        assert!(
            reader
                .retain_s3_listing_anchor("forbidden", b"position", 1, 20)
                .is_err()
        );
        let connection = store.open_connection().unwrap();
        connection.execute("UPDATE shardline_s3_listing_anchors_v1 SET payload=zeroblob(?1) WHERE upload_id='oversized'", [i64::try_from(MAX_ANCHOR_BYTES + 1).unwrap()]).unwrap();
        assert!(matches!(
            store.read_s3_listing_anchor("oversized", 2),
            Err(LocalIndexStoreError::MetadataTooLarge { .. })
        ));
        connection
            .execute(
                "UPDATE shardline_s3_listing_anchors_v1 SET expires_at=0 WHERE upload_id='expiry'",
                [],
            )
            .unwrap();
        assert!(store.read_s3_listing_anchor("expiry", 2).is_err());
    }
}
