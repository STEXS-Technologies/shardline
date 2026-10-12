//! Opaque maintenance evidence, separate from payload object inventory.
//!
//! This optional table deliberately does not change the domain migration ledger:
//! older binaries ignore it. Artifact codecs and verification belong to callers.
use rusqlite::{Connection, OptionalExtension, params};
use std::ops::ControlFlow;

use super::{LocalIndexStore, LocalIndexStoreError};

fn exists(connection: &Connection) -> Result<bool, LocalIndexStoreError> {
    Ok(connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'shardline_task_artifacts_v1'",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

impl LocalIndexStore {
    /// Checks an exact artifact key without loading its bytes or creating its table.
    ///
    /// # Errors
    /// Returns database failures.
    pub fn contains_task_artifact(&self, key: &str) -> Result<bool, LocalIndexStoreError> {
        let connection = self.open_connection()?;
        if !exists(&connection)? {
            return Ok(false);
        }
        Ok(connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM shardline_task_artifacts_v1 WHERE artifact_key = ?1)",
            [key],
            |row| row.get(0),
        )?)
    }

    /// Publishes opaque task evidence immutably in the existing metadata database.
    /// Repeating identical bytes succeeds; reusing a key for different bytes fails.
    /// No artifact encoding, signature or execution ownership is inferred here.
    ///
    /// # Errors
    /// Returns database failures or conflicting publication.
    pub fn publish_task_artifact(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<(), LocalIndexStoreError> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS shardline_task_artifacts_v1 (
                artifact_key TEXT PRIMARY KEY COLLATE BINARY,
                payload BLOB NOT NULL
            ) WITHOUT ROWID;",
        )?;
        transaction.execute(
            "INSERT INTO shardline_task_artifacts_v1 (artifact_key, payload)
             VALUES (?1, ?2) ON CONFLICT (artifact_key) DO NOTHING",
            params![key, bytes],
        )?;
        // Compare inside SQLite, avoiding allocation of a conflicting large blob.
        let identical: bool = transaction.query_row(
            "SELECT payload = ?2 FROM shardline_task_artifacts_v1 WHERE artifact_key = ?1",
            params![key, bytes],
            |row| row.get(0),
        )?;
        if !identical {
            return Err(std::io::Error::other("task artifact publication conflict").into());
        }
        transaction.commit()?;
        Ok(())
    }

    /// Reads opaque evidence without allocating a blob above the caller's bound.
    /// An older database without the optional artifact table returns absence.
    ///
    /// # Errors
    /// Returns database failures or evidence exceeding `maximum_bytes`.
    pub fn read_task_artifact(
        &self,
        key: &str,
        maximum_bytes: u64,
    ) -> Result<Option<Vec<u8>>, LocalIndexStoreError> {
        let connection = self.open_connection()?;
        if !exists(&connection)? {
            return Ok(None);
        }
        let bound = i64::try_from(maximum_bytes).unwrap_or(i64::MAX);
        let row: Option<(i64, Option<Vec<u8>>)> = connection
            .query_row(
                "SELECT length(payload), CASE WHEN length(payload) <= ?2 THEN payload END
                 FROM shardline_task_artifacts_v1 WHERE artifact_key = ?1",
                params![key, bound],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((length, bytes)) = row else {
            return Ok(None);
        };
        let observed_bytes = super::i64_to_u64(length)?;
        let Some(bytes) = bytes else {
            return Err(LocalIndexStoreError::MetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            });
        };
        Ok(Some(bytes))
    }

    /// Visits artifact keys and lengths in lexical order using pages of 64 rows.
    /// The callback may remove visited artifacts. Concurrent inserts behind the
    /// cursor can appear on the next inspection; this is not a snapshot or lease.
    /// Prefixes are literal, including SQL wildcard characters.
    ///
    /// # Errors
    /// Returns database or callback failures.
    pub fn visit_task_artifacts<E, F>(&self, prefix: &str, visit: F) -> Result<(), E>
    where
        E: From<LocalIndexStoreError>,
        F: FnMut(&str, u64) -> Result<(), E>,
    {
        self.visit_task_artifacts_after(prefix, None, visit)
    }

    /// Visits literal-prefix artifacts strictly after an optional full key.
    /// The cursor need not exist; deletion between pages does not reset progress.
    /// Uses the same bounded pages and callback semantics as [`Self::visit_task_artifacts`].
    ///
    /// # Errors
    /// Returns database or callback failures.
    pub fn visit_task_artifacts_after<E, F>(
        &self,
        prefix: &str,
        after: Option<&str>,
        mut visit: F,
    ) -> Result<(), E>
    where
        E: From<LocalIndexStoreError>,
        F: FnMut(&str, u64) -> Result<(), E>,
    {
        self.visit_task_artifacts_until(prefix, after, |key, length| {
            visit(key, length)?;
            Ok(ControlFlow::Continue(()))
        })
    }

    /// Visits ordered artifacts until the callback returns [`ControlFlow::Break`].
    /// Retains literal-prefix filtering, cursor seeking and 64-row storage pages.
    /// Stopping does not mutate history or persist a cursor.
    ///
    /// # Errors
    /// Returns database or callback failures.
    pub fn visit_task_artifacts_until<E, F>(
        &self,
        prefix: &str,
        after: Option<&str>,
        mut visit: F,
    ) -> Result<(), E>
    where
        E: From<LocalIndexStoreError>,
        F: FnMut(&str, u64) -> Result<ControlFlow<()>, E>,
    {
        let connection = self.open_connection()?;
        if !exists(&connection)? {
            return Ok(());
        }
        let mut after = after.map(str::to_owned);
        loop {
            let page = {
                let query = if after.is_some() {
                    "SELECT artifact_key, length(payload) FROM shardline_task_artifacts_v1
                     WHERE artifact_key >= ?1 AND artifact_key > ?2
                     ORDER BY artifact_key COLLATE BINARY LIMIT 64"
                } else {
                    "SELECT artifact_key, length(payload) FROM shardline_task_artifacts_v1
                     WHERE artifact_key >= ?1 AND ?2 IS NULL
                     ORDER BY artifact_key COLLATE BINARY LIMIT 64"
                };
                let mut statement = connection
                    .prepare(query)
                    .map_err(LocalIndexStoreError::from)?;
                let rows = statement
                    .query_map(params![prefix, after], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                    })
                    .map_err(LocalIndexStoreError::from)?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(LocalIndexStoreError::from)?
            };
            if page.is_empty() {
                return Ok(());
            }
            for (key, length) in page {
                if !key.starts_with(prefix) {
                    return Ok(());
                }
                if visit(&key, super::i64_to_u64(length)?)?.is_break() {
                    return Ok(());
                }
                after = Some(key);
            }
        }
    }

    /// Removes task evidence during verified retention, without touching payloads.
    /// Returns whether a row existed. Missing optional tables remain untouched.
    ///
    /// # Errors
    /// Returns database failures.
    pub fn delete_task_artifact(&self, key: &str) -> Result<bool, LocalIndexStoreError> {
        let connection = self.open_connection()?;
        if !exists(&connection)? {
            return Ok(false);
        }
        Ok(connection.execute(
            "DELETE FROM shardline_task_artifacts_v1 WHERE artifact_key = ?1",
            [key],
        )? != 0)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn immutable_artifacts_survive_reopen_and_reject_conflicts_and_large_reads() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(root.path().to_owned());
        assert_eq!(store.read_task_artifact("run", 0).unwrap(), None);
        assert!(!exists(&store.open_connection().unwrap()).unwrap());
        store.publish_task_artifact("run", b"signed bytes").unwrap();
        store.publish_task_artifact("run", b"signed bytes").unwrap();
        assert!(store.publish_task_artifact("run", b"different").is_err());
        assert!(matches!(
            store.read_task_artifact("run", 3),
            Err(LocalIndexStoreError::MetadataTooLarge {
                observed_bytes: 12,
                maximum_bytes: 3
            })
        ));
        let reopened = LocalIndexStore::open(root.path().to_owned());
        assert_eq!(
            reopened.read_task_artifact("run", 12).unwrap(),
            Some(b"signed bytes".to_vec())
        );
    }

    #[test]
    fn literal_prefix_scan_pages_and_allows_deletion_in_callback() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(root.path().to_owned());
        for index in 0..130 {
            store
                .publish_task_artifact(&format!("_task%/{index:03}"), b"opaque")
                .unwrap();
        }
        store
            .publish_task_artifact("_taskX/other", b"keep")
            .unwrap();
        let mut count = 0;
        store
            .visit_task_artifacts::<LocalIndexStoreError, _>("_task%/", |key, length| {
                assert_eq!(length, 6);
                assert!(store.delete_task_artifact(key)?);
                count += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(count, 130);
        assert_eq!(
            store.read_task_artifact("_taskX/other", 4).unwrap(),
            Some(b"keep".to_vec())
        );
    }

    #[test]
    fn concurrent_conflicting_publishers_cannot_replace_the_winning_bytes() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::new(root.path().to_owned()).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let handles = [b"first".as_slice(), b"second".as_slice()].map(|bytes| {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                (bytes, store.publish_task_artifact("same-key", bytes))
            })
        });
        let results = handles.map(|handle| handle.join().unwrap());
        assert_eq!(
            results.iter().filter(|(_, result)| result.is_ok()).count(),
            1
        );
        let winning = results.iter().find(|(_, result)| result.is_ok()).unwrap().0;
        assert_eq!(
            store
                .read_task_artifact("same-key", 100)
                .unwrap()
                .as_deref(),
            Some(winning)
        );
    }

    #[test]
    fn literal_prefix_scan_stops_and_resumes_after_a_deleted_cursor_across_pages() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(root.path().to_owned());
        for index in 0..130 {
            store
                .publish_task_artifact(&format!("_task%/{index:03}"), b"opaque")
                .unwrap();
        }
        store
            .publish_task_artifact("_taskX/other", b"keep")
            .unwrap();
        let mut inspected = Vec::new();
        store
            .visit_task_artifacts_until::<LocalIndexStoreError, _>("_task%/", None, |key, _| {
                inspected.push(key.to_owned());
                Ok(if inspected.len() == 65 {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                })
            })
            .unwrap();
        assert_eq!(
            inspected,
            (0..65)
                .map(|index| format!("_task%/{index:03}"))
                .collect::<Vec<_>>()
        );
        store.delete_task_artifact("_task%/063").unwrap();
        let mut keys = Vec::new();
        store
            .visit_task_artifacts_after::<LocalIndexStoreError, _>(
                "_task%/",
                Some("_task%/063"),
                |key, length| {
                    assert_eq!(length, 6);
                    keys.push(key.to_owned());
                    assert!(store.delete_task_artifact(key)?);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(
            keys,
            (64..130)
                .map(|index| format!("_task%/{index:03}"))
                .collect::<Vec<_>>()
        );
        let mut remaining = 0;
        store
            .visit_task_artifacts::<LocalIndexStoreError, _>("_task%/", |_, _| {
                remaining += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(remaining, 63);
        assert_eq!(
            store.read_task_artifact("_taskX/other", 4).unwrap(),
            Some(b"keep".to_vec())
        );
    }
}
