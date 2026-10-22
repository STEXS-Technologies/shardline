//! Optional, additive index of verified immutable manifest subjects.
use rusqlite::{OptionalExtension, params};

use super::{LocalIndexStore, LocalIndexStoreError};
use crate::OciManifestSubject;

const TABLE: &str = "shardline_oci_manifest_subjects_v1";
const MAX_PAGE: usize = 1000;

pub(super) fn forget_reclaimed_manifest(
    transaction: &rusqlite::Transaction<'_>,
    key: &crate::OciObjectKey,
) -> Result<(), LocalIndexStoreError> {
    if key.kind != crate::OciObjectKind::Manifest {
        return Ok(());
    }
    let exists = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [TABLE],
        |row| row.get::<_, bool>(0),
    )?;
    if exists {
        transaction.execute(
            "DELETE FROM shardline_oci_manifest_subjects_v1 WHERE scope_namespace=?1 AND repository=?2 AND manifest_digest=?3",
            params![key.scope_namespace, key.repository, key.digest_hex])?;
    }
    Ok(())
}

fn conflict() -> LocalIndexStoreError {
    LocalIndexStoreError::ReliabilityEventConflict("OCI manifest subject index mismatch".to_owned())
}

impl LocalIndexStore {
    /// Records a subject only after the caller verifies the immutable manifest.
    /// Identical retries do not rewrite rows; conflicting mappings fail closed.
    /// Older binaries can ignore this additive, derived table.
    ///
    /// # Errors
    /// Returns database, invalid digest, corruption or immutable mapping errors.
    pub fn retain_oci_manifest_subject(
        &self,
        entry: &OciManifestSubject,
    ) -> Result<(), LocalIndexStoreError> {
        self.retain_subject(entry, false)
    }

    /// Backfills verified bytes without resurrecting a physically reclaimed
    /// manifest's derived mapping. Publication uses the unconditional method.
    ///
    /// # Errors
    /// Returns database, evidence verification or immutable mapping errors.
    pub fn reconcile_oci_manifest_subject(
        &self,
        entry: &OciManifestSubject,
    ) -> Result<(), LocalIndexStoreError> {
        self.retain_subject(entry, true)
    }

    fn retain_subject(
        &self,
        entry: &OciManifestSubject,
        reconciliation: bool,
    ) -> Result<(), LocalIndexStoreError> {
        if !entry.valid() {
            return Err(conflict());
        }
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let key = crate::OciObjectKey {
            scope_namespace: entry.scope_namespace.clone(),
            repository: entry.repository.clone(),
            kind: crate::OciObjectKind::Manifest,
            digest_hex: entry.digest_hex.clone(),
        };
        if reconciliation
            && super::oci_objects::load_oci_evidence(&transaction, &key)?
                .events()
                .last()
                .is_some_and(|head| {
                    head.after.state == shardline_reliability::OciObjectLifecycleState::Reclaimed
                })
        {
            return Ok(());
        }
        transaction.execute_batch("CREATE TABLE IF NOT EXISTS shardline_oci_manifest_subjects_v1 (
            scope_namespace TEXT NOT NULL COLLATE BINARY,
            repository TEXT NOT NULL COLLATE BINARY,
            manifest_digest TEXT NOT NULL COLLATE BINARY,
            subject_digest TEXT NOT NULL COLLATE BINARY,
            commitment TEXT NOT NULL,
            PRIMARY KEY (scope_namespace, repository, manifest_digest)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS shardline_oci_manifest_subject_lookup_v1
        ON shardline_oci_manifest_subjects_v1(scope_namespace, repository, subject_digest, manifest_digest);")?;
        let expected = entry.commitment()?;
        let existing: Option<(String, String)> = transaction.query_row(
            "SELECT CASE WHEN length(subject_digest)<=64 THEN subject_digest ELSE 'invalid' END,
                    CASE WHEN length(commitment)<=128 THEN commitment ELSE '' END FROM shardline_oci_manifest_subjects_v1
             WHERE scope_namespace=?1 AND repository=?2 AND manifest_digest=?3",
            params![entry.scope_namespace, entry.repository, entry.digest_hex],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;
        if let Some((subject, commitment)) = existing {
            if subject != entry.subject_digest_hex.as_deref().unwrap_or("")
                || commitment != expected
            {
                return Err(conflict());
            }
        } else {
            transaction.execute(
                "INSERT INTO shardline_oci_manifest_subjects_v1 VALUES (?1,?2,?3,?4,?5)",
                params![
                    entry.scope_namespace,
                    entry.repository,
                    entry.digest_hex,
                    entry.subject_digest_hex.as_deref().unwrap_or(""),
                    expected
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Reads a bounded, verified page in immutable manifest digest order.
    /// `subject` selects a particular subject; `None` inventories all mappings
    /// for reconciliation. Missing tables return an empty page without writes.
    /// A derived mapping never replaces the publication/deletion visibility check.
    ///
    /// # Errors
    /// Returns database, invalid page bound or corruption errors.
    pub fn list_oci_manifest_subjects(
        &self,
        scope: &str,
        repository: &str,
        subject: Option<&str>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<OciManifestSubject>, LocalIndexStoreError> {
        if limit == 0 || limit > MAX_PAGE {
            return Err(conflict());
        }
        let connection = self.open_connection()?;
        if connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
                [TABLE],
                |_| Ok(()),
            )
            .optional()?
            .is_none()
        {
            return Ok(Vec::new());
        }
        // Separate statements keep subject lookup on its index instead
        // of hiding the indexed equality behind an optional-filter OR clause.
        let (sql, filter) = subject.map_or(
            ("SELECT CASE WHEN length(manifest_digest)=64 THEN manifest_digest ELSE '' END,
                     CASE WHEN length(subject_digest)<=64 THEN subject_digest ELSE 'invalid' END,
                     CASE WHEN length(commitment)<=128 THEN commitment ELSE '' END FROM shardline_oci_manifest_subjects_v1
              WHERE scope_namespace=?1 AND repository=?2 AND ?3='' AND manifest_digest>?4
              ORDER BY manifest_digest LIMIT ?5", ""), |subject| {
            ("SELECT CASE WHEN length(manifest_digest)=64 THEN manifest_digest ELSE '' END,
                     CASE WHEN length(subject_digest)<=64 THEN subject_digest ELSE 'invalid' END,
                     CASE WHEN length(commitment)<=128 THEN commitment ELSE '' END FROM shardline_oci_manifest_subjects_v1
              WHERE scope_namespace=?1 AND repository=?2 AND subject_digest=?3 AND manifest_digest>?4
              ORDER BY manifest_digest LIMIT ?5", subject)
        });
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map(
            params![scope, repository, filter, after.unwrap_or(""), limit as i64],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        let mut entries = Vec::with_capacity(limit);
        for row in rows {
            let (digest_hex, stored_subject, stored) = row?;
            let entry = OciManifestSubject {
                scope_namespace: scope.to_owned(),
                repository: repository.to_owned(),
                digest_hex,
                subject_digest_hex: if stored_subject.is_empty() {
                    None
                } else {
                    Some(stored_subject)
                },
            };
            if !entry.valid() || entry.commitment()? != stored {
                return Err(conflict());
            }
            entries.push(entry);
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn entry(digit: char, subject: Option<char>) -> OciManifestSubject {
        OciManifestSubject {
            scope_namespace: "scope".to_owned(),
            repository: "repo".to_owned(),
            digest_hex: digit.to_string().repeat(64),
            subject_digest_hex: subject.map(|subject_digit| subject_digit.to_string().repeat(64)),
        }
    }

    #[tokio::test]
    async fn subject_index_cleanup_requires_successful_manifest_reclamation() {
        use crate::{OciObjectKey, OciObjectKind, OciObjectStore as _};
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(root.path().to_owned());
        let mapping = entry('1', Some('a'));
        let unrelated = entry('2', Some('a'));
        store.retain_oci_manifest_subject(&mapping).unwrap();
        store.retain_oci_manifest_subject(&unrelated).unwrap();
        let key = OciObjectKey {
            scope_namespace: mapping.scope_namespace.clone(),
            repository: mapping.repository.clone(),
            kind: OciObjectKind::Manifest,
            digest_hex: mapping.digest_hex.clone(),
        };
        store.delete_oci_object(&key).await.unwrap();
        let tombstone = store
            .list_oci_object_tombstones()
            .await
            .unwrap()
            .pop()
            .unwrap();
        let mut stale = tombstone.clone();
        stale.deleted_at_unix_seconds += 1;
        assert!(
            !store
                .delete_oci_object_tombstone_if_unchanged(&stale)
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 10)
                .unwrap()
                .len(),
            2
        );
        let connection = store.open_connection().unwrap();
        connection.execute_batch("CREATE TRIGGER fail_subject_cleanup BEFORE DELETE ON shardline_oci_manifest_subjects_v1 BEGIN SELECT RAISE(ABORT, 'injected lookup cleanup failure'); END;").unwrap();
        assert!(
            store
                .delete_oci_object_tombstone_if_unchanged(&tombstone)
                .await
                .is_err()
        );
        assert!(store.oci_object_is_deleted(&key).await.unwrap());
        assert_eq!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 10)
                .unwrap()
                .len(),
            2
        );
        store.verify_reliability_events().unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_subject_cleanup")
            .unwrap();
        assert!(
            store
                .delete_oci_object_tombstone_if_unchanged(&tombstone)
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 10)
                .unwrap(),
            vec![unrelated]
        );
        store.reconcile_oci_manifest_subject(&mapping).unwrap();
        assert_eq!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 10)
                .unwrap()
                .len(),
            1,
            "stale reconciliation must not recreate a reclaimed mapping"
        );
        store.publish_oci_object(&key, &[]).await.unwrap();
        store.retain_oci_manifest_subject(&mapping).unwrap();
        assert_eq!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 10)
                .unwrap()
                .len(),
            2
        );
        store.verify_reliability_events().unwrap();
    }

    #[test]
    fn subject_index_persists_pages_and_preserves_missing_legacy_state() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(root.path().to_owned());
        assert!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 1)
                .unwrap()
                .is_empty()
        );
        let created: bool = store.open_connection().unwrap().query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='shardline_oci_manifest_subjects_v1')",
            [], |row| row.get(0)).unwrap();
        assert!(!created, "a legacy read must not create the derived table");
        let first = entry('1', Some('a'));
        let second = entry('2', None);
        let third = entry('3', Some('a'));
        for value in [&third, &first, &second, &first] {
            store.retain_oci_manifest_subject(value).unwrap();
        }
        let reopened = LocalIndexStore::open(root.path().to_owned());
        assert_eq!(
            reopened
                .list_oci_manifest_subjects("scope", "repo", None, None, 2)
                .unwrap(),
            vec![first.clone(), second.clone()]
        );
        assert_eq!(
            reopened
                .list_oci_manifest_subjects("scope", "repo", None, Some(&second.digest_hex), 2)
                .unwrap(),
            vec![third.clone()]
        );
        assert_eq!(
            reopened
                .list_oci_manifest_subjects(
                    "scope",
                    "repo",
                    first.subject_digest_hex.as_deref(),
                    None,
                    10
                )
                .unwrap(),
            vec![first, third]
        );
        assert!(
            reopened
                .list_oci_manifest_subjects("scope", "other", None, None, 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            reopened
                .list_oci_manifest_subjects("other", "repo", None, None, 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            reopened
                .list_oci_manifest_subjects("scope", "repo", None, None, 1001)
                .is_err()
        );
        let read_only = LocalIndexStore::open_read_only(root.path().to_owned());
        assert_eq!(
            read_only
                .list_oci_manifest_subjects("scope", "repo", None, None, 10)
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn subject_index_rejects_conflicts_corruption_and_invalid_digests() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(root.path().to_owned());
        let value = entry('1', Some('a'));
        store.retain_oci_manifest_subject(&value).unwrap();
        assert!(
            store
                .retain_oci_manifest_subject(&entry('1', None))
                .is_err()
        );
        assert!(
            store
                .retain_oci_manifest_subject(&entry('G', None))
                .is_err()
        );
        store
            .open_connection()
            .unwrap()
            .execute(
                "UPDATE shardline_oci_manifest_subjects_v1 SET subject_digest=?1",
                ["b".repeat(64)],
            )
            .unwrap();
        assert!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 10)
                .is_err()
        );
        assert!(store.retain_oci_manifest_subject(&value).is_err());
        store
            .open_connection()
            .unwrap()
            .execute(
                "UPDATE shardline_oci_manifest_subjects_v1 SET subject_digest=zeroblob(1048576)",
                [],
            )
            .unwrap();
        assert!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 10)
                .is_err()
        );
    }
}
