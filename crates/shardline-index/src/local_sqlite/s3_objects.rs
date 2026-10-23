#[cfg(test)]
use rusqlite::Connection;
use rusqlite::{OptionalExtension, Transaction, params_from_iter};
use std::fmt::Write as _;

use super::{LocalIndexStore, LocalIndexStoreError, collect_rows, helpers};
use crate::s3_objects::s3_multipart_count_proof;
use crate::{S3ObjectEntry, S3ObjectIndexStore, S3ObjectScanStart, s3_prefix_successor};
use shardline_reliability::{
    OperationKind, S3ObjectLifecycleEvent, SnapshotEvidence, verify_and_append_snapshot_transition,
    verify_snapshot_event,
};

fn s3_object_entry_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<S3ObjectEntry> {
    let user_metadata_json: String = row.get("user_metadata")?;
    let user_metadata = serde_json::from_str(&user_metadata_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(S3ObjectEntry {
        scope_namespace: row.get("scope_namespace")?,
        object_key: row.get("object_key")?,
        file_id: row.get("file_id")?,
        size_bytes: helpers::i64_to_u64(row.get("size_bytes")?).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::new(e),
            )
        })?,
        content_hash: row.get("content_hash")?,
        etag: row.get("etag")?,
        user_metadata,
        updated_at_unix_seconds: row.get("updated_at_unix_seconds")?,
    })
}

fn multipart_count_from_row(row: &rusqlite::Row<'_>, entry: &S3ObjectEntry) -> Option<u32> {
    let part_count: Option<i64> = row.get("multipart_part_count").ok()?;
    let count = u32::try_from(part_count?).ok()?;
    let file_id: Option<String> = row.get("multipart_file_id").ok()?;
    let content_hash: Option<String> = row.get("multipart_content_hash").ok()?;
    let updated_at: Option<i64> = row.get("multipart_updated_at").ok()?;
    let proof: Option<String> = row.get("multipart_count_proof").ok()?;
    if file_id.as_deref() != Some(&entry.file_id)
        || content_hash.as_deref() != Some(&entry.content_hash)
        || updated_at != Some(entry.updated_at_unix_seconds)
    {
        return None;
    }
    s3_multipart_count_proof(
        &entry.scope_namespace,
        &entry.object_key,
        &entry.file_id,
        &entry.content_hash,
        count,
        entry.updated_at_unix_seconds,
    )
    .ok()
    .filter(|expected| proof.as_ref() == Some(expected))
    .map(|_| count)
}

fn persist_multipart_count_sql(
    transaction: &Transaction<'_>,
    entry: &S3ObjectEntry,
    part_count: Option<u32>,
) -> Result<(), LocalIndexStoreError> {
    if let Some(part_count) = part_count {
        let proof = s3_multipart_count_proof(
            &entry.scope_namespace,
            &entry.object_key,
            &entry.file_id,
            &entry.content_hash,
            part_count,
            entry.updated_at_unix_seconds,
        )
        .map_err(LocalIndexStoreError::Reliability)?;
        transaction.execute(
            "INSERT INTO shardline_s3_multipart_counts
                (scope_namespace, object_key, file_id, content_hash, part_count,
                 updated_at_unix_seconds, proof)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT (scope_namespace, object_key)
             DO UPDATE SET file_id = excluded.file_id,
                           content_hash = excluded.content_hash,
                           part_count = excluded.part_count,
                           updated_at_unix_seconds = excluded.updated_at_unix_seconds,
                           proof = excluded.proof",
            rusqlite::params![
                entry.scope_namespace,
                entry.object_key,
                entry.file_id,
                entry.content_hash,
                i64::from(part_count),
                entry.updated_at_unix_seconds,
                proof,
            ],
        )?;
    } else {
        transaction.execute(
            "DELETE FROM shardline_s3_multipart_counts
             WHERE scope_namespace = ?1 AND object_key = ?2",
            rusqlite::params![entry.scope_namespace, entry.object_key],
        )?;
    }
    Ok(())
}

/// Serializes S3 user metadata as JSON text for the index row.
fn user_metadata_to_json(
    user_metadata: &[(String, String)],
) -> Result<String, LocalIndexStoreError> {
    serde_json::to_string(user_metadata)
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))
}

fn upsert_s3_object_sql(
    transaction: &Transaction<'_>,
    entry: &S3ObjectEntry,
    part_count: Option<u32>,
) -> Result<(), LocalIndexStoreError> {
    let before = current_s3_object_sql(transaction, &entry.scope_namespace, &entry.object_key)?;
    transaction.execute(
        "INSERT INTO shardline_s3_objects (
            scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
            user_metadata, updated_at_unix_seconds
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT (scope_namespace, object_key)
         DO UPDATE SET
            file_id = excluded.file_id,
            size_bytes = excluded.size_bytes,
            content_hash = excluded.content_hash,
            etag = excluded.etag,
            user_metadata = excluded.user_metadata,
            updated_at_unix_seconds = excluded.updated_at_unix_seconds
         WHERE shardline_s3_objects.file_id IS NOT excluded.file_id
            OR shardline_s3_objects.size_bytes IS NOT excluded.size_bytes
            OR shardline_s3_objects.content_hash IS NOT excluded.content_hash
            OR shardline_s3_objects.etag IS NOT excluded.etag
            OR shardline_s3_objects.user_metadata IS NOT excluded.user_metadata
            OR shardline_s3_objects.updated_at_unix_seconds IS NOT excluded.updated_at_unix_seconds",
        rusqlite::params![
            entry.scope_namespace,
            entry.object_key,
            entry.file_id,
            helpers::u64_to_i64(entry.size_bytes)?,
            entry.content_hash,
            entry.etag,
            user_metadata_to_json(&entry.user_metadata)?,
            entry.updated_at_unix_seconds,
        ],
    )?;
    persist_multipart_count_sql(transaction, entry, part_count)?;
    record_s3_object_transition(transaction, before.as_ref(), Some(entry))?;
    Ok(())
}

fn compare_and_swap_s3_object_sql(
    transaction: &Transaction<'_>,
    expected: Option<&S3ObjectEntry>,
    replacement: &S3ObjectEntry,
    part_count: Option<u32>,
    update_part_count: bool,
) -> Result<bool, LocalIndexStoreError> {
    let preserved_part_count = if update_part_count {
        None
    } else {
        read_multipart_count_sql(transaction, expected.unwrap_or(replacement))?
    };
    if expected == Some(replacement) {
        // The caller holds an IMMEDIATE transaction, so the comparison and
        // evidence append retain the same writer exclusion as a real UPDATE.
        let current = current_s3_object_sql(
            transaction,
            &replacement.scope_namespace,
            &replacement.object_key,
        )?;
        if current.as_ref() != expected {
            return Ok(false);
        }
        if update_part_count {
            persist_multipart_count_sql(transaction, replacement, part_count)?;
        } else if let Some(part_count) = preserved_part_count {
            persist_multipart_count_sql(transaction, replacement, Some(part_count))?;
        }
        record_s3_object_transition(transaction, expected, Some(replacement))?;
        return Ok(true);
    }
    let replacement_metadata = user_metadata_to_json(&replacement.user_metadata)?;
    let changed = if let Some(expected) = expected {
        transaction.execute(
            "UPDATE shardline_s3_objects
             SET file_id = ?3, size_bytes = ?4, content_hash = ?5, etag = ?6,
                 user_metadata = ?7, updated_at_unix_seconds = ?8
             WHERE scope_namespace = ?1 AND object_key = ?2
               AND file_id = ?9 AND size_bytes = ?10 AND content_hash = ?11
               AND etag = ?12 AND user_metadata = ?13
               AND updated_at_unix_seconds = ?14",
            rusqlite::params![
                replacement.scope_namespace,
                replacement.object_key,
                replacement.file_id,
                helpers::u64_to_i64(replacement.size_bytes)?,
                replacement.content_hash,
                replacement.etag,
                replacement_metadata,
                replacement.updated_at_unix_seconds,
                expected.file_id,
                helpers::u64_to_i64(expected.size_bytes)?,
                expected.content_hash,
                expected.etag,
                user_metadata_to_json(&expected.user_metadata)?,
                expected.updated_at_unix_seconds,
            ],
        )?
    } else {
        transaction.execute(
            "INSERT INTO shardline_s3_objects (
                scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                user_metadata, updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT (scope_namespace, object_key) DO NOTHING",
            rusqlite::params![
                replacement.scope_namespace,
                replacement.object_key,
                replacement.file_id,
                helpers::u64_to_i64(replacement.size_bytes)?,
                replacement.content_hash,
                replacement.etag,
                replacement_metadata,
                replacement.updated_at_unix_seconds,
            ],
        )?
    };
    if changed == 1 {
        if update_part_count {
            persist_multipart_count_sql(transaction, replacement, part_count)?;
        } else if let Some(part_count) = preserved_part_count {
            persist_multipart_count_sql(transaction, replacement, Some(part_count))?;
        }
        let before = expected;
        record_s3_object_transition(transaction, before, Some(replacement))?;
    }
    Ok(changed == 1)
}

fn delete_s3_object_sql(
    transaction: &Transaction<'_>,
    scope_namespace: &str,
    object_key: &str,
) -> Result<bool, LocalIndexStoreError> {
    let before = current_s3_object_sql(transaction, scope_namespace, object_key)?;
    let changed = transaction.execute(
        "DELETE FROM shardline_s3_objects WHERE scope_namespace = ?1 AND object_key = ?2",
        rusqlite::params![scope_namespace, object_key],
    )?;
    if changed > 0 {
        transaction.execute(
            "DELETE FROM shardline_s3_multipart_counts
             WHERE scope_namespace = ?1 AND object_key = ?2",
            rusqlite::params![scope_namespace, object_key],
        )?;
        record_s3_object_transition(transaction, before.as_ref(), None)?;
    }
    Ok(changed > 0)
}

fn current_s3_object_sql(
    connection: &Transaction<'_>,
    scope_namespace: &str,
    object_key: &str,
) -> Result<Option<S3ObjectEntry>, LocalIndexStoreError> {
    let mut statement = connection.prepare(
        "SELECT scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                user_metadata, updated_at_unix_seconds
         FROM shardline_s3_objects WHERE scope_namespace = ?1 AND object_key = ?2 LIMIT 1",
    )?;
    Ok(statement
        .query_row(
            rusqlite::params![scope_namespace, object_key],
            s3_object_entry_from_row,
        )
        .optional()?)
}

fn record_s3_object_transition(
    transaction: &Transaction<'_>,
    before: Option<&S3ObjectEntry>,
    after: Option<&S3ObjectEntry>,
) -> Result<(), LocalIndexStoreError> {
    let (scope_namespace, object_key) = after
        .or(before)
        .map(|entry| (entry.scope_namespace.as_str(), entry.object_key.as_str()))
        .ok_or_else(|| {
            LocalIndexStoreError::Io(std::io::Error::other("missing S3 object identity"))
        })?;
    let before_snapshot = helpers::s3_object_snapshot(scope_namespace, object_key, before)?;
    let after_snapshot = helpers::s3_object_snapshot(scope_namespace, object_key, after)?;
    let evidence = verify_and_append_snapshot_transition(
        helpers::current_s3_object_evidence(transaction, scope_namespace, object_key, before)?,
        before_snapshot,
        after_snapshot,
    )?
    .0;
    if let Some(event) = evidence.events().last() {
        helpers::persist_s3_object_evidence(transaction, event)?;
    }
    Ok(())
}

fn scan_s3_objects_sql(
    connection: &Transaction<'_>,
    scope_namespace: &str,
    prefix: &str,
    start: Option<S3ObjectScanStart<'_>>,
    limit: usize,
) -> Result<Vec<S3ObjectEntry>, LocalIndexStoreError> {
    use rusqlite::types::Value;
    let (lower, inclusive) = match start {
        Some(S3ObjectScanStart::Inclusive(key)) if key >= prefix => (key, true),
        Some(S3ObjectScanStart::Exclusive(key)) if key >= prefix => (key, false),
        _ => (prefix, true),
    };
    let upper = s3_prefix_successor(prefix);
    if limit == 0 || upper.as_deref().is_some_and(|upper| lower >= upper) {
        return Ok(Vec::new());
    }
    let comparison = if inclusive { ">=" } else { ">" };
    let mut sql = format!(
        "SELECT scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                user_metadata, updated_at_unix_seconds
         FROM shardline_s3_objects
         WHERE scope_namespace = ?1 AND object_key COLLATE BINARY {comparison} ?2"
    );
    let mut args = vec![
        Value::Text(scope_namespace.to_owned()),
        Value::Text(lower.to_owned()),
    ];
    if let Some(upper) = upper {
        sql.push_str(" AND object_key COLLATE BINARY < ?3");
        args.push(Value::Text(upper));
    }
    let limit_i64 =
        i64::try_from(limit).map_err(|e| LocalIndexStoreError::IntegerOutOfRange(e.to_string()))?;
    sql.push_str(" ORDER BY object_key COLLATE BINARY");
    write!(sql, " LIMIT ?{}", args.len().saturating_add(1))
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?;
    args.push(Value::Integer(limit_i64));
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(args.iter()), s3_object_entry_from_row)?;
    collect_rows(rows)
}

/// Verifies only the latest committed boundary for a paginated listing.
/// Historical-chain verification remains the fsck/recovery responsibility;
/// this keeps normal listings bounded to batched seeks and one event per object.
fn verify_s3_object_listing_evidence(
    transaction: &Transaction<'_>,
    values: &[S3ObjectEntry],
) -> Result<(), LocalIndexStoreError> {
    if values.is_empty() {
        return Ok(());
    }
    let snapshots = values
        .iter()
        .map(|value| {
            helpers::s3_object_snapshot(&value.scope_namespace, &value.object_key, Some(value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let operations = snapshots
        .iter()
        .map(|snapshot| {
            snapshot
                .evidence_operation()
                .map(|operation| operation.operation_id)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut latest = helpers::load_latest_verified_event_json_batch(
        transaction,
        OperationKind::S3Object,
        &operations,
    )?;
    for (operation_id, expected) in operations.iter().zip(&snapshots) {
        let Some(event_json) = latest.remove(operation_id) else {
            return Err(LocalIndexStoreError::Reliability(
                shardline_reliability::ReliabilityError::OperationMismatch,
            ));
        };
        let event: S3ObjectLifecycleEvent = serde_json::from_value(event_json)?;
        verify_snapshot_event(&event, expected)?;
    }
    Ok(())
}

fn scan_s3_object_exact_sql(
    connection: &Transaction<'_>,
    scope_namespace: &str,
    object_key: &str,
) -> Result<Option<S3ObjectEntry>, LocalIndexStoreError> {
    let mut statement = connection.prepare(
        "SELECT scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                user_metadata, updated_at_unix_seconds
         FROM shardline_s3_objects WHERE scope_namespace = ?1 AND object_key = ?2 LIMIT 1",
    )?;
    let rows = statement.query_map(
        rusqlite::params![scope_namespace, object_key],
        s3_object_entry_from_row,
    )?;
    Ok(collect_rows(rows)?.pop())
}

fn read_multipart_count_sql(
    connection: &rusqlite::Connection,
    entry: &S3ObjectEntry,
) -> Result<Option<u32>, LocalIndexStoreError> {
    let metadata: Option<(String, String, i64, i64, String)> = connection
        .query_row(
            "SELECT file_id, content_hash, part_count, updated_at_unix_seconds, proof
             FROM shardline_s3_multipart_counts
             WHERE scope_namespace=?1 AND object_key=?2",
            rusqlite::params![entry.scope_namespace, entry.object_key],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    let Some((file_id, content_hash, count, updated_at, proof)) = metadata else {
        return Ok(None);
    };
    let Ok(count) = u32::try_from(count) else {
        return Ok(None);
    };
    if file_id != entry.file_id
        || content_hash != entry.content_hash
        || updated_at != entry.updated_at_unix_seconds
    {
        return Ok(None);
    }
    let expected = s3_multipart_count_proof(
        &entry.scope_namespace,
        &entry.object_key,
        &entry.file_id,
        &entry.content_hash,
        count,
        updated_at,
    )
    .map_err(LocalIndexStoreError::Reliability)?;
    Ok((expected == proof).then_some(count))
}

#[async_trait::async_trait]
impl S3ObjectIndexStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    async fn upsert_s3_object(&self, entry: &S3ObjectEntry) -> Result<(), Self::Error> {
        self.upsert_s3_object_with_optional_count(entry, None).await
    }

    async fn upsert_s3_object_with_multipart_count(
        &self,
        entry: &S3ObjectEntry,
        part_count: u32,
    ) -> Result<(), Self::Error> {
        self.upsert_s3_object_with_optional_count(entry, Some(part_count))
            .await
    }

    async fn s3_multipart_part_count(
        &self,
        entry: &S3ObjectEntry,
    ) -> Result<Option<u32>, Self::Error> {
        let store = self.clone();
        let entry = entry.clone();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            read_multipart_count_sql(&connection, &entry)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn scan_s3_object_exact_with_multipart_count(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<Option<(S3ObjectEntry, Option<u32>)>, Self::Error> {
        let store = self.clone();
        let scope_namespace = scope_namespace.to_owned();
        let object_key = object_key.to_owned();
        tokio::task::spawn_blocking(move || {
            helpers::retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                let transaction = connection.transaction()?;
                let row = transaction
                    .query_row(
                        "SELECT o.scope_namespace, o.object_key, o.file_id, o.size_bytes,
                                o.content_hash, o.etag, o.user_metadata,
                                o.updated_at_unix_seconds,
                                c.file_id AS multipart_file_id,
                                c.content_hash AS multipart_content_hash,
                                c.part_count AS multipart_part_count,
                                c.updated_at_unix_seconds AS multipart_updated_at,
                                c.proof AS multipart_count_proof
                         FROM shardline_s3_objects AS o
                         LEFT JOIN shardline_s3_multipart_counts AS c
                           USING (scope_namespace, object_key)
                         WHERE o.scope_namespace=?1 AND o.object_key=?2 LIMIT 1",
                        rusqlite::params![scope_namespace, object_key],
                        |row| {
                            let entry = s3_object_entry_from_row(row)?;
                            let count = multipart_count_from_row(row, &entry);
                            Ok((entry, count))
                        },
                    )
                    .optional()?;
                if let Some((entry, _)) = row.as_ref() {
                    helpers::verify_s3_object_evidence(
                        &transaction,
                        &scope_namespace,
                        &object_key,
                        Some(entry),
                    )?;
                }
                transaction.commit()?;
                Ok(row)
            })
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn compare_and_swap_s3_object_with_multipart_count(
        &self,
        expected: Option<&S3ObjectEntry>,
        replacement: &S3ObjectEntry,
        part_count: u32,
    ) -> Result<bool, LocalIndexStoreError> {
        self.compare_and_swap_s3_object_with_optional_count(
            expected,
            replacement,
            Some(part_count),
            true,
        )
        .await
    }

    async fn compare_and_swap_s3_object(
        &self,
        expected: Option<&S3ObjectEntry>,
        replacement: &S3ObjectEntry,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_s3_object_with_optional_count(expected, replacement, None, true)
            .await
    }

    async fn compare_and_swap_s3_object_metadata(
        &self,
        expected: &S3ObjectEntry,
        replacement: &S3ObjectEntry,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_s3_object_with_optional_count(
            Some(expected),
            replacement,
            None,
            false,
        )
        .await
    }

    async fn delete_s3_object(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<bool, Self::Error> {
        let store = self.clone();
        let scope_namespace = scope_namespace.to_owned();
        let object_key = object_key.to_owned();
        tokio::task::spawn_blocking(move || {
            helpers::retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let deleted = delete_s3_object_sql(&transaction, &scope_namespace, &object_key)?;
                transaction.commit()?;
                Ok(deleted)
            })
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn scan_s3_objects(
        &self,
        scope_namespace: &str,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<S3ObjectEntry>, Self::Error> {
        self.scan_s3_objects_from(
            scope_namespace,
            prefix,
            cursor.map(S3ObjectScanStart::Exclusive),
            limit,
        )
        .await
    }

    async fn scan_s3_objects_from(
        &self,
        scope_namespace: &str,
        prefix: &str,
        start: Option<S3ObjectScanStart<'_>>,
        limit: usize,
    ) -> Result<Vec<S3ObjectEntry>, Self::Error> {
        let store = self.clone();
        let scope_namespace = scope_namespace.to_owned();
        let prefix = prefix.to_owned();
        let start = start.map(|start| match start {
            S3ObjectScanStart::Inclusive(key) => (key.to_owned(), true),
            S3ObjectScanStart::Exclusive(key) => (key.to_owned(), false),
        });
        tokio::task::spawn_blocking(move || {
            helpers::retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                let transaction = connection.transaction()?;
                let values = scan_s3_objects_sql(
                    &transaction,
                    &scope_namespace,
                    &prefix,
                    start.as_ref().map(|(key, inclusive)| {
                        if *inclusive {
                            S3ObjectScanStart::Inclusive(key)
                        } else {
                            S3ObjectScanStart::Exclusive(key)
                        }
                    }),
                    limit,
                )?;
                verify_s3_object_listing_evidence(&transaction, &values)?;
                transaction.commit()?;
                Ok(values)
            })
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn scan_s3_object_exact(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<Option<S3ObjectEntry>, Self::Error> {
        let store = self.clone();
        let scope_namespace = scope_namespace.to_owned();
        let object_key = object_key.to_owned();
        tokio::task::spawn_blocking(move || {
            helpers::retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                let transaction = connection.transaction()?;
                let value = scan_s3_object_exact_sql(&transaction, &scope_namespace, &object_key)?;
                if let Some(value) = value.as_ref() {
                    helpers::verify_s3_object_evidence(
                        &transaction,
                        &scope_namespace,
                        &object_key,
                        Some(value),
                    )?;
                }
                transaction.commit()?;
                Ok(value)
            })
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }
}

impl LocalIndexStore {
    async fn upsert_s3_object_with_optional_count(
        &self,
        entry: &S3ObjectEntry,
        part_count: Option<u32>,
    ) -> Result<(), LocalIndexStoreError> {
        let store = self.clone();
        let entry = entry.clone();
        tokio::task::spawn_blocking(move || {
            helpers::retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                // BEGIN IMMEDIATE prevents two unconditional upserts from both
                // reading the old evidence chain and then racing while upgrading
                // to a writer transaction. SQLite otherwise returns SQLITE_BUSY
                // for the losing deferred-to-write upgrade.
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                upsert_s3_object_sql(&transaction, &entry, part_count)?;
                transaction.commit()?;
                Ok(())
            })
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn compare_and_swap_s3_object_with_optional_count(
        &self,
        expected: Option<&S3ObjectEntry>,
        replacement: &S3ObjectEntry,
        part_count: Option<u32>,
        update_part_count: bool,
    ) -> Result<bool, LocalIndexStoreError> {
        let store = self.clone();
        let expected = expected.cloned();
        let replacement = replacement.clone();
        tokio::task::spawn_blocking(move || {
            helpers::retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let changed = compare_and_swap_s3_object_sql(
                    &transaction,
                    expected.as_ref(),
                    &replacement,
                    part_count,
                    update_part_count,
                )?;
                transaction.commit()?;
                Ok(changed)
            })
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]
    use super::*;

    fn make_store() -> LocalIndexStore {
        let storage = shardline_test_support::TempStorage::new();
        LocalIndexStore::new(storage.path_buf()).expect("failed to create local index store")
    }

    fn file_id(n: u8) -> String {
        format!("{:064x}", n)
    }

    fn entry(
        scope_namespace: &str,
        object_key: &str,
        file_id: &str,
        size_bytes: u64,
        updated_at_unix_seconds: i64,
    ) -> S3ObjectEntry {
        S3ObjectEntry {
            scope_namespace: scope_namespace.to_owned(),
            object_key: object_key.to_owned(),
            file_id: file_id.to_owned(),
            size_bytes,
            content_hash: "ab".repeat(32),
            etag: "ab".repeat(32),
            user_metadata: Vec::new(),
            updated_at_unix_seconds,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compare_and_swap_create_has_exactly_one_winner() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let first = entry("cas-create", "model.bin", &file_id(1), 10, 1);
        let second = entry("cas-create", "model.bin", &file_id(2), 20, 2);

        let (first_won, second_won) = tokio::join!(
            store.compare_and_swap_s3_object(None, &first),
            store.compare_and_swap_s3_object(None, &second),
        );
        let first_won = first_won.unwrap();
        let second_won = second_won.unwrap();
        assert_ne!(first_won, second_won);

        let stored = store
            .scan_s3_object_exact("cas-create", "model.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, if first_won { first } else { second });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_unconditional_upserts_keep_a_verifiable_chain() {
        let store = make_store();
        let first = entry("concurrent-upsert", "model.bin", &file_id(1), 10, 1);
        let second = entry("concurrent-upsert", "model.bin", &file_id(2), 20, 2);

        let (first_result, second_result) = tokio::join!(
            store.upsert_s3_object(&first),
            store.upsert_s3_object(&second),
        );
        first_result.unwrap();
        second_result.unwrap();
        let stored = store
            .scan_s3_object_exact("concurrent-upsert", "model.bin")
            .await
            .unwrap()
            .unwrap();
        assert!(stored == first || stored == second);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compare_and_swap_update_rejects_a_stale_expected_row() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let original = entry("cas-update", "model.bin", &file_id(1), 10, 1);
        let first = entry("cas-update", "model.bin", &file_id(2), 20, 2);
        let second = entry("cas-update", "model.bin", &file_id(3), 30, 3);
        store.upsert_s3_object(&original).await.unwrap();

        let (first_won, second_won) = tokio::join!(
            store.compare_and_swap_s3_object(Some(&original), &first),
            store.compare_and_swap_s3_object(Some(&original), &second),
        );
        let first_won = first_won.unwrap();
        let second_won = second_won.unwrap();
        assert_ne!(first_won, second_won);

        let stored = store
            .scan_s3_object_exact("cas-update", "model.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, if first_won { first } else { second });
    }

    async fn scan(
        store: &LocalIndexStore,
        scope_namespace: &str,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Vec<String> {
        S3ObjectIndexStore::scan_s3_objects(store, scope_namespace, prefix, cursor, limit)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.object_key)
            .collect()
    }

    #[tokio::test]
    async fn upsert_s3_object_overwrites_existing_row() {
        let store = make_store();
        let first = entry("global", "data/model.pt", &file_id(1), 100, 1000);
        S3ObjectIndexStore::upsert_s3_object(&store, &first)
            .await
            .unwrap();
        let overwrite = entry("global", "data/model.pt", &file_id(2), 200, 2000);
        S3ObjectIndexStore::upsert_s3_object(&store, &overwrite)
            .await
            .unwrap();

        let rows = scan(&store, "global", "", None, 100).await;
        assert_eq!(rows, vec!["data/model.pt"]);
        let loaded = S3ObjectIndexStore::scan_s3_objects(&store, "global", "", None, 100)
            .await
            .unwrap();
        assert_eq!(loaded[0], overwrite);
    }

    #[tokio::test]
    async fn legacy_object_update_invalidates_multipart_count_sidecar() {
        let store = make_store();
        let multipart = entry("legacy-writer", "same-bytes.bin", &file_id(7), 12, 77);
        store
            .upsert_s3_object_with_multipart_count(&multipart, 2)
            .await
            .unwrap();

        // Simulate an older binary, which updates only the authoritative
        // object row and knows nothing about the new sidecar. The no-op field
        // assignment matters: identical content can be uploaded through a
        // different protocol with a different multipart shape.
        let connection = store.open_connection().expect("open test connection");
        connection
            .execute(
                "UPDATE shardline_s3_objects SET file_id=file_id
                 WHERE scope_namespace=?1 AND object_key=?2",
                rusqlite::params![multipart.scope_namespace, multipart.object_key],
            )
            .expect("simulate legacy object-row update");

        let stored = store
            .scan_s3_object_exact("legacy-writer", "same-bytes.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(store.s3_multipart_part_count(&stored).await.unwrap(), None);
    }

    #[tokio::test]
    async fn metadata_cas_preserves_verified_multipart_count_and_appends_evidence() {
        let store = make_store();
        let expected = entry("global", "tagged.bin", &file_id(9), 123, 1000);
        store
            .upsert_s3_object_with_multipart_count(&expected, 3)
            .await
            .unwrap();
        let mut replacement = expected.clone();
        replacement.user_metadata.push((
            "shardline-internal-s3-tagging-v1".to_owned(),
            r#"[["env","test"]]"#.to_owned(),
        ));

        assert!(
            store
                .compare_and_swap_s3_object_metadata(&expected, &replacement)
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .scan_s3_object_exact("global", "tagged.bin")
                .await
                .unwrap(),
            Some(replacement.clone())
        );
        assert_eq!(
            store.s3_multipart_part_count(&replacement).await.unwrap(),
            Some(3)
        );
    }

    #[tokio::test]
    async fn identical_s3_object_upsert_and_cas_skip_row_update_and_keep_evidence_valid() {
        let store = make_store();
        let value = entry("global", "data/replay.pt", &file_id(8), 100, 1000);
        let connection = store.open_connection().expect("open test connection");
        connection
            .execute_batch(
                "CREATE TABLE s3_object_update_audit (updates INTEGER NOT NULL);
                 INSERT INTO s3_object_update_audit VALUES (0);
                 CREATE TRIGGER count_s3_object_updates
                 AFTER UPDATE ON shardline_s3_objects
                 BEGIN
                     UPDATE s3_object_update_audit SET updates = updates + 1;
                 END;",
            )
            .expect("install update counter");

        store.upsert_s3_object(&value).await.unwrap();
        store.upsert_s3_object(&value).await.unwrap();
        assert!(
            store
                .compare_and_swap_s3_object(Some(&value), &value)
                .await
                .unwrap()
        );

        let updates: i64 = connection
            .query_row("SELECT updates FROM s3_object_update_audit", [], |row| {
                row.get(0)
            })
            .expect("read update counter");
        assert_eq!(updates, 0);
        assert_eq!(
            store
                .scan_s3_object_exact("global", "data/replay.pt")
                .await
                .unwrap(),
            Some(value.clone())
        );
        let events: i64 = connection.query_row(
            "SELECT count(*) FROM shardline_reliability_events WHERE operation_kind = 'S3Object'",
            [], |row| row.get(0),
        ).unwrap();
        assert_eq!(
            events, 4,
            "the no-op CAS still appends its evidence transition"
        );
        let mut newer = value.clone();
        newer.updated_at_unix_seconds = 1001;
        assert!(
            store
                .compare_and_swap_s3_object(Some(&value), &newer)
                .await
                .unwrap()
        );
        assert!(
            !store
                .compare_and_swap_s3_object(Some(&value), &value)
                .await
                .unwrap()
        );
        let updates: i64 = connection
            .query_row("SELECT updates FROM s3_object_update_audit", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            updates, 1,
            "timestamp-changing CAS must still update the row"
        );
    }

    #[tokio::test]
    async fn delete_s3_object_returns_true_then_false() {
        let store = make_store();
        let e = entry("global", "data/delete.pt", &file_id(3), 100, 1000);
        S3ObjectIndexStore::upsert_s3_object(&store, &e)
            .await
            .unwrap();

        assert!(
            S3ObjectIndexStore::delete_s3_object(&store, "global", "data/delete.pt")
                .await
                .unwrap()
        );
        assert!(
            !S3ObjectIndexStore::delete_s3_object(&store, "global", "data/delete.pt")
                .await
                .unwrap()
        );
        assert!(scan(&store, "global", "", None, 100).await.is_empty());
        // Other scope namespaces are isolated.
        assert!(
            !S3ObjectIndexStore::delete_s3_object(&store, "other-scope", "data/delete.pt")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn scan_s3_objects_prefix_and_cursor_keyset() {
        let store = make_store();
        for (key, id) in [
            ("a.txt", 1),
            ("data/1.txt", 2),
            ("data/2.txt", 3),
            ("data/sub/3.txt", 4),
            ("zz.txt", 5),
        ] {
            S3ObjectIndexStore::upsert_s3_object(&store, &entry("global", key, &file_id(id), 0, 0))
                .await
                .unwrap();
        }

        // Empty prefix lists every key in raw-key order.
        assert_eq!(
            scan(&store, "global", "", None, 100).await,
            vec![
                "a.txt",
                "data/1.txt",
                "data/2.txt",
                "data/sub/3.txt",
                "zz.txt"
            ]
        );

        // Prefix filtering.
        assert_eq!(
            scan(&store, "global", "data", None, 100).await,
            vec!["data/1.txt", "data/2.txt", "data/sub/3.txt"]
        );

        // Keyset cursor resumes strictly after the given raw key.
        assert_eq!(
            scan(&store, "global", "", Some("data/2.txt"), 100).await,
            vec!["data/sub/3.txt", "zz.txt"]
        );

        // Cursor combined with prefix filtering.
        assert_eq!(
            scan(&store, "global", "data", Some("data/2.txt"), 100).await,
            vec!["data/sub/3.txt"]
        );

        // Other scope namespaces are isolated.
        assert!(scan(&store, "other-scope", "", None, 100).await.is_empty());
    }

    #[tokio::test]
    async fn scan_s3_objects_respects_limit() {
        let store = make_store();
        for id in 1..=5 {
            S3ObjectIndexStore::upsert_s3_object(
                &store,
                &entry("global", &format!("f{id}.txt"), &file_id(id), 1, 1),
            )
            .await
            .unwrap();
        }
        let rows = S3ObjectIndexStore::scan_s3_objects(&store, "global", "", None, 3)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        // Cursor resumes after the last returned raw key.
        let cursor = rows.last().unwrap().object_key.clone();
        assert_eq!(
            scan(&store, "global", "", Some(&cursor), 100).await,
            vec!["f4.txt", "f5.txt"]
        );
    }

    #[tokio::test]
    async fn listing_verifies_evidence_after_first_batch() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::new(root.path().to_path_buf()).unwrap();
        let mut connection = store.open_connection().unwrap();
        let transaction = connection.transaction().unwrap();
        let mut last_operation = String::new();
        for index in 0..901 {
            let value = entry(
                "batched-listing",
                &format!("key-{index:04}"),
                &file_id(1),
                1,
                1,
            );
            upsert_s3_object_sql(&transaction, &value, None).unwrap();
            last_operation = helpers::s3_object_snapshot(
                &value.scope_namespace,
                &value.object_key,
                Some(&value),
            )
            .unwrap()
            .evidence_operation()
            .unwrap()
            .operation_id;
        }
        transaction.commit().unwrap();
        let values = store
            .scan_s3_objects("batched-listing", "", None, 901)
            .await
            .unwrap();
        assert_eq!(values.len(), 901);
        connection
            .execute(
                "UPDATE shardline_reliability_events SET merkle_commit_json=NULL
             WHERE operation_kind='S3Object' AND operation_id=?1",
                [&last_operation],
            )
            .unwrap();
        assert!(
            store
                .scan_s3_objects("batched-listing", "", None, 901)
                .await
                .is_err()
        );
        let missing: i64 = connection
            .query_row(
                "SELECT count(*) FROM shardline_reliability_events
             WHERE operation_kind='S3Object' AND operation_id=?1 AND merkle_commit_json IS NULL",
                [&last_operation],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            missing > 0,
            "listing must reject corruption without repairing it"
        );
    }

    #[tokio::test]
    async fn scan_s3_objects_rejects_missing_evidence_without_writing() {
        let store = make_store();
        S3ObjectIndexStore::upsert_s3_object(
            &store,
            &entry("global", "missing-evidence", "file", 1, 1),
        )
        .await
        .unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'S3Object'",
                [],
            )
            .unwrap();
        drop(connection);

        assert!(
            S3ObjectIndexStore::scan_s3_objects(&store, "global", "", None, 10)
                .await
                .is_err()
        );
        let connection = store.open_connection().unwrap();
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM shardline_reliability_events
                 WHERE operation_kind = 'S3Object'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn scan_s3_objects_prefix_matches_literal_string() {
        // A prefix containing LIKE metacharacters must match literally (string
        // prefix semantics, not SQL LIKE pattern semantics).
        let store = make_store();
        for key in ["100%done", "100_plus", "100done", "plain"] {
            S3ObjectIndexStore::upsert_s3_object(&store, &entry("global", key, "f", 0, 0))
                .await
                .unwrap();
        }
        assert_eq!(
            scan(&store, "global", "100%", None, 100).await,
            vec!["100%done"]
        );
        assert_eq!(
            scan(&store, "global", "100_", None, 100).await,
            vec!["100_plus"]
        );
        assert_eq!(
            scan(&store, "global", "100", None, 100).await,
            vec!["100%done", "100_plus", "100done"]
        );
    }

    #[tokio::test]
    async fn scan_s3_object_exact_requires_full_key_match() {
        // F-33: `scan_s3_objects` matches object_key as a string PREFIX, so a
        // sibling key with the target as a prefix must never satisfy the
        // exact-key lookup. Only the row with the exact raw key is returned.
        let store = make_store();
        for (key, id) in [("a/b", 1), ("a/b/c", 2)] {
            S3ObjectIndexStore::upsert_s3_object(&store, &entry("global", key, &file_id(id), 0, 0))
                .await
                .unwrap();
        }

        let exact = S3ObjectIndexStore::scan_s3_object_exact(&store, "global", "a")
            .await
            .unwrap();
        assert!(
            exact.is_none(),
            "a prefix-sibling must never satisfy an exact lookup"
        );

        let found = S3ObjectIndexStore::scan_s3_object_exact(&store, "global", "a/b")
            .await
            .unwrap();
        assert_eq!(found.expect("exact row").object_key, "a/b");

        // Exact lookup is scope-namespace isolated.
        assert!(
            S3ObjectIndexStore::scan_s3_object_exact(&store, "other-scope", "a/b")
                .await
                .unwrap()
                .is_none()
        );

        // The prefix scan still lists the siblings (listing semantics
        // unchanged).
        assert_eq!(
            scan(&store, "global", "a", None, 100).await,
            vec!["a/b", "a/b/c"]
        );
    }

    #[tokio::test]
    async fn s3_object_read_rejects_tampered_evidence() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let value = entry("global", "model.bin", "file-a", 7, 1);
        store.upsert_s3_object(&value).await.unwrap();

        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"tampered\":true}'
                 WHERE operation_kind = 'S3Object'",
                [],
            )
            .unwrap();

        assert!(
            store
                .scan_s3_object_exact("global", "model.bin")
                .await
                .is_err()
        );
        assert!(
            store
                .compare_and_swap_s3_object(Some(&value), &value)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn s3_object_read_rejects_evidence_bound_to_another_operation() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let value = entry("global", "model.bin", "file-a", 7, 1);
        store.upsert_s3_object(&value).await.unwrap();

        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET operation_id = 'rewritten-operation'
                 WHERE operation_kind = 'S3Object'",
                [],
            )
            .unwrap();

        assert!(
            store
                .scan_s3_object_exact("global", "model.bin")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn s3_object_read_rejects_missing_predecessor_commitment() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        store
            .upsert_s3_object(&entry("global", "model.bin", "file-a", 7, 1))
            .await
            .unwrap();
        store
            .upsert_s3_object(&entry("global", "model.bin", "file-b", 8, 2))
            .await
            .unwrap();

        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET merkle_commit_json = NULL
                 WHERE operation_kind = 'S3Object'
                   AND sequence = (
                       SELECT MIN(sequence)
                       FROM shardline_reliability_events
                       WHERE operation_kind = 'S3Object'
                   )",
                [],
            )
            .unwrap();

        assert!(
            store
                .scan_s3_object_exact("global", "model.bin")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn s3_object_read_rejects_malformed_user_metadata() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let mut value = entry("global", "metadata.bin", "file-a", 7, 1);
        value.user_metadata = vec![("mode".to_owned(), "fast".to_owned())];
        store.upsert_s3_object(&value).await.unwrap();

        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_s3_objects
                 SET user_metadata = '{malformed:true}'
                 WHERE scope_namespace = 'global' AND object_key = 'metadata.bin'",
                [],
            )
            .unwrap();

        assert!(matches!(
            store.scan_s3_object_exact("global", "metadata.bin").await,
            Err(LocalIndexStoreError::Sqlite(
                rusqlite::Error::FromSqlConversionFailure(..)
            ))
        ));
    }

    #[tokio::test]
    async fn s3_object_read_rejects_missing_baseline_evidence_without_writing() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let value = entry("global", "repair.bin", "file-a", 7, 1);
        store.upsert_s3_object(&value).await.unwrap();

        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'S3Object'",
                [],
            )
            .unwrap();
        drop(connection);

        assert!(
            store
                .scan_s3_object_exact("global", "repair.bin")
                .await
                .is_err()
        );
        let connection = store.open_connection().unwrap();
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM shardline_reliability_events
                 WHERE operation_kind = 'S3Object'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn s3_object_migration_up_down_roundtrip() {
        use crate::local_sqlite::LOCAL_SQLITE_MIGRATIONS;

        let migration = LOCAL_SQLITE_MIGRATIONS
            .iter()
            .find(|m| m.version == "20260813000000")
            .expect("s3_object_index migration must be registered");
        let connection = Connection::open_in_memory().expect("open sqlite");

        connection
            .execute_batch(migration.up_sql)
            .expect("apply up migration");
        let exists = table_exists(&connection);
        assert!(exists, "up migration must create shardline_s3_objects");

        connection
            .execute_batch(migration.down_sql)
            .expect("apply down migration");
        assert!(
            !table_exists(&connection),
            "down migration must drop the table"
        );
    }

    fn table_exists(connection: &Connection) -> bool {
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'shardline_s3_objects'",
                [],
                |row| row.get(0),
            )
            .expect("query sqlite_master");
        count != 0
    }

    #[tokio::test]
    async fn scan_s3_objects_superset_prefix_is_empty() {
        let store = make_store();
        for key in ["a.txt", "dir/1.txt", "dir/sub/2.txt"] {
            S3ObjectIndexStore::upsert_s3_object(&store, &entry("global", key, "f", 0, 0))
                .await
                .unwrap();
        }
        // A prefix longer than any stored key (a superset) matches nothing.
        assert!(
            scan(&store, "global", "dir/sub/very/long", None, 100)
                .await
                .is_empty()
        );
        assert!(scan(&store, "global", "zzz", None, 100).await.is_empty());
    }
    #[tokio::test]
    async fn inclusive_s3_key_bounds_preserve_unicode_and_exact_successor() {
        let store = make_store();
        for key in [
            "a/child",
            "a0",
            "a1",
            "é/child",
            "é0",
            "\u{d7ff}/child",
            "\u{e000}",
            "\u{10ffff}/child",
        ] {
            store
                .upsert_s3_object(&entry("global", key, &file_id(1), 1, 0))
                .await
                .unwrap();
        }
        for (prefix, bound, expected) in [
            ("a", "a0", vec!["a0", "a1"]),
            ("é", "é0", vec!["é0"]),
            ("", "\u{e000}", vec!["\u{e000}", "\u{10ffff}/child"]),
            ("\u{10ffff}", "\u{10ffff}", vec!["\u{10ffff}/child"]),
        ] {
            let rows = store
                .scan_s3_objects_from(
                    "global",
                    prefix,
                    Some(S3ObjectScanStart::Inclusive(bound)),
                    2,
                )
                .await
                .unwrap();
            assert_eq!(
                rows.iter()
                    .map(|row| row.object_key.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
        }
        let rows = store
            .scan_s3_objects_from("global", "a", Some(S3ObjectScanStart::Exclusive("a0")), 2)
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.object_key.as_str())
                .collect::<Vec<_>>(),
            vec!["a1"]
        );
        assert!(
            store
                .scan_s3_objects_from("global", "a", Some(S3ObjectScanStart::Inclusive("a0")), 0)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
