use serde_json::{from_value, to_value};
use sqlx::{PgConnection, Row, postgres::PgRow, query, query_scalar};

use super::{
    PostgresIndexStore, PostgresMetadataStoreError, i64_to_u64,
    load_postgres_latest_evidence_heads, u64_to_i64,
};
use crate::s3_objects::s3_multipart_count_proof;
use crate::{S3ObjectEntry, S3ObjectIndexStore, S3ObjectScanStart, s3_prefix_successor};
use shardline_reliability::{
    OperationKind, ReliabilityMerkleCommit, S3ObjectEvidenceLog, S3ObjectLifecycleEvent,
    S3ObjectSnapshot, S3ObjectState, SnapshotEvidence,
    build_reliability_merkle_commit_with_previous, persisted_event_sequence,
    verify_and_append_snapshot_transition, verify_persisted_merkle_commit_with_previous,
    verify_snapshot_event, verify_snapshot_evidence,
};

fn s3_object_entry_from_row(row: &PgRow) -> Result<S3ObjectEntry, PostgresMetadataStoreError> {
    let user_metadata_json: String = row.try_get("user_metadata")?;
    let user_metadata = if user_metadata_json.is_empty() {
        Vec::new()
    } else {
        serde_json::from_str(&user_metadata_json)?
    };
    Ok(S3ObjectEntry {
        scope_namespace: row.try_get("scope_namespace")?,
        object_key: row.try_get("object_key")?,
        file_id: row.try_get("file_id")?,
        size_bytes: i64_to_u64(row.try_get("size_bytes")?)?,
        content_hash: row.try_get("content_hash")?,
        etag: row.try_get("etag")?,
        user_metadata,
        updated_at_unix_seconds: row.try_get("updated_at_unix_seconds")?,
    })
}

fn multipart_count_from_row(
    row: &PgRow,
    entry: &S3ObjectEntry,
) -> Result<Option<u32>, PostgresMetadataStoreError> {
    let count: Option<i32> = row.try_get("multipart_part_count")?;
    let Some(count) = count.and_then(|count| u32::try_from(count).ok()) else {
        return Ok(None);
    };
    let file_id: Option<String> = row.try_get("multipart_file_id")?;
    let content_hash: Option<String> = row.try_get("multipart_content_hash")?;
    let updated_at: Option<i64> = row.try_get("multipart_updated_at")?;
    let proof: Option<String> = row.try_get("multipart_count_proof")?;
    if file_id.as_deref() != Some(&entry.file_id)
        || content_hash.as_deref() != Some(&entry.content_hash)
        || updated_at != Some(entry.updated_at_unix_seconds)
    {
        return Ok(None);
    }
    let expected = s3_multipart_count_proof(
        &entry.scope_namespace,
        &entry.object_key,
        &entry.file_id,
        &entry.content_hash,
        count,
        entry.updated_at_unix_seconds,
    )?;
    Ok((proof.as_deref() == Some(expected.as_str())).then_some(count))
}

async fn upsert_s3_object_with_optional_count(
    store: &PostgresIndexStore,
    entry: &S3ObjectEntry,
    part_count: Option<u32>,
) -> Result<(), PostgresMetadataStoreError> {
    let user_metadata_json = user_metadata_to_json(&entry.user_metadata)?;
    let mut transaction = store.pool.begin().await?;
    lock_s3_object(&mut transaction, &entry.scope_namespace, &entry.object_key).await?;
    let before = current_s3_object_on_connection(
        &mut transaction,
        &entry.scope_namespace,
        &entry.object_key,
    )
    .await?;
    query(
        "INSERT INTO shardline_s3_objects (
            scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
            user_metadata, updated_at_unix_seconds
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (scope_namespace, object_key)
         DO UPDATE SET
            file_id = EXCLUDED.file_id,
            size_bytes = EXCLUDED.size_bytes,
            content_hash = EXCLUDED.content_hash,
            etag = EXCLUDED.etag,
            user_metadata = EXCLUDED.user_metadata,
            updated_at_unix_seconds = EXCLUDED.updated_at_unix_seconds
         WHERE (shardline_s3_objects.file_id,
                shardline_s3_objects.size_bytes,
                shardline_s3_objects.content_hash,
                shardline_s3_objects.etag,
                shardline_s3_objects.user_metadata,
                shardline_s3_objects.updated_at_unix_seconds)
               IS DISTINCT FROM
               (EXCLUDED.file_id,
                EXCLUDED.size_bytes,
                EXCLUDED.content_hash,
                EXCLUDED.etag,
                EXCLUDED.user_metadata,
                EXCLUDED.updated_at_unix_seconds)",
    )
    .bind(&entry.scope_namespace)
    .bind(&entry.object_key)
    .bind(&entry.file_id)
    .bind(u64_to_i64(entry.size_bytes)?)
    .bind(&entry.content_hash)
    .bind(&entry.etag)
    .bind(user_metadata_json)
    .bind(entry.updated_at_unix_seconds)
    .execute(&mut *transaction)
    .await?;
    persist_multipart_count(&mut transaction, entry, part_count).await?;
    record_s3_object_transition(&mut transaction, before.as_ref(), Some(entry)).await?;
    transaction.commit().await?;
    Ok(())
}

async fn compare_and_swap_s3_object_with_optional_count(
    store: &PostgresIndexStore,
    expected: Option<&S3ObjectEntry>,
    replacement: &S3ObjectEntry,
    part_count: Option<u32>,
) -> Result<bool, PostgresMetadataStoreError> {
    let replacement_metadata = user_metadata_to_json(&replacement.user_metadata)?;
    let mut transaction = store.pool.begin().await?;
    lock_s3_object(
        &mut transaction,
        &replacement.scope_namespace,
        &replacement.object_key,
    )
    .await?;
    if expected == Some(replacement) {
        let matches: bool = query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM shardline_s3_objects
                 WHERE scope_namespace = $1 AND object_key = $2
                   AND file_id = $3 AND size_bytes = $4 AND content_hash = $5
                   AND etag = $6 AND user_metadata = $7
                   AND updated_at_unix_seconds = $8
                 FOR UPDATE
             )",
        )
        .bind(&replacement.scope_namespace)
        .bind(&replacement.object_key)
        .bind(&replacement.file_id)
        .bind(u64_to_i64(replacement.size_bytes)?)
        .bind(&replacement.content_hash)
        .bind(&replacement.etag)
        .bind(&replacement_metadata)
        .bind(replacement.updated_at_unix_seconds)
        .fetch_one(&mut *transaction)
        .await?;
        if matches {
            persist_multipart_count(&mut transaction, replacement, part_count).await?;
            record_s3_object_transition(&mut transaction, expected, Some(replacement)).await?;
        }
        transaction.commit().await?;
        return Ok(matches);
    }
    let result = if let Some(expected) = expected {
        let expected_metadata = user_metadata_to_json(&expected.user_metadata)?;
        query(
            "UPDATE shardline_s3_objects
             SET file_id = $3, size_bytes = $4, content_hash = $5, etag = $6,
                 user_metadata = $7, updated_at_unix_seconds = $8
             WHERE scope_namespace = $1 AND object_key = $2
               AND file_id = $9 AND size_bytes = $10 AND content_hash = $11
               AND etag = $12 AND user_metadata = $13
               AND updated_at_unix_seconds = $14",
        )
        .bind(&replacement.scope_namespace)
        .bind(&replacement.object_key)
        .bind(&replacement.file_id)
        .bind(u64_to_i64(replacement.size_bytes)?)
        .bind(&replacement.content_hash)
        .bind(&replacement.etag)
        .bind(replacement_metadata)
        .bind(replacement.updated_at_unix_seconds)
        .bind(&expected.file_id)
        .bind(u64_to_i64(expected.size_bytes)?)
        .bind(&expected.content_hash)
        .bind(&expected.etag)
        .bind(expected_metadata)
        .bind(expected.updated_at_unix_seconds)
        .execute(&mut *transaction)
        .await?
    } else {
        query(
            "INSERT INTO shardline_s3_objects (
                scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                user_metadata, updated_at_unix_seconds
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (scope_namespace, object_key) DO NOTHING",
        )
        .bind(&replacement.scope_namespace)
        .bind(&replacement.object_key)
        .bind(&replacement.file_id)
        .bind(u64_to_i64(replacement.size_bytes)?)
        .bind(&replacement.content_hash)
        .bind(&replacement.etag)
        .bind(replacement_metadata)
        .bind(replacement.updated_at_unix_seconds)
        .execute(&mut *transaction)
        .await?
    };
    let changed = result.rows_affected() == 1;
    if changed {
        persist_multipart_count(&mut transaction, replacement, part_count).await?;
        record_s3_object_transition(&mut transaction, expected, Some(replacement)).await?;
    }
    transaction.commit().await?;
    Ok(changed)
}

pub(super) async fn persist_multipart_count(
    connection: &mut PgConnection,
    entry: &S3ObjectEntry,
    part_count: Option<u32>,
) -> Result<(), PostgresMetadataStoreError> {
    if let Some(part_count) = part_count {
        let proof = s3_multipart_count_proof(
            &entry.scope_namespace,
            &entry.object_key,
            &entry.file_id,
            &entry.content_hash,
            part_count,
            entry.updated_at_unix_seconds,
        )?;
        query(
            "INSERT INTO shardline_s3_multipart_counts
                (scope_namespace, object_key, file_id, content_hash, part_count,
                 updated_at_unix_seconds, proof)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (scope_namespace, object_key)
             DO UPDATE SET file_id = EXCLUDED.file_id,
                           content_hash = EXCLUDED.content_hash,
                           part_count = EXCLUDED.part_count,
                           updated_at_unix_seconds = EXCLUDED.updated_at_unix_seconds,
                           proof = EXCLUDED.proof",
        )
        .bind(&entry.scope_namespace)
        .bind(&entry.object_key)
        .bind(&entry.file_id)
        .bind(&entry.content_hash)
        .bind(
            i32::try_from(part_count)
                .map_err(|error| PostgresMetadataStoreError::Unsupported(error.to_string()))?,
        )
        .bind(entry.updated_at_unix_seconds)
        .bind(proof)
        .execute(&mut *connection)
        .await?;
    } else {
        query(
            "DELETE FROM shardline_s3_multipart_counts
             WHERE scope_namespace = $1 AND object_key = $2",
        )
        .bind(&entry.scope_namespace)
        .bind(&entry.object_key)
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

/// Serializes S3 user metadata as JSON text for the index row.
fn user_metadata_to_json(
    user_metadata: &[(String, String)],
) -> Result<String, PostgresMetadataStoreError> {
    serde_json::to_string(user_metadata).map_err(PostgresMetadataStoreError::from)
}

fn s3_object_snapshot(
    scope_namespace: &str,
    object_key: &str,
    entry: Option<&S3ObjectEntry>,
) -> Result<S3ObjectSnapshot, PostgresMetadataStoreError> {
    Ok(S3ObjectSnapshot::new(
        scope_namespace,
        object_key,
        entry.map(|entry| S3ObjectState {
            file_id: entry.file_id.clone(),
            size_bytes: entry.size_bytes,
            content_hash: entry.content_hash.clone(),
            etag: entry.etag.clone(),
            user_metadata: entry.user_metadata.clone(),
            updated_at_unix_seconds: entry.updated_at_unix_seconds,
        }),
    )?)
}

struct S3ObjectEvidenceHead {
    evidence: S3ObjectEvidenceLog,
    commitment: Option<serde_json::Value>,
}

async fn load_s3_object_evidence_head(
    connection: &mut PgConnection,
    scope_namespace: &str,
    object_key: &str,
) -> Result<S3ObjectEvidenceHead, PostgresMetadataStoreError> {
    let operation = s3_object_snapshot(scope_namespace, object_key, None)?.evidence_operation()?;
    let row = query(
        "SELECT evidence.sequence, evidence.event_json,
                evidence.merkle_commit_json AS evidence_merkle_json,
                previous_evidence.merkle_commit_json AS previous_evidence_merkle_json
         FROM LATERAL (
             SELECT sequence, event_json, merkle_commit_json
             FROM shardline_reliability_events
             WHERE operation_kind = $1 AND operation_id = $2
             ORDER BY sequence DESC
             LIMIT 1
         ) AS evidence
         LEFT JOIN LATERAL (
             SELECT CASE WHEN EXISTS (
                 SELECT 1
                 FROM shardline_reliability_events AS missing
                 WHERE missing.operation_kind = $1 AND missing.operation_id = $2
                   AND missing.sequence < evidence.sequence
                   AND missing.merkle_commit_json IS NULL
             ) THEN '{\"missing_previous_merkle_commit\":true}'::jsonb ELSE (
                 SELECT previous.merkle_commit_json
                 FROM shardline_reliability_events AS previous
                 WHERE previous.operation_kind = $1 AND previous.operation_id = $2
                   AND previous.sequence < evidence.sequence
                 ORDER BY previous.sequence DESC LIMIT 1
             ) END AS merkle_commit_json
             FROM shardline_reliability_events
             WHERE operation_kind = $1 AND operation_id = $2
               AND sequence < evidence.sequence
             ORDER BY sequence DESC
             LIMIT 1
         ) AS previous_evidence ON TRUE",
    )
    .bind(OperationKind::S3Object.as_str())
    .bind(&operation.operation_id)
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else {
        return Ok(S3ObjectEvidenceHead {
            evidence: S3ObjectEvidenceLog::default(),
            commitment: None,
        });
    };
    let sequence = i64_to_u64(row.try_get("sequence")?)?;
    let event_json: serde_json::Value = row.try_get("event_json")?;
    let event_sequence = persisted_event_sequence(OperationKind::S3Object, event_json.clone())?;
    if sequence != event_sequence {
        return Err(PostgresMetadataStoreError::Reliability(
            shardline_reliability::ReliabilityError::Merkle(
                "S3 evidence row sequence does not match its event".into(),
            ),
        ));
    }
    let event: S3ObjectLifecycleEvent = from_value(event_json.clone())?;
    if event.operation.operation_id != operation.operation_id {
        return Err(PostgresMetadataStoreError::Reliability(
            shardline_reliability::ReliabilityError::OperationMismatch,
        ));
    }
    let commitment_json: Option<serde_json::Value> = row.try_get("evidence_merkle_json")?;
    verify_persisted_merkle_commit_with_previous(
        OperationKind::S3Object,
        event_json,
        commitment_json.clone(),
        row.try_get("previous_evidence_merkle_json")?,
    )?;
    Ok(S3ObjectEvidenceHead {
        evidence: S3ObjectEvidenceLog::from_head(event)?,
        commitment: commitment_json,
    })
}

async fn lock_s3_object(
    connection: &mut PgConnection,
    scope_namespace: &str,
    object_key: &str,
) -> Result<(), PostgresMetadataStoreError> {
    query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("s3-object:{scope_namespace}:{object_key}"))
        .execute(&mut *connection)
        .await?;
    Ok(())
}

async fn verify_s3_object_evidence(
    connection: &mut PgConnection,
    scope_namespace: &str,
    object_key: &str,
    entry: &S3ObjectEntry,
) -> Result<(), PostgresMetadataStoreError> {
    let snapshot = s3_object_snapshot(scope_namespace, object_key, Some(entry))?;
    let evidence = load_s3_object_evidence_head(connection, scope_namespace, object_key).await?;
    verify_snapshot_evidence(&evidence.evidence, &snapshot)?;
    Ok(())
}

async fn persist_s3_object_evidence(
    connection: &mut PgConnection,
    evidence: &S3ObjectEvidenceLog,
    persisted_sequence: i64,
    mut previous: Option<ReliabilityMerkleCommit>,
) -> Result<(), PostgresMetadataStoreError> {
    for event in evidence.events() {
        if u64_to_i64(event.sequence)? > persisted_sequence {
            previous = Some(persist_s3_object_event(connection, event, previous.as_ref()).await?);
        }
    }
    Ok(())
}

async fn persist_s3_object_event(
    connection: &mut PgConnection,
    event: &S3ObjectLifecycleEvent,
    previous: Option<&ReliabilityMerkleCommit>,
) -> Result<ReliabilityMerkleCommit, PostgresMetadataStoreError> {
    event.verify_integrity()?;
    let sequence = u64_to_i64(event.sequence)?;
    let commitment = build_reliability_merkle_commit_with_previous(event, previous)?;
    let result = query(
        "INSERT INTO shardline_reliability_events
            (operation_kind, operation_id, sequence, event_json, created_at_unix_seconds,
             merkle_commit_json)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (operation_kind, operation_id, sequence) DO UPDATE
         SET merkle_commit_json = COALESCE(
             shardline_reliability_events.merkle_commit_json,
             EXCLUDED.merkle_commit_json
         )
         WHERE shardline_reliability_events.event_json = EXCLUDED.event_json",
    )
    .bind(event.operation.kind.as_str())
    .bind(&event.operation.operation_id)
    .bind(sequence)
    .bind(to_value(event)?)
    .bind(shardline_protocol::unix_now_seconds_lossy() as i64)
    .bind(to_value(&commitment)?)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() == 0 {
        return Err(PostgresMetadataStoreError::ReliabilityEventConflict(
            event.operation.operation_id.clone(),
        ));
    }
    Ok(commitment)
}

pub(super) async fn current_s3_object_on_connection(
    connection: &mut PgConnection,
    scope_namespace: &str,
    object_key: &str,
) -> Result<Option<S3ObjectEntry>, PostgresMetadataStoreError> {
    let row = query(
        "SELECT scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                user_metadata, updated_at_unix_seconds
         FROM shardline_s3_objects WHERE scope_namespace = $1 AND object_key = $2 LIMIT 1",
    )
    .bind(scope_namespace)
    .bind(object_key)
    .fetch_optional(&mut *connection)
    .await?;
    row.map(|row| s3_object_entry_from_row(&row)).transpose()
}

/// Returns the exact removed row for the transaction's deletion evidence.
/// Callers retain their existing ownership/locking and must roll back on error.
pub(super) async fn remove_s3_object_on_connection(
    connection: &mut PgConnection,
    scope_namespace: &str,
    object_key: &str,
) -> Result<Option<S3ObjectEntry>, PostgresMetadataStoreError> {
    let before = current_s3_object_on_connection(connection, scope_namespace, object_key).await?;
    let deleted = query(
        "DELETE FROM shardline_s3_objects
         WHERE scope_namespace = $1 AND object_key = $2",
    )
    .bind(scope_namespace)
    .bind(object_key)
    .execute(&mut *connection)
    .await?;
    query(
        "DELETE FROM shardline_s3_multipart_counts
         WHERE scope_namespace = $1 AND object_key = $2",
    )
    .bind(scope_namespace)
    .bind(object_key)
    .execute(&mut *connection)
    .await?;
    Ok((deleted.rows_affected() > 0).then_some(before).flatten())
}

pub(super) async fn record_s3_object_transition(
    connection: &mut PgConnection,
    before: Option<&S3ObjectEntry>,
    after: Option<&S3ObjectEntry>,
) -> Result<(), PostgresMetadataStoreError> {
    let entry = after.or(before).ok_or_else(|| {
        PostgresMetadataStoreError::Unsupported("missing S3 object identity".into())
    })?;
    let before_snapshot = s3_object_snapshot(&entry.scope_namespace, &entry.object_key, before)?;
    let after_snapshot = s3_object_snapshot(&entry.scope_namespace, &entry.object_key, after)?;
    let stored =
        load_s3_object_evidence_head(connection, &entry.scope_namespace, &entry.object_key).await?;
    // Reuse the verified persisted head from this transition's transaction.
    // Capture it before baseline repair, whose sequence-zero event is new.
    // Carry its verified commitment into each append, then chain any newly
    // persisted baseline locally. Conflicting event inserts still fail.
    let persisted_sequence = stored
        .evidence
        .events()
        .last()
        .map(|event| u64_to_i64(event.sequence))
        .transpose()?
        .unwrap_or(-1);
    let evidence =
        verify_and_append_snapshot_transition(stored.evidence, before_snapshot, after_snapshot)?.0;
    let previous = stored.commitment.map(from_value).transpose()?;
    persist_s3_object_evidence(connection, &evidence, persisted_sequence, previous).await
}

#[async_trait::async_trait]
impl S3ObjectIndexStore for PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    async fn upsert_s3_object(&self, entry: &S3ObjectEntry) -> Result<(), Self::Error> {
        upsert_s3_object_with_optional_count(self, entry, None).await
    }

    async fn upsert_s3_object_with_multipart_count(
        &self,
        entry: &S3ObjectEntry,
        part_count: u32,
    ) -> Result<(), Self::Error> {
        upsert_s3_object_with_optional_count(self, entry, Some(part_count)).await
    }

    async fn s3_multipart_part_count(
        &self,
        entry: &S3ObjectEntry,
    ) -> Result<Option<u32>, Self::Error> {
        let row = query(
            "SELECT part_count, proof FROM shardline_s3_multipart_counts
             WHERE scope_namespace=$1 AND object_key=$2 AND file_id=$3
               AND content_hash=$4 AND updated_at_unix_seconds=$5",
        )
        .bind(&entry.scope_namespace)
        .bind(&entry.object_key)
        .bind(&entry.file_id)
        .bind(&entry.content_hash)
        .bind(entry.updated_at_unix_seconds)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let count: i32 = row.try_get("part_count")?;
        let Ok(count) = u32::try_from(count) else {
            return Ok(None);
        };
        let proof: String = row.try_get("proof")?;
        let expected = s3_multipart_count_proof(
            &entry.scope_namespace,
            &entry.object_key,
            &entry.file_id,
            &entry.content_hash,
            count,
            entry.updated_at_unix_seconds,
        )?;
        Ok((proof == expected).then_some(count))
    }

    async fn scan_s3_object_exact_with_multipart_count(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<Option<(S3ObjectEntry, Option<u32>)>, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        let row = query(
            "SELECT o.scope_namespace, o.object_key, o.file_id, o.size_bytes,
                    o.content_hash, o.etag, o.user_metadata, o.updated_at_unix_seconds,
                    c.file_id AS multipart_file_id,
                    c.content_hash AS multipart_content_hash,
                    c.part_count AS multipart_part_count,
                    c.updated_at_unix_seconds AS multipart_updated_at,
                    c.proof AS multipart_count_proof
             FROM shardline_s3_objects AS o
             LEFT JOIN shardline_s3_multipart_counts AS c USING (scope_namespace, object_key)
             WHERE o.scope_namespace=$1 AND o.object_key=$2 LIMIT 1",
        )
        .bind(scope_namespace)
        .bind(object_key)
        .fetch_optional(&mut *transaction)
        .await?;
        let result = row
            .as_ref()
            .map(|row| {
                let entry = s3_object_entry_from_row(row)?;
                let count = multipart_count_from_row(row, &entry)?;
                Ok::<_, PostgresMetadataStoreError>((entry, count))
            })
            .transpose()?;
        if let Some((entry, _)) = result.as_ref() {
            verify_s3_object_evidence(&mut transaction, scope_namespace, object_key, entry).await?;
        }
        transaction.commit().await?;
        Ok(result)
    }

    async fn compare_and_swap_s3_object(
        &self,
        expected: Option<&S3ObjectEntry>,
        replacement: &S3ObjectEntry,
    ) -> Result<bool, Self::Error> {
        compare_and_swap_s3_object_with_optional_count(self, expected, replacement, None).await
    }

    async fn compare_and_swap_s3_object_with_multipart_count(
        &self,
        expected: Option<&S3ObjectEntry>,
        replacement: &S3ObjectEntry,
        part_count: u32,
    ) -> Result<bool, Self::Error> {
        compare_and_swap_s3_object_with_optional_count(
            self,
            expected,
            replacement,
            Some(part_count),
        )
        .await
    }

    async fn delete_s3_object(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<bool, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        lock_s3_object(&mut transaction, scope_namespace, object_key).await?;
        let before =
            remove_s3_object_on_connection(&mut transaction, scope_namespace, object_key).await?;
        let deleted = before.is_some();
        if deleted {
            record_s3_object_transition(&mut transaction, before.as_ref(), None).await?;
        }
        transaction.commit().await?;
        Ok(deleted)
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
        use std::fmt::Write as _;

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
             WHERE scope_namespace = $1 AND object_key COLLATE \"C\" {comparison} $2"
        );
        let mut index = 3usize;
        if upper.is_some() {
            write!(sql, " AND object_key COLLATE \"C\" < ${index}")
                .map_err(|e| PostgresMetadataStoreError::IntegerOutOfRange(e.to_string()))?;
            index = index.saturating_add(1);
        }
        let limit_i64 = u64_to_i64(u64::try_from(limit).unwrap_or(u64::MAX))?;
        sql.push_str(" ORDER BY object_key COLLATE \"C\"");
        write!(sql, " LIMIT ${index}")
            .map_err(|e| PostgresMetadataStoreError::IntegerOutOfRange(e.to_string()))?;
        // Only fixed SQL fragments and placeholder numbers are assembled; values stay bound.
        let mut q = query(sqlx::AssertSqlSafe(sql))
            .bind(scope_namespace)
            .bind(lower);
        if let Some(upper) = upper {
            q = q.bind(upper);
        }
        q = q.bind(limit_i64);
        let mut transaction = self.pool.begin().await?;
        let rows = q.fetch_all(&mut *transaction).await?;
        let values = rows
            .iter()
            .map(s3_object_entry_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let snapshots = values
            .iter()
            .map(|value| s3_object_snapshot(&value.scope_namespace, &value.object_key, Some(value)))
            .collect::<Result<Vec<_>, _>>()?;
        let operation_ids = snapshots
            .iter()
            .map(|snapshot| {
                snapshot
                    .evidence_operation()
                    .map(|operation| operation.operation_id)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut evidence_heads = load_postgres_latest_evidence_heads(
            &mut *transaction,
            OperationKind::S3Object,
            &operation_ids,
        )
        .await?;
        for (operation_id, expected) in operation_ids.iter().zip(&snapshots) {
            let event_json = evidence_heads.remove(operation_id).ok_or_else(|| {
                PostgresMetadataStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                )
            })?;
            let event: S3ObjectLifecycleEvent = from_value(event_json)?;
            verify_snapshot_event(&event, expected)?;
        }
        transaction.commit().await?;
        Ok(values)
    }

    async fn scan_s3_object_exact(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<Option<S3ObjectEntry>, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        let row = query(
            "SELECT scope_namespace, object_key, file_id, size_bytes, content_hash, etag,
                    user_metadata, updated_at_unix_seconds
             FROM shardline_s3_objects WHERE scope_namespace = $1 AND object_key = $2 LIMIT 1",
        )
        .bind(scope_namespace)
        .bind(object_key)
        .fetch_optional(&mut *transaction)
        .await?;
        let value = row.as_ref().map(s3_object_entry_from_row).transpose()?;
        if let Some(value) = value.as_ref() {
            verify_s3_object_evidence(&mut transaction, scope_namespace, object_key, value).await?;
        }
        transaction.commit().await?;
        Ok(value)
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
    use sqlx::query_scalar;

    async fn connect_postgres() -> Option<sqlx::PgPool> {
        super::super::connect_isolated_postgres().await
    }

    fn entry(scope_namespace: &str, object_key: &str, file_id: &str) -> S3ObjectEntry {
        S3ObjectEntry {
            scope_namespace: scope_namespace.to_owned(),
            object_key: object_key.to_owned(),
            file_id: file_id.to_owned(),
            size_bytes: 123,
            content_hash: "ab".repeat(32),
            etag: "ab".repeat(32),
            user_metadata: Vec::new(),
            updated_at_unix_seconds: 1000,
        }
    }

    async fn cleanup(pool: &sqlx::PgPool, scope_namespace: &str) {
        // The production delete gate intentionally rejects raw row cleanup.
        // Seed a valid terminal deletion boundary for each fixture row so the
        // test cleanup exercises the same deferred-trigger contract as a real
        // adapter mutation, including after evidence-tampering tests.
        sqlx::query(
            "WITH fixture_rows AS (
                 SELECT o.scope_namespace, o.object_key,
                        COALESCE(MAX(e.sequence), -1) + 1 AS next_sequence
                 FROM shardline_s3_objects AS o
                 LEFT JOIN shardline_reliability_events AS e
                   ON e.operation_kind = 'S3Object'
                  AND e.operation_id = octet_length(o.scope_namespace)::text || ':' || o.scope_namespace
                      || octet_length(o.object_key)::text || ':' || o.object_key
                 WHERE o.scope_namespace = $1
                 GROUP BY o.scope_namespace, o.object_key
             )
             INSERT INTO shardline_reliability_events
                 (operation_kind, operation_id, sequence, event_json, created_at_unix_seconds)
             SELECT 'S3Object',
                    octet_length(scope_namespace)::text || ':' || scope_namespace
                        || octet_length(object_key)::text || ':' || object_key,
                    next_sequence,
                    jsonb_build_object(
                        'after', jsonb_build_object(
                            'entry', NULL,
                            'object_key', object_key,
                            'scope_namespace', scope_namespace
                        )
                    ),
                    EXTRACT(EPOCH FROM now())::bigint
             FROM fixture_rows
             ON CONFLICT (operation_kind, operation_id, sequence) DO NOTHING",
        )
        .bind(scope_namespace)
        .execute(pool)
        .await
        .expect("seed deletion evidence");
        sqlx::query("DELETE FROM shardline_s3_objects WHERE scope_namespace = $1")
            .bind(scope_namespace)
            .execute(pool)
            .await
            .expect("cleanup s3 object rows");
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = $1
               AND operation_id LIKE $2 || '%'",
        )
        .bind(OperationKind::S3Object.as_str())
        .bind(format!("{}:{scope_namespace}", scope_namespace.len()))
        .execute(pool)
        .await
        .expect("cleanup s3 object evidence");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_compare_and_swap_has_one_cross_connection_winner() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = "pg-s3-cas";
        cleanup(&pool, scope).await;
        let first_store = PostgresIndexStore::new(pool.clone());
        let second_store = PostgresIndexStore::new(pool.clone());
        let first = entry(scope, "model.bin", "first");
        let second = entry(scope, "model.bin", "second");

        let (first_won, second_won) = tokio::join!(
            first_store.compare_and_swap_s3_object(None, &first),
            second_store.compare_and_swap_s3_object(None, &second),
        );
        let first_won = first_won.unwrap();
        let second_won = second_won.unwrap();
        assert_ne!(first_won, second_won);

        let stored = first_store
            .scan_s3_object_exact(scope, "model.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, if first_won { first } else { second });

        let third = entry(scope, "model.bin", "third");
        let fourth = entry(scope, "model.bin", "fourth");
        let (third_won, fourth_won) = tokio::join!(
            first_store.compare_and_swap_s3_object(Some(&stored), &third),
            second_store.compare_and_swap_s3_object(Some(&stored), &fourth),
        );
        let third_won = third_won.unwrap();
        let fourth_won = fourth_won.unwrap();
        assert_ne!(third_won, fourth_won);

        let updated = first_store
            .scan_s3_object_exact(scope, "model.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated, if third_won { third } else { fourth });
        cleanup(&pool, scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_concurrent_unconditional_upserts_keep_a_verifiable_chain() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-concurrent-upsert-{}", std::process::id());
        cleanup(&pool, &scope).await;
        let first_store = PostgresIndexStore::new(pool.clone());
        let second_store = PostgresIndexStore::new(pool.clone());
        let first = entry(&scope, "model.bin", "first");
        let second = entry(&scope, "model.bin", "second");

        let (first_result, second_result) = tokio::join!(
            first_store.upsert_s3_object(&first),
            second_store.upsert_s3_object(&second),
        );
        first_result.unwrap();
        second_result.unwrap();
        let stored = first_store
            .scan_s3_object_exact(&scope, "model.bin")
            .await
            .unwrap()
            .unwrap();
        assert!(stored == first || stored == second);
        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_upsert_overwrites_existing_row() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-overwrite-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        let first = entry(&scope, "data/model.pt", "f1");
        let overwrite = entry(&scope, "data/model.pt", "f2");
        S3ObjectIndexStore::upsert_s3_object(&store, &first)
            .await
            .expect("first upsert");
        S3ObjectIndexStore::upsert_s3_object(&store, &overwrite)
            .await
            .expect("overwrite upsert");

        let rows = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", None, 100)
            .await
            .expect("scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], overwrite);

        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_identical_s3_object_upsert_and_cas_skip_row_update_and_keep_evidence() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-replay-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        let value = entry(&scope, "data/replay.pt", "file");

        store.upsert_s3_object(&value).await.unwrap();
        let initial_xmin: String = sqlx::query_scalar(
            "SELECT xmin::text FROM shardline_s3_objects
             WHERE scope_namespace = $1 AND object_key = $2",
        )
        .bind(&scope)
        .bind("data/replay.pt")
        .fetch_one(&pool)
        .await
        .unwrap();
        store.upsert_s3_object(&value).await.unwrap();
        let replay_xmin: String = sqlx::query_scalar(
            "SELECT xmin::text FROM shardline_s3_objects
             WHERE scope_namespace = $1 AND object_key = $2",
        )
        .bind(&scope)
        .bind("data/replay.pt")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(initial_xmin, replay_xmin);
        assert!(
            store
                .compare_and_swap_s3_object(Some(&value), &value)
                .await
                .unwrap()
        );
        let cas_xmin: String = sqlx::query_scalar(
            "SELECT xmin::text FROM shardline_s3_objects WHERE scope_namespace = $1 AND object_key = $2",
        )
        .bind(&scope)
        .bind("data/replay.pt")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(initial_xmin, cas_xmin);
        assert_eq!(
            store
                .scan_s3_object_exact(&scope, "data/replay.pt")
                .await
                .unwrap(),
            Some(value.clone())
        );
        let event_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM shardline_reliability_events
             WHERE operation_kind = 'S3Object'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            event_count, 4,
            "the replay should still be recorded in evidence"
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
        assert_eq!(
            store
                .scan_s3_object_exact(&scope, "data/replay.pt")
                .await
                .unwrap(),
            Some(newer)
        );
        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_delete_then_absent() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-delete-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        let e = entry(&scope, "data/delete.pt", "f3");
        S3ObjectIndexStore::upsert_s3_object(&store, &e)
            .await
            .expect("upsert");

        assert!(
            S3ObjectIndexStore::delete_s3_object(&store, &scope, "data/delete.pt")
                .await
                .expect("delete")
        );
        assert!(
            !S3ObjectIndexStore::delete_s3_object(&store, &scope, "data/delete.pt")
                .await
                .expect("second delete")
        );
        assert!(
            S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", None, 100)
                .await
                .expect("scan")
                .is_empty()
        );

        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_recreated_object_preserves_complete_evidence_sequence() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-recreate-history-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        let first = entry(&scope, "history.bin", "first");
        let replacement = entry(&scope, "history.bin", "replacement");
        for value in [&first, &replacement] {
            store
                .upsert_s3_object(value)
                .await
                .expect("publish version");
        }
        assert!(
            store
                .delete_s3_object(&scope, "history.bin")
                .await
                .expect("delete")
        );
        store.upsert_s3_object(&first).await.expect("recreate");
        let operation = s3_object_snapshot(&scope, "history.bin", None)
            .expect("snapshot")
            .evidence_operation()
            .expect("operation");
        let rows = query(
            "SELECT sequence, event_json, merkle_commit_json FROM shardline_reliability_events
             WHERE operation_kind = $1 AND operation_id = $2 ORDER BY sequence",
        )
        .bind(OperationKind::S3Object.as_str())
        .bind(&operation.operation_id)
        .fetch_all(&pool)
        .await
        .expect("persisted history");
        let sequences = rows
            .iter()
            .map(|row| {
                i64_to_u64(row.try_get("sequence").expect("sequence")).expect("valid sequence")
            })
            .collect::<Vec<_>>();
        let json = rows
            .iter()
            .map(|row| {
                row.try_get::<serde_json::Value, _>("event_json")
                    .expect("event JSON")
            })
            .collect::<Vec<_>>();
        let commitments = rows
            .iter()
            .map(|row| {
                row.try_get::<Option<serde_json::Value>, _>("merkle_commit_json")
                    .expect("commitment JSON")
            })
            .collect::<Vec<_>>();
        shardline_reliability::verify_persisted_event_merkle_chain_with_sequences(
            OperationKind::S3Object,
            &sequences,
            &json,
            &commitments,
        )
        .expect("all persisted Merkle parent links remain valid");
        let events: Vec<S3ObjectLifecycleEvent> = json
            .into_iter()
            .map(|value| from_value(value).expect("event"))
            .collect();
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );
        let history = S3ObjectEvidenceLog::from_events(events).expect("complete valid history");
        verify_snapshot_evidence(
            &history,
            &s3_object_snapshot(&scope, "history.bin", Some(&first)).expect("final snapshot"),
        )
        .expect("history agrees with recreated object");
        assert_eq!(
            store
                .scan_s3_object_exact(&scope, "history.bin")
                .await
                .expect("verified read"),
            Some(first)
        );
        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_read_rejects_tampered_evidence() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-tamper-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        let value = entry(&scope, "model.bin", "file-a");
        S3ObjectIndexStore::upsert_s3_object(&store, &value)
            .await
            .expect("upsert");

        query(
            "UPDATE shardline_reliability_events
             SET event_json = '{\"tampered\":true}'::jsonb
             WHERE operation_kind = $1
               AND event_json->'operation'->>'tenant' = $2",
        )
        .bind(OperationKind::S3Object.as_str())
        .bind(&scope)
        .execute(&pool)
        .await
        .expect("tamper evidence");

        assert!(store.delete_s3_object(&scope, "model.bin").await.is_err());
        let record_store = super::super::PostgresRecordStore::new(pool.clone());
        let mut connection = pool.acquire().await.expect("delete connection");
        assert!(
            record_store
                .delete_s3_object_on_connection(
                    &mut connection,
                    &scope,
                    "model.bin",
                    &value.file_id,
                )
                .await
                .is_err()
        );
        drop(connection);
        let remaining: i64 = query_scalar(
            "SELECT count(*) FROM shardline_s3_objects WHERE scope_namespace = $1 AND object_key = $2",
        )
        .bind(&scope)
        .bind("model.bin")
        .fetch_one(&pool)
        .await
        .expect("rollback row");
        assert_eq!(remaining, 1, "both deletion transactions must roll back");

        assert!(
            S3ObjectIndexStore::scan_s3_object_exact(&store, &scope, "model.bin")
                .await
                .is_err()
        );
        assert!(
            S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", None, 10)
                .await
                .is_err()
        );
        assert!(
            store
                .compare_and_swap_s3_object(Some(&value), &value)
                .await
                .is_err()
        );
        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_read_rejects_evidence_bound_to_another_operation() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-operation-key-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        let value = entry(&scope, "model.bin", "file-a");
        S3ObjectIndexStore::upsert_s3_object(&store, &value)
            .await
            .expect("upsert");
        query(
            "UPDATE shardline_reliability_events
             SET operation_id = $1
             WHERE operation_kind = $2
               AND event_json->'operation'->>'tenant' = $3",
        )
        .bind("rewritten-operation")
        .bind(OperationKind::S3Object.as_str())
        .bind(&scope)
        .execute(&pool)
        .await
        .expect("rewrite evidence row key");

        assert!(
            S3ObjectIndexStore::scan_s3_object_exact(&store, &scope, "model.bin")
                .await
                .is_err()
        );
        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_read_and_write_reject_missing_predecessor_commitment() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-missing-predecessor-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        S3ObjectIndexStore::upsert_s3_object(&store, &entry(&scope, "model.bin", "file-a"))
            .await
            .expect("first upsert");
        S3ObjectIndexStore::upsert_s3_object(&store, &entry(&scope, "model.bin", "file-b"))
            .await
            .expect("second upsert");

        query(
            "UPDATE shardline_reliability_events
             SET merkle_commit_json = NULL
             WHERE operation_kind = $1
               AND event_json->'operation'->>'tenant' = $2
               AND sequence = (
                   SELECT MIN(sequence)
                   FROM shardline_reliability_events
                   WHERE operation_kind = $1
                     AND event_json->'operation'->>'tenant' = $2
               )",
        )
        .bind(OperationKind::S3Object.as_str())
        .bind(&scope)
        .execute(&pool)
        .await
        .expect("remove predecessor commitment");

        assert!(
            S3ObjectIndexStore::scan_s3_object_exact(&store, &scope, "model.bin")
                .await
                .is_err()
        );
        assert!(
            store
                .upsert_s3_object(&entry(&scope, "model.bin", "file-c"))
                .await
                .is_err(),
            "publication must reject incomplete history before appending"
        );
        let file_id: String = query_scalar(
            "SELECT file_id FROM shardline_s3_objects WHERE scope_namespace=$1 AND object_key='model.bin'",
        )
        .bind(&scope)
        .fetch_one(&pool)
        .await
        .expect("unchanged row");
        assert_eq!(
            file_id, "file-b",
            "failed append must roll back publication"
        );
        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_read_rejects_malformed_user_metadata() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-malformed-metadata-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        let value = entry(&scope, "model.bin", "file-a");
        S3ObjectIndexStore::upsert_s3_object(&store, &value)
            .await
            .expect("upsert");

        // Simulate storage corruption below the database gate. Production
        // writes cannot commit this state; disabling only the test trigger
        // lets the read verifier prove it still fails closed.
        let mut corruption = pool.begin().await.expect("begin corruption transaction");
        sqlx::raw_sql(
            "ALTER TABLE shardline_s3_objects
             DISABLE TRIGGER shardline_s3_object_reliability_gate",
        )
        .execute(&mut *corruption)
        .await
        .expect("disable test trigger");
        query(
            "UPDATE shardline_s3_objects
             SET user_metadata = '{not-json}'
             WHERE scope_namespace = $1 AND object_key = $2",
        )
        .bind(&scope)
        .bind("model.bin")
        .execute(&mut *corruption)
        .await
        .expect("corrupt user metadata");
        sqlx::raw_sql(
            "ALTER TABLE shardline_s3_objects
             ENABLE TRIGGER shardline_s3_object_reliability_gate",
        )
        .execute(&mut *corruption)
        .await
        .expect("restore test trigger");
        corruption
            .commit()
            .await
            .expect("commit corruption fixture");

        assert!(
            S3ObjectIndexStore::scan_s3_object_exact(&store, &scope, "model.bin")
                .await
                .is_err()
        );
        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_read_rejects_missing_baseline_evidence_without_writing() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-repair-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        let value = entry(&scope, "repair.bin", "file-a");
        S3ObjectIndexStore::upsert_s3_object(&store, &value)
            .await
            .expect("upsert");
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = $1
               AND event_json->'operation'->>'tenant' = $2",
        )
        .bind(OperationKind::S3Object.as_str())
        .bind(&scope)
        .execute(&pool)
        .await
        .expect("remove evidence");

        assert!(
            S3ObjectIndexStore::scan_s3_object_exact(&store, &scope, "repair.bin")
                .await
                .is_err()
        );
        assert!(
            S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", None, 10)
                .await
                .is_err(),
            "paginated listings must fail closed when a returned row has no evidence"
        );
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shardline_reliability_events
             WHERE operation_kind = $1
               AND event_json->'operation'->>'tenant' = $2",
        )
        .bind(OperationKind::S3Object.as_str())
        .bind(&scope)
        .fetch_one(&pool)
        .await
        .expect("count evidence");
        assert_eq!(count, 0);
        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_scan_prefix_cursor_and_limit() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-scan-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        for (key, id) in [
            ("a.txt", 1),
            ("data/1.txt", 2),
            ("data/2.txt", 3),
            ("data/sub/3.txt", 4),
            ("zz.txt", 5),
        ] {
            S3ObjectIndexStore::upsert_s3_object(&store, &entry(&scope, key, &format!("f{id}")))
                .await
                .expect("upsert");
        }

        // Empty prefix lists every key in raw-key order.
        let rows = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", None, 100)
            .await
            .expect("scan");
        let keys: Vec<&str> = rows.iter().map(|row| row.object_key.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "a.txt",
                "data/1.txt",
                "data/2.txt",
                "data/sub/3.txt",
                "zz.txt"
            ]
        );

        // Prefix filtering.
        let rows = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "data", None, 100)
            .await
            .expect("scan");
        let keys: Vec<&str> = rows.iter().map(|row| row.object_key.as_str()).collect();
        assert_eq!(keys, vec!["data/1.txt", "data/2.txt", "data/sub/3.txt"]);

        // Keyset cursor resumes strictly after the given raw key.
        let rows = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", Some("data/2.txt"), 100)
            .await
            .expect("scan");
        let keys: Vec<&str> = rows.iter().map(|row| row.object_key.as_str()).collect();
        assert_eq!(keys, vec!["data/sub/3.txt", "zz.txt"]);

        // Limit truncation, then cursor resumes after the last returned key.
        let rows = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", None, 3)
            .await
            .expect("scan");
        assert_eq!(rows.len(), 3);
        let cursor = rows.last().expect("non-empty").object_key.clone();
        let more = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "", Some(&cursor), 100)
            .await
            .expect("scan");
        assert_eq!(more.len(), 2);

        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_same_key_legacy_writer_is_rejected_by_reliability_gate() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-mixed-version-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        let original = entry(&scope, "same-key.bin", "file-a");
        S3ObjectIndexStore::upsert_s3_object(&store, &original)
            .await
            .expect("seed object");

        let mut transaction = pool.begin().await.expect("begin legacy transaction");
        sqlx::query(
            "UPDATE shardline_s3_objects
             SET file_id = $3
             WHERE scope_namespace = $1 AND object_key = $2",
        )
        .bind(&scope)
        .bind("same-key.bin")
        .bind("legacy-overwrite")
        .execute(&mut *transaction)
        .await
        .expect("legacy write reaches deferred gate");
        assert!(
            transaction.commit().await.is_err(),
            "an N-1 same-key write must not commit without reliability evidence"
        );

        assert_eq!(
            S3ObjectIndexStore::scan_s3_object_exact(&store, &scope, "same-key.bin")
                .await
                .expect("read after rejected write"),
            Some(original)
        );
        cleanup(&pool, &scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_exact_requires_full_key_match() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("s3-exact-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
        for (key, id) in [("a/b", 1), ("a/b/c", 2)] {
            S3ObjectIndexStore::upsert_s3_object(&store, &entry(&scope, key, &format!("f{id}")))
                .await
                .expect("upsert");
        }

        // F-33: a prefix-sibling must never satisfy the exact lookup.
        let exact = S3ObjectIndexStore::scan_s3_object_exact(&store, &scope, "a")
            .await
            .expect("exact lookup");
        assert!(
            exact.is_none(),
            "a prefix-sibling must never satisfy an exact lookup"
        );

        let found = S3ObjectIndexStore::scan_s3_object_exact(&store, &scope, "a/b")
            .await
            .expect("exact lookup");
        assert_eq!(found.expect("exact row").object_key, "a/b");

        // The prefix scan still lists the siblings (listing unchanged).
        let rows = S3ObjectIndexStore::scan_s3_objects(&store, &scope, "a", None, 100)
            .await
            .expect("scan");
        let keys: Vec<&str> = rows.iter().map(|row| row.object_key.as_str()).collect();
        assert_eq!(keys, vec!["a/b", "a/b/c"]);

        cleanup(&pool, &scope).await;
    }

    /// Round-trips the bundled `s3_object_index` migration inside a rolled-back
    /// transaction so the shared CI database is left exactly as found.
    #[tokio::test(flavor = "multi_thread")]
    async fn pg_s3_object_migration_up_down_roundtrip() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        // The Postgres migration set lives at the workspace root `migrations/`
        // (the `crates/shardline-index/migrations/` set is the SQLite variant).
        let up = include_str!("../../../../migrations/20260813000000_s3_object_index.up.sql");
        let down = include_str!("../../../../migrations/20260813000000_s3_object_index.down.sql");

        let mut transaction = pool.begin().await.expect("begin transaction");
        sqlx::raw_sql("DROP TABLE IF EXISTS shardline_s3_objects")
            .execute(&mut *transaction)
            .await
            .expect("drop table");
        sqlx::raw_sql(up)
            .execute(&mut *transaction)
            .await
            .expect("apply up migration");
        assert!(
            s3_object_table_exists(&mut transaction).await,
            "up migration must create shardline_s3_objects"
        );

        sqlx::raw_sql(down)
            .execute(&mut *transaction)
            .await
            .expect("apply down migration");
        assert!(
            !s3_object_table_exists(&mut transaction).await,
            "down migration must drop shardline_s3_objects"
        );

        transaction.rollback().await.expect("rollback transaction");
    }

    async fn s3_object_table_exists(connection: &mut sqlx::PgConnection) -> bool {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = 'shardline_s3_objects'
             )",
        )
        .fetch_one(&mut *connection)
        .await
        .expect("check table existence")
    }
    #[tokio::test]
    async fn inclusive_s3_key_bounds_preserve_unicode_and_exact_successor() {
        let Some(pool) = connect_postgres().await else {
            return;
        };
        let scope = format!("s3-inclusive-{}", std::process::id());
        let store = PostgresIndexStore::new(pool.clone());
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
                .upsert_s3_object(&entry(&scope, key, "file"))
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
                .scan_s3_objects_from(&scope, prefix, Some(S3ObjectScanStart::Inclusive(bound)), 2)
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
            .scan_s3_objects_from(&scope, "a", Some(S3ObjectScanStart::Exclusive("a0")), 2)
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
                .scan_s3_objects_from(&scope, "a", Some(S3ObjectScanStart::Inclusive("a0")), 0)
                .await
                .unwrap()
                .is_empty()
        );
        cleanup(&pool, &scope).await;
    }
}
