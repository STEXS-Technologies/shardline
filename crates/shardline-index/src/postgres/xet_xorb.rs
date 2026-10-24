use shardline_protocol::RepositoryScope;
use sqlx::{PgConnection, Row, query};

use super::{PostgresMetadataStoreError, PostgresRecordStore};
use crate::{XetXorbDescriptor, record_key::repository_scope_key};

fn invalid() -> PostgresMetadataStoreError {
    sqlx::Error::Protocol("invalid or conflicting Xorb descriptor".to_owned()).into()
}

fn ordered_descriptors(descriptors: &[XetXorbDescriptor]) -> Vec<&XetXorbDescriptor> {
    let mut ordered: Vec<_> = descriptors.iter().collect();
    ordered.sort_unstable_by(|left, right| left.hash.cmp(&right.hash));
    ordered
}

pub(super) async fn stage_descriptors(
    connection: &mut PgConnection,
    descriptors: &[XetXorbDescriptor],
) -> Result<(), PostgresMetadataStoreError> {
    // Both insertion conflicts and derived-member repair can retain row locks
    // until commit. All overlapping batches must acquire them in one order.
    for descriptor in ordered_descriptors(descriptors) {
        if !descriptor.has_valid_structure() {
            return Err(invalid());
        }
        let json = serde_json::to_value(descriptor)?;
        let inserted = query("INSERT INTO shardline_xet_xorbs(xorb_hash,descriptor) VALUES($1,$2) ON CONFLICT(xorb_hash) DO NOTHING")
            .bind(&descriptor.hash).bind(&json).execute(&mut *connection).await?.rows_affected();
        if inserted == 0 {
            let row = query("SELECT CASE WHEN octet_length(descriptor::text)<=4194304 THEN descriptor END FROM shardline_xet_xorbs WHERE xorb_hash=$1")
                .bind(&descriptor.hash)
                .fetch_one(&mut *connection)
                .await?;
            let stored: Option<serde_json::Value> = row.try_get(0)?;
            if decode(stored.ok_or_else(invalid)?, &descriptor.hash)? != *descriptor {
                return Err(invalid());
            }
            continue;
        }
        let hashes: Vec<_> = descriptor
            .chunks
            .iter()
            .map(|chunk| chunk.hash.clone())
            .collect();
        query("INSERT INTO shardline_xet_chunks(xorb_hash,chunk_index,chunk_hash) SELECT $1,ordinal-1,hash FROM unnest($2::text[]) WITH ORDINALITY AS members(hash,ordinal)")
            .bind(&descriptor.hash).bind(hashes).execute(&mut *connection).await?;
    }
    Ok(())
}

impl PostgresRecordStore {
    /// Reads a descriptor only when its complete derived member index agrees.
    /// The snapshot does not establish source existence or authentication.
    ///
    /// # Errors
    /// Returns database errors and rejects invalid or oversized descriptors.
    pub async fn xet_indexed_xorb_descriptor(
        &self,
        hash: &str,
    ) -> Result<Option<XetXorbDescriptor>, PostgresMetadataStoreError> {
        let mut transaction = self.pool.begin().await?;
        query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *transaction)
            .await?;
        let row = query("SELECT CASE WHEN octet_length(descriptor::text)<=4194304 THEN descriptor END FROM shardline_xet_xorbs WHERE xorb_hash=$1")
            .bind(hash).fetch_optional(&mut *transaction).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let descriptor = decode(
            row.try_get::<Option<serde_json::Value>, _>(0)?
                .ok_or_else(invalid)?,
            hash,
        )?;
        let rows = query("SELECT chunk_index,chunk_hash FROM shardline_xet_chunks WHERE xorb_hash=$1 ORDER BY chunk_index LIMIT 8193")
            .bind(hash).fetch_all(&mut *transaction).await?
            .into_iter().map(|member_row| Ok((member_row.try_get(0)?, member_row.try_get(1)?)))
            .collect::<Result<Vec<(i64, String)>, sqlx::Error>>()?;
        transaction.commit().await?;
        Ok(crate::xet_xorb::member_rows_match(&descriptor, &rows).then_some(descriptor))
    }

    /// Finds one exact derived member location without scanning other containers.
    /// This does not establish authorization, source existence or authenticity.
    ///
    /// # Errors
    /// Returns database errors and rejects invalid or oversized descriptors.
    pub async fn xet_chunk_location(
        &self,
        member: &str,
        xorb: &str,
    ) -> Result<Option<XetXorbDescriptor>, PostgresMetadataStoreError> {
        let row = query(
            "SELECT CASE WHEN octet_length(d.descriptor::text)<=4194304 THEN d.descriptor END
             FROM shardline_xet_xorbs d WHERE d.xorb_hash=$1 AND EXISTS(
               SELECT 1 FROM shardline_xet_chunks c WHERE c.chunk_hash=$2 AND c.xorb_hash=d.xorb_hash)",
        ).bind(xorb).bind(member).fetch_optional(&self.pool).await?;
        row.map(|row| {
            decode(
                row.try_get::<Option<serde_json::Value>, _>(0)?
                    .ok_or_else(invalid)?,
                xorb,
            )
        })
        .transpose()
    }

    /// Reads rebuildable metadata, without establishing source existence or authority.
    ///
    /// # Errors
    /// Returns an error for database or descriptor parsing failure.
    pub async fn xet_xorb_descriptor(
        &self,
        hash: &str,
    ) -> Result<Option<XetXorbDescriptor>, PostgresMetadataStoreError> {
        let row = query("SELECT CASE WHEN octet_length(descriptor::text)<=4194304 THEN descriptor END AS descriptor FROM shardline_xet_xorbs WHERE xorb_hash=$1")
            .bind(hash)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            decode(
                row.try_get::<Option<serde_json::Value>, _>(0)?
                    .ok_or_else(invalid)?,
                hash,
            )
        })
        .transpose()
    }

    /// Installs verified derived descriptors without publishing a file reference.
    ///
    /// # Errors
    /// Returns an error for invalid/conflicting metadata or database failure.
    pub async fn cache_xet_xorb_descriptors(
        &self,
        descriptors: &[XetXorbDescriptor],
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = self.pool.begin().await?;
        stage_descriptors(&mut transaction, descriptors).await?;
        for descriptor in ordered_descriptors(descriptors) {
            repair_member_rows(&mut transaction, descriptor).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Finds bounded scoped candidates. Native root, source records and object
    /// liveness must still be verified by the caller.
    ///
    /// # Errors
    /// Returns an error if the database or descriptors cannot be read.
    pub async fn xet_chunk_candidates(
        &self,
        hash: &str,
        scope: Option<&RepositoryScope>,
    ) -> Result<Vec<XetXorbDescriptor>, PostgresMetadataStoreError> {
        self.xet_chunk_candidates_after(hash, scope, None).await
    }

    /// Reads a bounded page after an exclusive Xorb-hash cursor. Authority and
    /// source liveness remain the caller's responsibility.
    ///
    /// # Errors
    /// Returns an error if the database or descriptors cannot be read.
    pub async fn xet_chunk_candidates_after(
        &self,
        hash: &str,
        scope: Option<&RepositoryScope>,
        after: Option<&str>,
    ) -> Result<Vec<XetXorbDescriptor>, PostgresMetadataStoreError> {
        let scope = scope.map(|scope| repository_scope_key(Some(scope)));
        let rows = query("SELECT xorb_hash,CASE WHEN octet_length(descriptor::text)<=4194304 THEN descriptor END AS descriptor FROM shardline_xet_xorbs d WHERE d.xorb_hash IN (
            SELECT DISTINCT c.xorb_hash FROM shardline_xet_chunks c WHERE c.chunk_hash=$1
            AND ($3::text IS NULL OR c.xorb_hash>$3)
            AND ($2::text IS NULL OR EXISTS(SELECT 1 FROM shardline_record_membership m WHERE m.member_hash=c.xorb_hash AND m.scope_key=$2))
            ORDER BY c.xorb_hash LIMIT 32) ORDER BY d.xorb_hash").bind(hash).bind(scope).bind(after).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                decode(
                    row.try_get::<Option<serde_json::Value>, _>(1)?
                        .ok_or_else(invalid)?,
                    row.try_get::<&str, _>(0)?,
                )
            })
            .collect()
    }
}

async fn repair_member_rows(
    connection: &mut PgConnection,
    descriptor: &XetXorbDescriptor,
) -> Result<(), PostgresMetadataStoreError> {
    // Serialize recovery for this derived descriptor. A second repair must
    // inspect the first repair's committed rows, not race its replacement.
    query("SELECT 1 FROM shardline_xet_xorbs WHERE xorb_hash=$1 FOR UPDATE")
        .bind(&descriptor.hash)
        .fetch_one(&mut *connection)
        .await?;
    let rows = sqlx::query_as::<_, (i64, String)>("SELECT chunk_index,chunk_hash FROM shardline_xet_chunks WHERE xorb_hash=$1 ORDER BY chunk_index LIMIT 8193")
        .bind(&descriptor.hash).fetch_all(&mut *connection).await?;
    if crate::xet_xorb::member_rows_match(descriptor, &rows) {
        return Ok(());
    }
    query("DELETE FROM shardline_xet_chunks WHERE xorb_hash=$1")
        .bind(&descriptor.hash)
        .execute(&mut *connection)
        .await?;
    let hashes: Vec<_> = descriptor
        .chunks
        .iter()
        .map(|chunk| chunk.hash.clone())
        .collect();
    query("INSERT INTO shardline_xet_chunks(xorb_hash,chunk_index,chunk_hash) SELECT $1,ordinal-1,hash FROM unnest($2::text[]) WITH ORDINALITY AS members(hash,ordinal)")
        .bind(&descriptor.hash).bind(hashes).execute(&mut *connection).await?;
    Ok(())
}

fn decode(
    json: serde_json::Value,
    hash: &str,
) -> Result<XetXorbDescriptor, PostgresMetadataStoreError> {
    let descriptor: XetXorbDescriptor = serde_json::from_value(json)?;
    if descriptor.hash != hash || !descriptor.has_valid_structure() {
        return Err(invalid());
    }
    Ok(descriptor)
}
