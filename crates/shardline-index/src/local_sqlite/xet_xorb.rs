use std::io::{Error, ErrorKind};

use rusqlite::{OptionalExtension, Transaction, params};
use shardline_protocol::RepositoryScope;

use super::{LocalIndexStoreError, LocalRecordStore, retry_sqlite_busy};
use crate::{XetXorbDescriptor, record_key::repository_scope_key};

fn invalid() -> LocalIndexStoreError {
    Error::new(
        ErrorKind::InvalidData,
        "invalid or conflicting Xorb descriptor",
    )
    .into()
}

pub(super) fn stage_descriptors(
    transaction: &Transaction<'_>,
    descriptors: &[XetXorbDescriptor],
) -> Result<(), LocalIndexStoreError> {
    for descriptor in descriptors {
        if !descriptor.has_valid_structure() {
            return Err(invalid());
        }
        let json = serde_json::to_string(descriptor)?;
        let inserted = transaction.execute(
            "INSERT INTO shardline_xet_xorbs(xorb_hash,descriptor) VALUES(?1,?2)
             ON CONFLICT(xorb_hash) DO NOTHING",
            params![descriptor.hash, json],
        )?;
        if inserted == 0 {
            let stored: Option<String> = transaction.query_row(
                "SELECT CASE WHEN length(CAST(descriptor AS BLOB))<=4194304 THEN descriptor END FROM shardline_xet_xorbs WHERE xorb_hash=?1",
                [&descriptor.hash],
                |row| row.get(0),
            )?;
            if decode(&stored.ok_or_else(invalid)?, &descriptor.hash)? != *descriptor {
                return Err(invalid());
            }
            continue;
        }
        let mut insert = transaction.prepare_cached(
            "INSERT INTO shardline_xet_chunks(xorb_hash,chunk_index,chunk_hash) VALUES(?1,?2,?3)",
        )?;
        for (index, chunk) in descriptor.chunks.iter().enumerate() {
            insert.execute(params![
                descriptor.hash,
                i64::try_from(index).map_err(|_error| invalid())?,
                chunk.hash
            ])?;
        }
    }
    Ok(())
}

impl LocalRecordStore {
    /// Reads a descriptor only when its complete derived member index agrees.
    /// The snapshot does not establish source existence or authentication.
    ///
    /// # Errors
    /// Returns database errors and rejects invalid or oversized descriptors.
    pub async fn xet_indexed_xorb_descriptor(
        &self,
        hash: &str,
    ) -> Result<Option<XetXorbDescriptor>, LocalIndexStoreError> {
        let store = self.clone();
        let hash = hash.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut connection = store.open_connection()?;
            let transaction = connection.transaction()?;
            let json: Option<Option<String>> = transaction.query_row(
                "SELECT CASE WHEN length(CAST(descriptor AS BLOB))<=4194304 THEN descriptor END FROM shardline_xet_xorbs WHERE xorb_hash=?1",
                [&hash], |row| row.get(0),
            ).optional()?;
            let Some(json) = json else { return Ok(None); };
            let descriptor = decode(&json.ok_or_else(invalid)?, &hash)?;
            let mut select = transaction.prepare(
                "SELECT chunk_index,chunk_hash FROM shardline_xet_chunks WHERE xorb_hash=?1 ORDER BY chunk_index LIMIT 8193",
            )?;
            let rows = select.query_map([&hash], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<Vec<(i64, String)>, _>>()?;
            Ok(crate::xet_xorb::member_rows_match(&descriptor, &rows).then_some(descriptor))
        }).await.map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
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
    ) -> Result<Option<XetXorbDescriptor>, LocalIndexStoreError> {
        let store = self.clone();
        let member = member.to_owned();
        let xorb = xorb.to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            let json: Option<Option<String>> = connection.query_row(
                "SELECT CASE WHEN length(CAST(d.descriptor AS BLOB))<=4194304 THEN d.descriptor END
                 FROM shardline_xet_xorbs d WHERE d.xorb_hash=?1 AND EXISTS(
                   SELECT 1 FROM shardline_xet_chunks c WHERE c.chunk_hash=?2 AND c.xorb_hash=d.xorb_hash)",
                params![xorb, member], |row| row.get(0),
            ).optional()?;
            json.map(|json| decode(&json.ok_or_else(invalid)?, &xorb)).transpose()
        }).await.map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    /// Reads derived metadata without treating it as source existence or authority.
    ///
    /// # Errors
    /// Returns an error if storage or bounded descriptor parsing fails.
    pub async fn xet_xorb_descriptor(
        &self,
        hash: &str,
    ) -> Result<Option<XetXorbDescriptor>, LocalIndexStoreError> {
        let store = self.clone();
        let hash = hash.to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            let json: Option<Option<String>> = connection
                .query_row(
                    "SELECT CASE WHEN length(CAST(descriptor AS BLOB))<=4194304 THEN descriptor END FROM shardline_xet_xorbs WHERE xorb_hash=?1",
                    [&hash],
                    |row| row.get(0),
                )
                .optional()?;
            json.map(|json| decode(&json.ok_or_else(invalid)?, &hash)).transpose()
        })
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    /// Installs verified rebuildable descriptors. This does not publish files.
    ///
    /// # Errors
    /// Returns an error for invalid/conflicting descriptors or database failure.
    pub async fn cache_xet_xorb_descriptors(
        &self,
        descriptors: &[XetXorbDescriptor],
    ) -> Result<(), LocalIndexStoreError> {
        let store = self.clone();
        let descriptors = descriptors.to_vec();
        tokio::task::spawn_blocking(move || {
            retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                let transaction = connection.transaction()?;
                stage_descriptors(&transaction, &descriptors)?;
                for descriptor in &descriptors {
                    repair_member_rows(&transaction, descriptor)?;
                }
                transaction.commit()?;
                Ok(())
            })
        })
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    /// Finds bounded candidate containers for a chunk in the requested scope.
    /// Callers must recheck native root, record terms and source liveness.
    ///
    /// # Errors
    /// Returns an error if the database or descriptor is invalid.
    pub async fn xet_chunk_candidates(
        &self,
        hash: &str,
        scope: Option<&RepositoryScope>,
    ) -> Result<Vec<XetXorbDescriptor>, LocalIndexStoreError> {
        self.xet_chunk_candidates_after(hash, scope, None).await
    }

    /// Reads the next bounded page in strictly increasing Xorb-hash order.
    /// The cursor is exclusive; callers must still verify authority and liveness.
    ///
    /// # Errors
    /// Returns an error if the database or descriptor is invalid.
    pub async fn xet_chunk_candidates_after(
        &self,
        hash: &str,
        scope: Option<&RepositoryScope>,
        after: Option<&str>,
    ) -> Result<Vec<XetXorbDescriptor>, LocalIndexStoreError> {
        let store = self.clone();
        let hash = hash.to_owned();
        let scope = scope.map(|scope| repository_scope_key(Some(scope)));
        let after = after.map(str::to_owned);
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            let mut query = connection.prepare("SELECT xorb_hash,CASE WHEN length(CAST(descriptor AS BLOB))<=4194304 THEN descriptor END FROM shardline_xet_xorbs d WHERE d.xorb_hash IN (
                SELECT DISTINCT c.xorb_hash FROM shardline_xet_chunks c WHERE c.chunk_hash=?1
                AND (?3 IS NULL OR c.xorb_hash>?3)
                AND (?2 IS NULL OR EXISTS(SELECT 1 FROM shardline_record_membership m WHERE m.member_hash=c.xorb_hash AND m.scope_key=?2))
                ORDER BY c.xorb_hash LIMIT 32) ORDER BY d.xorb_hash")?;
            let rows = query.query_map(params![hash, scope, after], |row| Ok((row.get::<_,String>(0)?,row.get::<_,Option<String>>(1)?)))?;
            rows.map(|row| { let (candidate_hash, json) = row?; decode(&json.ok_or_else(invalid)?, &candidate_hash) }).collect()
        }).await.map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }
}

fn repair_member_rows(
    transaction: &Transaction<'_>,
    descriptor: &XetXorbDescriptor,
) -> Result<(), LocalIndexStoreError> {
    let mut select = transaction.prepare("SELECT chunk_index,chunk_hash FROM shardline_xet_chunks WHERE xorb_hash=?1 ORDER BY chunk_index LIMIT 8193")?;
    let rows = select
        .query_map([&descriptor.hash], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<(i64, String)>, _>>()?;
    if crate::xet_xorb::member_rows_match(descriptor, &rows) {
        return Ok(());
    }
    transaction.execute(
        "DELETE FROM shardline_xet_chunks WHERE xorb_hash=?1",
        [&descriptor.hash],
    )?;
    let mut insert = transaction.prepare_cached(
        "INSERT INTO shardline_xet_chunks(xorb_hash,chunk_index,chunk_hash) VALUES(?1,?2,?3)",
    )?;
    for (index, chunk) in descriptor.chunks.iter().enumerate() {
        insert.execute(params![
            descriptor.hash,
            i64::try_from(index).map_err(|_error| invalid())?,
            chunk.hash
        ])?;
    }
    Ok(())
}

fn decode(json: &str, hash: &str) -> Result<XetXorbDescriptor, LocalIndexStoreError> {
    if json.len() > 4 * 1024 * 1024 {
        return Err(invalid());
    }
    let descriptor: XetXorbDescriptor = serde_json::from_str(json)?;
    if descriptor.hash != hash || !descriptor.has_valid_structure() {
        return Err(invalid());
    }
    Ok(descriptor)
}
