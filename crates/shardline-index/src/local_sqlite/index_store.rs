// Read-before-write mutations acquire the SQLite writer lock before observing
// authoritative rows or evidence. A deferred read transaction cannot wait out
// a competing writer when upgrading its snapshot, even with a busy timeout.
// Read-only operations retain deferred transactions and can run alongside WAL
// writers; no callbacks or mutations are replayed by an automatic retry.
use rusqlite::{OptionalExtension, Transaction, params};
use shardline_protocol::{RepositoryProvider, ShardlineHash, unix_now_seconds_lossy};
use shardline_reliability::{
    LifecycleEvent, ProviderEvidenceLog, QuarantineLifecycleState, RetentionHoldLifecycleState,
    SnapshotEvidence, WebhookDeliveryLifecycleState, append_or_baseline_snapshot_evidence,
    upload_lifecycle_event, upload_lifecycle_identity, verify_and_append_snapshot_transition,
    verify_and_append_webhook_delivery_retry, verify_and_reactivate_quarantine,
    verify_and_reactivate_retention_hold, verify_provider_lifecycle_events,
    verify_snapshot_evidence, verify_upload_lifecycle_events, verify_upload_lifecycle_head,
};
use shardline_storage::ObjectKey;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{LocalIndexStore, LocalIndexStoreError, collect_rows, u64_to_i64};
use crate::{
    DedupeShardMapping, DedupeStore, FileId, FileReconstruction, LifecycleStore,
    ProviderRepositoryState, QuarantineCandidate, ReconstructionStore, RetentionHold,
    StoredObjectId, WebhookDelivery,
    local_sqlite::helpers::{
        load_provider_evidence_batch, load_quarantine_evidence_batch, load_retention_evidence,
        load_retention_evidence_batch, load_webhook_evidence, load_webhook_evidence_batch,
        persist_retention_evidence, persist_webhook_evidence, retention_snapshot, webhook_snapshot,
    },
    parse_xet_hash_hex,
    provider_evidence::snapshot_from_state,
    upload_intent::{UploadIntent, UploadIntentState, UploadIntentStore},
    xet_hash_hex_string,
};

// Active and cancelled async cursors share a bounded close budget. Admission is
// nonwaiting: nested visitors receive WouldBlock at capacity rather than wait
// for the enclosing callback's cursor. A permit survives until SQLite closes.
const ASYNC_CURSOR_LIMIT: usize = 64;

struct ClosePool {
    sender: std::sync::mpsc::SyncSender<CloseJob>,
    admission: std::sync::Arc<tokio::sync::Semaphore>,
    faulted: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

pub(crate) struct CloseReservation {
    pool: std::sync::Arc<ClosePool>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct CloseJob {
    // Declaration order also preserves close-before-permit-release on unwind.
    _connection: rusqlite::Connection,
    _reservation: CloseReservation,
    #[cfg(test)]
    before_close: Option<Box<dyn FnOnce() + Send>>,
}

impl CloseJob {
    fn close(self) {
        #[cfg(test)]
        let mut job = self;
        #[cfg(not(test))]
        let job = self;
        #[cfg(test)]
        if let Some(hook) = job.before_close.take() {
            hook();
        }
        drop(job);
    }
}

impl ClosePool {
    fn new_with_spawn<Spawn>(capacity: usize, spawn: Spawn) -> std::io::Result<Self>
    where
        Spawn: FnOnce(Box<dyn FnOnce() + Send>) -> std::io::Result<()>,
    {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<CloseJob>(capacity);
        let faulted = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_faulted = faulted.clone();
        spawn(Box::new(move || {
            // The static pool retains a sender. No user callback is executed
            // here; an unexpected per-job panic faults admission, but does not
            // abandon the remaining already admitted close jobs.
            while let Ok(job) = receiver.recv() {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    job.close();
                }));
                if result.is_err() {
                    worker_faulted.store(true, std::sync::atomic::Ordering::Release);
                }
            }
        }))?;
        Ok(Self {
            sender,
            admission: std::sync::Arc::new(tokio::sync::Semaphore::new(capacity)),
            faulted,
        })
    }

    fn reserve(self: &std::sync::Arc<Self>) -> Result<CloseReservation, LocalIndexStoreError> {
        if self.faulted.load(std::sync::atomic::Ordering::Acquire) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "SQLite cursor close worker faulted",
            )
            .into());
        }
        let permit = self.admission.clone().try_acquire_owned().map_err(|admission_error| std::io::Error::new(std::io::ErrorKind::WouldBlock, format!("SQLite traversal capacity reached (64 active or closing cursors); try again after a traversal closes: {admission_error}")))?;
        Ok(CloseReservation {
            pool: self.clone(),
            _permit: permit,
        })
    }

    fn enqueue(&self, job: CloseJob) {
        // Every job retains a reserved permit. With this job still outside
        // the queue, at most capacity-1 jobs can be queued, so Full is excluded.
        if let Err(error) = self.sender.try_send(job) {
            // Exceptional internal-fault-only fallback: never leak a SQLite
            // handle or create unbounded rescue jobs. This synchronous close
            // can block its caller, and all future admission is rejected.
            self.faulted
                .store(true, std::sync::atomic::Ordering::Release);
            let fallback = match error {
                std::sync::mpsc::TrySendError::Full(payload)
                | std::sync::mpsc::TrySendError::Disconnected(payload) => payload,
            };
            fallback.close();
        }
    }
}

pub(crate) fn reserve_async_cursor() -> Result<CloseReservation, LocalIndexStoreError> {
    static POOL: std::sync::OnceLock<Result<std::sync::Arc<ClosePool>, std::io::Error>> =
        std::sync::OnceLock::new();
    let pool = POOL
        .get_or_init(|| {
            ClosePool::new_with_spawn(ASYNC_CURSOR_LIMIT, |worker| {
                std::thread::Builder::new()
                    .name("shardline-sqlite-close".into())
                    .spawn(worker)
                    .map(drop)
            })
            .map(std::sync::Arc::new)
        })
        .as_ref()
        .map_err(|error| {
            std::io::Error::new(
                error.kind(),
                format!("could not start SQLite cursor close worker: {error}"),
            )
        })?;
    pool.reserve()
}

pub(crate) struct ReadConnection {
    connection: Option<rusqlite::Connection>,
    reservation: Option<CloseReservation>,
}

impl ReadConnection {
    pub(crate) const fn new(
        connection: rusqlite::Connection,
        reservation: Option<CloseReservation>,
    ) -> Self {
        Self {
            connection: Some(connection),
            reservation,
        }
    }

    pub(crate) fn get(&self) -> Result<&rusqlite::Connection, LocalIndexStoreError> {
        self.connection.as_ref().ok_or_else(|| {
            LocalIndexStoreError::BlockingTask("SQLite read cursor already closed".into())
        })
    }

    // This method is called only by synchronous APIs or a blocking task. Close
    // before releasing admission, including any last-WAL checkpoint work.
    pub(crate) fn close(mut self) {
        drop(self.connection.take());
        drop(self.reservation.take());
    }
}

impl Drop for ReadConnection {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            if let Some(reservation) = self.reservation.take() {
                let pool = reservation.pool.clone();
                pool.enqueue(CloseJob {
                    _connection: connection,
                    _reservation: reservation,
                    #[cfg(test)]
                    before_close: None,
                });
            } else {
                drop(connection);
            }
        }
    }
}

// A visitor retains one deferred read snapshot, but never retains an entire
// inventory. Decode and evidence passes finish before any visitor side effect.
pub(crate) const INVENTORY_BATCH_SIZE: usize = 256;

const WEBHOOK_RETENTION_NEXT_PAGE_SQL: &str = "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                     FROM shardline_webhook_deliveries
                     WHERE processed_at_unix_seconds < ?1
                       AND (processed_at_unix_seconds, provider, owner, repo, delivery_id)
                         > (?2, ?3, ?4, ?5, ?6)
                     ORDER BY processed_at_unix_seconds, provider, owner, repo, delivery_id LIMIT ?7";

const WEBHOOK_RETENTION_FIRST_PAGE_SQL: &str = "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                     FROM shardline_webhook_deliveries
                     WHERE processed_at_unix_seconds < ?1
                     ORDER BY processed_at_unix_seconds, provider, owner, repo, delivery_id LIMIT ?2";

pub(crate) trait InventoryEntry: Sized + Send + 'static {
    const TABLE: &'static str;
    const COLUMNS: &'static str;
    const KEYS: &'static [&'static str];
    const HAS_EVIDENCE: bool = false;
    const PREVALIDATE_SNAPSHOTS: bool = false;
    const OPERATION_KIND: Option<shardline_reliability::OperationKind> = None;
    fn operation_ids(_batch: &[Self]) -> Result<Vec<String>, LocalIndexStoreError> {
        Ok(Vec::new())
    }
    fn verify_raw_heads(
        connection: &rusqlite::Connection,
        batch: &[Self],
    ) -> Result<(), LocalIndexStoreError> {
        if let Some(kind) = Self::OPERATION_KIND {
            drop(super::helpers::load_latest_verified_event_json_batch(
                connection,
                kind,
                &Self::operation_ids(batch)?,
            )?);
        }
        Ok(())
    }
    fn verify_typed_heads(
        _connection: &rusqlite::Connection,
        _batch: &[Self],
    ) -> Result<(), LocalIndexStoreError> {
        Ok(())
    }

    fn validate_snapshots(_batch: &[Self]) -> Result<(), LocalIndexStoreError> {
        Ok(())
    }
    fn from_row(row: &rusqlite::Row<'_>) -> Result<Self, LocalIndexStoreError>;
    fn verify_batch(
        _connection: &rusqlite::Connection,
        _batch: &[Self],
    ) -> Result<(), LocalIndexStoreError> {
        Ok(())
    }
}

pub(crate) struct InventoryScan<Entry> {
    connection: ReadConnection,
    after: Option<Vec<String>>,
    entry: std::marker::PhantomData<Entry>,
}

impl<Entry: InventoryEntry> InventoryScan<Entry> {
    pub(crate) fn new(store: &LocalIndexStore) -> Result<Self, LocalIndexStoreError> {
        Self::open(store, None)
    }

    pub(crate) fn open(
        store: &LocalIndexStore,
        reservation: Option<CloseReservation>,
    ) -> Result<Self, LocalIndexStoreError> {
        let connection = ReadConnection::new(store.open_connection()?, reservation);
        if let Err(error) = connection.get()?.execute_batch("BEGIN DEFERRED") {
            connection.close();
            return Err(error.into());
        }
        Ok(Self {
            connection,
            after: None,
            entry: std::marker::PhantomData,
        })
    }

    pub(crate) fn rewind(&mut self) {
        self.after = None;
    }

    pub(crate) fn next_batch(&mut self, phase: u8) -> Result<Vec<Entry>, LocalIndexStoreError> {
        let keys = Entry::KEYS.join(", ");
        let predicate = if self.after.is_some() {
            let placeholders = (1..=Entry::KEYS.len())
                .map(|i| format!("?{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" WHERE ({keys}) > ({placeholders})")
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT {} FROM {}{} ORDER BY {} LIMIT {}",
            Entry::COLUMNS,
            Entry::TABLE,
            predicate,
            keys,
            INVENTORY_BATCH_SIZE
        );
        let (batch, last_key) = {
            let mut statement = self.connection.get()?.prepare(&sql)?;
            let mut rows =
                statement.query(rusqlite::params_from_iter(self.after.iter().flatten()))?;
            let mut batch = Vec::with_capacity(INVENTORY_BATCH_SIZE);
            let mut last_key = None;
            while let Some(row) = rows.next()? {
                batch.push(Entry::from_row(row)?);
                last_key = Some(
                    Entry::KEYS
                        .iter()
                        .map(|key| row.get::<_, String>(*key))
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            (batch, last_key)
        };
        if phase == 1 {
            Entry::validate_snapshots(&batch)?;
        }
        if phase == 2 {
            Entry::verify_raw_heads(self.connection.get()?, &batch)?;
        }
        if phase == 3 {
            Entry::verify_typed_heads(self.connection.get()?, &batch)?;
        }
        if phase == 4 {
            Entry::verify_batch(self.connection.get()?, &batch)?;
        }
        if let Some(key) = last_key {
            self.after = Some(key);
        }
        Ok(batch)
    }

    pub(crate) fn finish(self) -> Result<(), LocalIndexStoreError> {
        let result = self
            .connection
            .get()?
            .execute_batch("COMMIT")
            .map_err(LocalIndexStoreError::from);
        self.connection.close();
        result
    }

    pub(crate) fn abort(self) {
        self.connection.close();
    }
}

fn visit_inventory<Entry, Visitor, VisitorError>(
    store: &LocalIndexStore,
    mut visitor: Visitor,
) -> Result<(), VisitorError>
where
    Entry: InventoryEntry,
    Visitor: FnMut(Entry) -> Result<(), VisitorError>,
    LocalIndexStoreError: Into<VisitorError>,
{
    let mut scan = InventoryScan::<Entry>::new(store).map_err(Into::into)?;
    for phase in 0..6 {
        if (phase == 1 && !Entry::PREVALIDATE_SNAPSHOTS)
            || ((2..=4).contains(&phase) && !Entry::HAS_EVIDENCE)
        {
            continue;
        }
        scan.rewind();
        loop {
            let batch = scan.next_batch(phase).map_err(Into::into)?;
            if batch.is_empty() {
                break;
            }
            if phase == 5 {
                for entry in batch {
                    visitor(entry)?;
                }
            }
        }
    }
    scan.finish().map_err(Into::into)
}

impl InventoryEntry for FileId {
    const TABLE: &'static str = "shardline_file_reconstructions";
    const COLUMNS: &'static str = "file_id";
    const KEYS: &'static [&'static str] = &["file_id"];
    fn from_row(row: &rusqlite::Row<'_>) -> Result<Self, LocalIndexStoreError> {
        Ok(Self::new(parse_xet_hash_hex(&row.get::<_, String>(0)?)?))
    }
}

impl InventoryEntry for DedupeShardMapping {
    const TABLE: &'static str = "shardline_dedupe_shards";
    const COLUMNS: &'static str = "chunk_hash, shard_object_key";
    const KEYS: &'static [&'static str] = &["chunk_hash"];
    fn from_row(row: &rusqlite::Row<'_>) -> Result<Self, LocalIndexStoreError> {
        Ok(super::helpers::dedupe_shard_mapping_from_row(row)?)
    }
}

impl InventoryEntry for QuarantineCandidate {
    const TABLE: &'static str = "shardline_quarantine_candidates";
    const COLUMNS: &'static str = "object_key, observed_length, first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds";
    const KEYS: &'static [&'static str] = &["object_key"];
    const HAS_EVIDENCE: bool = true;
    const OPERATION_KIND: Option<shardline_reliability::OperationKind> =
        Some(shardline_reliability::OperationKind::GarbageCollection);
    fn operation_ids(batch: &[Self]) -> Result<Vec<String>, LocalIndexStoreError> {
        Ok(batch
            .iter()
            .map(|entry| entry.object_key().as_str().to_owned())
            .collect())
    }
    fn verify_typed_heads(
        connection: &rusqlite::Connection,
        batch: &[Self],
    ) -> Result<(), LocalIndexStoreError> {
        drop(load_quarantine_evidence_batch(
            connection,
            &Self::operation_ids(batch)?,
        )?);
        Ok(())
    }

    fn from_row(row: &rusqlite::Row<'_>) -> Result<Self, LocalIndexStoreError> {
        Ok(super::helpers::quarantine_candidate_from_row(row)?)
    }
    fn verify_batch(
        connection: &rusqlite::Connection,
        batch: &[Self],
    ) -> Result<(), LocalIndexStoreError> {
        let keys = batch
            .iter()
            .map(|entry| entry.object_key().as_str().to_owned())
            .collect::<Vec<_>>();
        let evidence = load_quarantine_evidence_batch(connection, &keys)?;
        for entry in batch {
            let snapshot =
                super::helpers::quarantine_snapshot(entry, QuarantineLifecycleState::Active)?;
            let stored = evidence.get(entry.object_key().as_str()).ok_or(
                LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                ),
            )?;
            verify_snapshot_evidence(stored, &snapshot)?;
        }
        Ok(())
    }
}

impl InventoryEntry for RetentionHold {
    const TABLE: &'static str = "shardline_retention_holds";
    const COLUMNS: &'static str =
        "object_key, reason, held_at_unix_seconds, release_after_unix_seconds";
    const KEYS: &'static [&'static str] = &["object_key"];
    const HAS_EVIDENCE: bool = true;
    const OPERATION_KIND: Option<shardline_reliability::OperationKind> =
        Some(shardline_reliability::OperationKind::RetentionHold);
    fn operation_ids(batch: &[Self]) -> Result<Vec<String>, LocalIndexStoreError> {
        Ok(batch
            .iter()
            .map(|entry| entry.object_key().as_str().to_owned())
            .collect())
    }
    fn verify_typed_heads(
        connection: &rusqlite::Connection,
        batch: &[Self],
    ) -> Result<(), LocalIndexStoreError> {
        drop(load_retention_evidence_batch(
            connection,
            &Self::operation_ids(batch)?,
        )?);
        Ok(())
    }

    fn from_row(row: &rusqlite::Row<'_>) -> Result<Self, LocalIndexStoreError> {
        Ok(super::helpers::retention_hold_from_row(row)?)
    }
    fn verify_batch(
        connection: &rusqlite::Connection,
        batch: &[Self],
    ) -> Result<(), LocalIndexStoreError> {
        let keys = batch
            .iter()
            .map(|entry| entry.object_key().as_str().to_owned())
            .collect::<Vec<_>>();
        let evidence = load_retention_evidence_batch(connection, &keys)?;
        for entry in batch {
            let snapshot = retention_snapshot(entry, RetentionHoldLifecycleState::Active)?;
            let stored = evidence.get(entry.object_key().as_str()).ok_or(
                LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                ),
            )?;
            verify_snapshot_evidence(stored, &snapshot)?;
        }
        Ok(())
    }
}

impl InventoryEntry for WebhookDelivery {
    const TABLE: &'static str = "shardline_webhook_deliveries";
    const COLUMNS: &'static str = "provider, owner, repo, delivery_id, processed_at_unix_seconds";
    const KEYS: &'static [&'static str] = &["provider", "owner", "repo", "delivery_id"];
    const HAS_EVIDENCE: bool = true;
    const OPERATION_KIND: Option<shardline_reliability::OperationKind> =
        Some(shardline_reliability::OperationKind::WebhookDelivery);
    fn operation_ids(batch: &[Self]) -> Result<Vec<String>, LocalIndexStoreError> {
        batch
            .iter()
            .map(|entry| {
                Ok(
                    webhook_snapshot(entry, WebhookDeliveryLifecycleState::Processed)?
                        .evidence_operation()?
                        .operation_id,
                )
            })
            .collect()
    }
    fn verify_typed_heads(
        connection: &rusqlite::Connection,
        batch: &[Self],
    ) -> Result<(), LocalIndexStoreError> {
        drop(load_webhook_evidence_batch(connection, batch)?);
        Ok(())
    }

    fn from_row(row: &rusqlite::Row<'_>) -> Result<Self, LocalIndexStoreError> {
        Ok(super::helpers::webhook_delivery_from_row(row)?)
    }
    fn verify_batch(
        connection: &rusqlite::Connection,
        batch: &[Self],
    ) -> Result<(), LocalIndexStoreError> {
        let evidence = load_webhook_evidence_batch(connection, batch)?;
        for entry in batch {
            let snapshot = webhook_snapshot(entry, WebhookDeliveryLifecycleState::Processed)?;
            let id = snapshot.evidence_operation()?.operation_id;
            let stored = evidence.get(&id).ok_or(LocalIndexStoreError::Reliability(
                shardline_reliability::ReliabilityError::OperationMismatch,
            ))?;
            verify_snapshot_evidence(stored, &snapshot)?;
        }
        Ok(())
    }
}

impl InventoryEntry for ProviderRepositoryState {
    const TABLE: &'static str = "shardline_provider_repository_states";
    const COLUMNS: &'static str = "provider, owner, repo, last_access_changed_at_unix_seconds, last_revision_pushed_at_unix_seconds, last_pushed_revision, last_cache_invalidated_at_unix_seconds, last_authorization_rechecked_at_unix_seconds, last_drift_checked_at_unix_seconds";
    const KEYS: &'static [&'static str] = &["provider", "owner", "repo"];
    const HAS_EVIDENCE: bool = true;
    const OPERATION_KIND: Option<shardline_reliability::OperationKind> =
        Some(shardline_reliability::OperationKind::ProviderEvent);
    fn operation_ids(batch: &[Self]) -> Result<Vec<String>, LocalIndexStoreError> {
        batch
            .iter()
            .map(|entry| {
                Ok(super::helpers::provider_evidence_operation_id(
                    &snapshot_from_state(entry)?,
                ))
            })
            .collect()
    }
    fn verify_typed_heads(
        connection: &rusqlite::Connection,
        batch: &[Self],
    ) -> Result<(), LocalIndexStoreError> {
        let snapshots = batch
            .iter()
            .map(snapshot_from_state)
            .collect::<Result<Vec<_>, _>>()?;
        drop(load_provider_evidence_batch(connection, &snapshots)?);
        Ok(())
    }

    const PREVALIDATE_SNAPSHOTS: bool = true;
    fn validate_snapshots(batch: &[Self]) -> Result<(), LocalIndexStoreError> {
        for state in batch {
            snapshot_from_state(state)?;
        }
        Ok(())
    }
    fn from_row(row: &rusqlite::Row<'_>) -> Result<Self, LocalIndexStoreError> {
        Ok(super::helpers::provider_repository_state_from_row(row)?)
    }
    fn verify_batch(
        connection: &rusqlite::Connection,
        batch: &[Self],
    ) -> Result<(), LocalIndexStoreError> {
        let snapshots = batch
            .iter()
            .map(snapshot_from_state)
            .collect::<Result<Vec<_>, _>>()?;
        let evidence = load_provider_evidence_batch(connection, &snapshots)?;
        for snapshot in snapshots {
            let id = super::helpers::provider_evidence_operation_id(&snapshot);
            let stored = evidence.get(&id).ok_or(LocalIndexStoreError::Reliability(
                shardline_reliability::ReliabilityError::OperationMismatch,
            ))?;
            if stored.events().is_empty() {
                verify_provider_lifecycle_events(
                    ProviderEvidenceLog::baseline(snapshot.clone())?.events(),
                    &snapshot,
                )?;
            } else {
                stored.verify_for(&snapshot)?;
            }
        }
        Ok(())
    }
}

fn verify_sqlite_intent_evidence(
    transaction: &Transaction<'_>,
    intent: &UploadIntent,
) -> Result<(), LocalIndexStoreError> {
    let event = super::helpers::load_latest_verified_event_json(
        transaction,
        shardline_reliability::OperationKind::Upload,
        intent.intent_id(),
    )?
    .ok_or(LocalIndexStoreError::Reliability(
        shardline_reliability::ReliabilityError::OperationMismatch,
    ))
    .and_then(|value| {
        serde_json::from_value::<LifecycleEvent>(value).map_err(LocalIndexStoreError::from)
    })?;
    verify_upload_lifecycle_head(
        &event,
        event.operation.tenant.as_str(),
        event.operation.repository.as_str(),
        intent.intent_id(),
        intent.object_key(),
        intent.object_hash(),
        intent.state(),
    )
    .map_err(LocalIndexStoreError::Reliability)
}

impl ReconstructionStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn reconstruction(&self, file_id: &FileId) -> Result<Option<FileReconstruction>, Self::Error> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT terms
                 FROM shardline_file_reconstructions
                 WHERE file_id = ?1",
                params![xet_hash_hex_string(file_id.hash())],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| super::helpers::parse_reconstruction_json(&value))
            .transpose()
    }

    fn list_reconstruction_file_ids(&self) -> Result<Vec<FileId>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT file_id
             FROM shardline_file_reconstructions
             ORDER BY file_id",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut file_ids = Vec::new();
        for row in rows {
            let hash = parse_xet_hash_hex(&row?)?;
            file_ids.push(FileId::new(hash));
        }
        Ok(file_ids)
    }

    fn visit_reconstruction_file_ids<Visitor, VisitorError>(
        &self,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(FileId) -> Result<(), VisitorError>,
    {
        visit_inventory::<FileId, _, _>(self, visitor)
    }

    fn delete_reconstruction(&self, file_id: &FileId) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_file_reconstructions WHERE file_id = ?1",
            params![xet_hash_hex_string(file_id.hash())],
        )?;
        Ok(changed > 0)
    }

    fn delete_reconstruction_if_matches(
        &self,
        file_id: &FileId,
        expected: &FileReconstruction,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let Some(terms) = transaction
            .query_row(
                "SELECT terms FROM shardline_file_reconstructions WHERE file_id = ?1",
                params![xet_hash_hex_string(file_id.hash())],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        else {
            transaction.commit()?;
            return Ok(false);
        };
        if super::helpers::parse_reconstruction_json(&terms)? != *expected {
            transaction.commit()?;
            return Ok(false);
        }
        let changed = transaction.execute(
            "DELETE FROM shardline_file_reconstructions WHERE file_id = ?1 AND terms = ?2",
            params![xet_hash_hex_string(file_id.hash()), terms],
        )?;
        transaction.commit()?;
        Ok(changed > 0)
    }

    fn contains_object(&self, object_id: &StoredObjectId) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let exists = connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM shardline_stored_objects WHERE object_hash = ?1
            )",
            params![xet_hash_hex_string(object_id.hash())],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(exists != 0)
    }
}

impl DedupeStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn dedupe_shard_mapping(
        &self,
        chunk_hash: &ShardlineHash,
    ) -> Result<Option<DedupeShardMapping>, Self::Error> {
        let connection = self.open_connection()?;
        let result: Result<Option<DedupeShardMapping>, _> = connection
            .query_row(
                "SELECT chunk_hash, shard_object_key
                 FROM shardline_dedupe_shards
                 WHERE chunk_hash = ?1",
                params![xet_hash_hex_string(chunk_hash)],
                super::helpers::dedupe_shard_mapping_from_row,
            )
            .optional()
            .map_err(LocalIndexStoreError::from);
        let hit = result.as_ref().is_ok_and(|r| r.is_some());
        shardline_metrics::record_xet_dedupe_shard_query(hit);
        result
    }

    fn list_dedupe_shard_mappings(&self) -> Result<Vec<DedupeShardMapping>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT chunk_hash, shard_object_key
             FROM shardline_dedupe_shards
             ORDER BY chunk_hash",
        )?;
        let rows = statement.query_map([], super::helpers::dedupe_shard_mapping_from_row)?;
        collect_rows(rows)
    }

    fn visit_dedupe_shard_mappings<Visitor, VisitorError>(
        &self,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError>,
    {
        visit_inventory::<DedupeShardMapping, _, _>(self, visitor)
    }

    fn delete_dedupe_shard_mapping(&self, chunk_hash: &ShardlineHash) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_dedupe_shards WHERE chunk_hash = ?1",
            params![xet_hash_hex_string(chunk_hash)],
        )?;
        Ok(changed > 0)
    }

    fn delete_dedupe_shard_mapping_if_matches(
        &self,
        expected: &DedupeShardMapping,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let Some(current) = transaction
            .query_row(
                "SELECT chunk_hash, shard_object_key
                 FROM shardline_dedupe_shards WHERE chunk_hash = ?1",
                params![xet_hash_hex_string(expected.chunk_hash())],
                super::helpers::dedupe_shard_mapping_from_row,
            )
            .optional()?
        else {
            transaction.commit()?;
            return Ok(false);
        };
        if current != *expected {
            transaction.commit()?;
            return Ok(false);
        }
        let changed = transaction.execute(
            "DELETE FROM shardline_dedupe_shards
             WHERE chunk_hash = ?1 AND shard_object_key = ?2",
            params![
                xet_hash_hex_string(expected.chunk_hash()),
                expected.shard_object_key().as_str(),
            ],
        )?;
        transaction.commit()?;
        Ok(changed > 0)
    }
}

impl LifecycleStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn quarantine_candidate(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<QuarantineCandidate>, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let candidate = transaction
            .query_row(
                "SELECT object_key,
                        observed_length,
                        first_seen_unreachable_at_unix_seconds,
                        delete_after_unix_seconds
                 FROM shardline_quarantine_candidates
                 WHERE object_key = ?1",
                params![object_key.as_str()],
                super::helpers::quarantine_candidate_from_row,
            )
            .optional()?;
        if let Some(candidate) = &candidate {
            let snapshot =
                super::helpers::quarantine_snapshot(candidate, QuarantineLifecycleState::Active)?;
            let evidence =
                super::helpers::load_quarantine_evidence(&transaction, object_key.as_str())?;
            verify_snapshot_evidence(&evidence, &snapshot)?;
        }
        transaction.commit()?;
        Ok(candidate)
    }

    fn list_quarantine_candidates(&self) -> Result<Vec<QuarantineCandidate>, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let mut statement = transaction.prepare(
            "SELECT object_key,
                    observed_length,
                    first_seen_unreachable_at_unix_seconds,
                    delete_after_unix_seconds
             FROM shardline_quarantine_candidates
             ORDER BY object_key",
        )?;
        let rows = statement.query_map([], super::helpers::quarantine_candidate_from_row)?;
        let candidates = collect_rows(rows)?;
        drop(statement);
        let object_keys = candidates
            .iter()
            .map(|candidate| candidate.object_key().as_str().to_owned())
            .collect::<Vec<_>>();
        let evidence = load_quarantine_evidence_batch(&transaction, &object_keys)?;
        for candidate in &candidates {
            let snapshot =
                super::helpers::quarantine_snapshot(candidate, QuarantineLifecycleState::Active)?;
            let evidence = evidence.get(candidate.object_key().as_str()).ok_or(
                LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                ),
            )?;
            verify_snapshot_evidence(evidence, &snapshot)?;
        }
        transaction.commit()?;
        Ok(candidates)
    }

    fn visit_quarantine_candidates<Visitor, VisitorError>(
        &self,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError>,
    {
        visit_inventory::<QuarantineCandidate, _, _>(self, visitor)
    }

    fn upsert_quarantine_candidate(
        &self,
        candidate: &QuarantineCandidate,
    ) -> Result<(), Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let previous = transaction
            .query_row(
                "SELECT object_key, observed_length, first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds
                 FROM shardline_quarantine_candidates WHERE object_key = ?1",
                params![candidate.object_key().as_str()],
                super::helpers::quarantine_candidate_from_row,
            )
            .optional()?;
        transaction.execute(
            "INSERT INTO shardline_quarantine_candidates (
                object_key,
                observed_length,
                first_seen_unreachable_at_unix_seconds,
                delete_after_unix_seconds,
                updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (object_key)
             DO UPDATE SET
                observed_length = excluded.observed_length,
                first_seen_unreachable_at_unix_seconds =
                    excluded.first_seen_unreachable_at_unix_seconds,
                delete_after_unix_seconds = excluded.delete_after_unix_seconds,
                updated_at_unix_seconds = excluded.updated_at_unix_seconds",
            params![
                candidate.object_key().as_str(),
                u64_to_i64(candidate.observed_length())?,
                u64_to_i64(candidate.first_seen_unreachable_at_unix_seconds())?,
                u64_to_i64(candidate.delete_after_unix_seconds())?,
                u64_to_i64(unix_now_seconds_lossy())?,
            ],
        )?;
        let snapshot =
            super::helpers::quarantine_snapshot(candidate, QuarantineLifecycleState::Active)?;
        let evidence = super::helpers::load_quarantine_evidence(
            &transaction,
            candidate.object_key().as_str(),
        )?;
        let (evidence, evidence_was_empty) = if let Some(previous) = previous {
            let before =
                super::helpers::quarantine_snapshot(&previous, QuarantineLifecycleState::Active)?;
            verify_and_append_snapshot_transition(evidence, before, snapshot)?
        } else if evidence.events().is_empty() {
            (
                append_or_baseline_snapshot_evidence(evidence, snapshot)?,
                true,
            )
        } else {
            verify_and_reactivate_quarantine(evidence, snapshot)?
        };
        let event = evidence.events().last().ok_or_else(|| {
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::EmptyField(
                "quarantine evidence event",
            ))
        })?;
        if evidence_was_empty {
            for stored_event in evidence.events() {
                super::helpers::persist_quarantine_evidence(&transaction, stored_event)?;
            }
        } else {
            super::helpers::persist_quarantine_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn delete_quarantine_candidate(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let candidate = transaction
            .query_row(
                "SELECT object_key, observed_length, first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds
                 FROM shardline_quarantine_candidates WHERE object_key = ?1",
                params![object_key.as_str()],
                super::helpers::quarantine_candidate_from_row,
            )
            .optional()?;
        let changed = transaction.execute(
            "DELETE FROM shardline_quarantine_candidates WHERE object_key = ?1",
            params![object_key.as_str()],
        )?;
        if let Some(candidate) = candidate {
            let active =
                super::helpers::quarantine_snapshot(&candidate, QuarantineLifecycleState::Active)?;
            let released = super::helpers::quarantine_snapshot(
                &candidate,
                QuarantineLifecycleState::Released,
            )?;
            let evidence =
                super::helpers::load_quarantine_evidence(&transaction, object_key.as_str())?;
            let (evidence, evidence_was_empty) =
                verify_and_append_snapshot_transition(evidence, active, released)?;
            let event = evidence.events().last().ok_or_else(|| {
                LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::EmptyField(
                        "quarantine evidence event",
                    ),
                )
            })?;
            if evidence_was_empty {
                for stored_event in evidence.events() {
                    super::helpers::persist_quarantine_evidence(&transaction, stored_event)?;
                }
            } else {
                super::helpers::persist_quarantine_evidence(&transaction, event)?;
            }
        }
        transaction.commit()?;
        Ok(changed > 0)
    }

    fn delete_quarantine_candidate_if_matches(
        &self,
        expected: &QuarantineCandidate,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let candidate = transaction
            .query_row(
                "SELECT object_key, observed_length, first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds
                 FROM shardline_quarantine_candidates WHERE object_key = ?1",
                params![expected.object_key().as_str()],
                super::helpers::quarantine_candidate_from_row,
            )
            .optional()?;
        let Some(candidate) = candidate else {
            transaction.commit()?;
            return Ok(false);
        };
        if candidate != *expected {
            transaction.commit()?;
            return Ok(false);
        }
        let active =
            super::helpers::quarantine_snapshot(&candidate, QuarantineLifecycleState::Active)?;
        let released =
            super::helpers::quarantine_snapshot(&candidate, QuarantineLifecycleState::Released)?;
        let evidence =
            super::helpers::load_quarantine_evidence(&transaction, expected.object_key().as_str())?;
        let (evidence, evidence_was_empty) =
            verify_and_append_snapshot_transition(evidence, active, released)?;
        let changed = transaction.execute(
            "DELETE FROM shardline_quarantine_candidates
             WHERE object_key = ?1 AND observed_length = ?2
               AND first_seen_unreachable_at_unix_seconds = ?3
               AND delete_after_unix_seconds = ?4",
            params![
                expected.object_key().as_str(),
                u64_to_i64(expected.observed_length())?,
                u64_to_i64(expected.first_seen_unreachable_at_unix_seconds())?,
                u64_to_i64(expected.delete_after_unix_seconds())?,
            ],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(false);
        }
        let event = evidence.events().last().ok_or_else(|| {
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::EmptyField(
                "quarantine evidence event",
            ))
        })?;
        if evidence_was_empty {
            for stored_event in evidence.events() {
                super::helpers::persist_quarantine_evidence(&transaction, stored_event)?;
            }
        } else {
            super::helpers::persist_quarantine_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(true)
    }

    fn retention_hold(&self, object_key: &ObjectKey) -> Result<Option<RetentionHold>, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let hold = transaction
            .query_row(
                "SELECT object_key,
                        reason,
                        held_at_unix_seconds,
                        release_after_unix_seconds
                 FROM shardline_retention_holds
                 WHERE object_key = ?1",
                params![object_key.as_str()],
                super::helpers::retention_hold_from_row,
            )
            .optional()?;
        if let Some(hold) = hold.as_ref() {
            let snapshot = retention_snapshot(hold, RetentionHoldLifecycleState::Active)?;
            let evidence = load_retention_evidence(&transaction, object_key.as_str())?;
            verify_snapshot_evidence(&evidence, &snapshot)?;
        }
        transaction.commit()?;
        Ok(hold)
    }

    fn list_retention_holds(&self) -> Result<Vec<RetentionHold>, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let mut statement = transaction.prepare(
            "SELECT object_key,
                    reason,
                    held_at_unix_seconds,
                    release_after_unix_seconds
             FROM shardline_retention_holds
             ORDER BY object_key",
        )?;
        let rows = statement.query_map([], super::helpers::retention_hold_from_row)?;
        let holds = collect_rows(rows)?;
        drop(statement);
        let object_keys = holds
            .iter()
            .map(|hold| hold.object_key().as_str().to_owned())
            .collect::<Vec<_>>();
        let evidence = load_retention_evidence_batch(&transaction, &object_keys)?;
        for hold in &holds {
            let snapshot = retention_snapshot(hold, RetentionHoldLifecycleState::Active)?;
            let evidence = evidence.get(hold.object_key().as_str()).ok_or(
                LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                ),
            )?;
            verify_snapshot_evidence(evidence, &snapshot)?;
        }
        transaction.commit()?;
        Ok(holds)
    }

    fn visit_retention_holds<Visitor, VisitorError>(
        &self,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError>,
    {
        visit_inventory::<RetentionHold, _, _>(self, visitor)
    }

    fn upsert_retention_hold(&self, hold: &RetentionHold) -> Result<(), Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let previous = transaction
            .query_row(
                "SELECT object_key, reason, held_at_unix_seconds, release_after_unix_seconds
                 FROM shardline_retention_holds WHERE object_key = ?1",
                params![hold.object_key().as_str()],
                super::helpers::retention_hold_from_row,
            )
            .optional()?;
        let snapshot = retention_snapshot(hold, RetentionHoldLifecycleState::Active)?;
        let evidence = load_retention_evidence(&transaction, hold.object_key().as_str())?;
        let (evidence, evidence_was_empty) = if let Some(previous) = previous.as_ref() {
            let previous_snapshot =
                retention_snapshot(previous, RetentionHoldLifecycleState::Active)?;
            verify_and_append_snapshot_transition(evidence, previous_snapshot, snapshot)?
        } else if evidence.events().is_empty() {
            (
                append_or_baseline_snapshot_evidence(evidence, snapshot)?,
                true,
            )
        } else {
            verify_and_reactivate_retention_hold(evidence, snapshot)?
        };
        transaction.execute(
            "INSERT INTO shardline_retention_holds (
                object_key,
                reason,
                held_at_unix_seconds,
                release_after_unix_seconds,
                updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (object_key)
             DO UPDATE SET
                reason = excluded.reason,
                held_at_unix_seconds = excluded.held_at_unix_seconds,
                release_after_unix_seconds = excluded.release_after_unix_seconds,
                updated_at_unix_seconds = excluded.updated_at_unix_seconds",
            params![
                hold.object_key().as_str(),
                hold.reason(),
                u64_to_i64(hold.held_at_unix_seconds())?,
                hold.release_after_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                u64_to_i64(unix_now_seconds_lossy())?,
            ],
        )?;
        if evidence_was_empty {
            for event in evidence.events() {
                persist_retention_evidence(&transaction, event)?;
            }
        } else if let Some(event) = evidence.events().last() {
            persist_retention_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn delete_retention_hold(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let hold = transaction
            .query_row(
                "SELECT object_key, reason, held_at_unix_seconds, release_after_unix_seconds
                 FROM shardline_retention_holds WHERE object_key = ?1",
                params![object_key.as_str()],
                super::helpers::retention_hold_from_row,
            )
            .optional()?;
        let changed = transaction.execute(
            "DELETE FROM shardline_retention_holds WHERE object_key = ?1",
            params![object_key.as_str()],
        )?;
        if let Some(hold) = hold {
            let active = retention_snapshot(&hold, RetentionHoldLifecycleState::Active)?;
            let released = retention_snapshot(&hold, RetentionHoldLifecycleState::Released)?;
            let evidence = load_retention_evidence(&transaction, object_key.as_str())?;
            let (evidence, evidence_was_empty) =
                verify_and_append_snapshot_transition(evidence, active, released)?;
            if evidence_was_empty {
                for event in evidence.events() {
                    persist_retention_evidence(&transaction, event)?;
                }
            } else if let Some(event) = evidence.events().last() {
                persist_retention_evidence(&transaction, event)?;
            }
        }
        transaction.commit()?;
        Ok(changed > 0)
    }

    fn delete_retention_hold_if_matches(
        &self,
        expected: &RetentionHold,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let hold = transaction
            .query_row(
                "SELECT object_key, reason, held_at_unix_seconds, release_after_unix_seconds
                 FROM shardline_retention_holds WHERE object_key = ?1",
                params![expected.object_key().as_str()],
                super::helpers::retention_hold_from_row,
            )
            .optional()?;
        let Some(hold) = hold else {
            transaction.commit()?;
            return Ok(false);
        };
        if hold != *expected {
            transaction.commit()?;
            return Ok(false);
        }
        let active = retention_snapshot(&hold, RetentionHoldLifecycleState::Active)?;
        let released = retention_snapshot(&hold, RetentionHoldLifecycleState::Released)?;
        let evidence = load_retention_evidence(&transaction, expected.object_key().as_str())?;
        let (evidence, evidence_was_empty) =
            verify_and_append_snapshot_transition(evidence, active, released)?;
        let changed = transaction.execute(
            "DELETE FROM shardline_retention_holds
             WHERE object_key = ?1 AND reason = ?2 AND held_at_unix_seconds = ?3
               AND (release_after_unix_seconds IS ?4)",
            params![
                expected.object_key().as_str(),
                expected.reason(),
                u64_to_i64(expected.held_at_unix_seconds())?,
                expected
                    .release_after_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
            ],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(false);
        }
        if evidence_was_empty {
            for event in evidence.events() {
                persist_retention_evidence(&transaction, event)?;
            }
        } else if let Some(event) = evidence.events().last() {
            persist_retention_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(true)
    }

    fn record_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing = transaction
            .query_row(
                "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                 FROM shardline_webhook_deliveries
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4",
                params![
                    delivery.provider().as_str(),
                    delivery.owner(),
                    delivery.repo(),
                    delivery.delivery_id(),
                ],
                super::helpers::webhook_delivery_from_row,
            )
            .optional()?;
        if let Some(existing) = existing {
            let snapshot = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Processed)?;
            let evidence = load_webhook_evidence(&transaction, &existing)?;
            verify_snapshot_evidence(&evidence, &snapshot)?;
            transaction.commit()?;
            return Ok(false);
        }
        let snapshot = webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Processed)?;
        let evidence = load_webhook_evidence(&transaction, delivery)?;
        let processed_at_unix_seconds = evidence.events().last().map_or_else(
            || delivery.processed_at_unix_seconds(),
            |event| event.after.processed_at_unix_seconds,
        );
        let (evidence, evidence_was_empty) =
            verify_and_append_webhook_delivery_retry(evidence, snapshot)?;
        transaction.execute(
            "INSERT INTO shardline_webhook_deliveries (
                provider,
                owner,
                repo,
                delivery_id,
                processed_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (provider, owner, repo, delivery_id) DO NOTHING",
            params![
                delivery.provider().as_str(),
                delivery.owner(),
                delivery.repo(),
                delivery.delivery_id(),
                u64_to_i64(processed_at_unix_seconds)?,
            ],
        )?;
        if evidence_was_empty {
            for event in evidence.events() {
                persist_webhook_evidence(&transaction, event)?;
            }
        } else if let Some(event) = evidence.events().last() {
            persist_webhook_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(true)
    }

    fn list_webhook_deliveries(&self) -> Result<Vec<WebhookDelivery>, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let mut statement = transaction.prepare(
            "SELECT provider,
                    owner,
                    repo,
                    delivery_id,
                    processed_at_unix_seconds
             FROM shardline_webhook_deliveries
             ORDER BY provider, owner, repo, delivery_id",
        )?;
        let rows = statement.query_map([], super::helpers::webhook_delivery_from_row)?;
        let deliveries = collect_rows(rows)?;
        drop(statement);
        let evidence = load_webhook_evidence_batch(&transaction, &deliveries)?;
        for delivery in &deliveries {
            let snapshot = webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Processed)?;
            let operation_id = snapshot.evidence_operation()?.operation_id;
            let evidence = evidence
                .get(&operation_id)
                .ok_or(LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                ))?;
            verify_snapshot_evidence(evidence, &snapshot)?;
        }
        transaction.commit()?;
        Ok(deliveries)
    }

    fn visit_webhook_deliveries<Visitor, VisitorError>(
        &self,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(WebhookDelivery) -> Result<(), VisitorError>,
    {
        visit_inventory::<WebhookDelivery, _, _>(self, visitor)
    }

    fn delete_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing = transaction
            .query_row(
                "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                 FROM shardline_webhook_deliveries
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4",
                params![
                    delivery.provider().as_str(),
                    delivery.owner(),
                    delivery.repo(),
                    delivery.delivery_id(),
                ],
                super::helpers::webhook_delivery_from_row,
            )
            .optional()?;
        let Some(existing) = existing else {
            transaction.commit()?;
            return Ok(false);
        };
        let snapshot = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Processed)?;
        let released = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Released)?;
        let evidence = load_webhook_evidence(&transaction, &existing)?;
        let (evidence, evidence_was_empty) =
            verify_and_append_snapshot_transition(evidence, snapshot, released)?;
        let changed = transaction.execute(
            "DELETE FROM shardline_webhook_deliveries
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4",
            params![
                delivery.provider().as_str(),
                delivery.owner(),
                delivery.repo(),
                delivery.delivery_id(),
            ],
        )?;
        if evidence_was_empty {
            for event in evidence.events() {
                persist_webhook_evidence(&transaction, event)?;
            }
        } else if let Some(event) = evidence.events().last() {
            persist_webhook_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(changed > 0)
    }

    fn delete_webhook_delivery_if_matches(
        &self,
        expected: &WebhookDelivery,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing = transaction
            .query_row(
                "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                 FROM shardline_webhook_deliveries
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4",
                params![
                    expected.provider().as_str(),
                    expected.owner(),
                    expected.repo(),
                    expected.delivery_id(),
                ],
                super::helpers::webhook_delivery_from_row,
            )
            .optional()?;
        let Some(existing) = existing else {
            transaction.commit()?;
            return Ok(false);
        };
        if existing != *expected {
            transaction.commit()?;
            return Ok(false);
        }
        let active = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Processed)?;
        let released = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Released)?;
        let evidence = load_webhook_evidence(&transaction, &existing)?;
        let (evidence, evidence_was_empty) =
            verify_and_append_snapshot_transition(evidence, active, released)?;
        let changed = transaction.execute(
            "DELETE FROM shardline_webhook_deliveries
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4
               AND processed_at_unix_seconds = ?5",
            params![
                expected.provider().as_str(),
                expected.owner(),
                expected.repo(),
                expected.delivery_id(),
                u64_to_i64(expected.processed_at_unix_seconds())?,
            ],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(false);
        }
        if evidence_was_empty {
            for event in evidence.events() {
                persist_webhook_evidence(&transaction, event)?;
            }
        } else if let Some(event) = evidence.events().last() {
            persist_webhook_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(true)
    }

    fn purge_webhook_deliveries_older_than(
        &self,
        older_than_unix_seconds: u64,
    ) -> Result<u64, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let cutoff = u64_to_i64(older_than_unix_seconds)?;
        let batch_size = i64::try_from(INVENTORY_BATCH_SIZE)
            .map_err(|error| LocalIndexStoreError::IntegerOutOfRange(error.to_string()))?;
        let mut purged = 0_u64;
        let mut cursor: Option<(i64, String, String, String, String)> = None;
        // Keyset pages follow the covering retention index, so retained deliveries
        // beyond the cutoff are never scanned. Pages retain at most 256 rows
        // and their evidence. One Immediate transaction keeps every page atomic,
        // including late verification errors.
        loop {
            let deliveries =
                if let Some((processed_at, provider, owner, repo, delivery_id)) = &cursor {
                    let mut statement = transaction.prepare(WEBHOOK_RETENTION_NEXT_PAGE_SQL)?;
                    collect_rows(statement.query_map(
                        params![
                            cutoff,
                            processed_at,
                            provider,
                            owner,
                            repo,
                            delivery_id,
                            batch_size
                        ],
                        super::helpers::webhook_delivery_from_row,
                    )?)?
                } else {
                    let mut statement = transaction.prepare(WEBHOOK_RETENTION_FIRST_PAGE_SQL)?;
                    collect_rows(statement.query_map(
                        params![cutoff, batch_size],
                        super::helpers::webhook_delivery_from_row,
                    )?)?
                };
            let Some(last) = deliveries.last() else {
                break;
            };
            cursor = Some((
                u64_to_i64(last.processed_at_unix_seconds())?,
                last.provider().as_str().to_owned(),
                last.owner().to_owned(),
                last.repo().to_owned(),
                last.delivery_id().to_owned(),
            ));
            purged = purged
                .checked_add(
                    u64::try_from(deliveries.len()).map_err(|error| {
                        LocalIndexStoreError::IntegerOutOfRange(error.to_string())
                    })?,
                )
                .ok_or_else(|| {
                    LocalIndexStoreError::IntegerOutOfRange("webhook purge count overflow".into())
                })?;
            let operation_ids = deliveries
                .iter()
                .map(|delivery| {
                    Ok::<_, LocalIndexStoreError>(
                        webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Processed)?
                            .evidence_operation()?
                            .operation_id,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let heads = super::helpers::load_latest_verified_event_json_batch(
                &transaction,
                shardline_reliability::OperationKind::WebhookDelivery,
                &operation_ids,
            )?;
            for (delivery, operation_id) in deliveries.iter().zip(operation_ids) {
                let snapshot =
                    webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Processed)?;
                let released = webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Released)?;
                let evidence = heads
                    .get(&operation_id)
                    .map(|event| {
                        shardline_reliability::WebhookDeliveryEvidenceLog::from_head(
                            serde_json::from_value(event.clone())?,
                        )
                    })
                    .transpose()?
                    .unwrap_or_default();
                let (evidence, evidence_was_empty) =
                    verify_and_append_snapshot_transition(evidence, snapshot, released)?;
                transaction.execute(
                    "DELETE FROM shardline_webhook_deliveries
                     WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4",
                    params![
                        delivery.provider().as_str(),
                        delivery.owner(),
                        delivery.repo(),
                        delivery.delivery_id(),
                    ],
                )?;
                if evidence_was_empty {
                    for event in evidence.events() {
                        persist_webhook_evidence(&transaction, event)?;
                    }
                } else if let Some(event) = evidence.events().last() {
                    persist_webhook_evidence(&transaction, event)?;
                }
            }
        }
        transaction.commit()?;
        Ok(purged)
    }

    fn provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<Option<ProviderRepositoryState>, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let state = transaction
            .query_row(
                "SELECT provider,
                        owner,
                        repo,
                        last_access_changed_at_unix_seconds,
                        last_revision_pushed_at_unix_seconds,
                        last_pushed_revision,
                        last_cache_invalidated_at_unix_seconds,
                        last_authorization_rechecked_at_unix_seconds,
                        last_drift_checked_at_unix_seconds
                 FROM shardline_provider_repository_states
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
                params![provider.as_str(), owner, repo],
                super::helpers::provider_repository_state_from_row,
            )
            .optional()?;
        if let Some(state) = state.as_ref() {
            let snapshot = snapshot_from_state(state)?;
            let stored = super::helpers::load_provider_evidence(&transaction, &snapshot)?;
            if stored.events().is_empty() {
                verify_provider_lifecycle_events(
                    ProviderEvidenceLog::baseline(snapshot.clone())?.events(),
                    &snapshot,
                )?;
            } else {
                stored.verify_for(&snapshot)?;
            }
        }
        transaction.commit()?;
        Ok(state)
    }

    fn list_provider_repository_states(&self) -> Result<Vec<ProviderRepositoryState>, Self::Error> {
        let mut connection = self.open_connection()?;
        let states = {
            let mut statement = connection.prepare(
                "SELECT provider,
                    owner,
                    repo,
                    last_access_changed_at_unix_seconds,
                    last_revision_pushed_at_unix_seconds,
                    last_pushed_revision,
                    last_cache_invalidated_at_unix_seconds,
                    last_authorization_rechecked_at_unix_seconds,
                    last_drift_checked_at_unix_seconds
             FROM shardline_provider_repository_states
             ORDER BY provider, owner, repo",
            )?;
            let rows =
                statement.query_map([], super::helpers::provider_repository_state_from_row)?;
            collect_rows(rows)?
        };
        let transaction = connection.transaction()?;
        let snapshots = states
            .iter()
            .map(snapshot_from_state)
            .collect::<Result<Vec<_>, _>>()?;
        let evidence = load_provider_evidence_batch(&transaction, &snapshots)?;
        for (_state, snapshot) in states.iter().zip(snapshots) {
            let operation_id = super::helpers::provider_evidence_operation_id(&snapshot);
            let stored = evidence
                .get(&operation_id)
                .ok_or(LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                ))?;
            if stored.events().is_empty() {
                verify_provider_lifecycle_events(
                    ProviderEvidenceLog::baseline(snapshot.clone())?.events(),
                    &snapshot,
                )?;
            } else {
                stored.verify_for(&snapshot)?;
            }
        }
        transaction.commit()?;
        Ok(states)
    }

    fn visit_provider_repository_states<Visitor, VisitorError>(
        &self,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(ProviderRepositoryState) -> Result<(), VisitorError>,
    {
        visit_inventory::<ProviderRepositoryState, _, _>(self, visitor)
    }

    fn upsert_provider_repository_state(
        &self,
        state: &ProviderRepositoryState,
    ) -> Result<(), Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT provider,
                        owner,
                        repo,
                        last_access_changed_at_unix_seconds,
                        last_revision_pushed_at_unix_seconds,
                        last_pushed_revision,
                        last_cache_invalidated_at_unix_seconds,
                        last_authorization_rechecked_at_unix_seconds,
                        last_drift_checked_at_unix_seconds
                 FROM shardline_provider_repository_states
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
                params![state.provider().as_str(), state.owner(), state.repo()],
                super::helpers::provider_repository_state_from_row,
            )
            .optional()?;
        let current_snapshot = current.as_ref().map(snapshot_from_state).transpose()?;
        let evidence = if let Some(snapshot) = current_snapshot.as_ref() {
            super::helpers::load_provider_evidence(&transaction, snapshot)?
        } else {
            transaction.execute(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'ProviderEvent' AND operation_id = ?1",
                params![format!(
                    "{}:{}:{}",
                    state.provider().as_str(),
                    state.owner(),
                    state.repo()
                )],
            )?;
            ProviderEvidenceLog::default()
        };
        let now = unix_now_seconds_lossy();
        transaction.execute(
            "INSERT INTO shardline_provider_repository_states (
                provider,
                owner,
                repo,
                last_access_changed_at_unix_seconds,
                last_revision_pushed_at_unix_seconds,
                last_pushed_revision,
                last_cache_invalidated_at_unix_seconds,
                last_authorization_rechecked_at_unix_seconds,
                last_drift_checked_at_unix_seconds,
                created_at_unix_seconds,
                updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT (provider, owner, repo)
             DO UPDATE SET
                last_access_changed_at_unix_seconds = CASE
                    WHEN excluded.last_access_changed_at_unix_seconds IS NULL
                        THEN shardline_provider_repository_states.last_access_changed_at_unix_seconds
                    WHEN shardline_provider_repository_states.last_access_changed_at_unix_seconds IS NULL
                      OR excluded.last_access_changed_at_unix_seconds >= shardline_provider_repository_states.last_access_changed_at_unix_seconds
                        THEN excluded.last_access_changed_at_unix_seconds
                    ELSE shardline_provider_repository_states.last_access_changed_at_unix_seconds
                END,
                last_pushed_revision = CASE
                    WHEN excluded.last_revision_pushed_at_unix_seconds IS NOT NULL
                     AND (shardline_provider_repository_states.last_revision_pushed_at_unix_seconds IS NULL
                       OR excluded.last_revision_pushed_at_unix_seconds >= shardline_provider_repository_states.last_revision_pushed_at_unix_seconds)
                        THEN excluded.last_pushed_revision
                    ELSE shardline_provider_repository_states.last_pushed_revision
                END,
                last_revision_pushed_at_unix_seconds = CASE
                    WHEN excluded.last_revision_pushed_at_unix_seconds IS NULL
                        THEN shardline_provider_repository_states.last_revision_pushed_at_unix_seconds
                    WHEN shardline_provider_repository_states.last_revision_pushed_at_unix_seconds IS NULL
                      OR excluded.last_revision_pushed_at_unix_seconds >= shardline_provider_repository_states.last_revision_pushed_at_unix_seconds
                        THEN excluded.last_revision_pushed_at_unix_seconds
                    ELSE shardline_provider_repository_states.last_revision_pushed_at_unix_seconds
                END,
                last_cache_invalidated_at_unix_seconds = CASE
                    WHEN excluded.last_cache_invalidated_at_unix_seconds IS NULL
                        THEN shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds
                    WHEN shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds IS NULL
                      OR excluded.last_cache_invalidated_at_unix_seconds >= shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds
                        THEN excluded.last_cache_invalidated_at_unix_seconds
                    ELSE shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds
                END,
                last_authorization_rechecked_at_unix_seconds = CASE
                    WHEN excluded.last_authorization_rechecked_at_unix_seconds IS NULL
                        THEN shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds
                    WHEN shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds IS NULL
                      OR excluded.last_authorization_rechecked_at_unix_seconds >= shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds
                        THEN excluded.last_authorization_rechecked_at_unix_seconds
                    ELSE shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds
                END,
                last_drift_checked_at_unix_seconds = CASE
                    WHEN excluded.last_drift_checked_at_unix_seconds IS NULL
                        THEN shardline_provider_repository_states.last_drift_checked_at_unix_seconds
                    WHEN shardline_provider_repository_states.last_drift_checked_at_unix_seconds IS NULL
                      OR excluded.last_drift_checked_at_unix_seconds >= shardline_provider_repository_states.last_drift_checked_at_unix_seconds
                        THEN excluded.last_drift_checked_at_unix_seconds
                    ELSE shardline_provider_repository_states.last_drift_checked_at_unix_seconds
                END,
                updated_at_unix_seconds = excluded.updated_at_unix_seconds",
            params![
                state.provider().as_str(),
                state.owner(),
                state.repo(),
                state
                    .last_access_changed_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state
                    .last_revision_pushed_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state.last_pushed_revision(),
                state
                    .last_cache_invalidated_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state
                    .last_authorization_rechecked_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state
                    .last_drift_checked_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                u64_to_i64(now)?,
                u64_to_i64(now)?,
            ],
        )?;
        let merged = transaction.query_row(
            "SELECT provider,
                    owner,
                    repo,
                    last_access_changed_at_unix_seconds,
                    last_revision_pushed_at_unix_seconds,
                    last_pushed_revision,
                    last_cache_invalidated_at_unix_seconds,
                    last_authorization_rechecked_at_unix_seconds,
                    last_drift_checked_at_unix_seconds
             FROM shardline_provider_repository_states
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
            params![state.provider().as_str(), state.owner(), state.repo()],
            super::helpers::provider_repository_state_from_row,
        )?;
        let snapshot = snapshot_from_state(&merged)?;
        let evidence = if let Some(before) = current_snapshot {
            verify_and_append_snapshot_transition(evidence, before, snapshot)?.0
        } else {
            append_or_baseline_snapshot_evidence(evidence, snapshot)?
        };
        let event = evidence.events().last().ok_or_else(|| {
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::EmptyField(
                "provider evidence",
            ))
        })?;
        super::helpers::persist_provider_evidence(&transaction, event)?;
        transaction.commit()?;
        Ok(())
    }

    fn delete_provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT provider,
                        owner,
                        repo,
                        last_access_changed_at_unix_seconds,
                        last_revision_pushed_at_unix_seconds,
                        last_pushed_revision,
                        last_cache_invalidated_at_unix_seconds,
                        last_authorization_rechecked_at_unix_seconds,
                        last_drift_checked_at_unix_seconds
                 FROM shardline_provider_repository_states
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
                params![provider.as_str(), owner, repo],
                super::helpers::provider_repository_state_from_row,
            )
            .optional()?;
        if let Some(state) = current {
            let snapshot = snapshot_from_state(&state)?;
            let evidence = super::helpers::load_provider_evidence(&transaction, &snapshot)?;
            if evidence.events().is_empty() {
                let baseline = ProviderEvidenceLog::baseline(snapshot.clone())?;
                verify_provider_lifecycle_events(baseline.events(), &snapshot)?;
                for event in baseline.events() {
                    super::helpers::persist_provider_evidence(&transaction, event)?;
                }
            } else {
                verify_provider_lifecycle_events(evidence.events(), &snapshot)?;
            }
        }
        let changed = transaction.execute(
            "DELETE FROM shardline_provider_repository_states
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
            params![provider.as_str(), owner, repo],
        )?;
        transaction.execute(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'ProviderEvent' AND operation_id = ?1",
            params![format!("{}:{}:{}", provider.as_str(), owner, repo)],
        )?;
        transaction.commit()?;
        Ok(changed > 0)
    }
}

#[async_trait::async_trait]
impl UploadIntentStore for super::LocalIndexStore {
    type Error = LocalIndexStoreError;

    async fn create_intent(&self, intent: &UploadIntent) -> Result<(), Self::Error> {
        self.create_intent_scoped(intent, "shardline", "default")
            .await
    }

    async fn create_intent_scoped(
        &self,
        intent: &UploadIntent,
        tenant: &str,
        repository: &str,
    ) -> Result<(), Self::Error> {
        let store = self.clone();
        let intent = intent.clone();
        let tenant = tenant.to_owned();
        let repository = repository.to_owned();
        tokio::task::spawn_blocking(move || {
            let object_length = u64_to_i64(intent.object_length())?;
            let mut conn = store.open_connection()?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs() as i64;
            let created_event = upload_lifecycle_event(
                &tenant,
                &repository,
                intent.intent_id(),
                intent.object_key(),
                intent.object_hash(),
                shardline_reliability::UploadLifecycleState::Created,
                shardline_reliability::UploadLifecycleState::Created,
            )?;
            let transaction = conn.transaction()?;
            let inserted = transaction.execute(
                "INSERT OR IGNORE INTO shardline_upload_intents (intent_id, object_key, object_hash, object_length, state, created_at_unix_seconds, updated_at_unix_seconds) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    intent.intent_id(),
                    intent.object_key(),
                    intent.object_hash(),
                    object_length,
                    intent.state().as_str(),
                    now,
                    now,
                ],
            )?;
            if inserted == 0 {
                let matches_identity = transaction.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM shardline_upload_intents
                        WHERE intent_id = ?1 AND object_key = ?2 AND object_hash = ?3
                          AND object_length = ?4
                     )",
                    rusqlite::params![
                        intent.intent_id(),
                        intent.object_key(),
                        intent.object_hash(),
                        object_length,
                    ],
                    |row| row.get::<_, bool>(0),
                )?;
                if !matches_identity {
                    return Err(crate::UploadIntentConflictError::new(intent.intent_id()).into());
                }
            } else {
                transaction.execute(
                    "DELETE FROM shardline_reliability_events
                     WHERE operation_kind = 'Upload' AND operation_id = ?1",
                    rusqlite::params![intent.intent_id()],
                )?;
                super::helpers::persist_reliability_event_at(&transaction, &created_event, now)?;
            }
            transaction.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
    }

    async fn transition_intent(
        &self,
        intent_id: &str,
        new_state: UploadIntentState,
    ) -> Result<bool, Self::Error> {
        let current = self.intent_by_id(intent_id).await?;
        let Some(current) = current else {
            return Ok(false);
        };
        if current.state() == new_state {
            // Idempotent: the intent is already in the target state. This happens
            // when a duplicate concurrent caller performs the same transition, so
            // treat it as success rather than an invalid transition.
            return Ok(true);
        }
        if !current.state().can_transition_to(new_state) {
            return Ok(false);
        }
        let latest_event = {
            let store = self.clone();
            let operation_id = intent_id.to_owned();
            tokio::task::spawn_blocking(move || {
                let connection = store.open_connection()?;
                let transaction = connection.unchecked_transaction()?;
                let event = super::helpers::load_latest_verified_event_json(
                    &transaction,
                    shardline_reliability::OperationKind::Upload,
                    &operation_id,
                )?
                .ok_or(LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                ))?;
                transaction.commit()?;
                Ok::<LifecycleEvent, LocalIndexStoreError>(serde_json::from_value(event)?)
            })
            .await
            .map_err(|error| LocalIndexStoreError::Io(std::io::Error::other(error)))??
        };
        let tenant = latest_event.operation.tenant.clone();
        let repository = latest_event.operation.repository.clone();
        let event = upload_lifecycle_event(
            tenant,
            repository,
            current.intent_id(),
            current.object_key(),
            current.object_hash(),
            current.state(),
            new_state,
        )?;
        if self
            .transition_intent_with_event(intent_id, new_state, &event)
            .await?
        {
            return Ok(true);
        }
        Ok(self
            .intent_by_id(intent_id)
            .await?
            .is_some_and(|intent| intent.state() == new_state))
    }

    async fn transition_intent_with_event(
        &self,
        intent_id: &str,
        new_state: UploadIntentState,
        event: &LifecycleEvent,
    ) -> Result<bool, Self::Error> {
        let current = self.intent_by_id(intent_id).await?;
        let Some(current) = current else {
            return Ok(false);
        };
        if current.state() == new_state {
            return Ok(true);
        }
        if !current.state().can_transition_to(new_state) {
            return Ok(false);
        }
        event.validate_for_transition(intent_id, current.state(), new_state)?;
        let store = self.clone();
        let intent_id = intent_id.to_owned();
        let current_state = current.state();
        let event_json = serde_json::to_string(event)?;
        let event = event.clone();
        let operation_id = event.operation.operation_id.clone();
        let transitioned = tokio::task::spawn_blocking(move || -> Result<bool, LocalIndexStoreError> {
            let mut conn = store.open_connection()?;
            let transaction = conn.transaction()?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs() as i64;
            let rows = transaction.execute(
                "UPDATE shardline_upload_intents SET state = ?1, updated_at_unix_seconds = ?2 WHERE intent_id = ?3 AND state = ?4",
                rusqlite::params![new_state.as_str(), now, intent_id, current_state.as_str()],
            )?;
            if rows == 0 {
                transaction.rollback()?;
                return Ok(false);
            }
            super::helpers::persist_reliability_event_at(&transaction, &event, now)?;
            let stored_event: String = transaction.query_row(
                "SELECT event_json FROM shardline_reliability_events WHERE operation_kind = ?1 AND operation_id = ?2 AND sequence = ?3",
                rusqlite::params![
                    event.operation.kind.as_str(),
                    &event.operation.operation_id,
                    u64_to_i64(event.sequence)?,
                ],
                |row| row.get(0),
            )?;
            if stored_event != event_json {
                transaction.rollback()?;
                return Err(LocalIndexStoreError::ReliabilityEventConflict(
                    operation_id,
                ));
            }
            transaction.commit()?;
            Ok(true)
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))??;
        Ok(transitioned)
    }

    async fn intent_by_id(&self, intent_id: &str) -> Result<Option<UploadIntent>, Self::Error> {
        let store = self.clone();
        let intent_id = intent_id.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut conn = store.open_connection()?;
            let transaction = conn.transaction()?;
            let mut stmt = transaction.prepare(
                "SELECT intent_id, object_key, object_hash, object_length, state, created_at_unix_seconds, updated_at_unix_seconds FROM shardline_upload_intents WHERE intent_id = ?1"
            )?;
            let result = stmt.query_row(rusqlite::params![intent_id], |row| {
                let state_str: String = row.get(4)?;
                let state = UploadIntentState::parse(&state_str).ok_or_else(|| {
                    rusqlite::Error::InvalidColumnType(4, format!("invalid state: {state_str}"), rusqlite::types::Type::Text)
                })?;
                Ok(UploadIntent::from_parts(
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get::<_, i64>(3)? as u64,
                    state,
                    Duration::from_secs(row.get::<_, i64>(5)? as u64),
                    Duration::from_secs(row.get::<_, i64>(6)? as u64),
                ))
            });
            match result {
                Ok(intent) => {
                    drop(stmt);
                    verify_sqlite_intent_evidence(&transaction, &intent)?;
                    transaction.commit()?;
                    Ok(Some(intent))
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    drop(stmt);
                    transaction.commit()?;
                    Ok(None)
                }
                Err(e) => Err(LocalIndexStoreError::from(e)),
            }
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
    }

    async fn intents_by_state(
        &self,
        state: UploadIntentState,
    ) -> Result<Vec<UploadIntent>, Self::Error> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = store.open_connection()?;
            let transaction = conn.transaction()?;
            let mut stmt = transaction.prepare(
                "SELECT intent_id, object_key, object_hash, object_length, state, created_at_unix_seconds, updated_at_unix_seconds FROM shardline_upload_intents WHERE state = ?1 ORDER BY created_at_unix_seconds"
            )?;
            let intents = stmt
                .query_map(rusqlite::params![state.as_str()], |row| {
                    let state_str: String = row.get(4)?;
                    let s = UploadIntentState::parse(&state_str).ok_or_else(|| {
                        rusqlite::Error::InvalidColumnType(4, format!("invalid state: {state_str}"), rusqlite::types::Type::Text)
                    })?;
                    Ok(UploadIntent::from_parts(
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get::<_, i64>(3)? as u64,
                        s,
                        Duration::from_secs(row.get::<_, i64>(5)? as u64),
                        Duration::from_secs(row.get::<_, i64>(6)? as u64),
                    ))
                })
                .map_err(LocalIndexStoreError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(LocalIndexStoreError::from)?;
            drop(stmt);
            let operation_ids = intents
                .iter()
                .map(|intent| intent.intent_id().to_owned())
                .collect::<Vec<_>>();
            let heads = super::helpers::load_latest_verified_event_json_batch(
                &transaction,
                shardline_reliability::OperationKind::Upload,
                &operation_ids,
            )?;
            for intent in &intents {
                let event_json = heads.get(intent.intent_id()).ok_or(
                    LocalIndexStoreError::Reliability(
                        shardline_reliability::ReliabilityError::OperationMismatch,
                    ),
                )?;
                let event = serde_json::from_value::<LifecycleEvent>(event_json.clone())?;
                verify_upload_lifecycle_head(
                    &event,
                    event.operation.tenant.as_str(),
                    event.operation.repository.as_str(),
                    intent.intent_id(),
                    intent.object_key(),
                    intent.object_hash(),
                    intent.state(),
                )?;
            }
            transaction.commit()?;
            Ok(intents)
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
    }

    async fn stale_intents(
        &self,
        state: UploadIntentState,
        older_than: Duration,
    ) -> Result<Vec<UploadIntent>, Self::Error> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = store.open_connection()?;
            let transaction = conn.transaction()?;
            let cutoff = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .saturating_sub(older_than)
                .as_secs() as i64;
            let mut stmt = transaction.prepare(
                "SELECT intent_id, object_key, object_hash, object_length, state, created_at_unix_seconds, updated_at_unix_seconds FROM shardline_upload_intents WHERE state = ?1 AND created_at_unix_seconds < ?2 ORDER BY created_at_unix_seconds"
            )?;
            let intents = stmt
                .query_map(rusqlite::params![state.as_str(), cutoff], |row| {
                    let state_str: String = row.get(4)?;
                    let s = UploadIntentState::parse(&state_str).ok_or_else(|| {
                        rusqlite::Error::InvalidColumnType(4, format!("invalid state: {state_str}"), rusqlite::types::Type::Text)
                    })?;
                    Ok(UploadIntent::from_parts(
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get::<_, i64>(3)? as u64,
                        s,
                        Duration::from_secs(row.get::<_, i64>(5)? as u64),
                        Duration::from_secs(row.get::<_, i64>(6)? as u64),
                    ))
                })
                .map_err(LocalIndexStoreError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(LocalIndexStoreError::from)?;
            drop(stmt);
            let operation_ids = intents
                .iter()
                .map(|intent| intent.intent_id().to_owned())
                .collect::<Vec<_>>();
            let heads = super::helpers::load_latest_verified_event_json_batch(
                &transaction,
                shardline_reliability::OperationKind::Upload,
                &operation_ids,
            )?;
            for intent in &intents {
                let event_json = heads.get(intent.intent_id()).ok_or(
                    LocalIndexStoreError::Reliability(
                        shardline_reliability::ReliabilityError::OperationMismatch,
                    ),
                )?;
                let event = serde_json::from_value::<LifecycleEvent>(event_json.clone())?;
                verify_upload_lifecycle_head(
                    &event,
                    event.operation.tenant.as_str(),
                    event.operation.repository.as_str(),
                    intent.intent_id(),
                    intent.object_key(),
                    intent.object_hash(),
                    intent.state(),
                )?;
            }
            transaction.commit()?;
            Ok(intents)
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
    }

    async fn record_reliability_event(&self, event: &LifecycleEvent) -> Result<(), Self::Error> {
        event.verify_integrity()?;
        let store = self.clone();
        let event = event.clone();
        let event_json = serde_json::to_string(&event)?;
        let operation_id = event.operation.operation_id.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = store.open_connection()?;
            let transaction = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let (object_key, object_hash, state_text) = transaction
                .query_row(
                    "SELECT object_key, object_hash, state
                     FROM shardline_upload_intents
                     WHERE intent_id = ?1",
                    params![&operation_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?
                .ok_or_else(|| {
                    LocalIndexStoreError::Reliability(
                        shardline_reliability::ReliabilityError::EmptyField(
                            "reliability event has no authoritative upload intent",
                        ),
                    )
                })?;
            let state = UploadIntentState::parse(&state_text).ok_or_else(|| {
                LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::EmptyField(
                        "unknown upload intent state",
                    ),
                )
            })?;
            let mut events = super::helpers::load_verified_event_json(
                &transaction,
                shardline_reliability::OperationKind::Upload,
                &operation_id,
            )?
            .into_iter()
            .map(serde_json::from_value::<LifecycleEvent>)
            .collect::<Result<Vec<_>, _>>()?;
            if let Some(existing) = events
                .iter()
                .find(|existing| existing.sequence == event.sequence)
            {
                if existing != &event {
                    return Err(LocalIndexStoreError::ReliabilityEventConflict(
                        operation_id,
                    ));
                }
            } else {
                events.push(event.clone());
                events.sort_by_key(|stored_event| stored_event.sequence);
            }
            let (tenant, repository) = upload_lifecycle_identity(&events);
            verify_upload_lifecycle_events(
                &events,
                tenant,
                repository,
                &operation_id,
                &object_key,
                &object_hash,
                state,
            )
            .map_err(LocalIndexStoreError::Reliability)?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs() as i64;
            super::helpers::persist_reliability_event_at(&transaction, &event, now)?;
            let stored_event: String = transaction.query_row(
                "SELECT event_json FROM shardline_reliability_events WHERE operation_kind = ?1 AND operation_id = ?2 AND sequence = ?3",
                rusqlite::params![
                    event.operation.kind.as_str(),
                    &event.operation.operation_id,
                    u64_to_i64(event.sequence)?,
                ],
                |row| row.get(0),
            )?;
            if stored_event != event_json {
                return Err(LocalIndexStoreError::ReliabilityEventConflict(
                    operation_id,
                ));
            }
            transaction.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
    }

    async fn reliability_events(
        &self,
        operation_id: &str,
    ) -> Result<Vec<LifecycleEvent>, Self::Error> {
        let store = self.clone();
        let operation_id = operation_id.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut conn = store.open_connection()?;
            let transaction = conn.transaction()?;
            let events = super::helpers::load_verified_event_json(
                &transaction,
                shardline_reliability::OperationKind::Upload,
                &operation_id,
            )?
            .into_iter()
            .map(serde_json::from_value::<LifecycleEvent>)
            .collect::<Result<Vec<_>, _>>()?;
            let intent = transaction
                .query_row(
                    "SELECT object_key, object_hash, state
                     FROM shardline_upload_intents
                     WHERE intent_id = ?1",
                    rusqlite::params![&operation_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?;
            if let Some((object_key, object_hash, state_text)) = intent {
                let state = UploadIntentState::parse(&state_text).ok_or_else(|| {
                    LocalIndexStoreError::Reliability(
                        shardline_reliability::ReliabilityError::EmptyField(
                            "unknown upload intent state",
                        ),
                    )
                })?;
                let (tenant, repository) = upload_lifecycle_identity(&events);
                shardline_reliability::verify_upload_lifecycle_events(
                    &events,
                    tenant,
                    repository,
                    &operation_id,
                    &object_key,
                    &object_hash,
                    state,
                )?;
            } else {
                shardline_reliability::verify_lifecycle_chain(&events)
                    .map_err(LocalIndexStoreError::Reliability)?;
            }
            transaction.commit()?;
            Ok(events)
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
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
    use shardline_protocol::{ChunkRange, RepositoryProvider};
    use shardline_reliability::SnapshotEvidence;
    use shardline_storage::ObjectKey;

    use super::*;
    use crate::{
        ProviderRepositoryState, QuarantineCandidate, ReconstructionTerm, RetentionHold,
        WebhookDelivery,
    };

    fn make_store() -> LocalIndexStore {
        let storage = shardline_test_support::TempStorage::new();
        LocalIndexStore::new(storage.path_buf()).expect("failed to create local index store")
    }

    #[test]
    fn insert_and_get_reconstruction_roundtrip() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([1; 32]));
        let object_id = StoredObjectId::new(ShardlineHash::from_bytes([2; 32]));
        let range = ChunkRange::new(0, 3).unwrap();
        let reconstruction =
            FileReconstruction::new(vec![ReconstructionTerm::new(object_id, range, 256)]);

        store
            .insert_reconstruction(&file_id, &reconstruction)
            .expect("insert should succeed");
        let loaded =
            ReconstructionStore::reconstruction(&store, &file_id).expect("lookup should succeed");
        assert_eq!(loaded, Some(reconstruction));
    }

    #[test]
    fn reconstruction_returns_none_for_missing_file_id() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([99; 32]));
        let loaded =
            ReconstructionStore::reconstruction(&store, &file_id).expect("lookup should succeed");
        assert_eq!(loaded, None);
    }

    #[test]
    fn delete_reconstruction_returns_true_then_false() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([3; 32]));
        let reconstruction = FileReconstruction::new(vec![]);

        store
            .insert_reconstruction(&file_id, &reconstruction)
            .expect("insert should succeed");
        let deleted = ReconstructionStore::delete_reconstruction(&store, &file_id)
            .expect("delete should succeed");
        assert!(deleted);
        let deleted_again = ReconstructionStore::delete_reconstruction(&store, &file_id)
            .expect("second delete should succeed");
        assert!(!deleted_again);
    }

    #[test]
    fn conditional_reconstruction_delete_preserves_replacement() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([4; 32]));
        let original = FileReconstruction::new(vec![]);
        let replacement = FileReconstruction::new(vec![ReconstructionTerm::new(
            StoredObjectId::new(ShardlineHash::from_bytes([5; 32])),
            ChunkRange::new(0, 1).unwrap(),
            1,
        )]);
        store.insert_reconstruction(&file_id, &original).unwrap();
        store.insert_reconstruction(&file_id, &replacement).unwrap();

        assert!(
            !ReconstructionStore::delete_reconstruction_if_matches(&store, &file_id, &original)
                .unwrap()
        );
        assert_eq!(
            ReconstructionStore::reconstruction(&store, &file_id).unwrap(),
            Some(replacement)
        );
    }

    #[test]
    fn list_reconstruction_file_ids_empty_initially() {
        let store = make_store();
        let ids =
            ReconstructionStore::list_reconstruction_file_ids(&store).expect("list should succeed");
        assert!(ids.is_empty());
    }

    #[test]
    fn list_reconstruction_file_ids_after_insert() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([10; 32]));
        let reconstruction = FileReconstruction::new(vec![]);

        store
            .insert_reconstruction(&file_id, &reconstruction)
            .expect("insert should succeed");
        let ids =
            ReconstructionStore::list_reconstruction_file_ids(&store).expect("list should succeed");
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], file_id);
    }

    #[test]
    fn insert_object_and_contains_object_roundtrip() {
        let store = make_store();
        let object_id = StoredObjectId::new(ShardlineHash::from_bytes([5; 32]));

        assert!(
            !ReconstructionStore::contains_object(&store, &object_id)
                .expect("check should succeed")
        );
        store
            .insert_object(&object_id)
            .expect("insert should succeed");
        assert!(
            ReconstructionStore::contains_object(&store, &object_id).expect("check should succeed")
        );
    }

    #[test]
    fn upsert_and_get_dedupe_shard_mapping_roundtrip() {
        let store = make_store();
        let chunk_hash = ShardlineHash::from_bytes([7; 32]);
        let object_key = ObjectKey::parse("shards/aa/test.shard").unwrap();
        let mapping = DedupeShardMapping::new(chunk_hash, object_key);

        store
            .upsert_dedupe_shard_mapping(&mapping)
            .expect("upsert should succeed");
        let loaded =
            DedupeStore::dedupe_shard_mapping(&store, &chunk_hash).expect("lookup should succeed");
        assert_eq!(loaded, Some(mapping));
    }

    #[test]
    fn dedupe_shard_mapping_returns_none_for_missing_hash() {
        let store = make_store();
        let chunk_hash = ShardlineHash::from_bytes([99; 32]);
        let loaded =
            DedupeStore::dedupe_shard_mapping(&store, &chunk_hash).expect("lookup should succeed");
        assert_eq!(loaded, None);
    }

    #[test]
    fn delete_dedupe_shard_mapping_returns_true() {
        let store = make_store();
        let chunk_hash = ShardlineHash::from_bytes([8; 32]);
        let object_key = ObjectKey::parse("shards/bb/test.shard").unwrap();
        let mapping = DedupeShardMapping::new(chunk_hash, object_key);

        store
            .upsert_dedupe_shard_mapping(&mapping)
            .expect("upsert should succeed");
        let deleted = DedupeStore::delete_dedupe_shard_mapping(&store, &chunk_hash)
            .expect("delete should succeed");
        assert!(deleted);
        let loaded =
            DedupeStore::dedupe_shard_mapping(&store, &chunk_hash).expect("lookup should succeed");
        assert_eq!(loaded, None);
    }

    #[test]
    fn delete_dedupe_shard_mapping_returns_false_when_missing() {
        let store = make_store();
        let chunk_hash = ShardlineHash::from_bytes([99; 32]);
        let deleted = DedupeStore::delete_dedupe_shard_mapping(&store, &chunk_hash)
            .expect("delete should succeed");
        assert!(!deleted);
    }

    #[test]
    fn conditional_dedupe_delete_preserves_replacement() {
        let store = make_store();
        let chunk_hash = ShardlineHash::from_bytes([9; 32]);
        let original = DedupeShardMapping::new(
            chunk_hash,
            ObjectKey::parse("shards/cc/original.shard").unwrap(),
        );
        let replacement = DedupeShardMapping::new(
            chunk_hash,
            ObjectKey::parse("shards/cc/replacement.shard").unwrap(),
        );
        store.upsert_dedupe_shard_mapping(&original).unwrap();
        store.upsert_dedupe_shard_mapping(&replacement).unwrap();

        assert!(!DedupeStore::delete_dedupe_shard_mapping_if_matches(&store, &original).unwrap());
        assert_eq!(
            DedupeStore::dedupe_shard_mapping(&store, &chunk_hash).unwrap(),
            Some(replacement)
        );
    }

    // ── LifecycleStore: quarantine candidate ───────────────────────────────

    #[test]
    fn quarantine_candidate_returns_none_for_missing_key() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/aa/missing").unwrap();
        let loaded =
            LifecycleStore::quarantine_candidate(&store, &key).expect("lookup should succeed");
        assert!(loaded.is_none());
    }

    #[test]
    fn quarantine_candidate_upsert_and_read_roundtrip() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/aa/test-candidate").unwrap();
        let candidate = QuarantineCandidate::new(key.clone(), 100, 1000, 2000).unwrap();

        LifecycleStore::upsert_quarantine_candidate(&store, &candidate)
            .expect("upsert should succeed");
        let loaded =
            LifecycleStore::quarantine_candidate(&store, &key).expect("lookup should succeed");
        assert_eq!(loaded, Some(candidate));
    }

    #[test]
    fn quarantine_candidate_list_includes_upserted() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/bb/list-candidate").unwrap();
        let candidate = QuarantineCandidate::new(key, 200, 2000, 3000).unwrap();

        LifecycleStore::upsert_quarantine_candidate(&store, &candidate)
            .expect("upsert should succeed");
        let candidates =
            LifecycleStore::list_quarantine_candidates(&store).expect("list should succeed");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].observed_length(), 200);
    }

    #[test]
    fn quarantine_candidate_delete_returns_true_then_false() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/cc/del-candidate").unwrap();
        let candidate = QuarantineCandidate::new(key.clone(), 300, 3000, 4000).unwrap();

        LifecycleStore::upsert_quarantine_candidate(&store, &candidate)
            .expect("upsert should succeed");
        assert!(
            LifecycleStore::delete_quarantine_candidate(&store, &key)
                .expect("first delete should succeed")
        );
        assert!(
            !LifecycleStore::delete_quarantine_candidate(&store, &key)
                .expect("second delete should succeed")
        );
    }

    #[test]
    fn quarantine_candidate_reactivation_accepts_changed_observed_metadata() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/dd/reactivated-candidate").unwrap();
        let original = QuarantineCandidate::new(key.clone(), 300, 3000, 4000).unwrap();
        let reactivated = QuarantineCandidate::new(key.clone(), 301, 5000, 6000).unwrap();

        LifecycleStore::upsert_quarantine_candidate(&store, &original).unwrap();
        assert!(LifecycleStore::delete_quarantine_candidate(&store, &key).unwrap());
        LifecycleStore::upsert_quarantine_candidate(&store, &reactivated).unwrap();

        assert_eq!(
            LifecycleStore::quarantine_candidate(&store, &key).unwrap(),
            Some(reactivated)
        );
    }

    #[test]
    fn retention_hold_reactivation_accepts_changed_metadata() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/dd/reactivated-retention").unwrap();
        let original =
            RetentionHold::new(key.clone(), "original".to_owned(), 3000, Some(4000)).unwrap();
        let reactivated =
            RetentionHold::new(key.clone(), "updated".to_owned(), 5000, Some(6000)).unwrap();

        LifecycleStore::upsert_retention_hold(&store, &original).unwrap();
        assert!(LifecycleStore::delete_retention_hold(&store, &key).unwrap());
        LifecycleStore::upsert_retention_hold(&store, &reactivated).unwrap();

        assert_eq!(
            LifecycleStore::retention_hold(&store, &key).unwrap(),
            Some(reactivated)
        );
    }

    #[test]
    fn quarantine_candidate_conditional_delete_preserves_replacement() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/cc/conditional-candidate").unwrap();
        let original = QuarantineCandidate::new(key.clone(), 300, 3000, 4000).unwrap();
        let replacement = QuarantineCandidate::new(key, 301, 3000, 4000).unwrap();
        LifecycleStore::upsert_quarantine_candidate(&store, &original).unwrap();
        LifecycleStore::upsert_quarantine_candidate(&store, &replacement).unwrap();

        assert!(
            !LifecycleStore::delete_quarantine_candidate_if_matches(&store, &original).unwrap()
        );
        assert_eq!(
            LifecycleStore::quarantine_candidate(&store, replacement.object_key()).unwrap(),
            Some(replacement)
        );
    }

    // ── LifecycleStore: retention hold ─────────────────────────────────────

    #[test]
    fn retention_hold_returns_none_for_missing_key() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/aa/missing-hold").unwrap();
        let loaded = LifecycleStore::retention_hold(&store, &key).expect("lookup should succeed");
        assert!(loaded.is_none());
    }

    #[test]
    fn retention_hold_upsert_and_read_roundtrip() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/aa/test-hold").unwrap();
        let hold = RetentionHold::new(key.clone(), "test reason".into(), 100, Some(200)).unwrap();

        LifecycleStore::upsert_retention_hold(&store, &hold).expect("upsert should succeed");
        let loaded = LifecycleStore::retention_hold(&store, &key).expect("lookup should succeed");
        assert_eq!(loaded, Some(hold));
    }

    #[test]
    fn retention_hold_list_includes_upserted() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/bb/list-hold").unwrap();
        let hold = RetentionHold::new(key, "retain".into(), 300, None).unwrap();

        LifecycleStore::upsert_retention_hold(&store, &hold).expect("upsert should succeed");
        let holds = LifecycleStore::list_retention_holds(&store).expect("list should succeed");
        assert_eq!(holds.len(), 1);
        assert_eq!(holds[0].reason(), "retain");
    }

    #[test]
    fn retention_hold_delete_returns_true_then_false() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/cc/del-hold").unwrap();
        let hold = RetentionHold::new(key.clone(), "delete me".into(), 400, None).unwrap();

        LifecycleStore::upsert_retention_hold(&store, &hold).expect("upsert should succeed");
        assert!(
            LifecycleStore::delete_retention_hold(&store, &key)
                .expect("first delete should succeed")
        );
        assert!(
            !LifecycleStore::delete_retention_hold(&store, &key)
                .expect("second delete should succeed")
        );
    }

    #[test]
    fn retention_hold_conditional_delete_preserves_replacement() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/cc/conditional-hold").unwrap();
        let original =
            RetentionHold::new(key.clone(), "original".to_owned(), 3000, Some(4000)).unwrap();
        let replacement =
            RetentionHold::new(key, "replacement".to_owned(), 3000, Some(4000)).unwrap();
        LifecycleStore::upsert_retention_hold(&store, &original).unwrap();
        LifecycleStore::upsert_retention_hold(&store, &replacement).unwrap();

        assert!(!LifecycleStore::delete_retention_hold_if_matches(&store, &original).unwrap());
        assert_eq!(
            LifecycleStore::retention_hold(&store, replacement.object_key()).unwrap(),
            Some(replacement)
        );
    }

    // ── LifecycleStore: webhook delivery ───────────────────────────────────

    #[test]
    fn webhook_delivery_record_returns_true_for_new() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-1".into(),
            1000,
        )
        .unwrap();

        let recorded = LifecycleStore::record_webhook_delivery(&store, &delivery)
            .expect("record should succeed");
        assert!(recorded, "first record should return true");
    }

    #[test]
    fn webhook_delivery_record_returns_false_for_duplicate() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-dup".into(),
            1000,
        )
        .unwrap();

        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();
        let repeated = LifecycleStore::record_webhook_delivery(&store, &delivery)
            .expect("duplicate record should succeed");
        assert!(!repeated, "duplicate record should return false");
    }

    #[test]
    fn webhook_delivery_recreation_appends_without_rewriting_history() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-recreate".into(),
            1000,
        )
        .unwrap();

        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();
        assert!(LifecycleStore::delete_webhook_delivery(&store, &delivery).unwrap());

        let connection = store.open_connection().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER reject_reliability_history_rewrites
                 BEFORE UPDATE ON shardline_reliability_events
                 BEGIN
                     SELECT RAISE(ABORT, 'reliability history was rewritten');
                 END;",
            )
            .unwrap();
        drop(connection);

        let recreated = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-recreate".into(),
            2000,
        )
        .unwrap();
        assert!(LifecycleStore::record_webhook_delivery(&store, &recreated).unwrap());
        assert_eq!(
            LifecycleStore::list_webhook_deliveries(&store).unwrap(),
            vec![delivery]
        );
    }

    #[test]
    fn webhook_delivery_tampered_evidence_is_rejected_on_read() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-tampered".into(),
            1000,
        )
        .unwrap();
        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();

        let connection = store.open_connection().unwrap();
        let operation_id = webhook_snapshot(&delivery, WebhookDeliveryLifecycleState::Processed)
            .unwrap()
            .evidence_operation()
            .unwrap()
            .operation_id;
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"sequence\":99}'
                 WHERE operation_kind = 'WebhookDelivery' AND operation_id = ?1",
                rusqlite::params![operation_id],
            )
            .unwrap();

        assert!(LifecycleStore::list_webhook_deliveries(&store).is_err());
    }

    #[test]
    fn webhook_delivery_list_includes_recorded() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-list".into(),
            2000,
        )
        .unwrap();

        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();
        let deliveries =
            LifecycleStore::list_webhook_deliveries(&store).expect("list should succeed");
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].delivery_id(), "delivery-list");
    }

    #[test]
    fn webhook_delivery_delete_returns_true_then_false() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-del".into(),
            3000,
        )
        .unwrap();

        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();
        assert!(
            LifecycleStore::delete_webhook_delivery(&store, &delivery)
                .expect("delete should succeed")
        );
        assert!(
            !LifecycleStore::delete_webhook_delivery(&store, &delivery)
                .expect("second delete should succeed")
        );
    }

    #[test]
    fn webhook_delivery_conditional_delete_rejects_different_observation() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".to_owned(),
            "repo".to_owned(),
            "conditional-delivery".to_owned(),
            3000,
        )
        .unwrap();
        let different_observation = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".to_owned(),
            "repo".to_owned(),
            "conditional-delivery".to_owned(),
            3001,
        )
        .unwrap();
        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();

        assert!(
            !LifecycleStore::delete_webhook_delivery_if_matches(&store, &different_observation)
                .unwrap()
        );
        assert_eq!(
            LifecycleStore::list_webhook_deliveries(&store).unwrap(),
            vec![delivery]
        );
    }

    #[test]
    fn webhook_delivery_purge_removes_only_rows_older_than_cutoff() {
        let store = make_store();
        let old = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-old".into(),
            100,
        )
        .unwrap();
        let fresh = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-fresh".into(),
            900,
        )
        .unwrap();
        LifecycleStore::record_webhook_delivery(&store, &old).unwrap();
        LifecycleStore::record_webhook_delivery(&store, &fresh).unwrap();

        let purged = LifecycleStore::purge_webhook_deliveries_older_than(&store, 500)
            .expect("purge should succeed");
        assert_eq!(purged, 1, "only the row older than the cutoff is purged");
        let remaining = LifecycleStore::list_webhook_deliveries(&store).unwrap();
        assert_eq!(
            remaining.len(),
            1,
            "rows inside the retention window must survive the purge"
        );
        assert_eq!(remaining[0].delivery_id(), "delivery-fresh");
        assert_eq!(remaining[0].processed_at_unix_seconds(), 900);
        // Dedup semantics stay intact: the surviving claim still dedups.
        assert!(
            !LifecycleStore::record_webhook_delivery(&store, &fresh).unwrap(),
            "surviving claim must still dedup after the purge"
        );
    }

    #[test]
    fn webhook_delivery_purge_is_idempotent_and_counts_rows() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-purge".into(),
            100,
        )
        .unwrap();
        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();

        assert_eq!(
            LifecycleStore::purge_webhook_deliveries_older_than(&store, 200).unwrap(),
            1
        );
        assert_eq!(
            LifecycleStore::purge_webhook_deliveries_older_than(&store, 200).unwrap(),
            0
        );
        assert!(
            LifecycleStore::list_webhook_deliveries(&store)
                .unwrap()
                .is_empty()
        );
    }

    fn purge_fixture(count: usize, processed_at: u64) -> (LocalIndexStore, WebhookDelivery) {
        let store = make_store();
        let mut connection = store.open_connection().unwrap();
        let transaction = connection.transaction().unwrap();
        let mut last = None;
        for index in 0..count {
            // Timestamp order deliberately conflicts with the composite key.
            // Keep the final record latest for late-page corruption controls.
            let observed_at = processed_at
                + if index == count.saturating_sub(1) {
                    5
                } else {
                    (index * 31 % 5) as u64
                };
            let delivery = WebhookDelivery::new(
                RepositoryProvider::GitHub,
                format!("owner-{}", index / INVENTORY_BATCH_SIZE),
                "repo".into(),
                format!("batch-{:05}", index % INVENTORY_BATCH_SIZE),
                observed_at,
            )
            .unwrap();
            let snapshot =
                webhook_snapshot(&delivery, WebhookDeliveryLifecycleState::Processed).unwrap();
            let mut evidence =
                shardline_reliability::WebhookDeliveryEvidenceLog::baseline(snapshot.clone())
                    .unwrap();
            if index == count.saturating_sub(1) {
                evidence
                    .record(
                        webhook_snapshot(&delivery, WebhookDeliveryLifecycleState::Released)
                            .unwrap(),
                    )
                    .unwrap();
                evidence.record(snapshot).unwrap();
            }
            transaction.execute(
                "INSERT INTO shardline_webhook_deliveries(provider,owner,repo,delivery_id,processed_at_unix_seconds) VALUES('github',?1,'repo',?2,?3)",
                params![delivery.owner(),delivery.delivery_id(),u64_to_i64(observed_at).unwrap()],
            ).unwrap();
            for event in evidence.events() {
                persist_webhook_evidence(&transaction, event).unwrap();
            }
            last = Some(delivery);
        }
        transaction.commit().unwrap();
        (store, last.unwrap())
    }

    #[test]
    fn webhook_delivery_noop_purge_work_does_not_grow_with_retained_rows() {
        let mut steps = Vec::new();
        for count in [100, 2000] {
            let (store, _) = purge_fixture(count, 1000);
            let connection = store.open_connection().unwrap();
            let mut statement = connection
                .prepare(WEBHOOK_RETENTION_FIRST_PAGE_SQL)
                .unwrap();
            {
                let mut rows = statement
                    .query(params![200, i64::try_from(INVENTORY_BATCH_SIZE).unwrap()])
                    .unwrap();
                assert!(rows.next().unwrap().is_none());
            }
            steps.push(statement.get_status(rusqlite::StatementStatus::VmStep));
            assert_eq!(
                LifecycleStore::purge_webhook_deliveries_older_than(&store, 200).unwrap(),
                0
            );
        }
        // Without the retention index/order, the valid larger inventory takes
        // thousands of VM steps even though no row is eligible for retention.
        assert!(
            steps[1] <= steps[0] + 20,
            "no-op query work grew: {steps:?}"
        );
    }

    #[test]
    fn webhook_delivery_purge_batches_count_and_preserve_cutoff_and_retry() {
        let (store, last) = purge_fixture(INVENTORY_BATCH_SIZE * 2 + 1, 100);
        for timestamp in [200, 201] {
            let delivery = WebhookDelivery::new(
                RepositoryProvider::GitHub,
                "owner".into(),
                "repo".into(),
                format!("fresh-{timestamp}"),
                timestamp,
            )
            .unwrap();
            assert!(LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap());
        }
        // Missing evidence still gets a valid baseline and release transition.
        let operation = webhook_snapshot(&last, WebhookDeliveryLifecycleState::Processed)
            .unwrap()
            .evidence_operation()
            .unwrap();
        let connection = store.open_connection().unwrap();
        connection.execute("DELETE FROM shardline_reliability_events WHERE operation_kind='WebhookDelivery' AND operation_id=?1", [&operation.operation_id]).unwrap();
        assert_eq!(
            LifecycleStore::purge_webhook_deliveries_older_than(&store, 0).unwrap(),
            0
        );
        assert_eq!(
            LifecycleStore::purge_webhook_deliveries_older_than(&store, 200).unwrap(),
            u64::try_from(INVENTORY_BATCH_SIZE * 2 + 1).unwrap()
        );
        assert_eq!(
            LifecycleStore::purge_webhook_deliveries_older_than(&store, 200).unwrap(),
            0
        );
        let remaining = LifecycleStore::list_webhook_deliveries(&store).unwrap();
        assert_eq!(remaining.len(), 2);
        assert!(
            remaining
                .iter()
                .all(|delivery| delivery.processed_at_unix_seconds() >= 200)
        );
        assert!(LifecycleStore::record_webhook_delivery(&store, &last).unwrap());
        assert!(!LifecycleStore::record_webhook_delivery(&store, &last).unwrap());
    }

    #[test]
    fn webhook_delivery_purge_late_corruption_rolls_back_every_page() {
        let (store, last) = purge_fixture(INVENTORY_BATCH_SIZE * 2 + 1, 100);
        let operation = webhook_snapshot(&last, WebhookDeliveryLifecycleState::Processed)
            .unwrap()
            .evidence_operation()
            .unwrap();
        let connection = store.open_connection().unwrap();
        let original: String = connection.query_row("SELECT merkle_commit_json FROM shardline_reliability_events WHERE operation_kind='WebhookDelivery' AND operation_id=?1 AND sequence=2", [&operation.operation_id], |row| row.get(0)).unwrap();
        let baseline: String = connection.query_row("SELECT merkle_commit_json FROM shardline_reliability_events WHERE operation_kind='WebhookDelivery' AND operation_id=?1 AND sequence=0", [&operation.operation_id], |row| row.get(0)).unwrap();
        for sql in [
            "UPDATE shardline_reliability_events SET merkle_commit_json=NULL WHERE operation_kind='WebhookDelivery' AND operation_id=?1 AND sequence=2",
            "UPDATE shardline_reliability_events SET merkle_commit_json='{}' WHERE operation_kind='WebhookDelivery' AND operation_id=?1 AND sequence=2",
            "UPDATE shardline_reliability_events SET merkle_commit_json=NULL WHERE operation_kind='WebhookDelivery' AND operation_id=?1 AND sequence=0",
        ] {
            connection.execute(sql, [&operation.operation_id]).unwrap();
            assert!(LifecycleStore::purge_webhook_deliveries_older_than(&store, 200).is_err());
            let rows: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM shardline_webhook_deliveries",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let events: i64 = connection.query_row("SELECT COUNT(*) FROM shardline_reliability_events WHERE operation_kind='WebhookDelivery'", [], |row| row.get(0)).unwrap();
            assert_eq!(rows, i64::try_from(INVENTORY_BATCH_SIZE * 2 + 1).unwrap());
            assert_eq!(events, i64::try_from(INVENTORY_BATCH_SIZE * 2 + 3).unwrap());
            connection.execute("UPDATE shardline_reliability_events SET merkle_commit_json=?1 WHERE operation_kind='WebhookDelivery' AND operation_id=?2 AND sequence=2", params![original,operation.operation_id]).unwrap();
            connection.execute("UPDATE shardline_reliability_events SET merkle_commit_json=?1 WHERE operation_kind='WebhookDelivery' AND operation_id=?2 AND sequence=0", params![baseline,operation.operation_id]).unwrap();
        }
        connection.execute("UPDATE shardline_webhook_deliveries SET processed_at_unix_seconds=106 WHERE delivery_id=?1 AND owner=?2", params![last.delivery_id(), last.owner()]).unwrap();
        assert!(LifecycleStore::purge_webhook_deliveries_older_than(&store, 200).is_err());
        let rows: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM shardline_webhook_deliveries",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, i64::try_from(INVENTORY_BATCH_SIZE * 2 + 1).unwrap());
    }

    #[test]
    fn webhook_delivery_purge_rejects_negative_timestamp_without_writes() {
        let (store, last) = purge_fixture(INVENTORY_BATCH_SIZE * 2 + 1, 100);
        let connection = store.open_connection().unwrap();
        connection
            .execute_batch("PRAGMA ignore_check_constraints=ON")
            .unwrap();
        connection.execute("UPDATE shardline_webhook_deliveries SET processed_at_unix_seconds=-1 WHERE delivery_id=?1 AND owner=?2", params![last.delivery_id(), last.owner()]).unwrap();
        assert!(LifecycleStore::purge_webhook_deliveries_older_than(&store, 200).is_err());
        let rows: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM shardline_webhook_deliveries",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, i64::try_from(INVENTORY_BATCH_SIZE * 2 + 1).unwrap());
    }

    // ── LifecycleStore: provider repository state ──────────────────────────

    #[test]
    fn provider_repository_state_returns_none_for_missing() {
        let store = make_store();
        let loaded = LifecycleStore::provider_repository_state(
            &store,
            RepositoryProvider::GitHub,
            "no-owner",
            "no-repo",
        )
        .expect("lookup should succeed");
        assert!(loaded.is_none());
    }

    #[test]
    fn provider_repository_state_upsert_and_read_roundtrip() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "assets".into(),
            Some(100),
            Some(200),
            Some("refs/heads/main".into()),
        );

        LifecycleStore::upsert_provider_repository_state(&store, &state)
            .expect("upsert should succeed");
        let loaded = LifecycleStore::provider_repository_state(
            &store,
            RepositoryProvider::GitHub,
            "team",
            "assets",
        )
        .expect("lookup should succeed");
        assert_eq!(loaded, Some(state));
    }

    #[test]
    fn provider_repository_state_legacy_missing_evidence_remains_readable() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "legacy-provider".into(),
            Some(100),
            None,
            None,
        );
        LifecycleStore::upsert_provider_repository_state(&store, &state).unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'ProviderEvent'
                   AND operation_id = 'github:team:legacy-provider'",
                [],
            )
            .unwrap();
        assert_eq!(
            LifecycleStore::provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "legacy-provider",
            )
            .unwrap(),
            Some(state)
        );
    }

    #[test]
    fn provider_repository_state_tampered_evidence_is_rejected_on_read() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "tampered-provider".into(),
            Some(100),
            None,
            None,
        );
        LifecycleStore::upsert_provider_repository_state(&store, &state).unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"sequence\": 99}'
                 WHERE operation_kind = 'ProviderEvent'
                   AND operation_id = 'github:team:tampered-provider'",
                [],
            )
            .unwrap();
        assert!(
            LifecycleStore::provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "tampered-provider",
            )
            .is_err()
        );
    }

    #[test]
    fn provider_repository_state_delete_rejects_tampered_evidence() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "tampered-delete".into(),
            Some(100),
            None,
            None,
        );
        LifecycleStore::upsert_provider_repository_state(&store, &state).unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"sequence\": 99}'
                 WHERE operation_kind = 'ProviderEvent'
                   AND operation_id = 'github:team:tampered-delete'",
                [],
            )
            .unwrap();
        assert!(
            LifecycleStore::delete_provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "tampered-delete",
            )
            .is_err()
        );
        assert!(
            LifecycleStore::provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "tampered-delete",
            )
            .is_err()
        );
    }

    #[test]
    fn provider_repository_state_upsert_merges_partial_and_stale_observations() {
        let store = make_store();
        let revision = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "merge".into(),
            None,
            Some(200),
            Some("new-revision".into()),
        )
        .with_reconciliation(None, Some(180), None);
        let independent = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "merge".into(),
            Some(150),
            None,
            None,
        )
        .with_reconciliation(Some(170), None, Some(190));
        let stale = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "merge".into(),
            Some(100),
            Some(120),
            Some("stale-revision".into()),
        )
        .with_reconciliation(Some(110), Some(130), Some(140));

        for state in [&revision, &independent, &stale] {
            LifecycleStore::upsert_provider_repository_state(&store, state).unwrap();
        }

        let loaded = LifecycleStore::provider_repository_state(
            &store,
            RepositoryProvider::GitHub,
            "team",
            "merge",
        )
        .unwrap()
        .expect("merged state");
        assert_eq!(loaded.last_access_changed_at_unix_seconds(), Some(150));
        assert_eq!(loaded.last_revision_pushed_at_unix_seconds(), Some(200));
        assert_eq!(loaded.last_pushed_revision(), Some("new-revision"));
        assert_eq!(loaded.last_cache_invalidated_at_unix_seconds(), Some(170));
        assert_eq!(
            loaded.last_authorization_rechecked_at_unix_seconds(),
            Some(180)
        );
        assert_eq!(loaded.last_drift_checked_at_unix_seconds(), Some(190));
    }

    #[test]
    fn provider_repository_state_list_includes_upserted() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "other".into(),
            Some(300),
            None,
            None,
        );

        LifecycleStore::upsert_provider_repository_state(&store, &state).unwrap();
        let states = LifecycleStore::list_provider_repository_states(&store).unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].repo(), "other");
    }

    #[test]
    fn provider_repository_state_delete_returns_true_then_false() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "del-repo".into(),
            None,
            None,
            None,
        );

        LifecycleStore::upsert_provider_repository_state(&store, &state).unwrap();
        assert!(
            LifecycleStore::delete_provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "del-repo",
            )
            .expect("delete should succeed")
        );
        assert!(
            !LifecycleStore::delete_provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "del-repo",
            )
            .expect("second delete should succeed")
        );
    }

    // ── LifecycleStore: visit methods ──────────────────────────────────────

    #[test]
    fn visit_quarantine_candidates_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut count = 0u32;
        LifecycleStore::visit_quarantine_candidates(&store, |_| {
            count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn visit_retention_holds_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut count = 0u32;
        LifecycleStore::visit_retention_holds(&store, |_| {
            count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn visit_webhook_deliveries_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut count = 0u32;
        LifecycleStore::visit_webhook_deliveries(&store, |_| {
            count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn visit_provider_repository_states_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut count = 0u32;
        LifecycleStore::visit_provider_repository_states(&store, |_| {
            count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn visit_dedupe_shard_mappings_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut count = 0u32;
        DedupeStore::visit_dedupe_shard_mappings(&store, |_| {
            count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    // ── UploadIntentStore: transition idempotency ─────────────────────────

    #[test]
    fn create_intent_rejects_id_reuse_for_different_object() {
        let store = make_store();
        let original = UploadIntent::new(
            "conflicting-intent".to_owned(),
            "objects/a".to_owned(),
            "hash-a".to_owned(),
            42,
        );
        let conflicting = UploadIntent::new(
            "conflicting-intent".to_owned(),
            "objects/b".to_owned(),
            "hash-b".to_owned(),
            43,
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(store.create_intent(&original)).unwrap();
        runtime.block_on(store.create_intent(&original)).unwrap();
        assert!(matches!(
            runtime.block_on(store.create_intent(&conflicting)),
            Err(LocalIndexStoreError::UploadIntentConflict(_))
        ));
    }

    #[test]
    fn scoped_intent_baseline_and_transition_share_repository_identity() {
        let store = make_store();
        let intent = UploadIntent::new(
            "scoped-intent".to_owned(),
            "objects/scoped".to_owned(),
            "a".repeat(64),
            42,
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(store.create_intent_scoped(&intent, "tenant-a", "repo-a"))
            .unwrap();
        runtime
            .block_on(store.transition_intent(intent.intent_id(), UploadIntentState::Storing))
            .unwrap();
        let events = runtime
            .block_on(store.reliability_events(intent.intent_id()))
            .unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|stored_event| {
            stored_event.operation.tenant == "tenant-a"
                && stored_event.operation.repository == "repo-a"
        }));
    }

    #[test]
    fn sqlite_reliability_writer_rejects_tampered_event_before_insert() {
        let store = make_store();
        let mut event = upload_lifecycle_event(
            "tenant-a",
            "repo-a",
            "writer-integrity",
            "objects/integrity",
            "a".repeat(64),
            shardline_reliability::UploadLifecycleState::Created,
            shardline_reliability::UploadLifecycleState::Created,
        )
        .unwrap();
        event.state_digest = shardline_reliability::canonical_state_digest(&"tampered").unwrap();

        let mut connection = store.open_connection().unwrap();
        let transaction = connection.transaction().unwrap();
        let result =
            crate::local_sqlite::helpers::persist_reliability_event_at(&transaction, &event, 0);
        assert!(result.is_err());
        assert_eq!(
            transaction
                .query_row(
                    "SELECT COUNT(*) FROM shardline_reliability_events
                     WHERE operation_id = ?1",
                    rusqlite::params!["writer-integrity"],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn sqlite_reliability_writer_rejects_conflicting_sequence_body() {
        use shardline_reliability::{LifecycleEvent, OperationIdentity, OperationKind};

        let store = make_store();
        let operation = OperationIdentity::new(
            "tenant-a",
            "repo-a",
            "writer-conflict",
            OperationKind::Upload,
        )
        .unwrap()
        .with_object_key("objects/first")
        .with_content_sha256("a".repeat(64));
        let first = LifecycleEvent::new(
            operation.clone(),
            0,
            shardline_reliability::UploadLifecycleState::Created,
            shardline_reliability::UploadLifecycleState::Created,
        )
        .unwrap();
        let conflicting = LifecycleEvent::new(
            operation.with_object_key("objects/second"),
            0,
            shardline_reliability::UploadLifecycleState::Created,
            shardline_reliability::UploadLifecycleState::Created,
        )
        .unwrap();

        let mut connection = store.open_connection().unwrap();
        let transaction = connection.transaction().unwrap();
        crate::local_sqlite::helpers::persist_reliability_event_at(&transaction, &first, 0)
            .unwrap();
        assert!(matches!(
            crate::local_sqlite::helpers::persist_reliability_event_at(
                &transaction,
                &conflicting,
                0,
            ),
            Err(LocalIndexStoreError::ReliabilityEventConflict(operation_id))
                if operation_id == "writer-conflict"
        ));
    }

    #[test]
    fn transition_intent_to_same_state_is_idempotent() {
        let store = make_store();
        let intent = UploadIntent::new(
            "same-state-intent".to_owned(),
            "objects/test".to_owned(),
            "abcdef".to_owned(),
            42,
        );
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(store.create_intent(&intent)).unwrap();

        // Advance Created -> Storing.
        let first = rt
            .block_on(store.transition_intent("same-state-intent", UploadIntentState::Storing))
            .unwrap();
        assert!(first, "initial Created -> Storing should succeed");
        // Repeating the same transition (a duplicate concurrent caller's view)
        // must be a no-op success, not a false/invalid transition.
        let second = rt
            .block_on(store.transition_intent("same-state-intent", UploadIntentState::Storing))
            .unwrap();
        assert!(second, "same-state transition must be idempotent");
    }

    #[test]
    fn upload_intent_read_rejects_missing_evidence_without_writing() {
        let store = make_store();
        let intent = UploadIntent::new(
            "repair-upload-evidence".to_owned(),
            "objects/repair-upload-evidence".to_owned(),
            "d".repeat(64),
            42,
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(store.create_intent(&intent)).unwrap();
        assert!(
            runtime
                .block_on(store.transition_intent(intent.intent_id(), UploadIntentState::Storing))
                .unwrap()
        );
        assert!(
            runtime
                .block_on(store.transition_intent(intent.intent_id(), UploadIntentState::Stored))
                .unwrap()
        );
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'Upload' AND operation_id = ?1",
                params![intent.intent_id()],
            )
            .unwrap();
        drop(connection);

        assert!(
            runtime
                .block_on(store.intent_by_id(intent.intent_id()))
                .is_err()
        );
        assert!(
            runtime
                .block_on(store.reliability_events(intent.intent_id()))
                .is_err()
        );
    }

    #[test]
    fn transition_with_reliability_event_is_atomic_and_verifiable() {
        use shardline_reliability::{
            LifecycleEvent, OperationIdentity, OperationKind, verify_lifecycle_chain,
        };

        let store = make_store();
        let intent = UploadIntent::new(
            "reliability-intent".to_owned(),
            "objects/reliability".to_owned(),
            "a".repeat(64),
            42,
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.create_intent(&intent)).unwrap();
        let event = LifecycleEvent::new(
            OperationIdentity::new(
                "shardline",
                "default",
                intent.intent_id(),
                OperationKind::Upload,
            )
            .unwrap()
            .with_object_key(intent.object_key())
            .with_content_sha256(intent.object_hash()),
            1,
            shardline_reliability::UploadLifecycleState::Created,
            shardline_reliability::UploadLifecycleState::Storing,
        )
        .unwrap();

        assert!(
            rt.block_on(store.transition_intent_with_event(
                intent.intent_id(),
                UploadIntentState::Storing,
                &event,
            ))
            .unwrap()
        );
        let events = rt
            .block_on(store.reliability_events(intent.intent_id()))
            .unwrap();
        verify_lifecycle_chain(&events).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events.last(), Some(&event));
        assert_eq!(
            rt.block_on(store.intent_by_id(intent.intent_id()))
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Storing
        );
    }

    #[test]
    fn conflicting_reliability_evidence_rolls_back_the_state_transition() {
        use shardline_reliability::{
            LifecycleEvent, OperationIdentity, OperationKind, UploadLifecycleState,
        };

        let store = make_store();
        let intent = UploadIntent::new(
            "reliability-conflict".to_owned(),
            "objects/reliability-conflict".to_owned(),
            "b".repeat(64),
            42,
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.create_intent(&intent)).unwrap();
        let operation = OperationIdentity::new(
            "shardline",
            "default",
            intent.intent_id(),
            OperationKind::Upload,
        )
        .unwrap()
        .with_object_key(intent.object_key())
        .with_content_sha256(intent.object_hash());
        let first = LifecycleEvent::new(
            operation.clone(),
            1,
            UploadLifecycleState::Created,
            UploadLifecycleState::Storing,
        )
        .unwrap();
        assert!(
            rt.block_on(store.transition_intent_with_event(
                intent.intent_id(),
                UploadIntentState::Storing,
                &first,
            ))
            .unwrap()
        );

        let conflicting = LifecycleEvent::new(
            operation.with_object_key("objects/different"),
            1,
            UploadLifecycleState::Storing,
            UploadLifecycleState::Stored,
        )
        .unwrap();
        assert!(matches!(
            rt.block_on(store.transition_intent_with_event(
                intent.intent_id(),
                UploadIntentState::Stored,
                &conflicting,
            )),
            Err(LocalIndexStoreError::ReliabilityEventConflict(_))
        ));
        assert_eq!(
            rt.block_on(store.intent_by_id(intent.intent_id()))
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Storing
        );
    }

    #[test]
    fn tampered_reliability_evidence_is_rejected_on_read() {
        use shardline_reliability::{
            LifecycleEvent, OperationIdentity, OperationKind, UploadLifecycleState,
        };

        let store = make_store();
        let intent = UploadIntent::new(
            "reliability-tamper".to_owned(),
            "objects/reliability-tamper".to_owned(),
            "c".repeat(64),
            42,
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.create_intent(&intent)).unwrap();
        let event = LifecycleEvent::new(
            OperationIdentity::new(
                "shardline",
                "default",
                intent.intent_id(),
                OperationKind::Upload,
            )
            .unwrap()
            .with_object_key(intent.object_key())
            .with_content_sha256(intent.object_hash()),
            1,
            UploadLifecycleState::Created,
            UploadLifecycleState::Storing,
        )
        .unwrap();
        assert!(
            rt.block_on(store.transition_intent_with_event(
                intent.intent_id(),
                UploadIntentState::Storing,
                &event,
            ))
            .unwrap()
        );

        let connection = store.open_connection().unwrap();
        let mut tampered: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&event).unwrap()).unwrap();
        tampered["after"] = serde_json::Value::String("Stored".to_owned());
        connection
            .execute(
                "UPDATE shardline_reliability_events SET event_json = ?1
                 WHERE operation_kind = 'Upload' AND operation_id = ?2 AND sequence = 1",
                rusqlite::params![tampered.to_string(), intent.intent_id()],
            )
            .unwrap();

        assert!(matches!(
            rt.block_on(store.reliability_events(intent.intent_id())),
            Err(LocalIndexStoreError::Reliability(_))
        ));
    }

    #[test]
    fn concurrent_same_intent_transitions_both_succeed() {
        use std::sync::Arc;

        let store = make_store();
        let intent = UploadIntent::new(
            "concurrent-intent".to_owned(),
            "objects/test".to_owned(),
            "abcdef".to_owned(),
            42,
        );
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(store.create_intent(&intent)).unwrap();

        // Two callers racing to move the same intent into Storing. Both must end
        // up observing success (a spurious InvalidUploadTransition is a bug).
        let store = Arc::new(store);
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let handle = rt.handle().clone();
        let mut handles = Vec::new();
        for _ in 0..2 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let handle = handle.clone();
            handles.push(std::thread::spawn(move || {
                handle.block_on(async {
                    barrier.wait().await;
                    store
                        .transition_intent("concurrent-intent", UploadIntentState::Storing)
                        .await
                        .unwrap()
                })
            }));
        }
        for h in handles {
            assert!(
                h.join().unwrap(),
                "a concurrent same-intent transition must not fail"
            );
        }
    }

    #[test]
    fn quarantine_candidate_tampered_evidence_is_rejected_on_read() {
        let store = make_store();
        let candidate = QuarantineCandidate::new(
            ObjectKey::parse("aa/quarantine-object").unwrap(),
            42,
            100,
            200,
        )
        .unwrap();
        LifecycleStore::upsert_quarantine_candidate(&store, &candidate).unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"sequence\": 99}'
                 WHERE operation_kind = 'GarbageCollection'
                   AND operation_id = 'aa/quarantine-object'",
                [],
            )
            .unwrap();
        assert!(LifecycleStore::quarantine_candidate(&store, candidate.object_key()).is_err());
    }

    #[test]
    fn retention_hold_tampered_evidence_is_rejected_on_read() {
        let store = make_store();
        let hold = RetentionHold::new(
            ObjectKey::parse("aa/retention-object").unwrap(),
            "legal hold".to_owned(),
            100,
            Some(200),
        )
        .unwrap();
        LifecycleStore::upsert_retention_hold(&store, &hold).unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"sequence\": 99}'
                 WHERE operation_kind = 'RetentionHold'
                   AND operation_id = 'aa/retention-object'",
                [],
            )
            .unwrap();
        assert!(LifecycleStore::retention_hold(&store, hold.object_key()).is_err());
    }

    #[test]
    fn quarantine_delete_repairs_missing_baseline_chain() {
        let store = make_store();
        let candidate = QuarantineCandidate::new(
            ObjectKey::parse("aa/quarantine-repair").unwrap(),
            42,
            100,
            200,
        )
        .unwrap();
        LifecycleStore::upsert_quarantine_candidate(&store, &candidate).unwrap();
        {
            let connection = store.open_connection().unwrap();
            connection
                .execute(
                    "DELETE FROM shardline_reliability_events
                     WHERE operation_kind = 'GarbageCollection'
                       AND operation_id = 'aa/quarantine-repair'",
                    [],
                )
                .unwrap();
        }

        assert!(
            LifecycleStore::delete_quarantine_candidate(&store, candidate.object_key()).unwrap()
        );
        let connection = store.open_connection().unwrap();
        let (count, minimum, maximum): (i64, i64, i64) = connection
            .query_row(
                "SELECT COUNT(*), MIN(sequence), MAX(sequence)
                 FROM shardline_reliability_events
                 WHERE operation_kind = 'GarbageCollection'
                   AND operation_id = 'aa/quarantine-repair'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!((count, minimum, maximum), (2, 0, 1));
    }
    #[test]
    fn streaming_inventory_orders_all_types_across_batches() {
        let store = make_store();
        for i in 0..INVENTORY_BATCH_SIZE + 5 {
            let key = ObjectKey::parse(&format!("inventory/{i:04}")).unwrap();
            let mut hash = [0u8; 32];
            hash[..8].copy_from_slice(&u64::try_from(i).unwrap().to_be_bytes());
            let hash = ShardlineHash::from_bytes(hash);
            store
                .upsert_dedupe_shard_mapping(&DedupeShardMapping::new(hash, key.clone()))
                .unwrap();
            store
                .insert_reconstruction(&FileId::new(hash), &FileReconstruction::new(vec![]))
                .unwrap();
            LifecycleStore::upsert_quarantine_candidate(
                &store,
                &QuarantineCandidate::new(key.clone(), 10, 1, 2).unwrap(),
            )
            .unwrap();
            LifecycleStore::upsert_retention_hold(
                &store,
                &RetentionHold::new(key, "keep".into(), 1, None).unwrap(),
            )
            .unwrap();
            LifecycleStore::record_webhook_delivery(
                &store,
                &WebhookDelivery::new(
                    RepositoryProvider::GitHub,
                    "owner".into(),
                    format!("repo/{i:04}"),
                    format!("delivery:{i:04}"),
                    1,
                )
                .unwrap(),
            )
            .unwrap();
            LifecycleStore::upsert_provider_repository_state(
                &store,
                &ProviderRepositoryState::new(
                    RepositoryProvider::GitHub,
                    "owner".into(),
                    format!("repo-{i:04}"),
                    Some(1),
                    None,
                    None,
                ),
            )
            .unwrap();
        }
        macro_rules! same_inventory {
            ($trait:ident, $visit:ident, $list:ident) => {{
                let expected = $trait::$list(&store).unwrap();
                let mut actual = Vec::new();
                $trait::$visit(&store, |row| {
                    actual.push(row);
                    Ok::<_, LocalIndexStoreError>(())
                })
                .unwrap();
                assert_eq!(actual, expected);
                assert_eq!(actual.len(), INVENTORY_BATCH_SIZE + 5);
            }};
        }
        same_inventory!(
            DedupeStore,
            visit_dedupe_shard_mappings,
            list_dedupe_shard_mappings
        );
        same_inventory!(
            ReconstructionStore,
            visit_reconstruction_file_ids,
            list_reconstruction_file_ids
        );
        same_inventory!(
            LifecycleStore,
            visit_quarantine_candidates,
            list_quarantine_candidates
        );
        same_inventory!(LifecycleStore, visit_retention_holds, list_retention_holds);
        same_inventory!(
            LifecycleStore,
            visit_webhook_deliveries,
            list_webhook_deliveries
        );
        same_inventory!(
            LifecycleStore,
            visit_provider_repository_states,
            list_provider_repository_states
        );
    }

    #[test]
    fn streaming_inventory_late_corruption_prevalidates_and_decode_precedes_evidence() {
        let store = make_store();
        for i in 0..INVENTORY_BATCH_SIZE + 1 {
            LifecycleStore::upsert_retention_hold(
                &store,
                &RetentionHold::new(
                    ObjectKey::parse(&format!("inventory/{i:04}")).unwrap(),
                    "keep".into(),
                    1,
                    None,
                )
                .unwrap(),
            )
            .unwrap();
        }
        let connection = store.open_connection().unwrap();
        connection.execute("UPDATE shardline_reliability_events SET merkle_commit_json = '{}' WHERE operation_kind = 'RetentionHold' AND operation_id = ?1", [format!("inventory/{INVENTORY_BATCH_SIZE:04}")]).unwrap();
        let mut calls = 0;
        let error = LifecycleStore::visit_retention_holds(&store, |_| {
            calls += 1;
            Ok::<_, LocalIndexStoreError>(())
        })
        .unwrap_err();
        assert!(matches!(error, LocalIndexStoreError::Reliability(_)));
        assert_eq!(calls, 0);
        // Malformed materialized bytes in the last batch must win over an
        // earlier bad evidence head, as the old eager decode did.
        connection.execute("UPDATE shardline_reliability_events SET merkle_commit_json = '{}' WHERE operation_kind = 'RetentionHold' AND operation_id = 'inventory/0000'", []).unwrap();
        let triggers = {
            let mut statement = connection.prepare("SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'shardline_retention_holds'").unwrap();
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        for trigger in triggers {
            connection
                .execute_batch(&format!(
                    "DROP TRIGGER \"{}\"",
                    trigger.replace('"', "\"\"")
                ))
                .unwrap();
        }
        connection.execute("UPDATE shardline_retention_holds SET reason = CAST(reason AS BLOB) WHERE object_key = ?1", [format!("inventory/{INVENTORY_BATCH_SIZE:04}")]).unwrap();
        let error = LifecycleStore::visit_retention_holds(&store, |_| {
            calls += 1;
            Ok::<_, LocalIndexStoreError>(())
        })
        .unwrap_err();
        assert!(matches!(error, LocalIndexStoreError::Sqlite(_)));
        assert_eq!(calls, 0);
    }

    #[test]
    fn streaming_inventory_cursor_order_and_late_invalid_id() {
        let store = make_store();
        let connection = store.open_connection().unwrap();
        for i in 0..INVENTORY_BATCH_SIZE + 1 {
            let key = format!("{i:064x}");
            connection
                .execute(
                    "INSERT INTO shardline_file_reconstructions(file_id, terms, updated_at_unix_seconds) VALUES (?1, '[]', 0)",
                    [&key],
                )
                .unwrap();
        }
        let expected = ReconstructionStore::list_reconstruction_file_ids(&store).unwrap();
        let mut actual = Vec::new();
        ReconstructionStore::visit_reconstruction_file_ids(&store, |id| {
            actual.push(id);
            Ok::<_, LocalIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(actual, expected);
        connection.execute("INSERT INTO shardline_file_reconstructions(file_id, terms, updated_at_unix_seconds) VALUES ('zz-invalid', '[]', 0)", []).unwrap();
        let mut calls = 0;
        assert!(
            ReconstructionStore::visit_reconstruction_file_ids(&store, |_| {
                calls += 1;
                Ok::<_, LocalIndexStoreError>(())
            })
            .is_err()
        );
        assert_eq!(calls, 0);
    }
    #[test]
    fn streaming_inventory_all_heads_precede_early_snapshot_mismatch() {
        let store = make_store();
        for i in 0..INVENTORY_BATCH_SIZE + 1 {
            LifecycleStore::upsert_retention_hold(
                &store,
                &RetentionHold::new(
                    ObjectKey::parse(&format!("priority/{i:04}")).unwrap(),
                    "keep".into(),
                    1,
                    None,
                )
                .unwrap(),
            )
            .unwrap();
        }
        let connection = store.open_connection().unwrap();
        let triggers = {
            let mut statement = connection.prepare("SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'shardline_retention_holds'").unwrap();
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        for trigger in triggers {
            connection
                .execute_batch(&format!(
                    "DROP TRIGGER \"{}\"",
                    trigger.replace('"', "\"\"")
                ))
                .unwrap();
        }
        connection.execute("UPDATE shardline_retention_holds SET reason = 'wrong-snapshot' WHERE object_key = 'priority/0000'", []).unwrap();
        assert!(
            LifecycleStore::retention_hold(&store, &ObjectKey::parse("priority/0000").unwrap())
                .is_err()
        );
        connection.execute("UPDATE shardline_reliability_events SET merkle_commit_json = '{}' WHERE operation_kind = 'RetentionHold' AND operation_id = ?1", [format!("priority/{INVENTORY_BATCH_SIZE:04}")]).unwrap();
        let old = LifecycleStore::list_retention_holds(&store).unwrap_err();
        let mut calls = 0;
        let new = LifecycleStore::visit_retention_holds(&store, |_| {
            calls += 1;
            Ok::<_, LocalIndexStoreError>(())
        })
        .unwrap_err();
        assert!(matches!(
            old,
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::Serialize(
                _
            ))
        ));
        assert!(matches!(
            new,
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::Serialize(
                _
            ))
        ));
        assert_eq!(calls, 0);
    }
    #[test]
    fn async_close_budget_retains_all64_permits_and_never_waits_for_nested_admission() {
        let (release, gate) = std::sync::mpsc::channel();
        let pool = std::sync::Arc::new(
            ClosePool::new_with_spawn(ASYNC_CURSOR_LIMIT, move |worker| {
                std::thread::Builder::new()
                    .spawn(move || {
                        gate.recv().unwrap();
                        worker();
                    })
                    .map(drop)
            })
            .unwrap(),
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            // Each already completed task result owns a managed connection.
            // Dropping those results on the executor only posts reserved jobs.
            let mut completed = Vec::new();
            for _ in 0..ASYNC_CURSOR_LIMIT {
                let reservation = pool.reserve().unwrap();
                completed.push(tokio::task::spawn_blocking(move || ReadConnection::new(rusqlite::Connection::open_in_memory().unwrap(), Some(reservation))).await.unwrap());
            }
            drop(completed);
            assert_eq!(pool.admission.available_permits(), 0);
            let start = std::time::Instant::now();
            assert!(matches!(pool.reserve(), Err(LocalIndexStoreError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock));
            assert!(start.elapsed() < std::time::Duration::from_secs(1));
            // Timer progresses while the close worker cannot run any job.
            tokio::time::timeout(std::time::Duration::from_secs(1), tokio::time::sleep(std::time::Duration::from_millis(5))).await.unwrap();
            assert_eq!(pool.admission.available_permits(), 0);
            release.send(()).unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while pool.admission.available_permits() != ASYNC_CURSOR_LIMIT { tokio::time::sleep(std::time::Duration::from_millis(1)).await; }
            }).await.unwrap();
            assert!(pool.reserve().is_ok());
        });
    }

    #[test]
    fn async_close_worker_spawn_failure_is_typed_and_faulted_worker_drains_admitted_jobs() {
        let error = ClosePool::new_with_spawn(2, |_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "test worker spawn failure",
            ))
        })
        .err()
        .unwrap();
        assert!(
            matches!(LocalIndexStoreError::from(error), LocalIndexStoreError::Io(error) if error.kind() == std::io::ErrorKind::PermissionDenied)
        );
        let pool = std::sync::Arc::new(
            ClosePool::new_with_spawn(2, |worker| {
                std::thread::Builder::new().spawn(worker).map(drop)
            })
            .unwrap(),
        );
        let first = pool.reserve().unwrap();
        let second = pool.reserve().unwrap();
        pool.enqueue(CloseJob {
            _connection: rusqlite::Connection::open_in_memory().unwrap(),
            _reservation: first,
            before_close: Some(Box::new(|| panic!("test close worker fault"))),
        });
        pool.enqueue(CloseJob {
            _connection: rusqlite::Connection::open_in_memory().unwrap(),
            _reservation: second,
            before_close: None,
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while pool.admission.available_permits() != 2 {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(
            matches!(pool.reserve(), Err(LocalIndexStoreError::Io(error)) if error.kind() == std::io::ErrorKind::BrokenPipe)
        );
        // A disconnected queue fails closed without creating rescue workers.
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        drop(receiver);
        let broken = std::sync::Arc::new(ClosePool {
            sender,
            admission: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            faulted: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        let reservation = broken.reserve().unwrap();
        drop(ReadConnection::new(
            rusqlite::Connection::open_in_memory().unwrap(),
            Some(reservation),
        ));
        assert_eq!(broken.admission.available_permits(), 1);
        assert!(
            matches!(broken.reserve(), Err(LocalIndexStoreError::Io(error)) if error.kind() == std::io::ErrorKind::BrokenPipe)
        );
    }
}
