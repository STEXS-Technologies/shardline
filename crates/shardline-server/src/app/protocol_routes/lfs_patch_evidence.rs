use std::fs;
use std::io::{Error, ErrorKind, Write};
use std::path::{Component, Path, PathBuf};

use serde::{Serialize, de::DeserializeOwned};
use shardline_reliability::{
    DigestSnapshot, ResumableLifecycleState, ResumableSessionSnapshotDomain, SessionEvidenceLog,
    SnapshotEvidenceLog, append_or_baseline_snapshot_evidence,
    build_persisted_merkle_chain_with_previous, build_typed_merkle_chain, canonical_state_digest,
    persisted_event_sequence, resumable_session_snapshot_identity,
    verify_and_append_session_transition, verify_persisted_merkle_chain, verify_typed_merkle_chain,
};

use crate::ServerError;

/// The evidence sidecar is additive: historical LFS patch sessions without it
/// are reconstructed in memory and persisted by the next successful mutation.
const EVIDENCE_SUFFIX: &str = ".evidence";
const SNAPSHOT_SUFFIX: &str = ".snapshot";
const EVIDENCE_JOURNAL_SCHEMA: &str = "shardline.lfs.evidence-journal.v1";
const SNAPSHOT_JOURNAL_SCHEMA: &str = "shardline.lfs.snapshot-journal.v1";
const MERKLE_SUFFIX: &str = ".merkle";
const MERKLE_JOURNAL_SCHEMA: &str = "shardline.lfs.merkle-journal.v1";

struct LfsMerkleAppend {
    previous_head: u64,
    record: shardline_reliability::PersistedMerkleJournalRecord,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
struct JournalManifest {
    schema: String,
    head: u64,
    #[serde(default)]
    bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct LfsPatchMaterializedStateV1 {
    oid: String,
    scope_namespace: String,
    session_id: String,
    target_key: String,
    total_bytes: u64,
    ranges: Vec<(u64, u64)>,
    staging_length: u64,
    last_touched_unix_seconds: u64,
}

/// Operator-supplied identity and materialized-state facts used to rebuild a
/// corrupted LFS patch reliability envelope. The data-plane files remain the
/// authority; this value only binds the repaired evidence to the operator's
/// verified identity and reconstruction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct LfsPatchEvidenceRepairInput {
    pub oid: String,
    pub scope_namespace: String,
    pub session_id: String,
    pub target_key: String,
    pub total_bytes: u64,
    pub ranges: Vec<(u64, u64)>,
    pub staging_length: u64,
    pub last_touched_unix_seconds: u64,
}

pub(super) struct LfsPatchSnapshotInput<'input> {
    pub(super) oid: &'input str,
    pub(super) scope_namespace: &'input str,
    pub(super) session_id: &'input str,
    pub(super) target_key: &'input str,
    pub(super) total_bytes: u64,
    pub(super) ranges: &'input [(u64, u64)],
    pub(super) staging_length: u64,
    pub(super) last_touched_unix_seconds: u64,
}

pub(super) fn evidence_path(dir: &Path, oid: &str) -> PathBuf {
    dir.join(format!("{oid}{EVIDENCE_SUFFIX}"))
}

fn snapshot_path(dir: &Path, oid: &str) -> PathBuf {
    dir.join(format!("{oid}{SNAPSHOT_SUFFIX}"))
}

fn evidence_journal_path(dir: &Path, oid: &str) -> PathBuf {
    dir.join(format!("{oid}{EVIDENCE_SUFFIX}.log"))
}

fn snapshot_journal_path(dir: &Path, oid: &str) -> PathBuf {
    dir.join(format!("{oid}{SNAPSHOT_SUFFIX}.log"))
}

fn merkle_path(dir: &Path, oid: &str) -> PathBuf {
    dir.join(format!("{oid}{MERKLE_SUFFIX}"))
}

fn merkle_journal_path(dir: &Path, oid: &str) -> PathBuf {
    dir.join(format!("{oid}{MERKLE_SUFFIX}.log"))
}

fn journal_head(path: &Path, schema: &str) -> Result<Option<u64>, ServerError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let manifest: JournalManifest = serde_json::from_slice(&bytes).map_err(invalid_evidence)?;
    if manifest.schema != schema {
        return Err(invalid_evidence(format!(
            "unsupported LFS evidence journal schema: {}",
            manifest.schema
        )));
    }
    Ok(Some(manifest.head))
}

fn load_merkle_journal(
    dir: &Path,
    oid: &str,
) -> Result<Option<Vec<shardline_reliability::PersistedMerkleJournalRecord>>, ServerError> {
    let Some(head) = journal_head(&merkle_path(dir, oid), MERKLE_JOURNAL_SCHEMA)? else {
        return Ok(None);
    };
    let bytes = fs::read(merkle_journal_path(dir, oid))?;
    let mut records = bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(serde_json::from_slice)
        .collect::<Result<Vec<shardline_reliability::PersistedMerkleJournalRecord>, _>>()
        .map_err(invalid_evidence)?;
    let count =
        usize::try_from(head).map_err(|_| invalid_evidence("LFS Merkle journal head overflow"))?;
    if count > records.len() {
        return Err(invalid_evidence("LFS Merkle journal head exceeds its log"));
    }
    records.truncate(count);
    Ok(Some(records))
}

fn build_lfs_merkle_append(
    dir: &Path,
    oid: &str,
    evidence_events: &[shardline_reliability::StateTransitionEvent],
    snapshot_events: &[shardline_reliability::SnapshotEvidenceEvent<DigestSnapshot>],
) -> Result<Option<LfsMerkleAppend>, ServerError> {
    let records = load_merkle_journal(dir, oid)?.unwrap_or_default();
    let existing_evidence = records
        .iter()
        .flat_map(|record| record.evidence.iter().cloned())
        .collect::<Vec<_>>();
    let existing_evidence_commits = records
        .iter()
        .flat_map(|record| record.merkle_commits.iter().cloned())
        .collect::<Vec<_>>();
    let existing_snapshots = records
        .iter()
        .flat_map(|record| record.snapshot_evidence.iter().cloned())
        .collect::<Vec<_>>();
    let existing_snapshot_commits = records
        .iter()
        .flat_map(|record| record.snapshot_merkle_commits.iter().cloned())
        .collect::<Vec<_>>();
    if !existing_evidence.is_empty() || !existing_evidence_commits.is_empty() {
        verify_persisted_merkle_chain(
            shardline_reliability::OperationKind::ResumableSession,
            &existing_evidence,
            &existing_evidence_commits,
        )
        .map_err(invalid_evidence)?;
    }
    if !existing_snapshots.is_empty() || !existing_snapshot_commits.is_empty() {
        verify_typed_merkle_chain::<shardline_reliability::SnapshotEvidenceEvent<DigestSnapshot>>(
            &existing_snapshots,
            &existing_snapshot_commits,
        )
        .map_err(invalid_evidence)?;
    }
    let previous_evidence = existing_evidence_commits.last().cloned();
    let previous_snapshot = existing_snapshot_commits.last().cloned();
    let last_evidence_sequence = existing_evidence
        .last()
        .map(|event| {
            persisted_event_sequence(
                shardline_reliability::OperationKind::ResumableSession,
                event.clone(),
            )
        })
        .transpose()
        .map_err(invalid_evidence)?;
    let last_snapshot_sequence = existing_snapshots
        .last()
        .map(|event| {
            serde_json::from_value::<shardline_reliability::SnapshotEvidenceEvent<DigestSnapshot>>(
                event.clone(),
            )
            .map(|value| value.sequence)
        })
        .transpose()
        .map_err(invalid_evidence)?;
    let new_evidence = evidence_events
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|event| {
            last_evidence_sequence.is_none_or(|sequence| {
                persisted_event_sequence(
                    shardline_reliability::OperationKind::ResumableSession,
                    event.clone(),
                )
                .is_ok_and(|value| value > sequence)
            })
        })
        .collect::<Vec<_>>();
    let new_snapshots = snapshot_events
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|event| {
            last_snapshot_sequence.is_none_or(|sequence| {
                serde_json::from_value::<
                    shardline_reliability::SnapshotEvidenceEvent<DigestSnapshot>,
                >(event.clone())
                .is_ok_and(|value| value.sequence > sequence)
            })
        })
        .collect::<Vec<_>>();
    let merkle_commits = build_persisted_merkle_chain_with_previous(
        shardline_reliability::OperationKind::ResumableSession,
        &new_evidence,
        previous_evidence.as_ref(),
    )
    .map_err(invalid_evidence)?;
    let snapshot_merkle_commits = build_typed_merkle_chain::<
        shardline_reliability::SnapshotEvidenceEvent<DigestSnapshot>,
    >(&new_snapshots, previous_snapshot.as_ref())
    .map_err(invalid_evidence)?;
    if merkle_commits.is_empty() && snapshot_merkle_commits.is_empty() {
        return Ok(None);
    }
    Ok(Some(LfsMerkleAppend {
        previous_head: u64::try_from(records.len()).map_err(invalid_evidence)?,
        record: shardline_reliability::PersistedMerkleJournalRecord {
            evidence: new_evidence,
            merkle_commits,
            snapshot_evidence: new_snapshots,
            snapshot_merkle_commits,
        },
    }))
}

fn verify_lfs_merkle_journal(
    dir: &Path,
    oid: &str,
    evidence: &SessionEvidenceLog,
    snapshots: &SnapshotEvidenceLog<DigestSnapshot>,
) -> Result<(), ServerError> {
    let Some(records) = load_merkle_journal(dir, oid)? else {
        return Ok(());
    };
    let merkle_evidence = records
        .iter()
        .flat_map(|record| record.evidence.iter().cloned())
        .collect::<Vec<_>>();
    let merkle_commits = records
        .iter()
        .flat_map(|record| record.merkle_commits.iter().cloned())
        .collect::<Vec<_>>();
    let merkle_snapshots = records
        .iter()
        .flat_map(|record| record.snapshot_evidence.iter().cloned())
        .collect::<Vec<_>>();
    let merkle_snapshot_commits = records
        .iter()
        .flat_map(|record| record.snapshot_merkle_commits.iter().cloned())
        .collect::<Vec<_>>();
    let evidence_json = evidence
        .events()
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?;
    let snapshot_json = snapshots
        .events()
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?;
    if merkle_evidence != evidence_json || merkle_snapshots != snapshot_json {
        return Err(invalid_evidence(
            "LFS Merkle journal does not cover evidence journal",
        ));
    }
    verify_persisted_merkle_chain(
        shardline_reliability::OperationKind::ResumableSession,
        &merkle_evidence,
        &merkle_commits,
    )
    .map_err(invalid_evidence)?;
    verify_typed_merkle_chain::<shardline_reliability::SnapshotEvidenceEvent<DigestSnapshot>>(
        &merkle_snapshots,
        &merkle_snapshot_commits,
    )
    .map_err(invalid_evidence)
}

fn read_journal<T: DeserializeOwned>(
    manifest_path: &Path,
    journal_path: &Path,
    schema: &str,
) -> Result<Option<Vec<T>>, ServerError> {
    let Some(head) = journal_head(manifest_path, schema)? else {
        return Ok(None);
    };
    let bytes = fs::read(journal_path).map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            invalid_evidence("LFS evidence journal is missing its committed log")
        } else {
            ServerError::from(error)
        }
    })?;
    let mut events = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let mut record: Vec<T> = serde_json::from_slice(line).map_err(invalid_evidence)?;
        events.append(&mut record);
    }
    let count = usize::try_from(head).map_err(|_| invalid_evidence("LFS journal head overflow"))?;
    if count > events.len() {
        return Err(invalid_evidence(
            "LFS evidence journal head exceeds its log",
        ));
    }
    events.truncate(count);
    Ok(Some(events))
}

fn prepare_journal_append(
    manifest_path: &Path,
    journal_path: &Path,
    schema: &str,
) -> Result<(), ServerError> {
    let Some(head) = journal_head(manifest_path, schema)? else {
        if journal_path.exists() && fs::metadata(journal_path)?.len() != 0 {
            return Err(invalid_evidence(
                "LFS journal is missing its committed head",
            ));
        }
        return Ok(());
    };
    let manifest_bytes = fs::read(manifest_path).map_err(ServerError::from)?;
    let manifest: JournalManifest =
        serde_json::from_slice(&manifest_bytes).map_err(invalid_evidence)?;
    let bytes = match fs::read(journal_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(invalid_evidence("LFS journal is missing its committed log"));
        }
        Err(error) => return Err(error.into()),
    };
    if let Some(expected_bytes) = manifest.bytes {
        let actual_bytes = u64::try_from(bytes.len())
            .map_err(|_| invalid_evidence("LFS journal byte length overflow"))?;
        if actual_bytes < expected_bytes {
            return Err(invalid_evidence(
                "LFS journal is shorter than its committed byte length",
            ));
        }
        if actual_bytes > expected_bytes {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(journal_path)?;
            file.write_all(
                &bytes[..usize::try_from(expected_bytes)
                    .map_err(|_| invalid_evidence("LFS journal byte length overflow"))?],
            )?;
            file.sync_all()?;
        }
        return Ok(());
    }
    let mut records = 0_u64;
    let mut committed_bytes = 0_usize;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if !line.is_empty() && line.iter().any(|byte| !byte.is_ascii_whitespace()) {
            let units = if schema == MERKLE_JOURNAL_SCHEMA {
                1
            } else {
                let value: serde_json::Value =
                    serde_json::from_slice(line).map_err(invalid_evidence)?;
                let array = value
                    .as_array()
                    .ok_or_else(|| invalid_evidence("LFS journal record is not an array"))?;
                u64::try_from(array.len())
                    .map_err(|_| invalid_evidence("LFS journal event count overflow"))?
            };
            records = records
                .checked_add(units)
                .ok_or_else(|| invalid_evidence("LFS journal record count overflow"))?;
            if records <= head {
                committed_bytes = committed_bytes
                    .checked_add(line.len())
                    .ok_or_else(|| invalid_evidence("LFS journal byte count overflow"))?;
            } else if records - units < head {
                return Err(invalid_evidence(
                    "LFS journal head splits a persisted record",
                ));
            }
        }
    }
    if records < head {
        return Err(invalid_evidence("LFS journal head exceeds its log"));
    }
    if records > head {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(journal_path)?;
        file.write_all(&bytes[..committed_bytes])?;
        file.sync_all()?;
    }
    Ok(())
}

fn append_journal<T: Serialize>(
    dir: &Path,
    oid: &str,
    manifest_path: &Path,
    journal_path: &Path,
    schema: &str,
    previous_head: u64,
    events: &[T],
    merkle: Option<&LfsMerkleAppend>,
) -> Result<(), ServerError> {
    if events.is_empty() {
        return Ok(());
    }
    prepare_journal_append(manifest_path, journal_path, schema)?;
    let mut bytes = serde_json::to_vec(events).map_err(invalid_evidence)?;
    bytes.push(b'\n');
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(journal_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    let journal_bytes = u64::try_from(fs::metadata(journal_path)?.len())
        .map_err(|_| invalid_evidence("LFS journal byte length overflow"))?;
    if let Some(merkle) = merkle {
        prepare_journal_append(
            &merkle_path(dir, oid),
            &merkle_journal_path(dir, oid),
            MERKLE_JOURNAL_SCHEMA,
        )?;
        let mut merkle_bytes = serde_json::to_vec(&merkle.record).map_err(invalid_evidence)?;
        merkle_bytes.push(b'\n');
        let mut merkle_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(merkle_journal_path(dir, oid))?;
        merkle_file.write_all(&merkle_bytes)?;
        merkle_file.sync_all()?;
        let merkle_manifest = JournalManifest {
            schema: MERKLE_JOURNAL_SCHEMA.to_owned(),
            head: merkle
                .previous_head
                .checked_add(1)
                .ok_or_else(|| invalid_evidence("LFS Merkle journal head overflow"))?,
            bytes: Some(
                u64::try_from(fs::metadata(merkle_journal_path(dir, oid))?.len())
                    .map_err(|_| invalid_evidence("LFS Merkle journal byte length overflow"))?,
            ),
        };
        let merkle_manifest_bytes =
            serde_json::to_vec(&merkle_manifest).map_err(invalid_evidence)?;
        write_sidecar_atomically(dir, &merkle_path(dir, oid), &merkle_manifest_bytes)?;
    }
    let manifest = JournalManifest {
        schema: schema.to_owned(),
        head: previous_head
            .checked_add(
                u64::try_from(events.len())
                    .map_err(|_| invalid_evidence("LFS evidence journal event count overflow"))?,
            )
            .ok_or_else(|| invalid_evidence("LFS evidence journal head overflow"))?,
        bytes: Some(journal_bytes),
    };
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(invalid_evidence)?;
    write_sidecar_atomically(dir, manifest_path, &manifest_bytes)
}

fn materialized_snapshot(input: &LfsPatchSnapshotInput<'_>) -> Result<DigestSnapshot, ServerError> {
    let operation = resumable_session_snapshot_identity(
        ResumableSessionSnapshotDomain::LfsPatch,
        input.scope_namespace.to_owned(),
        input.session_id.to_owned(),
        input.target_key.to_owned(),
    )
    .map_err(invalid_evidence)?;
    let state = LfsPatchMaterializedStateV1 {
        oid: input.oid.to_owned(),
        scope_namespace: input.scope_namespace.to_owned(),
        session_id: input.session_id.to_owned(),
        target_key: input.target_key.to_owned(),
        total_bytes: input.total_bytes,
        ranges: input.ranges.to_vec(),
        staging_length: input.staging_length,
        last_touched_unix_seconds: input.last_touched_unix_seconds,
    };
    let digest = canonical_state_digest(&state).map_err(invalid_evidence)?;
    Ok(DigestSnapshot::new(operation, digest))
}

pub(super) fn record_snapshot(
    dir: &Path,
    input: &LfsPatchSnapshotInput<'_>,
) -> Result<(), ServerError> {
    let snapshot = materialized_snapshot(input)?;
    let path = snapshot_path(dir, input.oid);
    let head = journal_head(&path, SNAPSHOT_JOURNAL_SCHEMA)?;
    let previous = load_snapshot_log(dir, input.oid)?.unwrap_or_default();
    let previous_len = previous.events().len();
    let mut log = previous;
    log = append_or_baseline_snapshot_evidence(log, snapshot).map_err(invalid_evidence)?;
    let events = if head.is_some() {
        &log.events()[previous_len..]
    } else {
        log.events()
    };
    let merkle = build_lfs_merkle_append(dir, input.oid, &[], log.events())?;
    append_journal(
        dir,
        input.oid,
        &path,
        &snapshot_journal_path(dir, input.oid),
        SNAPSHOT_JOURNAL_SCHEMA,
        head.unwrap_or(0),
        events,
        merkle.as_ref(),
    )
}

/// Verifies the latest persisted materialized snapshot against the state the
/// caller reconstructed from disk. A missing snapshot is valid for legacy
/// sessions and will be recreated by the next successful mutation.
pub(super) fn verify_snapshot(
    dir: &Path,
    input: &LfsPatchSnapshotInput<'_>,
) -> Result<(), ServerError> {
    let Some(log) = load_snapshot_log(dir, input.oid)? else {
        return Ok(());
    };
    let expected = materialized_snapshot(input)?;
    log.verify_for(&expected).map_err(invalid_evidence)
}

fn load_snapshot_log(
    dir: &Path,
    oid: &str,
) -> Result<Option<SnapshotEvidenceLog<DigestSnapshot>>, ServerError> {
    if let Some(events) =
        read_journal::<shardline_reliability::SnapshotEvidenceEvent<DigestSnapshot>>(
            &snapshot_path(dir, oid),
            &snapshot_journal_path(dir, oid),
            SNAPSHOT_JOURNAL_SCHEMA,
        )?
    {
        return SnapshotEvidenceLog::from_events(events)
            .map(Some)
            .map_err(invalid_evidence);
    }
    let bytes = match fs::read(snapshot_path(dir, oid)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let events = serde_json::from_slice(&bytes).map_err(invalid_evidence)?;
    SnapshotEvidenceLog::from_events(events)
        .map(Some)
        .map_err(invalid_evidence)
}

pub(super) fn load(
    dir: &Path,
    oid: &str,
    scope_namespace: &str,
    session_id: &str,
    target_key: &str,
) -> Result<SessionEvidenceLog, ServerError> {
    let log = if let Some(events) = read_journal::<shardline_reliability::StateTransitionEvent>(
        &evidence_path(dir, oid),
        &evidence_journal_path(dir, oid),
        EVIDENCE_JOURNAL_SCHEMA,
    )? {
        SessionEvidenceLog::from_events(events).map_err(invalid_evidence)?
    } else {
        match fs::read(evidence_path(dir, oid)) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(invalid_evidence)?,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                SessionEvidenceLog::for_legacy_session(scope_namespace, session_id, target_key)
                    .map_err(invalid_evidence)?
            }
            Err(error) => return Err(error.into()),
        }
    };
    log.verify_for(scope_namespace, session_id, target_key)
        .map_err(invalid_evidence)?;
    verify_lfs_merkle_journal(
        dir,
        oid,
        &log,
        &load_snapshot_log(dir, oid)?.unwrap_or_default(),
    )?;
    Ok(log)
}

pub(super) fn record(
    dir: &Path,
    oid: &str,
    scope_namespace: &str,
    session_id: &str,
    target_key: &str,
    before: ResumableLifecycleState,
    after: ResumableLifecycleState,
) -> Result<(), ServerError> {
    let head = journal_head(&evidence_path(dir, oid), EVIDENCE_JOURNAL_SCHEMA)?;
    let log = load(dir, oid, scope_namespace, session_id, target_key)?;
    let previous_len = log.events().len();
    let (log, _) = verify_and_append_session_transition(
        log,
        scope_namespace,
        session_id,
        target_key,
        before,
        after,
    )
    .map_err(invalid_evidence)?;
    let path = evidence_path(dir, oid);
    let events = if head.is_some() {
        &log.events()[previous_len..]
    } else {
        log.events()
    };
    let merkle = build_lfs_merkle_append(dir, oid, log.events(), &[])?;
    append_journal(
        dir,
        oid,
        &path,
        &evidence_journal_path(dir, oid),
        EVIDENCE_JOURNAL_SCHEMA,
        head.unwrap_or(0),
        events,
        merkle.as_ref(),
    )
}

/// Explicitly rebuilds LFS reliability sidecars from operator-verified
/// materialized state. This is deliberately not called by any read or repair
/// sweep: replacing a broken evidence chain is a privileged recovery action.
pub fn repair_lfs_patch_evidence(
    dir: &Path,
    input: &LfsPatchEvidenceRepairInput,
) -> Result<(), ServerError> {
    let oid_path = Path::new(&input.oid);
    if input.oid.is_empty()
        || oid_path.components().count() != 1
        || !matches!(oid_path.components().next(), Some(Component::Normal(_)))
    {
        return Err(invalid_evidence("invalid LFS patch object id"));
    }
    let staging_path = dir.join(&input.oid);
    let staging_length = fs::metadata(&staging_path)
        .map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                invalid_evidence("LFS patch staging file is missing")
            } else {
                ServerError::from(error)
            }
        })?
        .len();
    if staging_length != input.staging_length {
        return Err(invalid_evidence(
            "operator staging length does not match materialized state",
        ));
    }
    let authoritative_ranges = read_repair_ranges(dir, &input.oid, input.total_bytes)?;
    if authoritative_ranges != input.ranges {
        return Err(invalid_evidence(
            "operator LFS patch ranges do not match the materialized range state",
        ));
    }
    let metadata_bytes = fs::read(lfs_meta_path(dir, &input.oid))?;
    let authoritative_touched = std::str::from_utf8(&metadata_bytes)
        .map_err(invalid_evidence)?
        .trim()
        .parse::<u64>()
        .map_err(invalid_evidence)?;
    if authoritative_touched != input.last_touched_unix_seconds {
        return Err(invalid_evidence(
            "operator LFS patch timestamp does not match materialized metadata",
        ));
    }
    let mut previous_end = 0_u64;
    for &(start, end) in &input.ranges {
        if start >= end || end > input.total_bytes || start < previous_end {
            return Err(invalid_evidence("operator LFS patch ranges are invalid"));
        }
        previous_end = end;
    }
    let snapshot = materialized_snapshot(&LfsPatchSnapshotInput {
        oid: &input.oid,
        scope_namespace: &input.scope_namespace,
        session_id: &input.session_id,
        target_key: &input.target_key,
        total_bytes: input.total_bytes,
        ranges: &input.ranges,
        staging_length: input.staging_length,
        last_touched_unix_seconds: input.last_touched_unix_seconds,
    })?;
    remove(dir, &input.oid);
    let lifecycle = SessionEvidenceLog::for_legacy_session(
        &input.scope_namespace,
        &input.session_id,
        &input.target_key,
    )
    .map_err(invalid_evidence)?;
    let lifecycle_merkle = build_lfs_merkle_append(dir, &input.oid, lifecycle.events(), &[])?;
    append_journal(
        dir,
        &input.oid,
        &evidence_path(dir, &input.oid),
        &evidence_journal_path(dir, &input.oid),
        EVIDENCE_JOURNAL_SCHEMA,
        0,
        lifecycle.events(),
        lifecycle_merkle.as_ref(),
    )?;
    let snapshot_log = SnapshotEvidenceLog::baseline(snapshot).map_err(invalid_evidence)?;
    let snapshot_merkle = build_lfs_merkle_append(dir, &input.oid, &[], snapshot_log.events())?;
    append_journal(
        dir,
        &input.oid,
        &snapshot_path(dir, &input.oid),
        &snapshot_journal_path(dir, &input.oid),
        SNAPSHOT_JOURNAL_SCHEMA,
        0,
        snapshot_log.events(),
        snapshot_merkle.as_ref(),
    )
}

fn lfs_meta_path(dir: &Path, oid: &str) -> PathBuf {
    dir.join(format!("{oid}.meta"))
}

fn read_repair_ranges(
    dir: &Path,
    oid: &str,
    total_bytes: u64,
) -> Result<Vec<(u64, u64)>, ServerError> {
    let bytes = fs::read_to_string(dir.join(format!("{oid}.ranges")))?;
    let mut lines = bytes.lines();
    let stored_total = lines
        .next()
        .ok_or_else(|| invalid_evidence("LFS patch range metadata is empty"))?
        .parse::<u64>()
        .map_err(invalid_evidence)?;
    if stored_total != total_bytes {
        return Err(invalid_evidence(
            "LFS patch range total does not match repair state",
        ));
    }
    let mut ranges = Vec::new();
    for line in lines {
        let line = line.strip_prefix('+').unwrap_or(line);
        let (start, end) = line
            .split_once(' ')
            .ok_or_else(|| invalid_evidence("invalid LFS patch range entry"))?;
        let start = start.parse::<u64>().map_err(invalid_evidence)?;
        let end = end.parse::<u64>().map_err(invalid_evidence)?;
        if start >= end || end > total_bytes {
            return Err(invalid_evidence("invalid LFS patch range bounds"));
        }
        ranges.push((start, end));
    }
    ranges.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        if let Some(last) = merged.last_mut()
            && start <= last.1
        {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }
    Ok(merged)
}

/// Commits one evidence sidecar atomically and removes the staging file on
/// every failed write or rename path. This keeps a failed local repair from
/// leaving an ambiguous second state artifact beside the verified sidecar.
fn write_sidecar_atomically(dir: &Path, path: &Path, bytes: &[u8]) -> Result<(), ServerError> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("sidecar");
    let temporary = path.with_extension(format!("{extension}.tmp"));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)?;
        sync_directory(dir)?;
        Ok(())
    })();
    if result.is_err() {
        let _ignored = fs::remove_file(&temporary);
    }
    result
}

pub(super) fn transition(
    dir: &Path,
    oid: &str,
    scope_namespace: &str,
    session_id: &str,
    target_key: &str,
    before: ResumableLifecycleState,
    after: ResumableLifecycleState,
) -> Result<(), ServerError> {
    let log = load(dir, oid, scope_namespace, session_id, target_key)?;
    let current = log
        .events()
        .last()
        .map_or(ResumableLifecycleState::Active, |event| event.after);
    if current == after {
        return Ok(());
    }
    if current != before {
        return Err(invalid_evidence(format!(
            "unexpected LFS patch state: expected {}, found {}",
            before.as_str(),
            current.as_str()
        )));
    }
    record(
        dir,
        oid,
        scope_namespace,
        session_id,
        target_key,
        before,
        after,
    )
}

pub(super) fn complete(
    dir: &Path,
    oid: &str,
    scope_namespace: &str,
    session_id: &str,
    target_key: &str,
) -> Result<(), ServerError> {
    let log = load(dir, oid, scope_namespace, session_id, target_key)?;
    let current = log
        .events()
        .last()
        .map_or(ResumableLifecycleState::Active, |event| event.after);
    if current == ResumableLifecycleState::Completed {
        return Ok(());
    }
    if current == ResumableLifecycleState::Active {
        record(
            dir,
            oid,
            scope_namespace,
            session_id,
            target_key,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Completing,
        )?;
    } else if current != ResumableLifecycleState::Completing {
        return Err(invalid_evidence(format!(
            "cannot complete LFS patch from {}",
            current.as_str()
        )));
    }
    record(
        dir,
        oid,
        scope_namespace,
        session_id,
        target_key,
        ResumableLifecycleState::Completing,
        ResumableLifecycleState::Completed,
    )
}

pub(super) fn verify_integrity(dir: &Path, oid: &str) -> Result<(), ServerError> {
    let evidence = if let Some(events) = read_journal::<shardline_reliability::StateTransitionEvent>(
        &evidence_path(dir, oid),
        &evidence_journal_path(dir, oid),
        EVIDENCE_JOURNAL_SCHEMA,
    )? {
        SessionEvidenceLog::from_events(events).map_err(invalid_evidence)?
    } else {
        match fs::read(evidence_path(dir, oid)) {
            Ok(bytes) => {
                let events = serde_json::from_slice(&bytes).map_err(invalid_evidence)?;
                SessionEvidenceLog::from_events(events).map_err(invalid_evidence)?
            }
            Err(error) if error.kind() == ErrorKind::NotFound => SessionEvidenceLog::default(),
            Err(error) => return Err(error.into()),
        }
    };
    let snapshots = load_snapshot_log(dir, oid)?.unwrap_or_default();
    verify_lfs_merkle_journal(dir, oid, &evidence, &snapshots)?;
    Ok(())
}

pub(super) fn remove(dir: &Path, oid: &str) {
    drop(fs::remove_file(evidence_path(dir, oid)));
    drop(fs::remove_file(evidence_journal_path(dir, oid)));
    drop(fs::remove_file(snapshot_path(dir, oid)));
    drop(fs::remove_file(snapshot_journal_path(dir, oid)));
    drop(fs::remove_file(merkle_path(dir, oid)));
    drop(fs::remove_file(merkle_journal_path(dir, oid)));
}

fn invalid_evidence(error: impl std::fmt::Display) -> ServerError {
    Error::new(
        ErrorKind::InvalidData,
        format!("invalid LFS patch evidence: {error}"),
    )
    .into()
}

#[cfg(unix)]
fn sync_directory(dir: &Path) -> Result<(), ServerError> {
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_dir: &Path) -> Result<(), ServerError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCOPE: &str = "scope";
    const SESSION: &str = "session";
    const TARGET: &str = "objects/target";
    const OID: &str = "a";

    #[test]
    fn legacy_session_gets_canonical_baseline_and_can_advance() {
        let directory = tempfile::tempdir().expect("tempdir");
        let log = load(directory.path(), OID, SCOPE, SESSION, TARGET).expect("legacy baseline");
        assert_eq!(
            log.events().last().expect("baseline event").after,
            ResumableLifecycleState::Active
        );
        assert!(!evidence_path(directory.path(), OID).exists());

        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("record baseline-compatible mutation");
        assert!(evidence_path(directory.path(), OID).is_file());
        let stored = load(directory.path(), OID, SCOPE, SESSION, TARGET).expect("stored evidence");
        assert_eq!(stored.events().len(), 2);
    }

    #[test]
    fn explicit_repair_rebuilds_corrupt_evidence_from_materialized_state() {
        let directory = tempfile::tempdir().expect("tempdir");
        fs::write(directory.path().join(OID), b"state").expect("staging file");
        fs::write(directory.path().join("a.ranges"), b"5\n0 5\n").expect("ranges");
        fs::write(directory.path().join("a.meta"), b"100").expect("metadata");
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("initial evidence");
        fs::write(merkle_path(directory.path(), OID), b"corrupt").expect("tamper merkle");
        assert!(verify_integrity(directory.path(), OID).is_err());

        repair_lfs_patch_evidence(
            directory.path(),
            &LfsPatchEvidenceRepairInput {
                oid: OID.to_owned(),
                scope_namespace: SCOPE.to_owned(),
                session_id: SESSION.to_owned(),
                target_key: TARGET.to_owned(),
                total_bytes: 5,
                ranges: vec![(0, 5)],
                staging_length: 5,
                last_touched_unix_seconds: 100,
            },
        )
        .expect("explicit repair");
        verify_integrity(directory.path(), OID).expect("repaired evidence");
        assert_eq!(
            load(directory.path(), OID, SCOPE, SESSION, TARGET)
                .expect("repaired lifecycle")
                .events()
                .len(),
            1
        );
    }

    #[test]
    fn uncommitted_lfs_journal_tail_is_ignored() {
        let directory = tempfile::tempdir().expect("tempdir");
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("write evidence");
        let journal = evidence_journal_path(directory.path(), OID);
        let committed_record = fs::read_to_string(&journal).expect("journal");
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&journal)
            .expect("open journal");
        file.write_all(committed_record.as_bytes())
            .expect("append uncommitted tail");
        let loaded = load(directory.path(), OID, SCOPE, SESSION, TARGET).expect("load evidence");
        assert_eq!(loaded.events().len(), 2);
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("append after uncommitted tail");
        let advanced =
            load(directory.path(), OID, SCOPE, SESSION, TARGET).expect("load advanced evidence");
        assert_eq!(advanced.events().len(), 3);
    }

    #[test]
    fn sidecar_write_failure_removes_temporary_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = evidence_path(directory.path(), OID);
        fs::create_dir(&path).expect("target directory");

        assert!(write_sidecar_atomically(directory.path(), &path, b"evidence").is_err());
        assert!(!directory.path().join("a.evidence.tmp").exists());
    }

    #[test]
    fn materialized_snapshot_history_is_append_only_and_tamper_checked() {
        let directory = tempfile::tempdir().expect("tempdir");
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("lifecycle baseline");
        record_snapshot(
            directory.path(),
            &LfsPatchSnapshotInput {
                oid: OID,
                scope_namespace: SCOPE,
                session_id: SESSION,
                target_key: TARGET,
                total_bytes: 10,
                ranges: &[],
                staging_length: 0,
                last_touched_unix_seconds: 100,
            },
        )
        .expect("snapshot baseline");
        record_snapshot(
            directory.path(),
            &LfsPatchSnapshotInput {
                oid: OID,
                scope_namespace: SCOPE,
                session_id: SESSION,
                target_key: TARGET,
                total_bytes: 10,
                ranges: &[(0, 5)],
                staging_length: 5,
                last_touched_unix_seconds: 101,
            },
        )
        .expect("snapshot append");
        let stored = load_snapshot_log(directory.path(), OID)
            .expect("snapshot log")
            .expect("snapshot");
        assert_eq!(stored.events().len(), 2);
        verify_integrity(directory.path(), OID).expect("snapshot chain verifies");

        let journal = snapshot_journal_path(directory.path(), OID);
        let mut value: serde_json::Value = serde_json::from_str(
            fs::read_to_string(&journal)
                .expect("snapshot journal")
                .lines()
                .next()
                .expect("snapshot journal record"),
        )
        .expect("json");
        value[0]["state_digest"] = serde_json::json!("tampered");
        fs::write(&journal, serde_json::to_vec(&value).expect("tampered json")).expect("rewrite");
        assert!(verify_integrity(directory.path(), OID).is_err());
    }

    #[test]
    fn tampered_merkle_commit_is_rejected_before_a_transition() {
        let directory = tempfile::tempdir().expect("tempdir");
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("write evidence");
        let path = merkle_journal_path(directory.path(), OID);
        let tampered = fs::read_to_string(&path)
            .expect("merkle journal")
            .replace("event_merkle_root", "tampered_merkle_root");
        fs::write(&path, tampered).expect("rewrite merkle journal");

        assert!(load(directory.path(), OID, SCOPE, SESSION, TARGET).is_err());
    }

    #[test]
    fn tampered_evidence_is_rejected_before_a_transition() {
        let directory = tempfile::tempdir().expect("tempdir");
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("write evidence");
        let path = evidence_journal_path(directory.path(), OID);
        let mut value: serde_json::Value = serde_json::from_str(
            fs::read_to_string(&path)
                .expect("evidence bytes")
                .lines()
                .next()
                .expect("evidence journal record"),
        )
        .expect("json");
        value[0]["operation"]["repository"] = serde_json::json!("wrong-scope");
        fs::write(&path, serde_json::to_vec(&value).expect("tampered json")).expect("rewrite");

        let error = load(directory.path(), OID, SCOPE, SESSION, TARGET).expect_err("tampering");
        assert!(matches!(
            error,
            ServerError::Io(ref io_error) if io_error.kind() == ErrorKind::InvalidData
        ));
    }

    #[test]
    fn materialized_state_tampering_is_rejected_before_snapshot_append() {
        let directory = tempfile::tempdir().expect("tempdir");
        let input = LfsPatchSnapshotInput {
            oid: OID,
            scope_namespace: SCOPE,
            session_id: SESSION,
            target_key: TARGET,
            total_bytes: 10,
            ranges: &[(0, 5)],
            staging_length: 5,
            last_touched_unix_seconds: 100,
        };
        record_snapshot(directory.path(), &input).expect("snapshot baseline");
        let tampered = LfsPatchSnapshotInput {
            ranges: &[(0, 4)],
            ..input
        };
        assert!(verify_snapshot(directory.path(), &tampered).is_err());
        verify_snapshot(directory.path(), &input).expect("canonical snapshot");
    }

    #[test]
    fn completion_path_is_ordered_and_idempotent() {
        let directory = tempfile::tempdir().expect("tempdir");
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("active evidence");
        transition(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Completing,
        )
        .expect("completion claim");
        complete(directory.path(), OID, SCOPE, SESSION, TARGET).expect("terminal evidence");
        complete(directory.path(), OID, SCOPE, SESSION, TARGET)
            .expect("repeated completion is idempotent");

        let log = load(directory.path(), OID, SCOPE, SESSION, TARGET).expect("verified log");
        assert_eq!(
            log.events().last().expect("terminal event").after,
            ResumableLifecycleState::Completed
        );
        assert_eq!(log.events().len(), 4);
    }

    #[test]
    fn identity_mismatch_cannot_advance_a_valid_chain() {
        let directory = tempfile::tempdir().expect("tempdir");
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("active evidence");
        let error = transition(
            directory.path(),
            OID,
            "other-scope",
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Completing,
        )
        .expect_err("identity mismatch");
        assert!(matches!(
            error,
            ServerError::Io(ref io_error) if io_error.kind() == ErrorKind::InvalidData
        ));
    }
}
