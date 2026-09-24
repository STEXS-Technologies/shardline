#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{
    DigestSnapshot, OperationKind, PersistedMerkleJournalRecord, SnapshotEvidenceEvent,
    verify_persisted_merkle_chain, verify_typed_merkle_chain,
};

fuzz_target!(|input: &[u8]| {
    let Ok(record) = serde_json::from_slice::<PersistedMerkleJournalRecord>(input) else {
        return;
    };
    let _ = verify_persisted_merkle_chain(
        OperationKind::ResumableSession,
        &record.evidence,
        &record.merkle_commits,
    );
    let _ = verify_typed_merkle_chain::<SnapshotEvidenceEvent<DigestSnapshot>>(
        &record.snapshot_evidence,
        &record.snapshot_merkle_commits,
    );
});
