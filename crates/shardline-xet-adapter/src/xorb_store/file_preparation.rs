//! Synchronous preparation of a staged native Xorb before immutable writes.

use std::{fs::File, path::PathBuf};

use shardline_index::parse_xet_hash_hex;
use shardline_storage::ObjectIntegrity;

use crate::{ValidatedXorb, XetAdapterError, validate_serialized_xorb};

pub(super) struct PreparedFile {
    pub normalized: Option<tempfile::NamedTempFile>,
    pub file: File,
    pub path: PathBuf,
    pub validated: ValidatedXorb,
    pub integrity: ObjectIntegrity,
    pub length: u64,
}

pub(super) fn prepare(expected_hash: &str, path: PathBuf) -> Result<PreparedFile, XetAdapterError> {
    let expected = parse_xet_hash_hex(expected_hash)?;
    let normalized = super::normalize_uploaded_xorb_file(expected_hash, &path)?;
    let path = normalized
        .as_ref()
        .map_or(path, |file| file.path().to_owned());
    let mut file = File::open(&path)?;
    let length = file.metadata()?.len();
    let validated = validate_serialized_xorb(&mut file, expected)?;
    let integrity = ObjectIntegrity::new(super::chunk_hash_file(&path)?, length);
    Ok(PreparedFile {
        normalized,
        file,
        path,
        validated,
        integrity,
        length,
    })
}
