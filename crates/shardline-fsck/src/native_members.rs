//! Resolve missing-copy findings only through the native read index.

use crate::{FsckError, FsckIssueDetail, FsckIssueKind, FsckReport};
use shardline_index::{XetXorbDescriptor, parse_xet_hash_hex};
use shardline_server_core::OpsRecordStore;
use shardline_xet_core::merklehash::{MerkleHash, xorb_hash};
use std::{collections::HashSet, path::Path};

pub(crate) async fn inspect_missing_copies<R: OpsRecordStore + Sync>(
    records: &R,
    report: &mut FsckReport,
) -> Result<(), FsckError>
where
    R::Error: Into<FsckError>,
{
    // This diagnostic is produced only after complete Xorb validation. A source
    // hash/length/format failure has another diagnostic and is never resolved here.
    let issues = std::mem::take(&mut report.issues);
    // Keep only the most recent container. Memory is bounded by one validated
    // descriptor's member count, independently of the number of files scanned.
    let mut cached: Option<(String, Option<HashSet<String>>)> = None;
    for issue in issues {
        let resolved = if let (
            FsckIssueKind::MissingChunk,
            FsckIssueDetail::ReferencedByNativeXetXorb { xorb_location },
        ) = (&issue.kind, &issue.detail)
        {
            let member = Path::new(&issue.location)
                .file_name()
                .and_then(|name| name.to_str());
            let source = Path::new(xorb_location)
                .file_stem()
                .and_then(|name| name.to_str());
            if let (Some(member), Some(source)) = (member, source) {
                if parse_xet_hash_hex(member).is_ok() && parse_xet_hash_hex(source).is_ok() {
                    if records.supports_native_locations() {
                        if cached.as_ref().is_none_or(|(hash, _)| hash != source) {
                            let descriptor = records
                                .native_indexed_xorb(source)
                                .await
                                .map_err(Into::into)?;
                            let members = descriptor
                                .filter(|descriptor| authenticates_descriptor(descriptor, source))
                                .map(|descriptor| {
                                    descriptor
                                        .chunks
                                        .into_iter()
                                        .map(|chunk| chunk.hash)
                                        .collect()
                                });
                            cached = Some((source.to_owned(), members));
                        }
                        cached
                            .as_ref()
                            .and_then(|(_, members)| members.as_ref())
                            .is_some_and(|members| members.contains(member))
                    } else {
                        records
                            .native_chunk_location(member, source)
                            .await
                            .map_err(Into::into)?
                            .is_some_and(|descriptor| {
                                authenticates_member(&descriptor, source, member)
                            })
                    }
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };
        if !resolved {
            report.issues.push(issue);
        }
    }
    Ok(())
}

fn authenticates_member(descriptor: &XetXorbDescriptor, source: &str, member: &str) -> bool {
    descriptor.chunks.iter().any(|chunk| chunk.hash == member)
        && authenticates_descriptor(descriptor, source)
}

fn authenticates_descriptor(descriptor: &XetXorbDescriptor, source: &str) -> bool {
    if descriptor.hash != source || !descriptor.has_valid_structure() {
        return false;
    }
    let Ok(expected) = parse_xet_hash_hex(source) else {
        return false;
    };
    let Some(chunks) = descriptor
        .chunks
        .iter()
        .map(|chunk| {
            let hash = parse_xet_hash_hex(&chunk.hash).ok()?;
            Some((
                MerkleHash::from(*hash.as_bytes()),
                chunk.unpacked_end.checked_sub(chunk.unpacked_start)?,
            ))
        })
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    xorb_hash(&chunks) == MerkleHash::from(*expected.as_bytes())
}

#[cfg(test)]
mod sqlite_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_index::XetXorbChunk;
    use shardline_xet_core::merklehash::compute_data_hash;

    #[test]
    fn descriptor_must_bind_the_member_and_lengths_to_the_verified_source_root() {
        let leaf = compute_data_hash(b"verified");
        let other = compute_data_hash(b"another");
        let source = xorb_hash(&[(leaf, 8), (other, 7)]).hex();
        let member = leaf.hex();
        let descriptor = XetXorbDescriptor {
            hash: source.clone(),
            total_bytes: 128,
            chunks: vec![
                XetXorbChunk {
                    hash: member.clone(),
                    packed_start: 0,
                    packed_end: 16,
                    unpacked_start: 0,
                    unpacked_end: 8,
                },
                XetXorbChunk {
                    hash: other.hex(),
                    packed_start: 16,
                    packed_end: 32,
                    unpacked_start: 8,
                    unpacked_end: 15,
                },
            ],
        };
        assert!(authenticates_member(&descriptor, &source, &member));
        assert!(!authenticates_member(
            &descriptor,
            &source,
            &compute_data_hash(b"other").hex()
        ));
        let mut forged = descriptor.clone();
        forged.chunks[1].unpacked_end += 1;
        assert!(!authenticates_member(&forged, &source, &member));
        forged = descriptor;
        forged.chunks[0].hash = compute_data_hash(b"forged").hex();
        assert!(!authenticates_member(
            &forged,
            &source,
            &forged.chunks[0].hash
        ));
    }
}
