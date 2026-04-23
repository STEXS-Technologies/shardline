use std::collections::BTreeMap;

use shardline_index::FileRecord;
use shardline_protocol::ByteRange;

use crate::{
    ServerError,
    model::{
        BatchReconstructionResponse, FileReconstructionResponse, FileReconstructionV2Response,
        ReconstructionChunkRange, ReconstructionFetchInfo, ReconstructionMultiRangeFetch,
        ReconstructionRangeDescriptor, ReconstructionTerm, ReconstructionUrlRange,
    },
};

use super::build_xorb_transfer_url;

pub(crate) fn build_reconstruction_response(
    public_base_url: &str,
    record: &FileRecord,
    requested_range: Option<ByteRange>,
) -> Result<FileReconstructionResponse, ServerError> {
    record.validate_reconstruction_plan()?;
    if record.total_bytes == 0 {
        if requested_range.is_some() {
            return Err(ServerError::RangeNotSatisfiable);
        }
        return Ok(FileReconstructionResponse {
            offset_into_first_range: 0,
            terms: Vec::new(),
            fetch_info: BTreeMap::new(),
        });
    }

    let effective_range = match requested_range {
        Some(value) => value,
        None => {
            let end_inclusive = record
                .total_bytes
                .checked_sub(1)
                .ok_or(ServerError::Overflow)?;
            ByteRange::new(0, end_inclusive).map_err(|_error| ServerError::Overflow)?
        }
    };
    if effective_range.end_inclusive() >= record.total_bytes {
        return Err(ServerError::RangeNotSatisfiable);
    }
    let mut offset_into_first_range = 0_u64;
    let mut first_term = true;
    let mut terms = Vec::with_capacity(record.chunks.len());
    let mut fetch_info = BTreeMap::new();
    for chunk in &record.chunks {
        let Some(chunk_end_inclusive) = chunk
            .offset
            .checked_add(chunk.length)
            .and_then(|value| value.checked_sub(1))
        else {
            return Err(ServerError::Overflow);
        };
        if chunk_end_inclusive < effective_range.start()
            || chunk.offset > effective_range.end_inclusive()
        {
            continue;
        }

        if first_term {
            offset_into_first_range = effective_range.start().saturating_sub(chunk.offset);
            first_term = false;
        }

        terms.push(ReconstructionTerm {
            hash: chunk.hash.clone(),
            unpacked_length: chunk.length,
            range: ReconstructionChunkRange {
                start: chunk.range_start,
                end: chunk.range_end,
            },
        });

        let byte_end = chunk
            .packed_end
            .checked_sub(1)
            .ok_or(ServerError::Overflow)?;
        let fetch_entry = ReconstructionFetchInfo {
            range: ReconstructionChunkRange {
                start: chunk.range_start,
                end: chunk.range_end,
            },
            url: build_xorb_transfer_url(public_base_url, &chunk.hash),
            url_range: ReconstructionUrlRange {
                start: chunk.packed_start,
                end: byte_end,
            },
        };
        let fetch_entries = fetch_info
            .entry(chunk.hash.clone())
            .or_insert_with(Vec::new);
        if !fetch_entries.iter().any(|entry| entry == &fetch_entry) {
            fetch_entries.push(fetch_entry);
        }
    }

    Ok(FileReconstructionResponse {
        offset_into_first_range,
        terms,
        fetch_info,
    })
}

pub(crate) fn reconstruction_v2_from_v1(
    response: FileReconstructionResponse,
) -> FileReconstructionV2Response {
    let xorbs = response
        .fetch_info
        .into_iter()
        .map(|(hash, fetch_entries)| {
            let entries = fetch_entries
                .into_iter()
                .map(|entry| ReconstructionMultiRangeFetch {
                    url: entry.url,
                    ranges: vec![ReconstructionRangeDescriptor {
                        chunks: entry.range,
                        bytes: entry.url_range,
                    }],
                })
                .collect();
            (hash, entries)
        })
        .collect();

    FileReconstructionV2Response {
        offset_into_first_range: response.offset_into_first_range,
        terms: response.terms,
        xorbs,
    }
}

pub(crate) fn build_batch_reconstruction_response(
    responses: impl IntoIterator<Item = (String, FileReconstructionResponse)>,
) -> BatchReconstructionResponse {
    let mut files = BTreeMap::new();
    let mut fetch_info = BTreeMap::new();

    for (file_id, response) in responses {
        files.insert(file_id, response.terms);
        for (hash, fetch_entries) in response.fetch_info {
            fetch_info
                .entry(hash)
                .or_insert_with(Vec::new)
                .extend(fetch_entries);
        }
    }

    BatchReconstructionResponse { files, fetch_info }
}

#[cfg(test)]
mod tests {
    use shardline_index::{FileChunkRecord, FileRecord};
    use shardline_protocol::{ByteRange, RepositoryProvider, RepositoryScope};

    use super::{
        build_batch_reconstruction_response, build_reconstruction_response,
        reconstruction_v2_from_v1,
    };
    use crate::ServerError;

    #[test]
    fn response_contains_xet_terms_and_fetch_info() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None);
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let record = FileRecord {
            file_id: "asset.bin".to_owned(),
            content_hash: "deadbeef".repeat(8),
            total_bytes: 8,
            chunk_size: 4,
            repository_scope: Some(scope),
            chunks: vec![
                FileChunkRecord {
                    hash: "a".repeat(64),
                    offset: 0,
                    length: 4,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 4,
                },
                FileChunkRecord {
                    hash: "b".repeat(64),
                    offset: 4,
                    length: 4,
                    range_start: 1,
                    range_end: 3,
                    packed_start: 4,
                    packed_end: 10,
                },
            ],
        };

        let response = build_reconstruction_response("http://127.0.0.1:8080", &record, None);

        assert!(response.is_ok());
        let Ok(response) = response else {
            return;
        };
        assert_eq!(response.offset_into_first_range, 0);
        assert_eq!(response.terms.len(), 2);
        assert_eq!(
            response.fetch_info.get(&"a".repeat(64)).map(Vec::len),
            Some(1)
        );
        assert_eq!(
            response
                .fetch_info
                .get(&"a".repeat(64))
                .and_then(|entries| entries.first())
                .map(|entry| entry.url.as_str()),
            Some(
                "http://127.0.0.1:8080/transfer/xorb/default/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )
        );
        assert_eq!(response.terms.first().map(|term| term.range.start), Some(0));
        assert_eq!(response.terms.first().map(|term| term.range.end), Some(1));
        assert_eq!(
            response
                .terms
                .get(1)
                .map(|term| (term.range.start, term.range.end)),
            Some((1, 3))
        );
    }

    #[test]
    fn response_applies_requested_logical_range() {
        let record = FileRecord {
            file_id: "asset.bin".to_owned(),
            content_hash: "deadbeef".repeat(8),
            total_bytes: 12,
            chunk_size: 4,
            repository_scope: None,
            chunks: vec![
                FileChunkRecord {
                    hash: "a".repeat(64),
                    offset: 0,
                    length: 4,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 4,
                },
                FileChunkRecord {
                    hash: "b".repeat(64),
                    offset: 4,
                    length: 4,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 4,
                    packed_end: 8,
                },
                FileChunkRecord {
                    hash: "c".repeat(64),
                    offset: 8,
                    length: 4,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 8,
                    packed_end: 12,
                },
            ],
        };
        let range = ByteRange::new(2, 9);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };

        let response = build_reconstruction_response("http://127.0.0.1:8080", &record, Some(range));

        assert!(response.is_ok());
        let Ok(response) = response else {
            return;
        };
        assert_eq!(response.offset_into_first_range, 2);
        assert_eq!(response.terms.len(), 3);
    }

    #[test]
    fn response_rejects_requested_range_past_file_length() {
        let record = FileRecord {
            file_id: "asset.bin".to_owned(),
            content_hash: "deadbeef".repeat(8),
            total_bytes: 4,
            chunk_size: 4,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: "a".repeat(64),
                offset: 0,
                length: 4,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 4,
            }],
        };
        let range = ByteRange::new(4, 4);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };

        let response = build_reconstruction_response("http://127.0.0.1:8080", &record, Some(range));

        assert!(matches!(response, Err(ServerError::RangeNotSatisfiable)));
    }

    #[test]
    fn zero_byte_record_rejects_requested_range() {
        let record = FileRecord {
            file_id: "asset.bin".to_owned(),
            content_hash: "deadbeef".repeat(8),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: None,
            chunks: Vec::new(),
        };
        let range = ByteRange::new(0, 0);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };

        let response = build_reconstruction_response("http://127.0.0.1:8080", &record, Some(range));

        assert!(matches!(response, Err(ServerError::RangeNotSatisfiable)));
    }

    #[test]
    fn v2_conversion_preserves_terms_and_emits_range_descriptors() {
        let record = FileRecord {
            file_id: "asset.bin".to_owned(),
            content_hash: "deadbeef".repeat(8),
            total_bytes: 8,
            chunk_size: 4,
            repository_scope: None,
            chunks: vec![
                FileChunkRecord {
                    hash: "a".repeat(64),
                    offset: 0,
                    length: 4,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 4,
                },
                FileChunkRecord {
                    hash: "b".repeat(64),
                    offset: 4,
                    length: 4,
                    range_start: 1,
                    range_end: 2,
                    packed_start: 4,
                    packed_end: 8,
                },
            ],
        };

        let v1 = build_reconstruction_response("http://127.0.0.1:8080", &record, None);
        assert!(v1.is_ok());
        let Ok(v1) = v1 else {
            return;
        };
        let v2 = reconstruction_v2_from_v1(v1);

        assert_eq!(v2.terms.len(), 2);
        assert_eq!(v2.xorbs.len(), 2);
        assert_eq!(
            v2.xorbs
                .get(&"a".repeat(64))
                .and_then(|entries| entries.first())
                .and_then(|entry| entry.ranges.first())
                .map(|entry| (
                    entry.chunks.start,
                    entry.chunks.end,
                    entry.bytes.start,
                    entry.bytes.end
                )),
            Some((0, 1, 0, 3))
        );
    }

    #[test]
    fn batch_reconstruction_response_merges_fetch_info_across_files() {
        let first = build_batch_reconstruction_response([
            ("a".repeat(64), {
                let response = build_reconstruction_response(
                    "http://127.0.0.1:8080",
                    &FileRecord {
                        file_id: "asset-a".to_owned(),
                        content_hash: "a".repeat(64),
                        total_bytes: 4,
                        chunk_size: 4,
                        repository_scope: None,
                        chunks: vec![FileChunkRecord {
                            hash: "c".repeat(64),
                            offset: 0,
                            length: 4,
                            range_start: 0,
                            range_end: 1,
                            packed_start: 0,
                            packed_end: 4,
                        }],
                    },
                    None,
                );
                assert!(response.is_ok());
                let Ok(response) = response else {
                    return;
                };
                response
            }),
            ("b".repeat(64), {
                let response = build_reconstruction_response(
                    "http://127.0.0.1:8080",
                    &FileRecord {
                        file_id: "asset-b".to_owned(),
                        content_hash: "b".repeat(64),
                        total_bytes: 4,
                        chunk_size: 4,
                        repository_scope: None,
                        chunks: vec![FileChunkRecord {
                            hash: "c".repeat(64),
                            offset: 0,
                            length: 4,
                            range_start: 0,
                            range_end: 1,
                            packed_start: 0,
                            packed_end: 4,
                        }],
                    },
                    None,
                );
                assert!(response.is_ok());
                let Ok(response) = response else {
                    return;
                };
                response
            }),
        ]);

        assert_eq!(first.files.len(), 2);
        assert_eq!(first.fetch_info.get(&"c".repeat(64)).map(Vec::len), Some(2));
    }
}
