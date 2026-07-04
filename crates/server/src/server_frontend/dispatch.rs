use shardline_index::FileChunkRecord;
use shardline_storage::ObjectKey;

use crate::{ServerError, object_store::ServerObjectStore};

use super::{ServerFrontend, xet};

pub(crate) fn optional_chunk_container_keys(
    frontends: &[ServerFrontend],
    chunk_hash: &str,
) -> Result<Vec<ObjectKey>, ServerError> {
    let mut object_keys = Vec::new();
    for frontend in frontends {
        match frontend {
            ServerFrontend::Xet => {
                xet::push_optional_chunk_container_key(&mut object_keys, chunk_hash)?
            }
            ServerFrontend::Lfs
            | ServerFrontend::BazelHttp
            | ServerFrontend::Oci
            | ServerFrontend::Hub => {}
        }
    }

    Ok(object_keys)
}

pub(crate) fn referenced_term_object_key(
    frontends: &[ServerFrontend],
    term_hash: &str,
) -> Result<ObjectKey, ServerError> {
    if frontends.contains(&ServerFrontend::Xet) {
        return xet::referenced_term_object_key(term_hash);
    }

    Err(ServerError::InvalidContentHash)
}

pub(crate) fn visit_protocol_object_member_chunks<Visitor>(
    frontends: &[ServerFrontend],
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    visitor: Visitor,
) -> Result<(), ServerError>
where
    Visitor: FnMut(String) -> Result<(), ServerError>,
{
    for frontend in frontends {
        match frontend {
            ServerFrontend::Xet => {
                if xet::owns_protocol_object(object_key)? {
                    return xet::visit_protocol_object_member_chunks(
                        object_store,
                        object_key,
                        visitor,
                    );
                }
            }
            ServerFrontend::Lfs
            | ServerFrontend::BazelHttp
            | ServerFrontend::Oci
            | ServerFrontend::Hub => {}
        }
    }

    Ok(())
}

pub(crate) fn append_referenced_term_bytes(
    frontends: &[ServerFrontend],
    object_store: &ServerObjectStore,
    term: &FileChunkRecord,
    output: &mut Vec<u8>,
) -> Result<(), ServerError> {
    if frontends.contains(&ServerFrontend::Xet) {
        return xet::append_referenced_term_bytes(object_store, term, output);
    }

    Err(ServerError::InvalidContentHash)
}
