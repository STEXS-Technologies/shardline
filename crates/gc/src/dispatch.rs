use shardline_server_core::ServerObjectStore;
use shardline_storage::ObjectKey;
use shardline_xet_adapter::{
    XetAdapterError, shard_hash_from_object_key_if_present, visit_stored_xorb_chunk_hashes,
    xorb_hash_from_object_key_if_present, xorb_object_key,
};

use crate::{GcError, ServerFrontend};

pub(super) fn optional_chunk_container_keys(
    frontends: &[ServerFrontend],
    chunk_hash: &str,
) -> Result<Vec<ObjectKey>, GcError> {
    let mut object_keys = Vec::new();
    for frontend in frontends {
        match frontend {
            ServerFrontend::Xet => {
                let object_key = xorb_object_key(chunk_hash)?;
                if !object_keys.contains(&object_key) {
                    object_keys.push(object_key);
                }
            }
            ServerFrontend::Lfs
            | ServerFrontend::BazelHttp
            | ServerFrontend::Oci
            | ServerFrontend::Hub => {}
        }
    }

    Ok(object_keys)
}

pub(super) fn referenced_term_object_key(
    frontends: &[ServerFrontend],
    term_hash: &str,
) -> Result<ObjectKey, GcError> {
    if frontends.contains(&ServerFrontend::Xet) {
        return Ok(xorb_object_key(term_hash)?);
    }

    Err(GcError::InvalidContentHash)
}

pub(super) fn managed_protocol_object_identity(
    frontends: &[ServerFrontend],
    key: &ObjectKey,
) -> Result<Option<String>, GcError> {
    for frontend in frontends {
        match frontend {
            ServerFrontend::Xet => {
                if let Some(hash) = xorb_hash_from_object_key_if_present(key)? {
                    return Ok(Some(hash.to_owned()));
                }
                if let Some(hash) = shard_hash_from_object_key_if_present(key)? {
                    return Ok(Some(hash.to_owned()));
                }
            }
            ServerFrontend::Lfs
            | ServerFrontend::BazelHttp
            | ServerFrontend::Oci
            | ServerFrontend::Hub => {}
        }
    }

    Ok(None)
}

pub(super) fn visit_protocol_object_member_chunks<Visitor>(
    frontends: &[ServerFrontend],
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    mut visitor: Visitor,
) -> Result<(), GcError>
where
    Visitor: FnMut(String) -> Result<(), GcError>,
{
    for frontend in frontends {
        match frontend {
            ServerFrontend::Xet => {
                if xorb_hash_from_object_key_if_present(object_key)?.is_some() {
                    let mut result = Ok(());
                    visit_stored_xorb_chunk_hashes(object_store, object_key, |chunk_hash_hex| {
                        match visitor(chunk_hash_hex) {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                result = Err(e);
                                Err(XetAdapterError::NotFound)
                            }
                        }
                    })?;
                    return result;
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
