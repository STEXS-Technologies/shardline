use shardline_server_core::ServerObjectStore;
use shardline_storage::ObjectKey;
use shardline_xet_adapter::{
    XetAdapterError, shard_hash_from_object_key_if_present,
    visit_stored_xorb_chunk_hashes_if_present, xorb_hash_from_object_key_if_present,
    xorb_object_key,
};

use crate::{GcError, ServerFrontend};

pub(super) fn uses_native_xet_storage(frontends: &[ServerFrontend]) -> bool {
    frontends
        .iter()
        .any(|frontend| matches!(frontend, ServerFrontend::Xet | ServerFrontend::Hub))
}

pub(super) fn optional_chunk_container_keys(
    frontends: &[ServerFrontend],
    chunk_hash: &str,
) -> Result<Vec<ObjectKey>, GcError> {
    if !uses_native_xet_storage(frontends) {
        return Ok(Vec::new());
    }
    let mut object_keys = vec![xorb_object_key(chunk_hash)?];
    // Keep the derived member-cache sidecar alongside its source container.
    let cache_prefix = chunk_hash.get(..2).ok_or(GcError::InvalidContentHash)?;
    let cache_key = format!("_xorb_chunks/{cache_prefix}/{chunk_hash}");
    if let Ok(cache_key) = ObjectKey::parse(&cache_key) {
        object_keys.push(cache_key);
    }
    Ok(object_keys)
}

pub(super) fn referenced_term_object_key(
    frontends: &[ServerFrontend],
    term_hash: &str,
) -> Result<ObjectKey, GcError> {
    if uses_native_xet_storage(frontends) {
        return Ok(xorb_object_key(term_hash)?);
    }

    Err(GcError::InvalidContentHash)
}

pub(super) fn managed_protocol_object_identity(
    frontends: &[ServerFrontend],
    key: &ObjectKey,
) -> Result<Option<String>, GcError> {
    if !uses_native_xet_storage(frontends) {
        return Ok(None);
    }
    if let Some(hash) = xorb_hash_from_object_key_if_present(key)? {
        return Ok(Some(hash.to_owned()));
    }
    Ok(shard_hash_from_object_key_if_present(key)?.map(str::to_owned))
}

pub(super) fn visit_protocol_object_member_chunks<Visitor>(
    frontends: &[ServerFrontend],
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    mut visitor: Visitor,
) -> Result<bool, GcError>
where
    Visitor: FnMut(String) -> Result<(), GcError>,
{
    if !uses_native_xet_storage(frontends)
        || xorb_hash_from_object_key_if_present(object_key)?.is_none()
    {
        return Ok(false);
    }
    let mut result = Ok(());
    let present =
        visit_stored_xorb_chunk_hashes_if_present(object_store, object_key, |chunk_hash_hex| {
            match visitor(chunk_hash_hex) {
                Ok(()) => Ok(()),
                Err(error) => {
                    result = Err(error);
                    Err(XetAdapterError::NotFound)
                }
            }
        });
    result?;
    present.map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hub_git_archives_are_excluded_from_orphan_sweep() {
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};
        let temp = tempfile::tempdir().unwrap();
        let store = ServerObjectStore::local(temp.path().to_owned()).unwrap();
        let frontends = [
            ServerFrontend::Xet,
            ServerFrontend::Lfs,
            ServerFrontend::BazelHttp,
            ServerFrontend::Oci,
            ServerFrontend::S3,
            ServerFrontend::Hub,
        ];
        for suffix in ["a".repeat(40), "b".repeat(40), "c".repeat(40)] {
            let key = ObjectKey::parse(&format!(
                "protocols/hub/git/global/{}/{suffix}",
                "d".repeat(64)
            ))
            .unwrap();
            let bytes = b"retained Git object";
            let integrity =
                ObjectIntegrity::new(shardline_server_core::chunk_hash(bytes), bytes.len() as u64);
            store
                .put_if_absent(&key, ObjectBody::from_slice(bytes), &integrity)
                .unwrap();
            assert_eq!(
                managed_protocol_object_identity(&frontends, &key).unwrap(),
                None
            );
        }
        let orphans = crate::reachability::scan_orphan_objects(
            &store,
            &frontends,
            &std::collections::HashSet::new(),
        )
        .unwrap();
        assert!(
            orphans.is_empty(),
            "Git archives must never become unindexed ordinary GC candidates"
        );
    }

    const VALID_HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn frontend_selection_preserves_native_keys_and_excludes_other_namespaces() {
        use ServerFrontend::{BazelHttp, Hub, Lfs, Oci, S3, Xet};
        let cases: &[(&[ServerFrontend], bool)] = &[
            (&[], false),
            (&[Lfs], false),
            (&[BazelHttp], false),
            (&[Oci], false),
            (&[S3], false),
            (&[Lfs, BazelHttp, Oci, S3], false),
            (&[Xet], true),
            (&[Hub], true),
            (&[Xet, Xet], true),
            (&[Xet, Lfs], true),
            (&[Xet, Lfs, Hub], true),
            (&[Lfs, Hub, Xet], true),
        ];
        let xorb = xorb_object_key(VALID_HASH).unwrap();
        let shard = shardline_xet_adapter::shard_object_key(VALID_HASH).unwrap();
        let chunk = shardline_server_core::chunk_object_key(VALID_HASH).unwrap();
        let sidecar = ObjectKey::parse(&format!("_xorb_chunks/aa/{VALID_HASH}")).unwrap();
        let unrelated = ObjectKey::parse("some/key").unwrap();
        let store = ServerObjectStore::blackhole();
        for &(frontends, native) in cases {
            assert_eq!(uses_native_xet_storage(frontends), native);
            let expected = if native {
                vec![xorb.clone(), sidecar.clone()]
            } else {
                Vec::new()
            };
            assert_eq!(
                optional_chunk_container_keys(frontends, VALID_HASH).unwrap(),
                expected
            );
            match referenced_term_object_key(frontends, VALID_HASH) {
                Ok(key) => {
                    assert!(native);
                    assert_eq!(key, xorb);
                }
                Err(error) => {
                    assert!(!native);
                    assert!(matches!(error, GcError::InvalidContentHash));
                }
            }
            for (key, managed) in [
                (&xorb, native),
                (&shard, native),
                (&chunk, false),
                (&unrelated, false),
            ] {
                assert_eq!(
                    managed_protocol_object_identity(frontends, key)
                        .unwrap()
                        .as_deref(),
                    managed.then_some(VALID_HASH),
                    "frontends={frontends:?}, key={key:?}",
                );
                let mut visited = false;
                assert!(
                    !visit_protocol_object_member_chunks(frontends, &store, key, |_| {
                        visited = true;
                        Ok(())
                    })
                    .unwrap()
                );
                assert!(
                    !visited,
                    "absent or unrelated sources must not expose members"
                );
            }
        }
    }

    #[test]
    fn native_term_keys_reject_invalid_hashes() {
        for frontend in [ServerFrontend::Xet, ServerFrontend::Hub] {
            for hash in ["abc123", "", "not-a-hash"] {
                assert!(referenced_term_object_key(&[frontend], hash).is_err());
                assert!(optional_chunk_container_keys(&[frontend], hash).is_err());
            }
        }
    }
}
