//! Simultaneous released/candidate requests against the same durable backend.

use super::*;

pub(super) async fn publish(
    old: &Path,
    root: &Path,
    postgres: Option<&str>,
    storage_env: &[(&str, String)],
    candidate: &str,
    token: &str,
    original: &NativeFile,
) -> (NativeFile, NativeFile) {
    let mut predecessor = LocalServer::spawn_with_storage(old, root, postgres, "xet", storage_env);
    predecessor.ready().await;
    let bytes: Vec<_> = original
        .members
        .iter()
        .flat_map(|(_, bytes)| bytes.iter().copied())
        .collect();
    let legacy_parts: Vec<_> = bytes.chunks(73_729).map(<[u8]>::to_vec).collect();
    let current_parts = bytes.chunks(65_537).map(<[u8]>::to_vec).collect();
    let (legacy, current) = tokio::join!(
        upload_parts(&predecessor.base, token, legacy_parts.clone(), true),
        upload_parts(candidate, token, current_parts, true),
    );
    assert_ne!(legacy.file_id, current.file_id);
    assert_ne!(legacy.file_id, original.file_id);
    assert_ne!(current.file_id, original.file_id);
    for base in [&predecessor.base, candidate] {
        for file in [&legacy, &current, original] {
            verify(base, token, file).await;
        }
    }
    // The released writer cannot populate the new identity indexes. Its
    // concurrent publication must not destroy the candidate's SHA binding.
    verify_identity_download(candidate, token, &current).await;
    // An identical candidate retry backfills the released writer's visible
    // intent, preserving both aliases while updating the canonical binding.
    let rebound = upload_parts(candidate, token, legacy_parts, true).await;
    assert_eq!(rebound.file_id, legacy.file_id);
    verify_identities(root, postgres, &rebound, &current).await;
    verify_identity_download(candidate, token, &rebound).await;
    (legacy, current)
}
