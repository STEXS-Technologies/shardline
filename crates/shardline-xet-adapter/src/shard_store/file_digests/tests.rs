#![allow(clippy::unwrap_used)]

use super::*;
use shardline_protocol::RepositoryProvider;

fn scope(name: &str) -> RepositoryScope {
    RepositoryScope::new(RepositoryProvider::GitHub, "prefix-tests", name, None).unwrap()
}

fn term(data: &[u8]) -> FileDataSequenceEntry {
    FileDataSequenceEntry::new(compute_data_hash(data), data.len() as u64, 0, 1)
}

fn feed(digest: &mut FileDigest, entry: &FileDataSequenceEntry, data: &[u8]) {
    digest.begin_terms(std::slice::from_ref(entry));
    digest.update_validating_member(entry, 0, data);
    digest.finish_terms();
}

#[test]
fn verified_prefix_reuses_real_sha_state_and_hashes_changed_suffix() {
    let scope = scope("changed-suffix");
    let prefix = term(b"verified prefix");
    let mut original = FileDigest::with_scope(None, Some(&scope));
    feed(&mut original, &prefix, b"verified prefix");
    feed(&mut original, &term(b"old"), b"old");
    original.finish().unwrap();

    let expected = format!("{:x}", Sha256::digest(b"verified prefixnew"));
    let mut changed = FileDigest::with_scope(Some(expected.clone()), Some(&scope));
    feed(&mut changed, &prefix, b"verified prefix");
    assert!(changed.reusing_prefix, "test must exercise a cache hit");
    feed(&mut changed, &term(b"new"), b"new");
    assert!(!changed.reusing_prefix);
    assert_eq!(changed.finish().unwrap(), Some(expected));
}

#[test]
fn wrong_final_identity_is_rejected_even_on_cache_hit() {
    let scope = scope("wrong-final");
    let entry = term(b"content");
    let mut good = FileDigest::with_scope(None, Some(&scope));
    feed(&mut good, &entry, b"content");
    good.finish().unwrap();
    let mut wrong = FileDigest::with_scope(Some("00".repeat(32)), Some(&scope));
    feed(&mut wrong, &entry, b"content");
    assert!(wrong.reusing_prefix);
    assert!(wrong.finish().is_err());
}

#[test]
fn parallel_digest_checks_final_identity_and_resets_to_verified_prefix() {
    let scope = scope("parallel-prefix");
    let data = vec![42; 512 * 1024];
    let entry = term(&data);
    let mut original = FileDigest::with_scope(None, Some(&scope));
    original.enable_parallel(true);
    assert!(original.worker.is_some());
    feed(&mut original, &entry, &data);
    assert_eq!(
        original.finish().unwrap(),
        Some(format!("{:x}", Sha256::digest(&data)))
    );

    let expected = format!(
        "{:x}",
        Sha256::digest([data.as_slice(), b"changed"].concat())
    );
    let mut changed = FileDigest::with_scope(Some(expected.clone()), Some(&scope));
    changed.enable_parallel(true);
    assert!(changed.worker.is_some());
    feed(&mut changed, &entry, &data);
    assert!(changed.reusing_prefix);
    feed(&mut changed, &term(b"changed"), b"changed");
    assert_eq!(changed.finish().unwrap(), Some(expected));

    let mut wrong = FileDigest::with_scope(Some("00".repeat(32)), Some(&scope));
    wrong.enable_parallel(true);
    feed(&mut wrong, &entry, &data);
    assert!(wrong.reusing_prefix);
    assert!(wrong.finish().is_err());
}

#[test]
fn cancelled_parallel_source_does_not_publish_a_prefix_and_can_retry() {
    use super::super::file_digests_tests::store_members;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let first = vec![31; 8 * 1024 * 1024];
    let second = vec![47; 8 * 1024 * 1024];
    let (hash, _) = store_members(&objects, &[&first, &second]);
    let entry = FileDataSequenceEntry::new(hash, 16 * 1024 * 1024, 0, 2);
    let (info, source) = load_xorb_range_info_with_source(&objects, &hash.hex(), None).unwrap();
    let scope = scope("parallel-source-cancellation");
    let mut cancelled = FileDigest::with_scope(None, Some(&scope));
    cancelled.enable_parallel(true);
    assert!(cancelled.worker.is_some());
    cancelled.begin_terms(std::slice::from_ref(&entry));
    let checks = AtomicUsize::new(0);
    let mut verifier = FileDigestVerifier::default();
    verifier.retain_verified_source(&hash.hex(), source);
    assert!(
        cancelled
            .update(&mut verifier, &objects, &entry, &info, &|| {
                if checks.fetch_add(1, Ordering::SeqCst) == 2 {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "cancelled",
                    ))
                } else {
                    Ok(())
                }
            })
            .is_err()
    );
    assert_eq!(verifier.member_rechecks, 1);
    drop(cancelled);
    let mut retry = FileDigest::with_scope(None, Some(&scope));
    retry.enable_parallel(true);
    retry.begin_terms(std::slice::from_ref(&entry));
    assert!(!retry.reusing_prefix);
    retry
        .update(&mut verifier, &objects, &entry, &info, &|| Ok(()))
        .unwrap();
    retry.finish_terms();
    let expected = format!("{:x}", Sha256::digest([first, second].concat()));
    assert_eq!(retry.finish().unwrap(), Some(expected));
}

#[test]
fn failed_and_abandoned_files_do_not_publish_prefixes() {
    for (name, finish) in [("failed", true), ("abandoned", false)] {
        let scope = scope(name);
        let entry = term(name.as_bytes());
        let mut failed = FileDigest::with_scope(Some("00".repeat(32)), Some(&scope));
        feed(&mut failed, &entry, name.as_bytes());
        if finish {
            assert!(failed.finish().is_err());
        } else {
            drop(failed);
        }
        let mut retry = FileDigest::with_scope(None, Some(&scope));
        feed(&mut retry, &entry, name.as_bytes());
        assert!(!retry.reusing_prefix);
    }
}

#[test]
fn changed_scope_order_and_range_cannot_reuse_other_prefix() {
    let original_scope = scope("isolated");
    let first = term(b"first");
    let second = term(b"second");
    let mut original = FileDigest::with_scope(None, Some(&original_scope));
    feed(&mut original, &first, b"first");
    feed(&mut original, &second, b"second");
    original.finish().unwrap();

    let other_scope = scope("other");
    let mut other = FileDigest::with_scope(None, Some(&other_scope));
    feed(&mut other, &first, b"first");
    assert!(!other.reusing_prefix);
    let mut reordered = FileDigest::with_scope(None, Some(&original_scope));
    feed(&mut reordered, &second, b"second");
    assert!(!reordered.reusing_prefix);
    let mut changed_range = first;
    changed_range.chunk_index_end = 2;
    let mut ranged = FileDigest::with_scope(None, Some(&original_scope));
    ranged.begin_terms(&[changed_range]);
    assert!(!ranged.reusing_prefix);
}

#[test]
fn warm_prefix_still_authenticates_source_members() {
    for parallel in [false, true] {
        use super::super::file_digests_tests::store_members;
        let root = tempfile::tempdir().unwrap();
        let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
        let (hash, _) = store_members(&objects, &[b"first".as_slice(), b"second".as_slice()]);
        let entry = FileDataSequenceEntry::new(hash, 11, 0, 2);
        let info = load_xorb_range_info(&objects, &hash.hex(), None).unwrap();
        let scope = scope(if parallel {
            "source-authentication-parallel"
        } else {
            "source-authentication"
        });
        let mut initial = FileDigest::with_scope(None, Some(&scope));
        initial.enable_parallel(parallel);
        let mut verifier = FileDigestVerifier::default();
        initial.begin_terms(std::slice::from_ref(&entry));
        initial
            .update(&mut verifier, &objects, &entry, &info, &|| Ok(()))
            .unwrap();
        initial.finish_terms();
        initial.finish().unwrap();

        let mut warm = FileDigest::with_scope(None, Some(&scope));
        warm.enable_parallel(parallel);
        warm.begin_terms(std::slice::from_ref(&entry));
        assert!(warm.reusing_prefix);
        let mut verifier = FileDigestVerifier::default();
        warm.update(&mut verifier, &objects, &entry, &info, &|| Ok(()))
            .unwrap();
        assert_eq!(verifier.member_rechecks, 2);
        assert_eq!(
            warm.finish().unwrap(),
            Some(format!("{:x}", Sha256::digest(b"firstsecond")))
        );

        let mut cancelled = FileDigest::with_scope(None, Some(&scope));
        cancelled.enable_parallel(parallel);
        cancelled.begin_terms(std::slice::from_ref(&entry));
        assert!(cancelled.reusing_prefix);
        assert!(
            cancelled
                .update(&mut verifier, &objects, &entry, &info, &|| {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "cancelled",
                    ))
                })
                .is_err()
        );

        let mut warm = FileDigest::with_scope(None, Some(&scope));
        warm.enable_parallel(parallel);
        warm.begin_terms(std::slice::from_ref(&entry));
        assert!(warm.reusing_prefix);
        let key = xorb_object_key(&hash.hex()).unwrap();
        std::fs::write(objects.local_root().unwrap().join(key.as_str()), b"corrupt").unwrap();
        // A valid cached SHA state must not bypass current source authentication.
        let mut verifier = FileDigestVerifier::default();
        assert!(
            warm.update(&mut verifier, &objects, &entry, &info, &|| Ok(()))
                .is_err()
        );
        assert!(verifier.member_rechecks > 0);
    }
}
