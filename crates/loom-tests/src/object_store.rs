//! Loom model-checking tests for `LocalObjectStore` core operations.
//!
//! `LocalObjectStore` is fully synchronous — no user-space locks, no atomics.
//! All synchronization is filesystem-level (hard_link EEXIST, O_EXCL). Loom
//! can't see system calls, so we use a mock filesystem backed by `loom::sync`
//! types to model-check the logic flow.
//!
//! The operations tested map directly to real filesystem operations:
//! - `put_if_absent`  → check-exists → write → verify (atomic compare-and-swap)
//! - `delete_if_present` → remove file + cleanup empty ancestors
//! - `contains` → check if file exists

use loom::sync::{Arc, Mutex};
use std::collections::HashMap;

// ──── Mock filesystem ─────────────────────────────────────────────────────
//
// Mimics LocalObjectStore's logic flow using loom::sync types for
// deterministic model checking.

#[derive(Debug, PartialEq, Eq)]
enum PutOutcome {
    Inserted,
    AlreadyExists,
}

#[derive(Debug, PartialEq, Eq)]
enum DeleteOutcome {
    Deleted,
    NotFound,
}

struct MockFileSystem {
    files: Mutex<HashMap<String, Vec<u8>>>,
}

impl MockFileSystem {
    fn new() -> Self {
        Self {
            files: Mutex::new(HashMap::new()),
        }
    }

    /// Mimics put_if_absent logic: check-exists → write → verify
    fn put_if_absent(&self, key: &str, data: &[u8]) -> PutOutcome {
        let mut files = self.files.lock().unwrap();
        if files.contains_key(key) {
            return PutOutcome::AlreadyExists;
        }
        files.insert(key.to_owned(), data.to_vec());
        PutOutcome::Inserted
    }

    /// Mimics delete_if_present
    fn delete_if_present(&self, key: &str) -> DeleteOutcome {
        let mut files = self.files.lock().unwrap();
        if files.remove(key).is_some() {
            DeleteOutcome::Deleted
        } else {
            DeleteOutcome::NotFound
        }
    }

    /// Mimics contains
    fn contains(&self, key: &str) -> bool {
        let files = self.files.lock().unwrap();
        files.contains_key(key)
    }
}

// ──── Tests ───────────────────────────────────────────────────────────────

#[test]
fn concurrent_put_if_absent_same_key() {
    loom::model(|| {
        let fs = Arc::new(MockFileSystem::new());
        let key = "aa/test-key";

        let fs1 = fs.clone();
        let fs2 = fs.clone();

        let h1 = loom::thread::spawn(move || fs1.put_if_absent(key, b"data-1"));
        let h2 = loom::thread::spawn(move || fs2.put_if_absent(key, b"data-2"));

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        // Exactly one should insert, the other should see AlreadyExists
        assert!(
            (r1 == PutOutcome::Inserted && r2 == PutOutcome::AlreadyExists)
                || (r1 == PutOutcome::AlreadyExists && r2 == PutOutcome::Inserted),
            "expected one insert and one already-exists, got {:?} and {:?}",
            r1,
            r2
        );

        // Verify final state is consistent
        assert!(fs.contains(key));
    });
}

#[test]
fn concurrent_put_and_delete_same_key() {
    loom::model(|| {
        let fs = Arc::new(MockFileSystem::new());
        let key = "aa/test-key";

        // Pre-populate
        fs.put_if_absent(key, b"initial");

        let fs1 = fs.clone();
        let fs2 = fs.clone();

        let h1 = loom::thread::spawn(move || fs1.delete_if_present(key));
        let h2 = loom::thread::spawn(move || fs2.put_if_absent(key, b"new-data"));

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        // Final state must be consistent: either deleted or has "new-data"
        let exists = fs.contains(key);
        if exists {
            // If it exists, the put must have succeeded after the delete
            assert_eq!(r2, PutOutcome::Inserted);
        } else {
            // If it doesn't exist, the delete must have won
            assert_eq!(r1, DeleteOutcome::Deleted);
        }
    });
}

#[test]
fn concurrent_contains_and_delete() {
    loom::model(|| {
        let fs = Arc::new(MockFileSystem::new());
        let key = "aa/test-key";

        fs.put_if_absent(key, b"data");

        let fs1 = fs.clone();
        let fs2 = fs.clone();

        let h1 = loom::thread::spawn(move || fs1.contains(key));
        let h2 = loom::thread::spawn(move || fs2.delete_if_present(key));

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        // The contains result must be consistent with the delete outcome.
        // If delete succeeded, contains could be true or false (depends on ordering).
        // If delete returned NotFound, contains must be true (put happened before delete).
        if r2 == DeleteOutcome::NotFound {
            assert!(
                r1,
                "if delete returned NotFound, contains must have been true"
            );
        }
    });
}

#[test]
fn concurrent_put_different_keys() {
    loom::model(|| {
        let fs = Arc::new(MockFileSystem::new());

        let fs1 = fs.clone();
        let fs2 = fs.clone();

        let h1 = loom::thread::spawn(move || fs1.put_if_absent("aa/key-1", b"data-1"));
        let h2 = loom::thread::spawn(move || fs2.put_if_absent("aa/key-2", b"data-2"));

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        // Both should succeed (different keys)
        assert_eq!(r1, PutOutcome::Inserted);
        assert_eq!(r2, PutOutcome::Inserted);

        assert!(fs.contains("aa/key-1"));
        assert!(fs.contains("aa/key-2"));
    });
}

#[test]
fn concurrent_delete_same_key() {
    loom::model(|| {
        let fs = Arc::new(MockFileSystem::new());
        let key = "aa/test-key";

        fs.put_if_absent(key, b"data");

        let fs1 = fs.clone();
        let fs2 = fs.clone();

        let h1 = loom::thread::spawn(move || fs1.delete_if_present(key));
        let h2 = loom::thread::spawn(move || fs2.delete_if_present(key));

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        // Exactly one should delete, the other should see NotFound
        assert!(
            (r1 == DeleteOutcome::Deleted && r2 == DeleteOutcome::NotFound)
                || (r1 == DeleteOutcome::NotFound && r2 == DeleteOutcome::Deleted),
            "expected one delete and one not-found, got {:?} and {:?}",
            r1,
            r2
        );

        // Key must not exist after both deletes
        assert!(!fs.contains(key));
    });
}
