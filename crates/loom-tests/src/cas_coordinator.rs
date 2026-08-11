//! Loom model-checking tests for the CAS coordinator concurrency contract.
//!
//! Covers `crates/shardline-cas/src/coordinator.rs` (`CasCoordinator`) and
//! `crates/shardline-cas/src/reachability.rs` (`ObjectReachability`):
//!
//! * `store_content_addressed_blob` forwards to `object_store.put_if_absent` and
//!   must be idempotent / exactly-once under concurrent callers for the same key:
//!   among N concurrent stores, exactly one reports `Inserted` and the object is
//!   durably present.
//! * `is_object_reachable` consults the index (`contains_object`). Combined with a
//!   concurrent quarantine sweep the intended invariant is: a sweep never frees an
//!   object a concurrent store just made reachable (no free-during-insert), and a
//!   reachable object is always present (no dangling index reference).
//!
//! # Coverage
//!
//! The real `CasCoordinator` is async (it awaits `AsyncObjectStore` /
//! `AsyncIndexStore` behind Tokio). Loom cannot drive Tokio futures, so this
//! module models the *contract* — the intended concurrency property — with
//! `loom::sync` primitives. It does NOT execute the real `CasCoordinator`.
//!
//! Modeled: exactly-once `put_if_absent` semantics and the reachability-vs-sweep
//! invariant, where store (make present + register reachable) and sweep (free only
//! provably-unreachable objects) are serialized on consistent guards.
//!
//! Not modeled: the durable upload-intent lifecycle (`with_upload_intent`), the
//! async object-store / index adapters, and crash-recovery ordering. If a sweep
//! were allowed to free based on a *stale* orphan snapshot while a concurrent
//! store registers reachability, loom would surface a reachable-but-absent
//! (dangling) state; the intended contract prevents this by serializing
//! registration and sweep on the same guard.

use loom::sync::{Arc, Mutex, MutexGuard};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

/// Acquires a loom `Mutex`, recovering the guard without `unwrap()`.
/// Loom's mutex never poisons, so the error arm is never taken.
fn lock_guard<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Models the coordinator's two stores: a durable object store (presence via
/// `put_if_absent`) and a separate reachability index (`contains_object`).
///
/// A single mutex serializes registration and sweep so that store and quarantine
/// observe a consistent view — this is the intended protection against
/// free-during-insert.
struct CasCoordinator {
    /// Object store: key -> present.
    objects: Mutex<HashMap<u64, ()>>,
    /// Reachability index consulted by `is_object_reachable`.
    reachable: Mutex<HashSet<u64>>,
}

impl CasCoordinator {
    fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
            reachable: Mutex::new(HashSet::new()),
        }
    }

    /// Mirrors `store_content_addressed_blob` -> `put_if_absent`. Returns `true`
    /// (`Inserted`) only when this caller wins the store; otherwise `false`
    /// (`AlreadyExists`). A winning store also registers the object as reachable.
    fn store_content_addressed_blob(&self, id: u64) -> bool {
        // Registering reachability while still holding the object-store guard keeps
        // present + reachable atomic w.r.t. a concurrent sweep.
        let mut objects = lock_guard(&self.objects);
        if objects.contains_key(&id) {
            return false; // AlreadyExists
        }
        objects.insert(id, ());
        lock_guard(&self.reachable).insert(id);
        true
    }

    /// Mirrors `ObjectReachability::is_object_reachable` -> index `contains_object`.
    fn is_object_reachable(&self, id: u64) -> bool {
        lock_guard(&self.reachable).contains(&id)
    }

    fn is_present(&self, id: u64) -> bool {
        lock_guard(&self.objects).contains_key(&id)
    }

    /// Mirrors a GC quarantine sweep: frees an object only when it is provably
    /// unreachable. Held in the same order as `store_content_addressed_blob`, so a
    /// just-registered (reachable) object is never freed.
    fn sweep(&self, id: u64) {
        let mut objects = lock_guard(&self.objects);
        let reachable = lock_guard(&self.reachable);
        if !reachable.contains(&id) {
            objects.remove(&id);
        }
    }
}

#[test]
fn concurrent_store_content_addressed_blob_is_exactly_once() {
    loom::model(|| {
        let coordinator = Arc::new(CasCoordinator::new());
        let inserted = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let c1 = coordinator.clone();
        let i1 = inserted.clone();
        let h1 = loom::thread::spawn(move || {
            if c1.store_content_addressed_blob(42) {
                i1.fetch_add(1, Ordering::SeqCst);
            }
        });

        let c2 = coordinator.clone();
        let i2 = inserted.clone();
        let h2 = loom::thread::spawn(move || {
            if c2.store_content_addressed_blob(42) {
                i2.fetch_add(1, Ordering::SeqCst);
            }
        });

        assert!(h1.join().is_ok());
        assert!(h2.join().is_ok());

        // Exactly one caller reports Inserted; the other sees AlreadyExists.
        assert_eq!(
            inserted.load(Ordering::SeqCst),
            1,
            "exactly one concurrent store must win"
        );
        // The object is durably present and reachable.
        assert!(coordinator.is_present(42), "inserted object must be present");
        assert!(
            coordinator.is_object_reachable(42),
            "inserted object must be reachable"
        );
    });
}

#[test]
fn sweep_never_frees_a_reachable_object() {
    loom::model(|| {
        let coordinator = Arc::new(CasCoordinator::new());
        let store_ran = Arc::new(AtomicBool::new(false));

        let c1 = coordinator.clone();
        let s1 = store_ran.clone();
        let h1 = loom::thread::spawn(move || {
            if c1.store_content_addressed_blob(9) {
                s1.store(true, Ordering::SeqCst);
            }
        });

        let c2 = coordinator.clone();
        let h2 = loom::thread::spawn(move || c2.sweep(9));

        assert!(h1.join().is_ok());
        assert!(h2.join().is_ok());

        // The store is the sole writer of a fresh key, so it must have won.
        assert!(store_ran.load(Ordering::SeqCst), "fresh key must be inserted");

        // Free-during-insert: a sweep must never free an object a concurrent
        // store just made reachable. A reachable object is always present.
        assert!(
            coordinator.is_present(9),
            "free-during-insert: just-registered object was swept"
        );
        assert!(
            coordinator.is_object_reachable(9),
            "registered object must be reachable"
        );
        // Corollary of the same invariant: reachable ⟹ present (no dangling ref).
        if coordinator.is_object_reachable(9) {
            assert!(coordinator.is_present(9));
        }
    });
}
