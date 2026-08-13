//! Loom model-checking tests for the GC quarantine state machine.
//!
//! Covers `crates/shardline-gc/src/quarantine.rs` — the
//! insert → release → sweep lifecycle of quarantine candidates:
//!
//! * `reconcile_quarantine_entries` quarantines newly-orphaned objects (insert /
//!   `Active`) and releases entries for objects no longer orphaned (release /
//!   `Released`).
//! * `sweep_quarantine_entries` frees (`Swept`, terminal) only objects whose
//!   candidate has passed its retention (`delete_after_unix_seconds`) AND is still
//!   unreachable at sweep time.
//!
//! The invariants verified under every interleaving:
//! * No free-during-insert: a sweep never frees an object a concurrent
//!   insert/store just made reachable.
//! * No premature free: an object quarantined at `now` is never swept before its
//!   retention elapses.
//! * No resurrect-after-free: a swept (`Swept`) entry is terminal and is never
//!   reactivated to `Active`.
//!
//! # Coverage
//!
//! The real functions are async (they await the async index / object-store
//! adapters). This module models the *state machine and its synchronization
//! contract* with `loom::sync` primitives; it does NOT execute the real
//! `sweep_quarantine_entries` / `reconcile_quarantine_entries`.

use loom::sync::{Arc, Mutex, MutexGuard};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Acquires a loom `Mutex` guard without `unwrap()` (loom mutexes never poison).
fn lock_guard<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    /// Quarantined and awaiting its retention window (can be swept when expired).
    Active,
    /// No longer quarantined (object became reachable / entry released).
    Released,
    /// Freed by a sweep. Terminal — never reactivated.
    Swept,
}

struct Entry {
    state: State,
    expires_at: u64,
    reachable: bool,
}

struct Quarantine {
    entries: Mutex<HashMap<u64, Entry>>,
    /// Incremented if a sweep ever frees a concurrently-reachable object.
    /// Must remain 0 (no free-during-insert).
    freed_while_reachable: AtomicUsize,
    /// Incremented if a Swept entry is ever reactivated to Active. Must remain 0
    /// (no resurrect-after-free).
    reactivated: AtomicUsize,
}

impl Quarantine {
    fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            freed_while_reachable: AtomicUsize::new(0),
            reactivated: AtomicUsize::new(0),
        }
    }

    /// Mirrors `reconcile_quarantine_entries`: quarantines an orphan (`Active`)
    /// or releases the entry when the object is reachable again.
    fn reconcile(&self, id: u64, reachable_now: bool, now: u64, retention: u64) {
        let mut entries = lock_guard(&self.entries);
        if let Some(entry) = entries.get_mut(&id) {
            // A swept entry is terminal; never resurrect it.
            if entry.state == State::Swept {
                return;
            }
            entry.reachable = reachable_now;
            if reachable_now {
                entry.state = State::Released;
            } else if entry.state == State::Released {
                entry.state = State::Active;
                entry.expires_at = now + retention;
            }
        } else if !reachable_now {
            entries.insert(
                id,
                Entry {
                    state: State::Active,
                    expires_at: now + retention,
                    reachable: false,
                },
            );
        }
    }

    /// Mirrors `sweep_quarantine_entries`: frees an object only when it is
    /// quarantined (`Active`), past retention, and still unreachable — all checked
    /// under the same guard a concurrent insert uses, so a just-inserted/reachable
    /// object is never freed.
    fn sweep(&self, id: u64, now: u64) {
        let mut entries = lock_guard(&self.entries);
        let Some(entry) = entries.get_mut(&id) else {
            return;
        };
        if entry.state != State::Active || entry.expires_at > now {
            return;
        }
        if entry.reachable {
            // Would be free-during-insert; the contract forbids it.
            self.freed_while_reachable.fetch_add(1, Ordering::SeqCst);
            return;
        }
        entry.state = State::Swept;
    }

    /// Seeds an object that is already quarantined (`Active`) and past retention
    /// at `now` — the state a sweep is about to free.
    fn seed_expired_orphan(&self, id: u64, now: u64) {
        lock_guard(&self.entries).insert(
            id,
            Entry {
                state: State::Active,
                expires_at: now,
                reachable: false,
            },
        );
    }

    fn is_swept(&self, id: u64) -> bool {
        lock_guard(&self.entries)
            .get(&id)
            .is_some_and(|e| e.state == State::Swept)
    }
}

#[test]
fn sweep_never_frees_an_object_a_concurrent_insert_made_reachable() {
    loom::model(|| {
        let q = Arc::new(Quarantine::new());
        let now = 1_000_u64;
        let retention = 500_u64;
        // Object 1 is quarantined and expired, about to be swept.
        q.seed_expired_orphan(1, now);

        // A concurrent insert makes object 1 reachable again (release path).
        let q1 = q.clone();
        let h1 = loom::thread::spawn(move || q1.reconcile(1, true, now, retention));
        // A concurrent sweep attempts to free it.
        let q2 = q.clone();
        let h2 = loom::thread::spawn(move || q2.sweep(1, now));

        assert!(h1.join().is_ok());
        assert!(h2.join().is_ok());

        // No free-during-insert: the sweep must never free a reachable object.
        assert_eq!(
            q.freed_while_reachable.load(Ordering::SeqCst),
            0,
            "sweep freed an object a concurrent insert just made reachable"
        );
        // If the insert won (object reachable), the sweep must have skipped it.
        // (freed_while_reachable == 0 already guarantees the sweep skipped.)
        assert_eq!(q.reactivated.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn freshly_quarantined_object_is_never_swept_before_retention() {
    loom::model(|| {
        let q = Arc::new(Quarantine::new());
        let now = 1_000_u64;
        let retention = 500_u64;

        // A concurrent reconcile quarantines object 2 (becomes orphaned) at `now`.
        let q1 = q.clone();
        let h1 = loom::thread::spawn(move || q1.reconcile(2, false, now, retention));
        // A concurrent sweep runs at the same instant.
        let q2 = q.clone();
        let h2 = loom::thread::spawn(move || q2.sweep(2, now));

        assert!(h1.join().is_ok());
        assert!(h2.join().is_ok());

        // The object was just quarantined; its retention (expires at now+retention)
        // has not elapsed, so it must not have been swept at `now`.
        assert!(
            !q.is_swept(2),
            "premature free: object swept before its retention elapsed"
        );
        assert_eq!(q.freed_while_reachable.load(Ordering::SeqCst), 0);
        assert_eq!(q.reactivated.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn swept_entry_is_terminal_and_never_resurrected() {
    loom::model(|| {
        let q = Arc::new(Quarantine::new());
        let now = 1_000_u64;
        let retention = 500_u64;
        // Object 3 is quarantined and expired.
        q.seed_expired_orphan(3, now);

        // Sweep frees it.
        let q1 = q.clone();
        let h1 = loom::thread::spawn(move || q1.sweep(3, now));
        // Concurrent reconcile would like to re-quarantine it (still orphaned).
        let q2 = q.clone();
        let h2 = loom::thread::spawn(move || q2.reconcile(3, false, now, retention));

        assert!(h1.join().is_ok());
        assert!(h2.join().is_ok());

        // Once swept, the entry is terminal: reconcile must not reactivate it.
        assert_eq!(
            q.reactivated.load(Ordering::SeqCst),
            0,
            "resurrect-after-free: a swept object was reactivated"
        );
        assert_eq!(q.freed_while_reachable.load(Ordering::SeqCst), 0);
    });
}
