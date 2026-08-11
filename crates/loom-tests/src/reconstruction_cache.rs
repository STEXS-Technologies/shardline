//! Loom model-checking tests for the reconstruction-cache load-once contract.
//!
//! Covers `crates/shardline-server/src/reconstruction_cache.rs`
//! (`ReconstructionCacheService::get_or_load`) together with the
//! `MemoryReconstructionCache::get_or_load` deduplication it relies on:
//!
//! * Among N concurrent requesters that arrive in a single wave for the same key,
//!   exactly one executes the loader (`loader_calls` == 1). The loader gate is an
//!   `AtomicUsize` counter driven by a CAS loop — the "become the exclusive
//!   loader" step.
//! * All non-loading requesters wait on a barrier (Mutex + Condvar) and observe
//!   the loaded value, or — when the single loader fails — all observe the error.
//!
//! A loom `Barrier` synchronizes the start of the wave so every requester
//! contends for the same load slot. This models the real pattern (AtomicUsize CAS
//! gate + barrier) more faithfully than the abstract `atomic_counters` /
//! `barrier_init` models elsewhere in this crate.
//!
//! # Coverage
//!
//! The real `ReconstructionCacheService` is async (it awaits the cache adapter and
//! `tokio::sync::Notify`). Loom cannot drive Tokio futures or the single-waiter
//! `Notify`, so this module models the *contract* with `loom::sync` primitives
//! (RwLock cache + Mutex/Condvar barrier). It does NOT execute the real
//! `ReconstructionCacheService` or `MemoryReconstructionCache`; it requires async
//! runtime support loom lacks.

use loom::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Acquires a loom `Mutex` guard without `unwrap()` (loom mutexes never poison).
fn lock_guard<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn read_guard<T>(r: &RwLock<T>) -> loom::sync::RwLockReadGuard<'_, T> {
    match r.read() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn write_guard<T>(r: &RwLock<T>) -> loom::sync::RwLockWriteGuard<'_, T> {
    match r.write() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn cv_wait<'guard, T>(
    cv: &Condvar,
    guard: MutexGuard<'guard, T>,
) -> MutexGuard<'guard, T> {
    match cv.wait(guard) {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// A simple N-way rendezvous used to start a synchronized wave of requesters.
/// (loom does not implement `std::sync::Barrier`, so we build one with a
/// Mutex + Condvar countdown.)
struct StartGate {
    count: Mutex<usize>,
    cv: Condvar,
    n: usize,
}

impl StartGate {
    fn new(n: usize) -> Self {
        Self {
            count: Mutex::new(0),
            cv: Condvar::new(),
            n,
        }
    }

    /// Blocks until all `n` participants have arrived, then releases them all.
    fn wait(&self) {
        let mut count = lock_guard(&self.count);
        *count += 1;
        if *count == self.n {
            self.cv.notify_all();
        } else {
            while *count < self.n {
                count = cv_wait(&self.cv, count);
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LoadError {
    LoaderFailed,
}

/// Models the load-once contract: a RwLock cache, an `AtomicUsize` CAS gate for
/// "be the loader", and a Mutex + Condvar barrier that wakes all waiters.
struct LoadOnceCache {
    value: RwLock<Option<u64>>,
    /// 0 = idle, 1 = a loader is running. Transitioned via CAS.
    loading: AtomicUsize,
    /// Barrier: set to true once the single loader has published its outcome.
    ready: (Mutex<bool>, Condvar),
    /// Number of times the loader executed.
    loader_calls: AtomicUsize,
    /// When set, the loader reports failure (mirrors a failed backend load).
    fail: bool,
}

impl LoadOnceCache {
    fn new(fail: bool) -> Self {
        Self {
            value: RwLock::new(None),
            loading: AtomicUsize::new(0),
            ready: (Mutex::new(false), Condvar::new()),
            loader_calls: AtomicUsize::new(0),
            fail,
        }
    }

    fn get_or_load(&self) -> Result<u64, LoadError> {
        // Fast path: a previously loaded value.
        if let Some(v) = *read_guard(&self.value) {
            return Ok(v);
        }

        // CAS loop to become the exclusive loader for this wave.
        loop {
            let current = self.loading.load(Ordering::SeqCst);
            if current == 1 {
                // Another requester is the loader; wait on the barrier.
                let (mtx, cv) = &self.ready;
                let mut ready = lock_guard(mtx);
                while !*ready {
                    ready = cv_wait(cv, ready);
                }
                let value = *read_guard(&self.value);
                if self.fail {
                    return Err(LoadError::LoaderFailed);
                }
                return value.ok_or(LoadError::LoaderFailed);
            }
            if self
                .loading
                .compare_exchange(current, 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // This requester is the exclusive loader for the wave.
                self.loader_calls.fetch_add(1, Ordering::SeqCst);
                let (mtx, cv) = &self.ready;
                if self.fail {
                    // Publish failure to all waiters via the barrier. The gate stays
                    // at 1 so the rest of this synchronized wave observes the
                    // in-flight load and waits, rather than becoming latecomer
                    // loaders. (A genuinely new request after the wave would reset
                    // the gate and retry — out of scope for this single-wave model.)
                    *lock_guard(mtx) = true;
                    cv.notify_all();
                    return Err(LoadError::LoaderFailed);
                }
                *write_guard(&self.value) = Some(42);
                *lock_guard(mtx) = true;
                cv.notify_all();
                self.loading.store(0, Ordering::SeqCst);
                return Ok(42);
            }
        }
    }
}

#[test]
fn exactly_one_loader_all_requesters_see_value() {
    loom::model(|| {
        let cache = Arc::new(LoadOnceCache::new(false));
        let gate = Arc::new(StartGate::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let c = cache.clone();
                let g = gate.clone();
                loom::thread::spawn(move || {
                    g.wait();
                    assert_eq!(c.get_or_load(), Ok(42));
                })
            })
            .collect();
        for h in handles {
            assert!(h.join().is_ok());
        }
        assert_eq!(
            cache.loader_calls.load(Ordering::SeqCst),
            1,
            "the loader must execute exactly once under N concurrent requesters"
        );
    });
}

#[test]
fn loader_error_is_observed_by_all_requesters() {
    loom::model(|| {
        let cache = Arc::new(LoadOnceCache::new(true));
        let gate = Arc::new(StartGate::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let c = cache.clone();
                let g = gate.clone();
                loom::thread::spawn(move || {
                    g.wait();
                    assert_eq!(c.get_or_load(), Err(LoadError::LoaderFailed));
                })
            })
            .collect();
        for h in handles {
            assert!(h.join().is_ok());
        }
        // Exactly one loader ran for the wave, and every requester observed its
        // failure.
        assert_eq!(cache.loader_calls.load(Ordering::SeqCst), 1);
    });
}
