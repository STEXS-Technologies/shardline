// Loom concurrency model-checking tests for Shardline synchronization patterns.
#![cfg(loom)]

use loom::sync::{Arc, Condvar, Mutex, RwLock};
use loom::thread;
use std::sync::atomic::{AtomicUsize, Ordering};

mod loading_dedup {
    use super::*;

    #[derive(Clone)]
    struct CacheSim {
        loaded: Arc<AtomicUsize>,
        lock: Arc<RwLock<Option<u64>>>,
        ready: Arc<(Mutex<bool>, Condvar)>,
    }

    impl CacheSim {
        fn new() -> Self {
            Self {
                loaded: Arc::new(AtomicUsize::new(0)),
                lock: Arc::new(RwLock::new(None)),
                ready: Arc::new((Mutex::new(false), Condvar::new())),
            }
        }

        fn get_or_load(&self) -> u64 {
            if let Some(value) = *self.lock.read().unwrap() {
                return value;
            }
            let mut write = self.lock.write().unwrap();
            if let Some(value) = *write {
                return value;
            }
            self.loaded.fetch_add(1, Ordering::SeqCst);
            let value = 42u64;
            *write = Some(value);
            let (lock, cvar) = &*self.ready;
            *lock.lock().unwrap() = true;
            cvar.notify_all();
            value
        }

        fn wait_and_get(&self) -> u64 {
            let (lock, cvar) = &*self.ready;
            let mut ready = lock.lock().unwrap();
            while !*ready {
                ready = cvar.wait(ready).unwrap();
            }
            self.lock.read().unwrap().unwrap()
        }
    }

    #[test]
    fn loading_dedup_exactly_one_load() {
        loom::model(|| {
            let cache = CacheSim::new();
            let cache1 = cache.clone();
            let cache2 = cache.clone();
            let h1 = thread::spawn(move || { let v = cache1.get_or_load(); assert_eq!(v, 42); });
            let h2 = thread::spawn(move || { let v = cache2.wait_and_get(); assert_eq!(v, 42); });
            h1.join().unwrap();
            h2.join().unwrap();
            assert_eq!(cache.loaded.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn loading_dedup_concurrent_access() {
        loom::model(|| {
            let cache = CacheSim::new();
            let handles: Vec<_> = (0..2).map(|_| {
                let c = cache.clone();
                thread::spawn(move || { let v = c.get_or_load(); assert_eq!(v, 42); })
            }).collect();
            for h in handles { h.join().unwrap(); }
            assert!(cache.loaded.load(Ordering::SeqCst) >= 1);
        });
    }
}

mod atomic_counter {
    use super::*;

    struct CounterSim { counter: AtomicUsize }

    impl CounterSim {
        fn new() -> Self { Self { counter: AtomicUsize::new(0) } }
        fn next(&self) -> usize { self.counter.fetch_add(1, Ordering::SeqCst) }
    }

    #[test]
    fn counter_never_duplicates() {
        loom::model(|| {
            let counter = Arc::new(CounterSim::new());
            let c1 = counter.clone();
            let c2 = counter.clone();
            let h1 = thread::spawn(move || { let v1 = c1.next(); assert!(v1 < 2); });
            let h2 = thread::spawn(move || { let v2 = c2.next(); assert!(v2 < 2); });
            h1.join().unwrap();
            h2.join().unwrap();
            assert_eq!(counter.counter.load(Ordering::SeqCst), 2);
        });
    }

    #[test]
    fn counter_concurrent_increments() {
        loom::model(|| {
            let counter = Arc::new(CounterSim::new());
            let handles: Vec<_> = (0..2).map(|_| {
                let c = counter.clone();
                thread::spawn(move || { c.next(); })
            }).collect();
            for h in handles { h.join().unwrap(); }
            assert_eq!(counter.counter.load(Ordering::SeqCst), 2);
        });
    }
}

mod condvar_notify {
    use super::*;

    #[test]
    fn condvar_broadcast_wakes_all() {
        loom::model(|| {
            let pair = Arc::new((Mutex::new(false), Condvar::new()));
            let loaded = Arc::new(AtomicUsize::new(0));
            let mut handles: Vec<_> = (0..1).map(|_| {
                let p = pair.clone();
                let l = loaded.clone();
                thread::spawn(move || {
                    let (lock, cvar) = &*p;
                    let mut ready = lock.lock().unwrap();
                    while !*ready { ready = cvar.wait(ready).unwrap(); }
                    l.fetch_add(1, Ordering::SeqCst);
                })
            }).collect();
            let p = pair.clone();
            let l = loaded.clone();
            handles.push(thread::spawn(move || {
                let (lock, cvar) = &*p;
                *lock.lock().unwrap() = true;
                cvar.notify_all();
                l.fetch_add(1, Ordering::SeqCst);
            }));
            for h in handles { h.join().unwrap(); }
            assert_eq!(loaded.load(Ordering::SeqCst), 2);
        });
    }
}

