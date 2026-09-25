//! Loom concurrency model-checking tests for Shardline synchronization patterns.
//!
//! These tests model the real concurrent patterns used throughout the Shardline
//! codebase and verify they are free of deadlocks, livelocks, and data races
//! under ALL possible thread interleavings (bounded model checking).
//!
//! Run with: `RUSTFLAGS="--cfg loom" cargo test -p shardline-loom-tests`

#![cfg(loom)]

mod cas_coordinator;
mod gc_quarantine;
mod models;
mod object_store;
mod reconstruction_cache;
