use std::sync::atomic::AtomicU64;

pub static XORB_BLOCK_SIZE: AtomicU64 = AtomicU64::new(16 * 1024 * 1024);
pub static TARGET_CHUNK_SIZE: AtomicU64 = AtomicU64::new(128 * 1024);
pub static MAX_CHUNK_SIZE: AtomicU64 = AtomicU64::new(16 * 1024 * 1024);
