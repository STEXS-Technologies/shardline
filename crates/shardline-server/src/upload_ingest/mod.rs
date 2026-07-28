pub(crate) mod cdc;
mod body_reader;
mod chunk_store;
mod ingestor;

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

pub(super) use body_reader::{RequestBodyReader, read_body_to_bytes};
pub(crate) use ingestor::FileUploadIngestor;

static UPLOAD_ATTEMPT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) fn upload_attempt_id(file_id: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let sequence = UPLOAD_ATTEMPT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("file-{file_id}-{timestamp:x}-{sequence:x}")
}
