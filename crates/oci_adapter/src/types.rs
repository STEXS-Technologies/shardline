use serde::{Deserialize, Serialize};
use sha2::{compress256, digest::generic_array::GenericArray};
use std::{fs::File, sync::LazyLock};
use tokio::sync::{Mutex, MutexGuard};

use crate::OciAdapterError;

pub(crate) const OCI_UPLOAD_DIR: &str = "oci-uploads";
pub(crate) const OCI_S3_MULTIPART_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const SHA256_INITIAL_STATE: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];
pub(crate) static OCI_UPLOAD_SESSION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub struct OciUploadSessionLock {
    pub(crate) _process_guard: MutexGuard<'static, ()>,
    pub(crate) _file_lock: OciFileLock,
}

pub(crate) struct OciFileLock {
    pub(crate) file: File,
}

impl Drop for OciFileLock {
    fn drop(&mut self) {
        let _ignored = self.file.unlock();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciUploadSession {
    pub repository: String,
    #[serde(default = "global_scope_namespace")]
    pub scope_namespace: String,
    pub created_at_unix_seconds: u64,
    pub last_touched_unix_seconds: u64,
    #[serde(default)]
    pub use_s3_multipart: bool,
    #[serde(default)]
    pub s3_multipart: Option<OciS3MultipartUploadSession>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciS3MultipartUploadSession {
    pub temporary_object_key: String,
    pub upload_id: String,
    pub uploaded_part_ids: Vec<String>,
    pub total_length: u64,
    pub sha256_state: SerializableSha256State,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableSha256State {
    state: [u32; 8],
    total_length: u64,
    buffer: Vec<u8>,
}

impl Default for SerializableSha256State {
    fn default() -> Self {
        Self {
            state: SHA256_INITIAL_STATE,
            total_length: 0,
            buffer: Vec::new(),
        }
    }
}

impl SerializableSha256State {
    pub(crate) fn update(&mut self, bytes: &[u8]) -> Result<(), OciAdapterError> {
        self.total_length =
            shardline_server_core::checked_add(self.total_length, u64::try_from(bytes.len())?)
                .map_err(|_e| OciAdapterError::Overflow)?;
        let mut remaining = bytes;
        if !self.buffer.is_empty() {
            let needed = 64_usize.saturating_sub(self.buffer.len());
            let to_take = needed.min(remaining.len());
            let (consumed, rest) = remaining.split_at(to_take);
            self.buffer.extend_from_slice(consumed);
            remaining = rest;
            if self.buffer.len() == 64 {
                let block: [u8; 64] = self
                    .buffer
                    .as_slice()
                    .try_into()
                    .map_err(|_error| OciAdapterError::Overflow)?;
                self.compress_block(&block);
                self.buffer.clear();
            }
        }

        let mut chunks = remaining.chunks_exact(64);
        for chunk in &mut chunks {
            let block: [u8; 64] = chunk
                .try_into()
                .map_err(|_error| OciAdapterError::Overflow)?;
            self.compress_block(&block);
        }
        self.buffer.extend_from_slice(chunks.remainder());
        Ok(())
    }

    pub(crate) fn finalize_hex(&self) -> Result<String, OciAdapterError> {
        Ok(hex::encode(self.finalize_bytes()?))
    }

    fn compress_block(&mut self, block: &[u8; 64]) {
        let generic = GenericArray::clone_from_slice(block);
        compress256(&mut self.state, &[generic]);
    }

    fn finalize_bytes(&self) -> Result<[u8; 32], OciAdapterError> {
        let mut state = self.state;
        let mut buffer = self.buffer.clone();
        buffer.push(0x80);
        while buffer.len() % 64 != 56 {
            buffer.push(0);
        }
        let bit_length = self
            .total_length
            .checked_mul(8)
            .ok_or(OciAdapterError::Overflow)?;
        buffer.extend_from_slice(&bit_length.to_be_bytes());
        for chunk in buffer.chunks_exact(64) {
            let block: [u8; 64] = chunk
                .try_into()
                .map_err(|_error| OciAdapterError::Overflow)?;
            let generic = GenericArray::clone_from_slice(&block);
            compress256(&mut state, &[generic]);
        }
        let mut output = [0_u8; 32];
        for (chunk, value) in output.chunks_exact_mut(4).zip(state.iter()) {
            chunk.copy_from_slice(&value.to_be_bytes());
        }
        Ok(output)
    }
}

pub(crate) fn global_scope_namespace() -> String {
    "global".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OciReference {
    Digest(String),
    Tag(String),
}
