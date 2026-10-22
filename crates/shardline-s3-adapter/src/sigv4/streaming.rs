//! Incremental AWS chunk and checksum-trailer authentication without buffering data.
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use shardline_protocol::SecretBytes;

use super::trailers::{Algorithm, TrailerVerifier};
use super::{S3Error, denied};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StreamingContext {
    pub(super) key: SecretBytes,
    pub(super) timestamp: String,
    pub(super) scope: String,
    pub(super) previous: [u8; 32],
    pub(super) length: u64,
    pub(super) trailers: Vec<Algorithm>,
    pub(super) unsigned_chunks: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Line,
    TrailerLine,
    Data(u64),
    Separator(u8, bool),
    Done,
    Failed,
}

/// Checks the selected streaming mode against verified request headers.
/// Buffers at most one 1 KiB framing line, independent of chunk or upload size.
/// Signed trailer mode additionally checks declared checksums and their signature.
/// Unsigned trailer mode checks checksums without a body MAC and requires TLS.
#[must_use = "Feed all wire bytes and require successful finish before publication"]
pub struct SigV4ChunkVerifier {
    context: StreamingContext,
    phase: Phase,
    line: Vec<u8>,
    hasher: Sha256,
    signature: [u8; 32],
    decoded: u64,
    trailers: TrailerVerifier,
}

impl SigV4ChunkVerifier {
    pub(super) fn new(context: StreamingContext) -> Self {
        Self {
            trailers: TrailerVerifier::new(&context.trailers, !context.unsigned_chunks),
            context,
            phase: Phase::Line,
            line: Vec::new(),
            hasher: Sha256::new(),
            signature: [0; 32],
            decoded: 0,
        }
    }

    /// Verifies each completed chunk and retains partial framing/hash state.
    ///
    /// # Errors
    /// Rejects malformed framing, excess bytes or a broken signature chain.
    /// Any error permanently poisons the verifier.
    pub fn update(&mut self, bytes: &[u8]) -> Result<(), S3Error> {
        let result = self.consume(bytes);
        if result.is_err() {
            self.phase = Phase::Failed;
        }
        result
    }

    /// Confirms termination, required signature/checksum checks and decoded length.
    ///
    /// # Errors
    /// Rejects truncation, missing final chunks and decoded-length mismatches.
    pub fn finish(&self) -> Result<(), S3Error> {
        if self.phase == Phase::Done && self.decoded == self.context.length {
            Ok(())
        } else {
            Err(denied())
        }
    }

    fn consume(&mut self, mut bytes: &[u8]) -> Result<(), S3Error> {
        if self.phase == Phase::Failed {
            return Err(denied());
        }
        while !bytes.is_empty() {
            match self.phase {
                Phase::Line | Phase::TrailerLine => {
                    let (byte, rest) = bytes.split_first().ok_or_else(denied)?;
                    bytes = rest;
                    if *byte == b'\n' {
                        // Native MinIO signed trailers use LF checksum lines.
                        // Chunk headers and the trailer MAC retain strict CRLF.
                        if self.line.last() == Some(&b'\r') {
                            self.line.pop();
                        } else if self.phase != Phase::TrailerLine
                            || self.context.unsigned_chunks
                            || !self.line.starts_with(b"x-amz-checksum-")
                        {
                            return Err(denied());
                        }
                        if self.phase == Phase::TrailerLine {
                            if self.trailers.accept_line(
                                &self.line,
                                self.context.key.expose_secret(),
                                &self.context.timestamp,
                                &self.context.scope,
                                &self.context.previous,
                            )? {
                                self.phase = Phase::Done;
                            }
                        } else {
                            self.begin_chunk()?;
                        }
                        self.line.clear();
                    } else {
                        if self.line.len() >= 1024 {
                            return Err(denied());
                        }
                        self.line.push(*byte);
                    }
                }
                Phase::Data(remaining) => {
                    let count =
                        remaining.min(u64::try_from(bytes.len()).map_err(|_overflow| denied())?);
                    let count_usize = usize::try_from(count).map_err(|_overflow| denied())?;
                    if !self.context.unsigned_chunks {
                        self.hasher
                            .update(bytes.get(..count_usize).ok_or_else(denied)?);
                    }
                    self.trailers
                        .update(bytes.get(..count_usize).ok_or_else(denied)?);
                    bytes = bytes.get(count_usize..).ok_or_else(denied)?;
                    self.decoded = self.decoded.checked_add(count).ok_or_else(denied)?;
                    let remaining = remaining.checked_sub(count).ok_or_else(denied)?;
                    self.phase = if remaining == 0 {
                        self.verify_chunk()?;
                        Phase::Separator(0, false)
                    } else {
                        Phase::Data(remaining)
                    };
                }
                Phase::Separator(index, last) => {
                    let (byte, rest) = bytes.split_first().ok_or_else(denied)?;
                    let expected = match index {
                        0 => b'\r',
                        1 => b'\n',
                        _ => return Err(denied()),
                    };
                    if *byte != expected {
                        return Err(denied());
                    }
                    bytes = rest;
                    self.phase = if index == 0 {
                        Phase::Separator(1, last)
                    } else if last {
                        Phase::Done
                    } else {
                        Phase::Line
                    };
                }
                Phase::Done | Phase::Failed => return Err(denied()),
            }
        }
        Ok(())
    }

    fn begin_chunk(&mut self) -> Result<(), S3Error> {
        let line = std::str::from_utf8(&self.line).map_err(|_invalid_line| denied())?;
        let (size, signature) = if self.context.unsigned_chunks {
            (line, None)
        } else {
            let (size, signature) = line.split_once(";chunk-signature=").ok_or_else(denied)?;
            (size, Some(signature))
        };
        if size.is_empty()
            || size.len() > 16
            || !size.bytes().all(|byte| byte.is_ascii_hexdigit())
            || signature.is_some_and(|signature| signature.len() != 64)
        {
            return Err(denied());
        }
        let size = u64::from_str_radix(size, 16).map_err(|_invalid_size| denied())?;
        if self
            .decoded
            .checked_add(size)
            .is_none_or(|length| length > self.context.length)
        {
            return Err(denied());
        }
        if let Some(signature) = signature {
            self.signature = hex::decode(signature)
                .map_err(|_invalid_signature| denied())?
                .try_into()
                .map_err(|_invalid_length| denied())?;
        }
        if size == 0 {
            self.verify_chunk()?;
            if self.decoded != self.context.length {
                return Err(denied());
            }
            self.phase = if self.context.trailers.is_empty() {
                Phase::Separator(0, true)
            } else {
                Phase::TrailerLine
            };
        } else {
            self.phase = Phase::Data(size);
        }
        Ok(())
    }

    fn verify_chunk(&mut self) -> Result<(), S3Error> {
        if self.context.unsigned_chunks {
            return Ok(());
        }
        let digest = std::mem::take(&mut self.hasher).finalize();
        let to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            self.context.timestamp,
            self.context.scope,
            hex::encode(self.context.previous),
            hex::encode(Sha256::digest(b"")),
            hex::encode(digest)
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(self.context.key.expose_secret())
            .map_err(|_invalid_key| denied())?;
        mac.update(to_sign.as_bytes());
        mac.verify_slice(&self.signature)
            .map_err(|_invalid_signature| denied())?;
        self.context.previous = self.signature;
        Ok(())
    }
}
