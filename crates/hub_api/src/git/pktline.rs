//! Git pkt-line format encoding.
//!
//! The pkt-line format is used by Git Smart HTTP protocol for framing
//! messages. Each packet is prefixed with a 4-hex-digit length (including
//! the 4-byte prefix itself). The flush packet `0000` signals end of a
//! section.

/// Maximum pkt-line payload size (65516 bytes).
const MAX_PAYLOAD: usize = 0xFFFF - 4;

/// Encodes a single pkt-line: 4-hex length + content.
///
/// # Errors
///
/// Returns `Err` if `line` exceeds 65516 bytes (pkt-line max payload).
pub fn encode_line(line: &str) -> Result<String, PktLineError> {
    if line.len() > MAX_PAYLOAD {
        return Err(PktLineError::PayloadTooLarge {
            size: line.len(),
            max: MAX_PAYLOAD,
        });
    }
    let len = line.len().wrapping_add(4);
    Ok(format!("{len:04x}{line}"))
}

/// Encodes a pkt-line with raw bytes (for binary content like SHA1 hashes).
///
/// # Errors
///
/// Returns `Err` if `data` exceeds 65516 bytes.
pub fn encode_line_bytes(data: &[u8]) -> Result<String, PktLineError> {
    if data.len() > MAX_PAYLOAD {
        return Err(PktLineError::PayloadTooLarge {
            size: data.len(),
            max: MAX_PAYLOAD,
        });
    }
    let len = data.len().wrapping_add(4);
    let mut out = format!("{len:04x}");
    out.push_str(&String::from_utf8_lossy(data));
    Ok(out)
}

/// Pkt-line encoding error.
#[derive(Debug, Clone)]
pub enum PktLineError {
    /// Payload exceeds the 65516-byte pkt-line limit.
    PayloadTooLarge {
        /// Actual payload size.
        size: usize,
        /// Maximum allowed size.
        max: usize,
    },
}

impl std::fmt::Display for PktLineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PayloadTooLarge { size, max } => {
                write!(f, "pkt-line payload too large: {size} bytes (max {max})")
            }
        }
    }
}

impl std::error::Error for PktLineError {}

/// Encodes a flush packet (`0000`).
pub const FLUSH: &str = "0000";

/// Encodes a delimiter packet (`0001`).
pub const DELIMITER: &str = "0001";

/// Encodes a response-end packet (`0002`).
pub const RESPONSE_END: &str = "0002";

/// Builds a sideband multiplexed response.
///
/// Git Smart HTTP uses sideband channels:
/// - `1`: pack data
/// - `2`: progress/error messages
/// - `3`: fatal error
#[must_use]
pub fn sideband_data(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    // Each chunk is at most 65516 bytes of payload
    for chunk in data.chunks(65516) {
        let len = chunk.len().wrapping_add(5); // 4-byte length prefix + 1-byte channel
        out.extend_from_slice(format!("{len:04x}").as_bytes());
        out.push(b'1'); // channel 1 = pack data
        out.extend_from_slice(chunk);
    }
    out
}

/// Wraps a message in sideband channel 2 (progress).
#[must_use]
pub fn sideband_progress(msg: &str) -> Vec<u8> {
    let len = msg.len().wrapping_add(5);
    let mut out = format!("{len:04x}").into_bytes();
    out.push(b'2');
    out.extend_from_slice(msg.as_bytes());
    out
}

/// Wraps a message in sideband channel 3 (fatal).
#[must_use]
pub fn sideband_fatal(msg: &str) -> Vec<u8> {
    let len = msg.len().wrapping_add(5);
    let mut out = format!("{len:04x}").into_bytes();
    out.push(b'3');
    out.extend_from_slice(msg.as_bytes());
    out
}

/// Decodes pkt-line packets from raw request body bytes.
///
/// Returns a list of decoded lines (without the 4-byte length prefix).
/// Stops at flush packet (`0000`).
#[must_use]
pub fn decode_lines(data: &[u8]) -> Vec<Vec<u8>> {
    let mut lines = Vec::new();
    let mut pos: usize = 0;

    // SAFETY: guarded by `data.len()` comparison on this same line
    while pos.wrapping_add(4) <= data.len() {
        // SAFETY: guarded by `pos + 4 <= data.len()` check in the while condition
        let Some(hex_len) = data.get(pos..pos.wrapping_add(4)) else {
            break;
        };
        let Ok(hex_str) = std::str::from_utf8(hex_len) else {
            break;
        };
        let Ok(len) = u16::from_str_radix(hex_str, 16) else {
            break;
        };

        if len == 0 {
            // flush
            break;
        }

        // SAFETY: checked against data.len() on this same line
        if (len as usize) < 4 || pos.wrapping_add(len as usize) > data.len() {
            break;
        }

        // SAFETY: guarded by the bounds check immediately above
        let Some(payload) = data.get(pos.wrapping_add(4)..pos.wrapping_add(len as usize)) else {
            break;
        };
        lines.push(payload.to_vec());
        // SAFETY: len is bounded by pkt-line max payload (65520 bytes), so wrapping is safe
        pos = pos.wrapping_add(len as usize);
    }

    lines
}

/// Decodes sideband multiplexed data.
///
/// Returns the reassembled data from channel 1 (pack data).
/// Channel 2 (progress) and channel 3 (fatal) are logged but not returned.
#[must_use]
pub fn decode_sideband(data: &[u8]) -> (Vec<u8>, Vec<String>) {
    let mut pack_data = Vec::new();
    let mut messages = Vec::new();
    let mut pos: usize = 0;

    // SAFETY: guarded by `data.len()` comparison on this same line
    while pos.wrapping_add(4) <= data.len() {
        // SAFETY: guarded by `pos + 4 <= data.len()` check in the while condition
        let Some(hex_len) = data.get(pos..pos.wrapping_add(4)) else {
            break;
        };
        let Ok(hex_str) = std::str::from_utf8(hex_len) else {
            break;
        };
        let Ok(len) = u16::from_str_radix(hex_str, 16) else {
            break;
        };

        if len == 0 {
            break;
        }

        // SAFETY: checked against data.len() on this same line
        if (len as usize) < 5 || pos.wrapping_add(len as usize) > data.len() {
            break;
        }

        // SAFETY: guarded by the bounds check immediately above (len >= 5, so pos + 5 <= pos + len <= data.len())
        let Some(&channel) = data.get(pos.wrapping_add(4)) else {
            break;
        };
        // SAFETY: guarded by the same bounds check above
        let Some(payload) = data.get(pos.wrapping_add(5)..pos.wrapping_add(len as usize)) else {
            break;
        };

        match channel {
            b'1' => pack_data.extend_from_slice(payload),
            b'2' | b'3' => {
                if let Ok(msg) = std::str::from_utf8(payload) {
                    messages.push(msg.to_owned());
                }
            }
            _ => {}
        }

        // SAFETY: len is bounded by pkt-line max payload (65520 bytes), so wrapping is safe
        pos = pos.wrapping_add(len as usize);
    }

    (pack_data, messages)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn encode_simple_line() {
        let encoded = encode_line("hello\n").expect("short line should not fail");
        assert_eq!(encoded, "000ahello\n");
    }

    #[test]
    fn encode_flush() {
        assert_eq!(FLUSH, "0000");
    }

    #[test]
    fn decode_simple_line() {
        let data = b"000ahello\n";
        let lines = decode_lines(data);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], b"hello\n");
    }

    #[test]
    fn decode_flush() {
        let data = b"0000";
        let lines = decode_lines(data);
        assert!(lines.is_empty());
    }

    #[test]
    fn sideband_roundtrip() {
        let original = b"pack data here";
        let multiplexed = sideband_data(original);
        let (decoded, _) = decode_sideband(&multiplexed);
        assert_eq!(decoded, original);
    }

    #[test]
    fn sideband_progress_is_channel_2() {
        let msg = sideband_progress("working");
        assert_eq!(msg[4], b'2');
    }
}
