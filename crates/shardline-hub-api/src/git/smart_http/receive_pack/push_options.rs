//! Parser for the optional receive-pack push-options phase.
//!
//! Shardline currently has no Git receive-hook interface, so there is no hook
//! consumer to retain these values for. We still validate and consume the
//! negotiated section before pack parsing, matching Git's no-hook behavior.

use crate::error::HubApiError;

const MAX_PACKET_LENGTH: usize = 65_520;

pub(super) fn skip_section(mut input: &[u8]) -> Result<&[u8], HubApiError> {
    let invalid = || HubApiError::BadRequest("malformed receive-pack push-options".to_owned());
    loop {
        let header = input.get(..4).ok_or_else(invalid)?;
        if header == b"0000" {
            return input.get(4..).ok_or_else(invalid);
        }
        if !header.iter().all(u8::is_ascii_hexdigit) {
            return Err(invalid());
        }
        let length =
            usize::from_str_radix(std::str::from_utf8(header).map_err(|_error| invalid())?, 16)
                .map_err(|_error| invalid())?;
        if !(5..=MAX_PACKET_LENGTH).contains(&length) {
            return Err(invalid());
        }
        let payload_length = length.checked_sub(4).ok_or_else(invalid)?;
        let payload = input.get(4..length).ok_or_else(invalid)?;
        // Git push-option is 1*(VCHAR | SP); preserve option bytes as opaque
        // values while rejecting NUL, control characters, and non-ASCII data.
        if payload.len() != payload_length
            || !payload
                .iter()
                .all(|byte| *byte == b' ' || (b'!'..=b'~').contains(byte))
        {
            return Err(invalid());
        }
        input = input.get(length..).ok_or_else(invalid)?;
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn consumes_multiple_options_and_preserves_pack_bytes() {
        let request = b"000bci.skip001bshardline.label=release0000PACK\0binary";
        let remaining = skip_section(request).unwrap();
        assert_eq!(remaining, b"PACK\0binary");
    }

    #[test]
    fn consumes_an_empty_option_section() {
        assert_eq!(skip_section(b"0000PACK").unwrap(), b"PACK");
    }

    #[test]
    fn rejects_empty_invalid_and_unterminated_options() {
        for request in [
            b"0004".as_slice(),
            b"0005\0",
            b"0005\n",
            b"0005\x7f",
            b"0005\xff",
            b"0005x",
            b"0008short",
            b"000z",
            b"000cvalid",
        ] {
            assert!(skip_section(request).is_err(), "accepted {request:?}");
        }
    }
}
