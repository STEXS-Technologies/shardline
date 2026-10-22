//! Strict receive-pack command framing, validated before payload publication.
use std::collections::HashSet;

use crate::error::HubApiError;

pub(in crate::git::smart_http) type RefUpdate = (String, String, String);

pub(in crate::git::smart_http) fn parse(
    body: &[u8],
) -> Result<(Vec<RefUpdate>, &[u8]), HubApiError> {
    let invalid = || HubApiError::BadRequest("malformed receive-pack commands".to_owned());
    let mut updates = Vec::new();
    let mut names = HashSet::new();
    let mut position: usize = 0;
    loop {
        let header = body
            .get(position..position.saturating_add(4))
            .ok_or_else(invalid)?;
        if !header.iter().all(u8::is_ascii_hexdigit) {
            return Err(invalid());
        }
        let length =
            usize::from_str_radix(std::str::from_utf8(header).map_err(|_error| invalid())?, 16)
                .map_err(|_error| invalid())?;
        position = position.saturating_add(4);
        if length == 0 {
            return Ok((updates, body.get(position..).ok_or_else(invalid)?));
        }
        if !(5..=65520).contains(&length) || updates.len() >= 10_000 {
            return Err(invalid());
        }
        let end = position.saturating_add(length.saturating_sub(4));
        let line = std::str::from_utf8(body.get(position..end).ok_or_else(invalid)?)
            .map_err(|_error| invalid())?;
        position = end;
        let line = line.strip_suffix('\n').unwrap_or(line);
        let command = if let Some((command, capabilities)) = line.split_once('\0') {
            if !updates.is_empty() || capabilities.bytes().any(|byte| byte < b' ' || byte == 127) {
                return Err(invalid());
            }
            command
        } else {
            line
        };
        let mut fields = command.split(' ');
        let old = fields.next().ok_or_else(invalid)?;
        let new = fields.next().ok_or_else(invalid)?;
        let name = fields.next().ok_or_else(invalid)?;
        let valid_oid =
            |oid: &str| oid.len() == 40 && oid.bytes().all(|byte| byte.is_ascii_hexdigit());
        if fields.next().is_some()
            || !valid_oid(old)
            || !valid_oid(new)
            || !super::super::ref_advertisement::is_valid_refname(name)
            || !names.insert(name.to_owned())
        {
            return Err(invalid());
        }
        // Git object IDs are case insensitive; storage identities are lowercase.
        updates.push((
            old.to_ascii_lowercase(),
            new.to_ascii_lowercase(),
            name.to_owned(),
        ));
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::git::pktline;

    #[test]
    fn malformed_tail_never_yields_partial_commands() {
        let first = format!(
            "{} {} refs/heads/feature\0report-status\n",
            "a".repeat(40),
            "0".repeat(40)
        );
        let prefix = pktline::encode_line(&first).unwrap();
        let malformed = [
            "bad command".to_owned(),
            format!("{} {} refs/heads/../bad", "0".repeat(40), "b".repeat(40)),
            format!("{} {} refs/heads/feature", "a".repeat(40), "0".repeat(40)),
            format!(
                "{} {} refs/heads/other extra",
                "a".repeat(40),
                "b".repeat(40)
            ),
            format!(
                "{} {} refs/heads/other\0report-status",
                "a".repeat(40),
                "b".repeat(40)
            ),
            format!("{} {} refs/heads/other", "a".repeat(39), "b".repeat(40)),
        ];
        for line in malformed {
            let request = format!("{prefix}{}0000", pktline::encode_line(&line).unwrap());
            assert!(parse(request.as_bytes()).is_err(), "accepted {line:?}");
        }
        for tail in [
            b"0001".as_slice(),
            b"0002",
            b"0003",
            b"0004",
            b"ffff",
            b"zzzz",
            b"0008x",
            b"0005\xff",
            b"",
        ] {
            let mut request = prefix.as_bytes().to_vec();
            request.extend_from_slice(tail);
            assert!(parse(&request).is_err());
        }
    }

    #[test]
    fn optional_lf_uppercase_ids_and_binary_pack_preserve_boundaries() {
        let line = format!(
            "{} {} refs/heads/feature\0report-status side-band-64k",
            "A".repeat(40),
            "B".repeat(40)
        );
        for ending in ["", "\n"] {
            let mut request = pktline::encode_line(&format!("{line}{ending}"))
                .unwrap()
                .into_bytes();
            request.extend_from_slice(b"0000PACK\0\xff0000");
            let (updates, pack) = parse(&request).unwrap();
            assert_eq!(
                updates,
                vec![(
                    "a".repeat(40),
                    "b".repeat(40),
                    "refs/heads/feature".to_owned()
                )]
            );
            assert_eq!(pack, b"PACK\0\xff0000");
        }
    }
}
