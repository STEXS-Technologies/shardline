//! Git protocol v2 upload-pack command parsing and packet responses.
use crate::error::HubApiError;
use crate::git::pktline;

pub(super) enum Request {
    LsRefs(LsRefs),
    Fetch(Fetch),
}

#[derive(Default)]
pub(super) struct LsRefs {
    pub(super) symrefs: bool,
    pub(super) unborn: bool,
    pub(super) ref_prefixes: Vec<String>,
}

pub(super) struct Fetch {
    pub(super) lines: Vec<Vec<u8>>,
    pub(super) done: bool,
}

/// Parse a protocol-v2 command. A non-v2 request returns `None`, leaving the
/// existing protocol-v0/v1 decoder and behavior unchanged.
pub(super) fn parse(bytes: &[u8]) -> Result<Option<Request>, HubApiError> {
    let (first, _) = first_packet(bytes)?;
    if first.starts_with(b"command") && !first.starts_with(b"command=") {
        return Err(invalid());
    }
    if !first.starts_with(b"command=") {
        return Ok(None);
    }
    let packets = decode(bytes)?;
    let Some(Some(command_line)) = packets.first() else {
        return Err(invalid());
    };
    let command = std::str::from_utf8(command_line)
        .map_err(|_error| invalid())?
        .strip_prefix("command=")
        .ok_or_else(invalid)?;
    let command = command.strip_suffix('\n').unwrap_or(command);
    let delimiter = packets
        .iter()
        .position(|packet| packet.is_none())
        .ok_or_else(invalid)?;
    if delimiter == 0 {
        return Err(invalid());
    }
    let capabilities = packets
        .get(1..delimiter)
        .ok_or_else(invalid)?
        .iter()
        .map(|packet| {
            let packet = packet.as_deref().ok_or_else(invalid)?;
            let line = std::str::from_utf8(packet).map_err(|_error| invalid())?;
            Ok(line.strip_suffix('\n').unwrap_or(line))
        })
        .collect::<Result<Vec<_>, HubApiError>>()?;
    validate_capabilities(&capabilities)?;
    let args = packets
        .get(delimiter.saturating_add(1)..)
        .ok_or_else(invalid)?
        .iter()
        .map(|packet| packet.as_ref().ok_or_else(invalid).cloned())
        .collect::<Result<Vec<_>, _>>()?;
    match command {
        "ls-refs" => parse_ls_refs(&args).map(Request::LsRefs).map(Some),
        "fetch" => parse_fetch(args).map(Request::Fetch).map(Some),
        _ => Err(HubApiError::BadRequest(
            "unsupported Git protocol v2 upload-pack command".to_owned(),
        )),
    }
}

pub(in crate::git::smart_http) fn is_requested(
    headers: &axum::http::HeaderMap,
) -> Result<bool, HubApiError> {
    let mut values = headers.get_all("git-protocol").iter();
    let Some(value) = values.next() else {
        return Ok(false);
    };
    if values.next().is_some() {
        return Err(invalid());
    }
    let value = value.to_str().map_err(|_error| invalid())?;
    let mut version = None;
    for parameter in value.split(':') {
        if let Some(requested) = parameter.strip_prefix("version=") {
            if version.replace(requested).is_some() {
                return Err(invalid());
            }
        } else if let Some(agent) = parameter.strip_prefix("agent=") {
            if agent.is_empty()
                || agent.len() > 255
                || !agent.bytes().all(|byte| (33..=126).contains(&byte))
            {
                return Err(invalid());
            }
        } else {
            return Err(invalid());
        }
    }
    match version {
        Some("2") => Ok(true),
        Some("0" | "1") | None => Ok(false),
        Some(_) => Err(HubApiError::BadRequest(
            "unsupported Git protocol version".to_owned(),
        )),
    }
}

pub(in crate::git::smart_http) fn advertisement() -> Result<Vec<u8>, HubApiError> {
    encode_lines(&[
        "version 2\n",
        "agent=shardline/1.12.1\n",
        "ls-refs\n",
        "fetch=shallow wait-for-done filter\n",
        "object-format=sha1\n",
    ])
}

pub(super) fn ls_refs_response(
    refs: &[super::super::ref_advertisement::GitRef],
    request: &LsRefs,
    default_symref: Option<&str>,
) -> Result<Vec<u8>, HubApiError> {
    let mut body = Vec::new();
    if request.unborn && refs.is_empty() {
        body.extend_from_slice(
            pktline::encode_line("unborn HEAD symref-target:refs/heads/main\n")?.as_bytes(),
        );
    }
    for reference in refs {
        if !request.ref_prefixes.is_empty()
            && !request
                .ref_prefixes
                .iter()
                .any(|prefix| reference.name.starts_with(prefix))
        {
            continue;
        }
        let mut attributes = Vec::new();
        if request.symrefs
            && reference.name == "HEAD"
            && let Some(target) = default_symref
        {
            attributes.push(format!("symref-target:{target}"));
        }
        // Shardline's Hub ref index currently stores commit refs rather than
        // annotated tag objects, so there is no peeled-object attribute.
        let suffix = if attributes.is_empty() {
            String::new()
        } else {
            format!(" {}", attributes.join(" "))
        };
        body.extend_from_slice(
            pktline::encode_line(&format!("{} {}{suffix}\n", reference.sha1, reference.name))?
                .as_bytes(),
        );
    }
    body.extend_from_slice(pktline::FLUSH.as_bytes());
    Ok(body)
}

pub(super) fn fetch_response(
    mut acknowledgments: Vec<String>,
    shallow_updates: &[u8],
    pack: Option<&[u8]>,
) -> Result<Vec<u8>, HubApiError> {
    let mut body = Vec::new();
    if let Some(pack) = pack {
        if !shallow_updates.is_empty() {
            let updates = shallow_updates
                .strip_suffix(pktline::FLUSH.as_bytes())
                .ok_or_else(invalid)?;
            if !updates.is_empty() {
                body.extend_from_slice(pktline::encode_line("shallow-info\n")?.as_bytes());
                body.extend_from_slice(updates);
            }
            if !updates.is_empty() {
                body.extend_from_slice(pktline::DELIMITER.as_bytes());
            }
        }
        body.extend_from_slice(pktline::encode_line("packfile\n")?.as_bytes());
        body.extend_from_slice(&pktline::sideband_data(pack));
        body.extend_from_slice(pktline::FLUSH.as_bytes());
    } else {
        body.extend_from_slice(pktline::encode_line("acknowledgments\n")?.as_bytes());
        if acknowledgments.is_empty() {
            body.extend_from_slice(pktline::encode_line("NAK\n")?.as_bytes());
        } else {
            acknowledgments.sort();
            acknowledgments.dedup();
            for oid in acknowledgments {
                body.extend_from_slice(pktline::encode_line(&format!("ACK {oid}\n"))?.as_bytes());
            }
        }
        body.extend_from_slice(pktline::FLUSH.as_bytes());
    }
    Ok(body)
}

fn parse_ls_refs(args: &[Vec<u8>]) -> Result<LsRefs, HubApiError> {
    let mut request = LsRefs::default();
    let mut peel = false;
    for argument in args {
        let line = std::str::from_utf8(argument).map_err(|_error| invalid())?;
        let line = line.strip_suffix('\n').unwrap_or(line);
        match line {
            "peel" if !peel => peel = true,
            "symrefs" if !request.symrefs => request.symrefs = true,
            "unborn" if !request.unborn => request.unborn = true,
            _ if line.starts_with("ref-prefix ") => {
                let prefix = line.strip_prefix("ref-prefix ").ok_or_else(invalid)?;
                if prefix.len() > 1024 || request.ref_prefixes.len() >= 1024 {
                    return Err(invalid());
                }
                request.ref_prefixes.push(prefix.to_owned());
            }
            _ => return Err(invalid()),
        }
    }
    Ok(request)
}

fn parse_fetch(args: Vec<Vec<u8>>) -> Result<Fetch, HubApiError> {
    let mut wants = 0usize;
    let mut done = false;
    let mut wait_for_done = false;
    let mut lines = Vec::with_capacity(args.len());
    for argument in args {
        let line = std::str::from_utf8(&argument).map_err(|_error| invalid())?;
        let line = line.strip_suffix('\n').unwrap_or(line);
        let fields = line.split_whitespace().collect::<Vec<_>>();
        match fields.as_slice() {
            ["want", oid] if valid_oid(oid) => wants = wants.saturating_add(1),
            ["have", oid] if valid_oid(oid) => {}
            ["done"] if !done => done = true,
            ["wait-for-done"] if !wait_for_done => wait_for_done = true,
            ["thin-pack"] | ["ofs-delta"] | ["no-progress"] | ["include-tag"] => {}
            ["shallow", oid] if valid_oid(oid) => {}
            ["deepen", depth] if depth.parse::<u32>().is_ok() => {}
            ["deepen-relative"] => {}
            ["deepen-since", timestamp]
                if !timestamp.is_empty() && timestamp.bytes().all(|byte| byte.is_ascii_digit()) => {
            }
            ["deepen-not", reference] if !reference.is_empty() && reference.len() <= 1024 => {}
            ["filter", filter] if !filter.is_empty() && filter.len() <= 128 => {}
            _ => return Err(invalid()),
        }
        lines.push(argument);
    }
    if wants == 0 {
        return Err(invalid());
    }
    Ok(Fetch { lines, done })
}

fn validate_capabilities(capabilities: &[&str]) -> Result<(), HubApiError> {
    let mut agent = false;
    let mut object_format = false;
    for capability in capabilities {
        if let Some(value) = capability.strip_prefix("agent=") {
            if agent
                || value.is_empty()
                || value.len() > 255
                || !value.bytes().all(|byte| (33..=126).contains(&byte))
            {
                return Err(invalid());
            }
            agent = true;
        } else if *capability == "object-format=sha1" && !object_format {
            object_format = true;
        } else {
            return Err(invalid());
        }
    }
    Ok(())
}

fn valid_oid(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn encode_lines(lines: &[&str]) -> Result<Vec<u8>, HubApiError> {
    let mut body = Vec::new();
    for line in lines {
        body.extend_from_slice(pktline::encode_line(line)?.as_bytes());
    }
    body.extend_from_slice(pktline::FLUSH.as_bytes());
    Ok(body)
}

fn first_packet(bytes: &[u8]) -> Result<(&[u8], usize), HubApiError> {
    let header = bytes.get(..4).ok_or_else(invalid)?;
    let length =
        usize::from_str_radix(std::str::from_utf8(header).map_err(|_error| invalid())?, 16)
            .map_err(|_error| invalid())?;
    if !(4..=65_520).contains(&length) {
        return Err(invalid());
    }
    let end = length;
    Ok((bytes.get(4..end).ok_or_else(invalid)?, end))
}

fn decode(bytes: &[u8]) -> Result<Vec<Option<Vec<u8>>>, HubApiError> {
    let mut packets = Vec::new();
    let mut offset = 0usize;
    let mut flushed = false;
    while offset < bytes.len() {
        let end_header = offset.checked_add(4).ok_or_else(invalid)?;
        let header = bytes.get(offset..end_header).ok_or_else(invalid)?;
        let length =
            usize::from_str_radix(std::str::from_utf8(header).map_err(|_error| invalid())?, 16)
                .map_err(|_error| invalid())?;
        if length > 65_520 {
            return Err(invalid());
        }
        offset = end_header;
        match length {
            0 => {
                if flushed || offset != bytes.len() {
                    return Err(invalid());
                }
                flushed = true;
            }
            1 => packets.push(None),
            2 | 3 => return Err(invalid()),
            _ => {
                let payload_length = length.checked_sub(4).ok_or_else(invalid)?;
                let end = offset.checked_add(payload_length).ok_or_else(invalid)?;
                packets.push(Some(bytes.get(offset..end).ok_or_else(invalid)?.to_vec()));
                offset = end;
            }
        }
        if flushed {
            break;
        }
    }
    if !flushed {
        return Err(invalid());
    }
    Ok(packets)
}

fn invalid() -> HubApiError {
    HubApiError::BadRequest("invalid Git protocol v2 upload-pack request".to_owned())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic, clippy::unwrap_used)]

    use super::*;

    fn request(lines: &[&str]) -> Vec<u8> {
        let mut body = Vec::new();
        for line in lines {
            body.extend_from_slice(pktline::encode_line(line).unwrap().as_bytes());
        }
        body.extend_from_slice(pktline::DELIMITER.as_bytes());
        for line in [
            "thin-pack\n",
            "no-progress\n",
            "ofs-delta\n",
            "want 0123456789012345678901234567890123456789\n",
            "done\n",
        ] {
            body.extend_from_slice(pktline::encode_line(line).unwrap().as_bytes());
        }
        body.extend_from_slice(pktline::FLUSH.as_bytes());
        body
    }

    #[test]
    fn parses_native_git_v2_fetch_request() {
        let body = request(&[
            "command=fetch\n",
            "agent=git/2.52.0-Linux\n",
            "object-format=sha1\n",
        ]);
        let Some(Request::Fetch(fetch)) = parse(&body).unwrap() else {
            panic!("expected protocol v2 fetch request");
        };
        assert!(fetch.done);
        assert_eq!(
            fetch
                .lines
                .iter()
                .filter(|line| line.starts_with(b"want "))
                .count(),
            1
        );
    }

    #[test]
    fn malformed_or_unsupported_v2_fetch_is_not_downgraded() {
        let mut body = request(&[
            "command=fetch\n",
            "agent=git/2.52.0-Linux\n",
            "object-format=sha1\n",
        ]);
        let before_flush = body.len().saturating_sub(pktline::FLUSH.len());
        body.splice(
            before_flush..before_flush,
            pktline::encode_line("want-ref refs/heads/main\n")
                .unwrap()
                .bytes(),
        );
        assert!(parse(&body).is_err());
    }

    #[test]
    fn legacy_upload_pack_request_is_left_to_existing_parser() {
        let body = pktline::encode_line("want 0123456789012345678901234567890123456789\n").unwrap();
        assert!(parse(body.as_bytes()).unwrap().is_none());
    }
}
