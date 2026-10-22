//! Refuse unimplemented transfer semantics before any Git publication.
use crate::error::HubApiError;

#[derive(Default)]
pub(super) struct Receive {
    pub sideband: bool,
    pub atomic: bool,
}

pub(super) fn receive(body: &[u8]) -> Result<Receive, HubApiError> {
    let mut negotiated = Receive::default();
    for line in super::super::pktline::decode_lines(body) {
        let Ok(line) = std::str::from_utf8(&line) else {
            continue;
        };
        let Some((_, capabilities)) = line.split_once('\0') else {
            continue;
        };
        for capability in capabilities.split_whitespace() {
            if capability == "side-band-64k" {
                negotiated.sideband = true;
            }
            if capability == "atomic" {
                negotiated.atomic = true;
            }
            if matches!(capability, "push-options" | "side-band") {
                return Err(HubApiError::BadRequest(format!(
                    "unsupported receive-pack capability: {capability}"
                )));
            }
        }
    }
    Ok(negotiated)
}

pub(super) fn upload(lines: &[Vec<u8>]) -> Result<(), HubApiError> {
    for line in lines {
        let Ok(line) = std::str::from_utf8(line) else {
            continue;
        };
        let command = line.split_whitespace().next().unwrap_or("");
        if matches!(command, "deepen-not" | "filter") || command.starts_with("command=") {
            return Err(HubApiError::BadRequest(format!(
                "unsupported upload-pack command: {command}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_push_modes_are_rejected_before_updates_are_processed() {
        for capability in ["push-options", "side-band"] {
            let line = format!(
                "{} {} refs/heads/main\0report-status {capability}\n",
                "0".repeat(40),
                "1".repeat(40)
            );
            let body = super::super::super::pktline::encode_line(&line).unwrap();
            assert!(receive(body.as_bytes()).is_err());
        }
        assert!(receive(b"0000").is_ok());
        let line = format!(
            "{} {} refs/heads/main\0report-status-v2\n",
            "0".repeat(40),
            "1".repeat(40)
        );
        assert!(
            receive(
                super::super::super::pktline::encode_line(&line)
                    .unwrap()
                    .as_bytes()
            )
            .is_ok()
        );
    }

    #[test]
    fn unsupported_fetch_semantics_are_not_silently_ignored() {
        for command in ["deepen-not main", "filter blob:none", "command=fetch"] {
            assert!(upload(&[command.as_bytes().to_vec()]).is_err());
        }
        assert!(upload(&[b"want deadbeef side-band-64k".to_vec(), b"done".to_vec()]).is_ok());
    }

    #[test]
    fn receive_sideband_requires_explicit_negotiation() {
        assert!(!receive(b"0000").unwrap().sideband);
        assert!(!receive(b"0000").unwrap().atomic);
        for (capabilities, expected) in [
            ("report-status", false),
            ("report-status-v2 side-band-64k", true),
        ] {
            let command = format!(
                "{} {} refs/heads/main\0{capabilities}\n",
                "0".repeat(40),
                "1".repeat(40)
            );
            let framed = super::super::super::pktline::encode_line(&command).unwrap();
            assert_eq!(receive(framed.as_bytes()).unwrap().sideband, expected);
        }
        let command = format!(
            "{} {} refs/heads/main\0report-status atomic\n",
            "0".repeat(40),
            "1".repeat(40)
        );
        let framed = super::super::super::pktline::encode_line(&command).unwrap();
        assert!(receive(framed.as_bytes()).unwrap().atomic);
    }
}
