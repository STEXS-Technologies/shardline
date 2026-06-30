use std::collections::HashMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::error::HubApiError;

/// A parsed commit instruction.
#[derive(Debug, Clone)]
pub enum CommitInstruction {
    /// Inline file content.
    InlineFile {
        /// Relative path.
        path: String,
        /// Decoded content bytes.
        content: Vec<u8>,
    },
    /// LFS pointer reference.
    LfsPointer {
        /// Relative path.
        path: String,
        /// LFS OID (SHA-256).
        oid: String,
        /// File size.
        size: u64,
    },
    /// File deletion.
    Delete {
        /// Relative path.
        path: String,
    },
}

/// Parsed commit request.
#[derive(Debug, Clone)]
pub struct ParsedCommit {
    /// Commit message.
    pub message: String,
    /// Parent revision SHA (if any).
    pub parent_commit: Option<String>,
    /// Parsed instructions.
    pub instructions: Vec<CommitInstruction>,
}

/// Parses a streaming NDJSON commit body.
///
/// Expected lines:
/// - `{"header":{"message":"...","parentCommit":"..."}}`
/// - `{"file":{"path":"...","content":"<base64>"}}`
/// - `{"lfsFile":{"path":"...","oid":"...","size":123}}`
/// - `{"deletedEntry":{"path":"..."}}`
///
/// # Errors
///
/// Returns [`HubApiError`] on malformed input.
pub fn parse_ndjson_commit(body: &str) -> Result<ParsedCommit, HubApiError> {
    let mut message = String::new();
    let mut parent_commit = None;
    let mut instructions = Vec::new();

    for (line_idx, line) in body.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parsed: HashMap<String, serde_json::Value> =
            serde_json::from_str(trimmed).map_err(HubApiError::Json)?;

        if let Some(header) = parsed.get("header") {
            if let Some(msg) = header.get("message").and_then(|v| v.as_str()) {
                message = msg.to_owned();
            }
            if let Some(parent) = header.get("parentCommit").and_then(|v| v.as_str())
                && !parent.is_empty()
            {
                parent_commit = Some(parent.to_owned());
            }
            continue;
        }

        if let Some(file) = parsed.get("file") {
            let path = file
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    HubApiError::PathValidation(format!("line {line_idx}: missing file path"))
                })?;
            let content_b64 = file
                .get("content")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    HubApiError::PathValidation(format!("line {line_idx}: missing file content"))
                })?;
            let content = STANDARD.decode(content_b64).map_err(|e| {
                HubApiError::PathValidation(format!("line {line_idx}: invalid base64: {e}"))
            })?;
            instructions.push(CommitInstruction::InlineFile {
                path: path.to_owned(),
                content,
            });
            continue;
        }

        if let Some(lfs_file) = parsed.get("lfsFile") {
            let path = lfs_file
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    HubApiError::PathValidation(format!("line {line_idx}: missing lfsFile path"))
                })?;
            let oid = lfs_file
                .get("oid")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    HubApiError::PathValidation(format!("line {line_idx}: missing lfsFile oid"))
                })?;
            let size = lfs_file
                .get("size")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| {
                    HubApiError::PathValidation(format!("line {line_idx}: missing lfsFile size"))
                })?;
            instructions.push(CommitInstruction::LfsPointer {
                path: path.to_owned(),
                oid: oid.to_owned(),
                size,
            });
            continue;
        }

        if let Some(deleted) = parsed.get("deletedEntry") {
            let path = deleted
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    HubApiError::PathValidation(format!(
                        "line {line_idx}: missing deletedEntry path"
                    ))
                })?;
            instructions.push(CommitInstruction::Delete {
                path: path.to_owned(),
            });
            continue;
        }

        return Err(HubApiError::PathValidation(format!(
            "line {line_idx}: unknown commit instruction type"
        )));
    }

    if message.is_empty() {
        return Err(HubApiError::PathValidation(
            "commit header with message is required".to_owned(),
        ));
    }

    Ok(ParsedCommit {
        message,
        parent_commit,
        instructions,
    })
}

/// Validates an LFS OID format (64 lowercase hex characters for SHA-256).
///
/// # Errors
///
/// Returns [`HubApiError::PathValidation`] if the OID is malformed.
pub fn validate_lfs_oid(oid: &str) -> Result<(), HubApiError> {
    if oid.len() != 64
        || !oid
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
    {
        return Err(HubApiError::PathValidation(format!(
            "invalid LFS OID: {oid}"
        )));
    }
    Ok(())
}
