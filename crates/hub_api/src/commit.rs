use std::collections::HashMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::error::HubApiError;

/// Maximum allowed commit file path length.
const MAX_COMMIT_PATH_LEN: usize = 1024;

/// Maximum number of instructions allowed in a single NDJSON commit.
const MAX_COMMIT_INSTRUCTIONS: usize = 100_000;

/// Validates a commit file path to prevent path traversal and injection.
///
/// Rejects paths that:
/// - Contain `..` components
/// - Start with `/` (absolute paths)
/// - Contain null bytes
/// - Contain control characters (ASCII 0x00–0x1F, 0x7F)
/// - Exceed the maximum allowed length
fn validate_commit_path(path: &str) -> Result<(), HubApiError> {
    if path.len() > MAX_COMMIT_PATH_LEN {
        return Err(HubApiError::PathValidation(format!(
            "commit path exceeds maximum length of {MAX_COMMIT_PATH_LEN}"
        )));
    }

    if path.starts_with('/') {
        return Err(HubApiError::PathValidation(
            "commit path must not be absolute".to_owned(),
        ));
    }

    if path.contains('\0') {
        return Err(HubApiError::PathValidation(
            "commit path contains null byte".to_owned(),
        ));
    }

    if path.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(HubApiError::PathValidation(
            "commit path contains control characters".to_owned(),
        ));
    }

    // Check for path traversal via `..` components.
    for component in path.split('/') {
        if component == ".." {
            return Err(HubApiError::PathValidation(
                "commit path must not contain '..' components".to_owned(),
            ));
        }
    }

    Ok(())
}

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

        if instructions.len() >= MAX_COMMIT_INSTRUCTIONS {
            return Err(HubApiError::PathValidation(format!(
                "commit contains too many instructions (max {MAX_COMMIT_INSTRUCTIONS})"
            )));
        }

        if let Some(file) = parsed.get("file") {
            let path = file
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    HubApiError::PathValidation(format!("line {line_idx}: missing file path"))
                })?;
            validate_commit_path(path)?;
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
            validate_commit_path(path)?;
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
            validate_commit_path(path)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_commit_path_accepts_simple_file() {
        assert!(validate_commit_path("README.md").is_ok());
    }

    #[test]
    fn validate_commit_path_accepts_nested_path() {
        assert!(validate_commit_path("src/main.rs").is_ok());
    }

    #[test]
    fn validate_commit_path_rejects_absolute() {
        assert!(validate_commit_path("/etc/passwd").is_err());
    }

    #[test]
    fn validate_commit_path_rejects_dotdot() {
        assert!(validate_commit_path("../secret").is_err());
    }

    #[test]
    fn validate_commit_path_rejects_dotdot_in_middle() {
        assert!(validate_commit_path("src/../../secret").is_err());
    }

    #[test]
    fn validate_commit_path_rejects_null_byte() {
        assert!(validate_commit_path("file\0.rs").is_err());
    }

    #[test]
    fn validate_commit_path_rejects_control_char() {
        assert!(validate_commit_path("file\n.rs").is_err());
    }

    #[test]
    fn validate_commit_path_rejects_tab() {
        assert!(validate_commit_path("file\t.rs").is_err());
    }

    #[test]
    fn validate_commit_path_rejects_delete_char() {
        assert!(validate_commit_path("file\x7f.rs").is_err());
    }

    #[test]
    fn validate_commit_path_rejects_too_long() {
        let long = "a".repeat(2000);
        assert!(validate_commit_path(&long).is_err());
    }

    #[test]
    fn validate_commit_path_rejects_single_dotdot() {
        assert!(validate_commit_path("..").is_err());
    }
}
