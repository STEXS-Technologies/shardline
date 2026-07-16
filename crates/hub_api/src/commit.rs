use std::collections::HashMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::error::HubApiError;

/// Maximum allowed commit file path length.
const MAX_COMMIT_PATH_LEN: usize = 1024;

/// Maximum number of instructions allowed in a single NDJSON commit.
const MAX_COMMIT_INSTRUCTIONS: usize = 100_000;

/// Maximum decoded size (in bytes) for a single inline file in a commit.
/// Files exceeding this limit must use LFS.
const MAX_INLINE_FILE_BYTES: usize = 10 * 1024 * 1024; // 10 MiB

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
/// - `{"header":{"message":"...","parentCommit":"..."}}` (or `"summary"` instead of `"message"`)
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
            // Accept both "message" (internal) and "summary" (HuggingFace Hub spec).
            if let Some(msg) = header
                .get("message")
                .or_else(|| header.get("summary"))
                .and_then(|v| v.as_str())
            {
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
            let path = file.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
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
            if content.len() > MAX_INLINE_FILE_BYTES {
                return Err(HubApiError::PathValidation(format!(
                    "line {line_idx}: inline file size {} exceeds maximum of {MAX_INLINE_FILE_BYTES} bytes; use LFS for large files",
                    content.len()
                )));
            }
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
            "commit header with message (or summary) is required".to_owned(),
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

    // --- parse_ndjson_commit tests ---

    #[test]
    #[allow(clippy::wildcard_enum_match_arm, clippy::panic)]
    fn parse_ndjson_valid_single_file() {
        let content_b64 = STANDARD.encode(b"hello world");
        let body = format!(
            "{{\"header\":{{\"message\":\"test commit\",\"parentCommit\":\"\"}}}}\n\
             {{\"file\":{{\"path\":\"README.md\",\"content\":\"{content_b64}\"}}}}"
        );
        let result = parse_ndjson_commit(&body).unwrap();
        assert_eq!(result.message, "test commit");
        assert!(result.parent_commit.is_none());
        assert_eq!(result.instructions.len(), 1);
        match &result.instructions[0] {
            CommitInstruction::InlineFile { path, content } => {
                assert_eq!(path, "README.md");
                assert_eq!(content, b"hello world");
            }
            other => panic!("expected InlineFile, got {other:?}"),
        }
    }

    #[test]
    fn parse_ndjson_accepts_summary_alias() {
        let content_b64 = STANDARD.encode(b"content via summary");
        let body = format!(
            "{{\"header\":{{\"summary\":\"uses summary field\",\"parentCommit\":\"\"}}}}\n\
             {{\"file\":{{\"path\":\"doc.md\",\"content\":\"{content_b64}\"}}}}"
        );
        let result = parse_ndjson_commit(&body).unwrap();
        assert_eq!(result.message, "uses summary field");
        assert_eq!(result.instructions.len(), 1);
    }

    #[test]
    fn parse_ndjson_message_takes_precedence_over_summary() {
        let body = "\
            {\"header\":{\"message\":\"from message\",\"summary\":\"from summary\",\"parentCommit\":\"\"}}";
        let result = parse_ndjson_commit(body).unwrap();
        assert_eq!(result.message, "from message");
    }

    #[test]
    #[allow(clippy::wildcard_enum_match_arm, clippy::panic)]
    fn parse_ndjson_valid_multiple_files() {
        let b1 = STANDARD.encode(b"file one");
        let b2 = STANDARD.encode(b"file two");
        let b3 = STANDARD.encode(b"file three");
        let body = format!(
            "{{\"header\":{{\"message\":\"multi-file\",\"parentCommit\":\"abc123\"}}}}\n\
             {{\"file\":{{\"path\":\"a.txt\",\"content\":\"{b1}\"}}}}\n\
             {{\"file\":{{\"path\":\"b.txt\",\"content\":\"{b2}\"}}}}\n\
             {{\"file\":{{\"path\":\"c.txt\",\"content\":\"{b3}\"}}}}"
        );
        let result = parse_ndjson_commit(&body).unwrap();
        assert_eq!(result.message, "multi-file");
        assert_eq!(result.parent_commit.as_deref(), Some("abc123"));
        assert_eq!(result.instructions.len(), 3);
        for (i, name) in ["a.txt", "b.txt", "c.txt"].iter().enumerate() {
            match &result.instructions[i] {
                CommitInstruction::InlineFile { path, .. } => assert_eq!(path, *name),
                other => panic!("expected InlineFile at index {i}, got {other:?}"),
            }
        }
    }

    #[test]
    #[allow(clippy::wildcard_enum_match_arm, clippy::panic)]
    fn parse_ndjson_valid_delete() {
        let body = "\
            {\"header\":{\"message\":\"delete file\",\"parentCommit\":\"\"}}\n\
            {\"deletedEntry\":{\"path\":\"old_file.txt\"}}";
        let result = parse_ndjson_commit(body).unwrap();
        assert_eq!(result.instructions.len(), 1);
        match &result.instructions[0] {
            CommitInstruction::Delete { path } => assert_eq!(path, "old_file.txt"),
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    #[allow(clippy::wildcard_enum_match_arm, clippy::panic)]
    fn parse_ndjson_valid_lfs_pointer() {
        let body = "\
            {\"header\":{\"message\":\"lfs file\",\"parentCommit\":\"\"}}\n\
            {\"lfsFile\":{\"path\":\"model.bin\",\"oid\":\"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789\",\"size\":1048576}}";
        let result = parse_ndjson_commit(body).unwrap();
        assert_eq!(result.instructions.len(), 1);
        match &result.instructions[0] {
            CommitInstruction::LfsPointer { path, oid, size } => {
                assert_eq!(path, "model.bin");
                assert_eq!(
                    oid,
                    "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
                );
                assert_eq!(*size, 1048576);
            }
            other => panic!("expected LfsPointer, got {other:?}"),
        }
    }

    #[test]
    fn parse_ndjson_missing_header() {
        let _b = STANDARD.encode(b"data");
        let body = "{{\"file\":{{\"path\":\"f.txt\",\"content\":\"{b}\"}}}}".to_string();
        let result = parse_ndjson_commit(&body);
        assert!(result.is_err());
    }

    #[test]
    fn parse_ndjson_invalid_json() {
        let body = "{{not valid json}}";
        let result = parse_ndjson_commit(body);
        assert!(result.is_err());
    }

    #[test]
    #[allow(clippy::unreachable)]
    fn parse_ndjson_oversized_file() {
        // MAX_INLINE_FILE_BYTES is 10 MiB.  Generate a valid base64 string
        // that decodes to exactly MAX_INLINE_FILE_BYTES + 1 bytes.
        // "AAAA" encodes 3 zero bytes (4 base64 chars).
        // Remainder 2 bytes: "AAA=" encodes 2 zero bytes with proper padding.
        let target_decoded = MAX_INLINE_FILE_BYTES + 1;
        let full_chunks = target_decoded / 3; // 3,495,253
        let remainder = target_decoded % 3; // 2
        let mut content_b64 = String::with_capacity(full_chunks * 4 + 4);
        for _ in 0..full_chunks {
            content_b64.push_str("AAAA");
        }
        match remainder {
            0 => {}
            1 => content_b64.push_str("AA=="),
            2 => content_b64.push_str("AAA="),
            _ => unreachable!(),
        }

        let body = format!(
            "{{\"header\":{{\"message\":\"big file\",\"parentCommit\":\"\"}}}}\n\
             {{\"file\":{{\"path\":\"big.bin\",\"content\":\"{content_b64}\"}}}}"
        );
        let result = parse_ndjson_commit(&body);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("exceeds maximum"),
            "expected oversized error, got: {err_msg}"
        );
    }

    // --- parse_ndjson edge cases ---

    #[test]
    fn parse_ndjson_unknown_instruction_type() {
        let body = "{\"header\":{\"message\":\"test\",\"parentCommit\":\"\"}}\n\
                     {\"unknownType\":{\"path\":\"x.txt\"}}";
        let result = parse_ndjson_commit(body);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unknown commit instruction"),
            "expected unknown instruction error, got: {err_msg}"
        );
    }

    #[test]
    fn parse_ndjson_too_many_instructions() {
        let content_b64 = STANDARD.encode(b"x");
        // Build a body with MAX_COMMIT_INSTRUCTIONS + 1 file instructions
        let mut body =
            format!("{{\"header\":{{\"message\":\"too many\",\"parentCommit\":\"\"}}}}\n");
        for _ in 0..MAX_COMMIT_INSTRUCTIONS + 1 {
            body.push_str(&format!(
                "{{\"file\":{{\"path\":\"f.txt\",\"content\":\"{content_b64}\"}}}}\n"
            ));
        }
        let result = parse_ndjson_commit(&body);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("too many instructions"),
            "expected too many instructions error, got: {err_msg}"
        );
    }

    #[test]
    fn parse_ndjson_file_missing_path() {
        let content_b64 = STANDARD.encode(b"data");
        let body = format!(
            "{{\"header\":{{\"message\":\"test\",\"parentCommit\":\"\"}}}}\n\
             {{\"file\":{{\"content\":\"{content_b64}\"}}}}"
        );
        let result = parse_ndjson_commit(&body);
        assert!(result.is_err());
    }

    #[test]
    fn parse_ndjson_file_missing_content() {
        let body = "{\"header\":{\"message\":\"test\",\"parentCommit\":\"\"}}\n\
                     {\"file\":{\"path\":\"f.txt\"}}";
        let result = parse_ndjson_commit(body);
        assert!(result.is_err());
    }

    #[test]
    fn parse_ndjson_lfs_missing_path() {
        let body = "{\"header\":{\"message\":\"test\",\"parentCommit\":\"\"}}\n\
                     {\"lfsFile\":{\"oid\":\"abc\",\"size\":1}}";
        let result = parse_ndjson_commit(body);
        assert!(result.is_err());
    }

    #[test]
    fn parse_ndjson_lfs_missing_oid() {
        let body = "{\"header\":{\"message\":\"test\",\"parentCommit\":\"\"}}\n\
                     {\"lfsFile\":{\"path\":\"f.bin\",\"size\":1}}";
        let result = parse_ndjson_commit(body);
        assert!(result.is_err());
    }

    #[test]
    fn parse_ndjson_lfs_missing_size() {
        let body = "{\"header\":{\"message\":\"test\",\"parentCommit\":\"\"}}\n\
                     {\"lfsFile\":{\"path\":\"f.bin\",\"oid\":\"abc\"}}";
        let result = parse_ndjson_commit(body);
        assert!(result.is_err());
    }

    #[test]
    fn parse_ndjson_delete_missing_path() {
        let body = "{\"header\":{\"message\":\"test\",\"parentCommit\":\"\"}}\n\
                     {\"deletedEntry\":{}}";
        let result = parse_ndjson_commit(body);
        assert!(result.is_err());
    }

    #[test]
    fn parse_ndjson_empty_body() {
        let result = parse_ndjson_commit("");
        assert!(result.is_err());
    }

    #[test]
    fn parse_ndjson_only_whitespace() {
        let result = parse_ndjson_commit("  \n  \n  ");
        assert!(result.is_err());
    }

    #[test]
    fn parse_ndjson_parent_commit_preserved() {
        let content_b64 = STANDARD.encode(b"data");
        let body = format!(
            "{{\"header\":{{\"message\":\"parent\",\"parentCommit\":\"abc123\"}}}}\n\
             {{\"file\":{{\"path\":\"f.txt\",\"content\":\"{content_b64}\"}}}}"
        );
        let result = parse_ndjson_commit(&body).unwrap();
        assert_eq!(result.parent_commit.as_deref(), Some("abc123"));
    }

    // --- validate_lfs_oid tests ---

    #[test]
    fn validate_lfs_oid_valid() {
        let valid_oid = "a".repeat(64);
        assert!(validate_lfs_oid(&valid_oid).is_ok());

        let hex_oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert!(validate_lfs_oid(hex_oid).is_ok());

        let mixed = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        assert!(validate_lfs_oid(mixed).is_ok());
    }

    #[test]
    fn validate_lfs_oid_invalid_too_short() {
        assert!(validate_lfs_oid("abc123").is_err());
    }

    #[test]
    fn validate_lfs_oid_invalid_too_long() {
        let long_oid = "a".repeat(65);
        assert!(validate_lfs_oid(&long_oid).is_err());
    }

    #[test]
    fn validate_lfs_oid_invalid_uppercase() {
        let uppercase = "A".repeat(64);
        assert!(validate_lfs_oid(&uppercase).is_err());
    }

    #[test]
    fn validate_lfs_oid_invalid_non_hex_chars() {
        let mut oid = "a".repeat(63);
        oid.push('g');
        assert!(validate_lfs_oid(&oid).is_err());
    }

    #[test]
    fn validate_lfs_oid_invalid_empty() {
        assert!(validate_lfs_oid("").is_err());
    }
}
