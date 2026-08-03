use std::io;

use thiserror::Error;

/// Failure to parse a 64-character Xet CAS API hexadecimal hash string.
#[derive(Debug, Clone, Error)]
pub enum XetHashParseError {
    /// The hash string did not contain exactly 64 hexadecimal characters.
    #[error("hash must contain exactly 64 lowercase hexadecimal characters")]
    InvalidLength,
    /// The hash string contained a character outside lowercase hexadecimal, or hex decoding failed.
    #[error("invalid hash character: {0}")]
    InvalidCharacter(String),
}

/// Transport and protocol errors from the CAS read path (reconstruction and
/// ranged xorb fetch).
///
/// HTTP status codes are mapped per the Xet protocol error semantics
/// (`docs/PROTOCOL_CONFORMANCE.md` "Error Semantics"): 400 malformed input,
/// 401 missing/invalid token, 403 valid token lacking scope, 404 missing
/// file/xorb, 416 unsatisfiable range, 429 overload, 5xx server failure.
#[derive(Debug, Error)]
pub enum TransferError {
    /// The server rejected the request as malformed (HTTP 400).
    #[error("bad request (400): {0}")]
    BadRequest(String),
    /// The credential was missing or invalid (HTTP 401).
    #[error("unauthorized (401): {0}")]
    Unauthorized(String),
    /// The credential is valid but lacks the required scope (HTTP 403).
    #[error("forbidden (403): {0}")]
    Forbidden(String),
    /// The requested file, xorb, or chunk does not exist (HTTP 404).
    #[error("not found (404): {0}")]
    NotFound(String),
    /// The requested byte range cannot be satisfied (HTTP 416).
    #[error("range not satisfiable (416): {0}")]
    RangeNotSatisfiable(String),
    /// The server is rate limiting or overloaded (HTTP 429).
    #[error("too many requests (429): {0}")]
    TooManyRequests(String),
    /// Some other non-success status (including 5xx).
    #[error("server returned HTTP {status}: {message}")]
    HttpStatus {
        /// The HTTP status code.
        status: u16,
        /// Error message from the response body, when present.
        message: String,
    },
    /// Transport-level failure (connect, DNS, timeout, TLS, ...).
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    /// The response was not a valid transfer response.
    #[error("invalid transfer response: {0}")]
    InvalidResponse(String),
    /// A `multipart/byteranges` response could not be parsed.
    #[error("malformed multipart/byteranges response: {0}")]
    MalformedMultipart(String),
}

/// Top-level errors surfaced by the sdx client read path.
#[derive(Debug, Error)]
pub enum SdxError {
    /// Token issuance / refresh failed.
    #[error(transparent)]
    Auth(#[from] crate::auth::AuthError),
    /// A CAS transfer request failed.
    #[error(transparent)]
    Transfer(#[from] TransferError),
    /// Serialized xorb bytes could not be decoded.
    #[error(transparent)]
    Xorb(#[from] crate::xorb::XorbError),
    /// The file identifier was not valid 64-character lowercase hex.
    #[error(transparent)]
    Hash(#[from] XetHashParseError),
    /// File I/O failed while writing a download.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// The supplied byte range is inverted (`start > end`).
    #[error("invalid byte range {start}..={end}: start exceeds end")]
    InvalidByteRange {
        /// Range start.
        start: u64,
        /// Range end.
        end: u64,
    },
    /// The reconstruction response contained no terms.
    #[error("reconstruction response for {file_id} contained no terms")]
    EmptyReconstruction {
        /// The requested file identifier.
        file_id: String,
    },
    /// A term referenced a xorb with no matching fetch information.
    #[error("reconstruction term {term_index} references xorb {hash} with no fetch information")]
    MissingFetchInfo {
        /// Index of the offending term.
        term_index: usize,
        /// Xorb hash the term referenced.
        hash: String,
    },
    /// A term's decoded byte count disagreed with its declared unpacked length.
    #[error(
        "term {term_index} unpacked length mismatch: expected {expected} bytes, decoded {actual}"
    )]
    UnpackedLengthMismatch {
        /// Index of the offending term.
        term_index: usize,
        /// The declared unpacked length.
        expected: u64,
        /// The number of bytes actually decoded.
        actual: u64,
    },
    /// A fetched xorb range decoded to a different number of chunks than the
    /// reconstruction declared.
    #[error("xorb fetch {url} produced {actual} chunks but the reconstruction expected {expected}")]
    FetchChunkCountMismatch {
        /// The transfer URL that was fetched.
        url: String,
        /// The declared chunk count for the fetched range.
        expected: u64,
        /// The number of chunks actually decoded.
        actual: u64,
    },
    /// The reconstructed range was shorter than requested (past end of file).
    #[error("requested range {start}..={end} is past the end of the file")]
    RangePastEnd {
        /// Range start.
        start: u64,
        /// Range end.
        end: u64,
    },
    /// The `xet://` endpoint URL could not be mapped to an API base and repository identity.
    #[error("invalid endpoint URL: {0}")]
    InvalidEndpoint(String),
}
