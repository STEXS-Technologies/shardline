//! Error types for Git Smart HTTP operations.

use super::super::pack::ObjectType;
use thiserror::Error;

/// Error type for Git Smart HTTP operations.
///
/// Covers errors from pack parsing, tree walking, commit parsing, and
/// store interactions during clone/fetch and push operations.
#[derive(Debug, Error)]
pub enum SmartHttpError {
    // ---- store_push_objects errors ----
    #[error("invalid commit SHA hex: {0}")]
    InvalidCommitShaHex(String),
    #[error("commit SHA must be 20 bytes")]
    CommitShaMustBe20Bytes,
    #[error("commit not found in pack: {0}")]
    CommitNotFoundInPack(String),
    #[error("expected commit object for new SHA")]
    ExpectedCommitObject,
    #[error("invalid tree SHA: {0}")]
    InvalidTreeSha(String),
    #[error("tree SHA must be 20 bytes")]
    TreeShaMustBe20Bytes,
    #[error("failed to store files: {0}")]
    StoreFiles(String),
    #[error("failed to store LFS object: {0}")]
    StoreLfsObject(String),
    #[error("{0}")]
    NonFastForward(String),
    #[error("failed to create revision: {0}")]
    CreateRevision(String),
    #[error("failed to delete ref: {0}")]
    DeleteRef(String),
    #[error("cannot delete a ref that does not exist")]
    CannotDeleteNonExistentRef,

    // ---- parse_commit_object errors ----
    #[error("invalid commit encoding: {0}")]
    CommitEncoding(String),
    #[error("commit missing tree header")]
    CommitMissingTree,

    // ---- walk_git_tree_inner errors ----
    #[error("tree nesting exceeds maximum depth")]
    TreeDepthExceeded,
    #[error("tree object not found: {0}")]
    TreeObjectNotFound(String),
    #[error("expected tree object, got {0:?}")]
    ExpectedTreeObject(ObjectType),
    #[error("tree position out of bounds")]
    TreePositionOutOfBounds,
    #[error("invalid tree entry: missing space after mode")]
    TreeMissingSpaceAfterMode,
    #[error("invalid tree entry: mode range out of bounds")]
    TreeModeRangeOutOfBounds,
    #[error("invalid mode encoding: {0}")]
    TreeModeEncoding(String),
    #[error("tree arithmetic overflow")]
    TreeArithmeticOverflow,
    #[error("name position out of bounds")]
    TreeNamePositionOutOfBounds,
    #[error("invalid tree entry: missing null after name")]
    TreeMissingNullAfterName,
    #[error("invalid tree entry: name range out of bounds")]
    TreeNameRangeOutOfBounds,
    #[error("invalid name encoding: {0}")]
    TreeNameEncoding(String),
    #[error("invalid tree entry: truncated SHA")]
    TreeTruncatedSha,
    #[error("invalid tree entry: SHA range out of bounds")]
    TreeShaRangeOutOfBounds,
    #[error("tree depth overflow")]
    TreeDepthOverflow,
    #[error("blob object not found: {0}")]
    BlobObjectNotFound(String),
    #[error("expected blob object for file, got {0:?}")]
    ExpectedBlobObject(ObjectType),
    #[error("invalid LFS pointer encoding: {0}")]
    LfsPointerEncoding(String),
    #[error("LFS pointer missing oid field")]
    LfsPointerMissingOid,
    #[error("LFS pointer missing size field")]
    LfsPointerMissingSize,
    #[error("invalid LFS size: {0}")]
    LfsPointerSize(String),
}
