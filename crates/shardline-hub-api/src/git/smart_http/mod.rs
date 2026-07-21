//! Git Smart HTTP protocol handlers.
//!
//! Implements the server side of Git Smart HTTP for clone/fetch (upload-pack)
//! and push (receive-pack) operations. Upload-pack generates real Git pack
//! files from HubStore metadata: tree objects, LFS pointer blobs, and commit
//! objects are all constructed from the file entries stored per revision.

pub mod error;
pub mod pack_parse;
pub mod receive_pack;
pub mod ref_advertisement;
pub mod tree_walk;
pub mod upload_pack;

#[cfg(test)]
mod tests;

pub use error::SmartHttpError;
pub use pack_parse::parse_pack_data;
pub use receive_pack::receive_pack;
pub use ref_advertisement::{
    InfoRefsQuery, info_refs, info_refs_receive_pack, info_refs_upload_pack,
};
pub use tree_walk::{parse_commit_object, walk_git_tree};
pub use upload_pack::upload_pack;
