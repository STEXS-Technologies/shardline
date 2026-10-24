//! Git Smart HTTP protocol handlers.
//!
//! Implements the server side of Git Smart HTTP for clone/fetch (upload-pack)
//! and push (receive-pack) operations. Upload-pack generates real Git pack
//! files from HubStore metadata: tree objects, LFS pointer blobs, and commit
//! objects are all constructed from the file entries stored per revision.

mod admission;
mod ancestry;
mod capabilities;
pub mod error;
mod identity;
mod limits;
pub(crate) mod model_routes;
pub mod pack_parse;
mod path_metadata;
mod projection;
mod publication;
pub mod receive_pack;
pub mod ref_advertisement;
pub(crate) mod repository;
pub mod tree_walk;
pub mod upload_pack;

#[cfg(test)]
mod scope_tests;
#[cfg(test)]
mod tests;

pub(crate) use ancestry::{is_ancestor, reachable_commits};
pub use error::SmartHttpError;
pub(crate) use identity::IDENTITY_ADMISSION;
pub(crate) use identity::{public_commit_id, resolve_commit_id};
pub(crate) use limits::{MAX_RECEIVE_PACK_REQUEST_BYTES, MAX_UPLOAD_PACK_REQUEST_BYTES};
pub use pack_parse::parse_pack_data;
#[cfg(test)]
pub(crate) use path_metadata::path_oids_sync;
pub(crate) use path_metadata::path_oids_with_budget_sync;
pub(crate) use path_metadata::{path_oids, tree_oids, xet_file_hash};
pub(crate) use projection::scoped_published_files;
pub(crate) use projection::validate_projected_path;
pub(crate) use projection::{GitArchiveMember, PublishedFileMember, file_key_at_revision};
pub(crate) use publication::prepare_publication;
pub use receive_pack::receive_pack;
pub use ref_advertisement::{
    InfoRefsQuery, info_refs, info_refs_receive_pack, info_refs_upload_pack,
};
pub use tree_walk::{parse_commit_object, walk_git_tree};
pub use upload_pack::upload_pack;
