//! Git Smart HTTP protocol implementation.
//!
//! Provides server-side Git Smart HTTP endpoints for `git clone`,
//! `git fetch`, and `git push` operations against Hub API repositories.

pub mod pack;
pub mod pktline;
pub mod smart_http;

pub use smart_http::{
    info_refs, info_refs_receive_pack, info_refs_upload_pack, receive_pack, upload_pack,
    SmartHttpError,
};
