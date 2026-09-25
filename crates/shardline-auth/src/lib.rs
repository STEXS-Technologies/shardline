#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string,
        clippy::panic
    )
)]

//! Authentication provider trait and implementations for the Shardline
//! ecosystem.  The [`AuthProvider`] trait is the main abstraction; see
//! [`local_hmac::LocalHmacProvider`] and [`passthrough::PassthroughProvider`]
//! for concrete implementations.  See [`ed25519::Ed25519AuthProvider`] for
//! asymmetric-key authentication.

pub mod ed25519;
pub mod local_hmac;
pub mod passthrough;
mod provider;
mod types;

pub use ed25519::Ed25519AuthProvider;
pub use local_hmac::LocalHmacProvider;
pub use passthrough::PassthroughProvider;
pub use provider::AuthProvider;
pub use types::{AuthContext, AuthError, VerifiedAuthContext};
