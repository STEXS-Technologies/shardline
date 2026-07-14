//! Built-in authentication provider implementations.

pub mod local_hmac;
pub mod passthrough;

pub use local_hmac::LocalHmacProvider;
pub use passthrough::PassthroughProvider;
