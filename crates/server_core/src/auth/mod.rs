//! Built-in authentication provider implementations.

pub mod local_ed25519;
pub mod passthrough;

pub use local_ed25519::LocalEd25519Provider;
pub use passthrough::PassthroughProvider;
