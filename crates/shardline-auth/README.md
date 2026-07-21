# shardline-auth

Authentication provider implementations for Shardline.
Defines the `AuthProvider` trait for pluggable authentication with
`LocalHmacProvider` (HMAC-SHA256 local signing) and `PassthroughProvider`
(trusted proxy mode). Provides `AuthContext` for credential resolution and
`AuthError` for typed error handling. Token types (`TokenSigner`, `TokenScope`,
etc.) live in `shardline-protocol`.

See the [main Shardline README](../../README.md) for the project overview.
