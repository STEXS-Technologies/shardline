# Authentication

Shardline uses a pluggable `AuthProvider` trait for bearer-token verification and
minting. The server selects a concrete provider at startup based on configuration.

## The `AuthProvider` Trait

Defined in `shardline-server-core`, the trait has two methods:

```rust
pub trait AuthProvider: Send + Sync {
    fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError>;
    fn mint_token(&self, claims: &TokenClaims) -> Result<String, AuthError>;
}
```

- `verify_token` decodes and validates an opaque bearer token, returning the decoded
  claims or an `AuthError`.
- `mint_token` signs claims into a new bearer token.
  Not all providers support minting (the passthrough provider returns an error).

## Built-in Adapters

| Adapter | Value | Description |
| --- | --- | --- |
| **Local HMAC** | `local` | Default. Signs and verifies tokens using a shared HMAC-SHA256 signing key (`SHARDLINE_TOKEN_SIGNING_KEY` or `_FILE`). Supports both verification and minting. |
| **Ed25519** | `ed25519` | Signs and verifies Shardline tokens with an Ed25519 private key, or verifies them with a public key only. |
| **OIDC** | `oidc` | Validates tokens against an OpenID Connect issuer. Fetches signing keys from the issuer's discovery endpoint. Verification only; does not support token minting. |
| **JWKS** | `jwks` | Validates tokens against a static JWKS endpoint. Keys are cached with a configurable TTL. Verification only; does not support token minting. |
| **Passthrough** | `passthrough` | Trust-all provider for development. Any non-empty token is accepted with full write scope. Does not support token minting. **Do not use in production.** |

## Configuration

Select the auth provider with environment variables:

```text
SHARDLINE_AUTH_PROVIDER=local          # local | ed25519 | oidc | jwks | passthrough
SHARDLINE_AUTH_OIDC_ISSUER=https://accounts.google.com   # required when provider=oidc
SHARDLINE_AUTH_JWKS_URL=https://example.com/.well-known/jwks.json  # required when provider=jwks
```

### Local HMAC

The default provider.
Requires a signing key for token verification and minting:

```bash
SHARDLINE_TOKEN_SIGNING_KEY=change-me-for-local-only
# or
SHARDLINE_TOKEN_SIGNING_KEY_FILE=/run/secrets/shardline-token-key
```

Mint tokens with the CLI:

```bash
shardline admin token \
  --issuer local \
  --subject operator-1 \
  --scope write \
  --provider generic \
  --owner team \
  --repo assets \
  --revision main \
  --key-file .shardline/token-signing-key
```

### Ed25519

Ed25519 mode uses Shardline's native token envelope and claims format with an Ed25519
signature. These tokens are not JWTs, and a generic EdDSA JWT is not accepted by this
provider.

For signing and verification, configure a private key:

```bash
SHARDLINE_AUTH_PROVIDER=ed25519
SHARDLINE_ED25519_PRIVATE_KEY_FILE=/run/secrets/shardline-ed25519-private-key
```

The private-key file may contain a raw 32-byte seed, a raw 64-byte keypair, the
hexadecimal encoding of either raw form, or a PKCS#8 PEM private key.
For verification-only operation, configure the corresponding public key instead:

```bash
SHARDLINE_AUTH_PROVIDER=ed25519
SHARDLINE_ED25519_PUBLIC_KEY_FILE=/run/secrets/shardline-ed25519-public-key
```

The public-key file may contain a raw 32-byte Ed25519 public key, its hexadecimal
encoding, or a SubjectPublicKeyInfo PEM public key.
Direct `SHARDLINE_ED25519_PRIVATE_KEY` and `SHARDLINE_ED25519_PUBLIC_KEY` values are
also accepted; use hexadecimal or PEM text for direct environment values.
`_FILE` is preferred for key material.
Configure either the direct value or its `_FILE` counterpart, not both.

The equivalent TOML configuration is:

```toml
[auth]
provider = "ed25519"

[auth.ed25519]
private_key_path = "/run/secrets/shardline-ed25519-private-key"
```

Use `public_key_path` instead of `private_key_path` for verification-only operation.
Configure one key mode at a time.
Verification-only mode cannot mint tokens.

The `shardline admin token` command currently creates Local HMAC tokens; it does not
create Ed25519 tokens.
Use an issuer built against Shardline's Ed25519 token format when deploying this
provider. This operator-tooling gap is why Ed25519 remains experimental.

### OIDC

Set the issuer URL and point `SHARDLINE_AUTH_PROVIDER=oidc`:

```bash
SHARDLINE_AUTH_PROVIDER=oidc
SHARDLINE_AUTH_OIDC_ISSUER=https://accounts.google.com
```

The provider fetches the issuer's JWKS signing keys at startup and caches them.
Tokens are validated against the issuer's public keys and standard OIDC claims
(expiration, issuer match).
Token minting is not supported; use an external identity provider to issue tokens.

### JWKS

Set a static JWKS endpoint URL:

```bash
SHARDLINE_AUTH_PROVIDER=jwks
SHARDLINE_AUTH_JWKS_URL=https://auth.example.com/.well-known/jwks.json
```

Keys are fetched and cached with a 300-second TTL. This is useful when you have an
external service that already issues JWTs signed with keys published at a JWKS endpoint.
Token minting is not supported.

### Passthrough

For local development and testing only:

```bash
SHARDLINE_AUTH_PROVIDER=passthrough
```

Any non-empty bearer token is accepted with full write scope to all repositories.
This is useful for quick local testing without minting tokens, but must never be used in
production.

## Migration Guide

### From hardcoded `SHARDLINE_TOKEN_SIGNING_KEY`

The default `local` provider uses the same signing key mechanism as previous versions.
No migration is needed if you were using `SHARDLINE_TOKEN_SIGNING_KEY` or
`SHARDLINE_TOKEN_SIGNING_KEY_FILE` — the `local` provider reads these variables
automatically.

### From no auth (providerless mode)

If you previously ran Shardline without any signing key, the server did not enforce
authentication on CAS routes.
With the pluggable auth system:

1. Choose a provider. For most self-hosted deployments, `local` is the simplest path.
2. Set `SHARDLINE_AUTH_PROVIDER=local` (or leave it unset — `local` is the default).
3. Set `SHARDLINE_TOKEN_SIGNING_KEY` or `SHARDLINE_TOKEN_SIGNING_KEY_FILE`.
4. Mint tokens with `shardline admin token` and pass them as
   `Authorization: Bearer <token>`.

### Switching to OIDC or JWKS

1. Set `SHARDLINE_AUTH_PROVIDER=oidc` or `SHARDLINE_AUTH_PROVIDER=jwks`.
2. Set the corresponding issuer or JWKS URL variable.
3. Remove `SHARDLINE_TOKEN_SIGNING_KEY` if switching away from the `local` provider, or
   leave it set if you want the `local` provider to remain available for CLI token
   minting (the server only uses one provider for request verification).
4. Restart the server. Existing tokens signed by the previous provider will be rejected
   until valid tokens from the new provider are issued.

### Switching to Ed25519

1. Provision an Ed25519 private key for signing mode or a public key for
   verification-only mode.
2. Set `SHARDLINE_AUTH_PROVIDER=ed25519` and the corresponding `_FILE` variable.
3. Remove the Local HMAC signing key unless it is still needed by separate operator
   tooling.
4. Restart the server. Existing HMAC, OIDC, or JWKS tokens are not accepted as Ed25519
   tokens.
