# Client Configuration

Shardline is meant to fit existing Git workflows. Clients should keep using normal Git
operations; configuration decides how they reach the native Xet CAS path.

For the shortest forge-backed setup path, see
[Provider Setup Guide](PROVIDER_QUICKSTART.md).

## Common Requirements

Every client path needs:

- a Shardline base URL
- a repository-scoped bearer token with read or write scope
- a trusted way to mint those tokens

Token delivery depends on the deployment style:

- providerless deployments can mint tokens locally with `shardline admin token` or an
  equivalent trusted signing workflow
- provider-backed deployments usually rely on a provider connector or bridge service
  that exchanges forge authorization for a Shardline token

Tokens should be short-lived and tied to one repository scope.

## Native Xet Clients

Configure the client or filter process to target the Shardline CAS base URL. The exact
flags depend on the Xet-compatible implementation, but the expected behavior is the
same:

- upload through the native chunked CAS routes
- download through reconstruction lookups
- reuse repository-scoped bearer tokens for read and write access

Quick verification:

- repeated upload of unchanged content does not store duplicate chunks
- changing a small region of a file uploads only the new chunks
- fetching an older version reconstructs the correct bytes
- a repository-scoped read token cannot download another repository's reconstruction
- ranged reads reconstruct the expected byte window

## Token Delivery

Shardline CLI commands read the bearer token from `SHARDLINE_BEARER_TOKEN`.

Hosted provider integrations should inject repository-scoped tokens without requiring
users to mint them manually.
