# Provider Adapters

Shardline must fit into the way repositories already work on GitHub, GitLab, Gitea, and
self-hosted Git platforms.

For a minimal operator path instead of the full adapter model, see
[Provider Setup Guide](PROVIDER_QUICKSTART.md).

The provider layer exists so the CAS stays generic while repository hosting details stay
replaceable.

## Purpose

Provider adapters are responsible for:

- checking whether a repository action is allowed
- evaluating the authenticated provider subject, not only repository identity
- mapping repository identity into CAS scopes
- resolving repository metadata such as visibility and default branch
- normalizing provider webhook payloads into Shardline events
- keeping GitHub, GitLab, Gitea, and custom integrations outside the CAS core

Provider adapters are not responsible for:

- chunk validation
- deduplication
- reconstruction planning
- xorb or shard storage
- object garbage collection policy

## Design Rule

The same repository should work with Shardline regardless of where the Git remote lives.

That means:

- the CAS protocol stays the same
- storage adapters stay the same
- only the provider adapter changes

From an operator perspective, swapping from GitHub to Gitea should feel like changing an
integration profile, not rebuilding the system.

## Required Adapter Capabilities

Every provider adapter must support:

- subject-aware authorization for read and write flows
- repository access checks for read and write flows
- repository metadata lookup
- webhook verification and decoding
- repository and revision identity normalization

At minimum, the adapter contract needs:

- `check_access(request)`
- `repository_metadata(repository)`
- `parse_webhook(request)`

## Supported Provider Families

Shardline supports these provider families:

- GitHub
- Gitea
- GitLab
- Generic Git forge adapters

The generic provider path matters for self-hosted adoption.
Shardline must not assume that a repository lives on a public SaaS forge.

## Seamless Integration Requirements

Provider integration succeeds only if it preserves normal Git workflows.

Required operator and user experience:

- existing Git remotes keep working
- repository permissions come from the Git host
- standard push and fetch flows remain recognizable
- repository-level rollout can happen without manual object migration first

The target experience is simple:

- it feels like normal Git from the user's perspective
- it stores and transfers like a chunked CAS underneath

## Token Issuance Boundary

Provider adapters should feed Shardline enough information to mint scoped CAS tokens.

Those scopes should include:

- provider family
- repository identity
- revision context when required
- read or write intent

The provider adapter decides whether access is granted.
The CAS decides what those granted scopes are allowed to do.

Authorization requests need enough context to answer "who is asking" as well as "which
repository and revision are being accessed."
Repository-scoped policy without a provider subject is not sufficient for production
use.

Shardline exposes this as a two-step runtime boundary:

1. a provider adapter evaluates an `AuthorizationRequest`
2. a token issuer signs a CAS token only from a granted authorization result

That token carries:

- provider family
- repository owner and name
- revision scope
- read or write scope
- issuer identity
- expiration

Denied provider decisions do not mint a token.
The CAS server accepts the resulting token on the normal reconstruction, download, xorb,
and shard routes.

The runtime issuance endpoint is:

- `POST /v1/providers/{provider}/tokens`

That endpoint is intended for trusted provider connectors and bridge services.
It is protected by a dedicated bootstrap key and is separate from ordinary CAS bearer
tokens.

The webhook ingestion endpoint is:

- `POST /v1/providers/{provider}/webhooks`

That endpoint is intended for signed provider webhook deliveries from GitHub, GitLab,
Gitea, or a compatible generic adapter.

## Webhooks and Repository Roots

Webhooks are operational inputs, not protocol truth.

Shardline uses them to:

- start cleanup workflows after repository deletion or rename
- reduce polling pressure on provider APIs
- provide normalized lifecycle signals for lifecycle reconciliation

Normalized webhook events cover:

- GitHub repository deletion, rename, access changes, and revision push movement
- GitLab repository deletion, rename and transfer, access changes, and revision push
  movement
- Gitea repository deletion, rename, access changes, and revision push movement
- generic normalized repository deletion, rename, access changes, and revision push
  movement

Those normalized events are exercised through live HTTP route tests for every
implemented GitHub, GitLab, Gitea, and generic webhook kind.

Repository deletion is wired into lifecycle handling for providers that emit a supported
deletion event.
Shardline scans the affected repository's recorded file versions, derives
the referenced chunk and serialized-xorb objects, expands native Xet xorb records to the
underlying unpacked chunk objects they keep alive, writes time-bounded retention holds,
and removes the deleted repository's latest and immutable version metadata from the live
root set before garbage collection can reclaim those bytes.
Repository rename events migrate visible latest-file state and immutable version
metadata into the new repository scope while preserving revision names, and remove the
old-scope records, so renamed repositories continue to resolve existing content only
under their new identity.
If the target scope already contains different metadata, the rename fails closed instead
of overwriting those records.
Repeated provider retries for the same signed delivery are deduplicated durably by
provider and delivery identifier after successful lifecycle application, so retries can
repair partial failures instead of being discarded.
`access_changed` and `revision_pushed` are normalized, authenticated, deduplicated, and
persisted as provider-derived repository lifecycle state.
Shardline now records the latest observed access-change timestamp and pushed revision
per provider repository so token issuance, repair tooling, and operator audits can
reason about provider state without trusting ephemeral webhook delivery logs.
A successful provider-issued token marks pending access-change and revision-push signals
as reconciled for authorization recheck, cache-invalidation accounting, and repository
drift detection.
`shardline repair lifecycle` can prune stale delivery claims after their
retention window and remove future-dated claims that indicate clock skew or corrupted
webhook metadata.

Webhook loss must not corrupt data.
At worst it should delay cleanup or cache refresh.

## Reliability Expectations

Provider outages must degrade safely.

Required behavior:

- deny new write token issuance when authorization cannot be confirmed
- allow already-issued, unexpired scoped operations to complete within policy
- keep reconstruction and existing immutable content readable when policy allows
- avoid hard-coding provider-specific assumptions into the CAS core

## Generic Adapter Path

The generic path exists for:

- custom self-hosted Git forges
- internal enterprise Git systems
- migration periods where an operator is between provider stacks

A generic adapter should still meet the same normalized contract.
The difference is the integration implementation, not the CAS behavior.

## Generic Webhook Contract

The generic adapter accepts a normalized webhook shape so self-hosted forges can bridge
into Shardline without pretending to be GitHub, GitLab, or Gitea.

Headers:

- `x-shardline-event`
- `x-shardline-delivery`
- `x-shardline-signature`

`x-shardline-signature` must be an HMAC-SHA256 value with the `sha256=` prefix.
Shardline requires a configured webhook secret for every provider entry.

The request body is JSON with one of these normalized event kinds:

- `revision_pushed`
- `repository_deleted`
- `repository_renamed`
- `access_changed`

Minimal payload examples:

```json
{
  "kind": "revision_pushed",
  "repository": { "owner": "team", "name": "assets" },
  "revision": "refs/heads/main"
}
```

```json
{
  "kind": "repository_renamed",
  "repository": { "owner": "team", "name": "assets" },
  "new_repository": { "owner": "team", "name": "new-assets" }
}
```
