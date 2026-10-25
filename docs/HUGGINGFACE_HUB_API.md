# HuggingFace Hub API Compatibility

Shardline includes a HuggingFace Hub API compatibility layer for the repository
workflows listed below. It is a compatible subset, not a drop-in replacement for every
Hugging Face Hub API or client transfer mode.

## Repository creation permissions

`HfApi.create_repo("owner/name")` defaults to a model repository. The generic
creation endpoint accepts an omitted `type`; explicit `model`, `dataset` and
`space` values keep their existing meaning.

Both creation routes accept `visibility: "private"` or `visibility: "public"`.
An explicit `private: true` keeps the repository private even if visibility is
public. Unsupported visibility values return HTTP 400 without creating metadata;
Shardline does not implement a separate protected visibility tier.
The type-specific route also rejects malformed privacy fields and non-object
request bodies rather than treating them as public creation requests.

Ordinary write tokens may create only the repository named in the token.
Issuers that have verified namespace ownership can opt into sibling creation
with `TokenClaims::with_namespace_repository_creation()` before signing. The
grant covers creation in the token's existing owner namespace only. It grants
no read, upload, update or delete access to sibling repositories; those actions
still require a token for the target repository. Read-only tokens cannot create
repositories even when this grant is present.

Existing tokens retain their serialized form and exact-repository permissions.
Both repository creation routes enforce the same rule. Creation grants are not
automatically issued by provider-token exchange.

Repository creation and retry responses use the configured public base URL,
including its path prefix. Set this to the address clients can reach when
deploying behind a proxy. Request Host and forwarding headers do not change
returned repository URLs.

Both creation routes return HTTP 409 when a repository already exists, including
when another server wins a concurrent creation. A retry never changes the
winner's visibility. A token scoped to the exact repository receives the usual
repository response for `exist_ok` compatibility; a creation-only namespace
grant receives a minimal conflict without private repository metadata.

### Optional shared creation limits

Owners can limit repository creation without changing client setup or running
another service. The policy lives in the existing SQLite or PostgreSQL metadata
database and is checked in the repository publication transaction.

After migrating and upgrading all servers, save a JSON file such as
`creation-policy.json`:

```json
{"max_repositories":100,"max_creations":10,"window_seconds":3600}
```

Apply it using the deployment's owner database credentials and configuration:

```bash
shardline admin repository-policy --namespace alice --policy-file creation-policy.json
shardline admin repository-policy --namespace alice
shardline admin repository-policy --namespace alice --clear
```

For a local deployment, add `--root /path/to/data`. With no policy configured,
existing creation behavior is preserved. Clearing a policy leaves repositories
and their visibility unchanged.

`max_repositories` limits repositories currently present in the namespace.
`max_creations` limits successful creations during the trailing `window_seconds`
(1–86,400 seconds). Deleting a repository does not erase its creation from that
window. Limits return HTTP 429; rate-limit responses include `Retry-After`.
Authorized retries of an existing repository still return 409 without consuming
another creation or changing privacy. Clients should use bounded backoff rather
than expect a retry to succeed immediately.

Set `enabled` to `false` to stop creation in the namespace. To deny creation by
an identity with an otherwise valid issued token, add
`"denied_creators":[{"issuer":"your-issuer","subject":"the-subject"}]`.
These checks return 403 before conflict metadata is returned. They govern
repository creation only; they do not revoke existing read or upload access.
Token permissions remain required even when a shared policy permits creation.

Configure policies only after all writers have upgraded. Older binaries can
continue reading existing data, but their repository creation is deliberately
rejected in namespaces with a configured policy. Clear the policy with the
upgraded owner CLI before rolling back if legacy creation is needed. Custom
`HubStore` implementations must supply their own shared-policy enforcement;
these database guarantees cover Shardline's built-in stores.

## Commit operations

The Hub commit API supports file additions, file deletion and recursive folder
deletion. Folder deletion removes descendants only: deleting `weights/` keeps
`weights-backup.bin`. Operations run in request order, so a commit can delete a
folder and then add replacement files. Earlier revisions retain their own trees.

Paginated tree listings keep the revision selected by the first page. Follow the
returned next-page links to finish that listing, even if the branch changes in
the meantime. Start a new listing to see newer commits. This applies to ordinary
and expanded tree responses, including nested directories.

The final tree must fit Git's supported path bounds: at most 1,024 UTF-8 bytes
and 128 components per file path. A deeper final path returns HTTP 400 before
the branch or revision history changes. Immutable content already staged by a
rejected request can remain unpublished for later cleanup.

Shardline verifies the complete Git identity and stores its tree/identity
archives before advancing a Hub branch. Object-limit errors or archive failures
leave the branch and revision history unchanged. Preparation alone does not make
a revision readable. SQLite and PostgreSQL then publish the exact prepared
timestamp, parent and message in the existing atomic ref/evidence transaction;
an intervening writer causes a conflict. As with any database commit, losing the
reply can leave the caller uncertain whether publication completed.

Custom `HubStore` implementations must implement `publish_prepared_revision`
to support Hub commit publication. Its default fails closed without ref writes.
Existing revision structures, database tables and stored Git identities remain
unchanged; the built-in stores support this operation directly.

Official SDK copies of regular files, basic LFS files and native Xet-backed LFS
files use the existing commit API. For an LFS copy whose instruction omits the
size, Shardline reads it from the authorized repository's persisted Xet identity
or stored LFS object metadata. An unknown copy source or malformed explicit size is
rejected before publishing a revision. Copy plus file deletion supports moves;
it does not delete the underlying immutable bytes needed by older revisions.

The stock SDK commit checks pass with `huggingface_hub` 1.25.1 and `hf_xet`
1.6.0/1.7.0 on local storage and PostgreSQL/MinIO. They cover folder replacement,
branch/history preservation, regular and LFS copies, native LFS moves and exact
download bytes. Transfer counters verify that copies reuse stored payloads.

## Repository credentials and stored objects

Deleting a repository or creating, listing and deleting its webhooks requires
a repository-wide token with no revision restriction. Deletion and webhook
changes also require Write permission. A token limited to `main`, another
branch, a tag or a commit receives HTTP 403 before configuration is read or
changed. Use a separate repository-wide credential for these administrative
operations; branch-scoped upload and download credentials remain restricted.
Both repository deletion routes enforce this rule.

Outbound Hub webhooks connect directly to public destinations. Shardline checks
DNS addresses at connection time and rejects private destinations and redirects.
The webhook client ignores proxy environment variables so a proxy cannot bypass
these destination checks. Applications supplying a custom `HubState` HTTP client
must also install `PublicWebhookDnsResolver`, disable proxies and disable redirects.
Delivery logs identify the destination origin; URL credentials, paths, query
strings and fragments are omitted because they can contain webhook tokens.

An LFS batch with an explicit `ref` must also fit the credential's revision
scope. Unauthorized requests return 403 before transfer actions are issued.
Uploads require the permitted branch; downloads retain authorized immutable
history access. Clients that omit `ref` continue using their token's existing
scope. Omitting it does not expand that token's permissions.

A repository-wide read token can read inline and basic LFS files previously
uploaded with a token scoped to `main`. Shardline records the original object
location in metadata; it does not copy or move the file. Existing files in the
legacy `main` namespace can be discovered on their first read. A token limited
to another revision does not gain access through this location index.

This adds a frontend metadata migration. PostgreSQL operators must apply the
pending migrations before starting the new binary.

Native Xet read tokens are pinned to an immutable Hub commit. Resolve responses
advertise a refresh URL containing that commit ID, so stock clients cache each
commit's credentials separately when a branch advances. Existing branch-based
token routes remain available. Before returning
reconstruction data, Shardline checks that the file's SHA-256 and size occur in
that repository commit, then finds the original native record. A signed download
URL permits only its exact serialized range. Newly issued URLs expire after at
most 15 minutes, or when the authorizing credential expires, whichever comes
first. An already expired credential cannot issue a new URL. Downloads remain
behind Shardline; clients receive no backend object-store URL or credentials.
Previously issued URLs keep their original expiry.
Expired URLs without fresh credentials are rejected before file or object-store
lookups. Official clients receive the refresh response; native clients can still
retry with a valid bearer credential and the original exact range.
Signed requests with missing or repeated Range fields also fail before storage
discovery. Valid ranges retain the same exact-range and repository checks.
Commit-scoped CAS credentials
cannot be reused as ordinary Hub credentials or unrestricted Xorb-read tokens.
An older commit's token cannot read a file added later. Write-token behavior is
unchanged.

When a verified native upload replaces the current Xet identity for the same
SHA-256 and size, Shardline retains the earlier binding for cached downloads.
It still checks the committed file and its storage record. The upgrade preserves
bindings present at installation; bindings already overwritten by older versions
require separate reconciliation.

Repository-wide credentials can read historical commits. A branch-scoped token
can request an immutable ancestor of its own branch; it cannot enter a sibling
branch by supplying that branch's commit ID. Tag and immutable-commit tokens
remain limited to their selected commit. Resolve, tree, paths-info, revision-info,
commit-history and revision-specific dataset reads check the requested revision
before returning file metadata or download locations. Branch and tag mutations
require permission for the exact target ref; managing sibling refs requires a
repository-wide credential. A preupload check does not grant permission to
publish a commit to another branch. Default-revision model info, model cards and
dataset previews check access to `main`; a sibling-only credential must use a
supported explicit-revision route for that sibling's data. Upgrade every Hub
replica before relying on these stricter checks; older replicas retain their
previous authorization behavior.

Git uses the same snapshot policy for tag and immutable-commit tokens. A tag
clone receives its selected commit as a shallow boundary; an immutable token
advertises a detached HEAD at that commit. Stock Git can clone these snapshots
without changing commit hashes. Fetching or unshallowing cannot retrieve parent
commits or file objects that exist only in older snapshots. The selected commit
still contains its original parent identifiers; those identifiers do not grant
access to the parent objects. Branch and repository tokens retain their
authorized history.

The SDK commit-history endpoint follows this policy too: tag and immutable
credentials receive only the selected commit. Expanded paths-info returns a
last-change commit only when that change belongs to the allowed snapshot;
unchanged paths have no last-change metadata rather than exposing older commit
messages or timestamps. A full tag name and its short alias are interchangeable
only when a branch or immutable revision does not take precedence over that alias.

Snapshot views negotiate legacy Git framing even when a client requests v2.
This avoids recursive lazy fetches observed with Git 2.52's v2 filtered-clone
handling of server-shallow tips before their pack is read. Standard and filtered
snapshot clones need no client configuration. Branch/repository views continue
to negotiate v2. Native v2 snapshot qualification remains open; the relevant
upstream flow is [`receive_shallow_info` in Git 2.52](https://github.com/git/git/blob/v2.52.0/fetch-pack.c#L1428).
Removing the advertised `filter` capability does not resolve this case: a
stock filtered clone can still recursively fetch the missing shallow tip.
Git's [`prepare_shallow_info`](https://github.com/git/git/blob/v2.52.0/shallow.c#L440)
checks the tip with promisor fetching enabled before the response pack has
been consumed. Snapshot client contracts have per-command deadlines and, on
Unix, terminate the client's subprocess group on timeout. A timed-out client
fails qualification.
Git 2.52.0 also reproduces the filtered-v2 timeout against Git's own
`upload-pack` serving a depth-one repository over `file://`; unfiltered v2 and
explicit `--depth=1 --filter=blob:none` clones succeed in that fixture. This
isolates a client/server-shallow interaction beyond Shardline's HTTP framing;
it does not qualify native v2 snapshots or all Git versions.

Ref listings show only refs allowed by the credential. The legacy revision
listing returns reachable history for a branch token and the selected commit
for a tag or immutable token. Its `ref_name` identifies the credential's selected
revision, so a reachable commit does not disclose another branch's name.
Repository-wide listing behavior is unchanged. Scoped history uses a single
bounded commit-only traversal and indexed revision lookups.

Cached public Git identities can be found across credential storage namespaces
within the same repository. These are internally verified commit metadata;
finding an identity does not authorize its revision or expose file bytes.
Ancestry checking reads bounded commit metadata, including both Git merge
parents, without reading file payloads. Some legacy objects and commit aliases
written under different credential namespaces still need location reconciliation;
these checks do not establish every credential/revision combination.

## Card validation

`POST /api/validate-yaml` checks README YAML front matter. Valid cards return
HTTP 200; malformed YAML, an unclosed metadata block, a non-mapping root, invalid
request fields, or a resource-limit violation return HTTP 400. Both responses
include `warnings` and `errors` arrays. This stateless endpoint accepts anonymous
requests, as required by the stock model-card client. Configured authentication
still rejects explicit invalid credentials. Repository reads and publication
retain their usual authorization.

Validation accepts nested metadata and bounded YAML aliases. Cards are limited
to 256 KiB, with separate depth, node and alias-expansion limits. A README without
front matter is valid. This checks syntax and structure; it does not certify
licenses, dataset identifiers or other Hugging Face catalog entries.
The JSON request body is capped at 2 MiB; larger bodies return HTTP 413.

Repository info and revision info use the same parser for README metadata,
including nested arrays and mappings. Invalid metadata is omitted; the README
remains downloadable and historical revisions retain their own metadata. These
reads inspect at most 256 KiB plus delimiter space, matching the validation budget.

With `huggingface_hub` 2.2.0, set `HF_ENDPOINT` before starting Python and use
`ModelCard.push_to_hub()` normally. Validation stays on Shardline, and publication
uses the repository token. Stock-client tests cover local and PostgreSQL/MinIO
storage, rejected excessive YAML nesting, private repository access and historical
card metadata after replacement.

In `huggingface_hub` 1.25.1, `ModelCard.validate()` sends its request directly to
`https://huggingface.co/api/validate-yaml`, even when `HF_ENDPOINT` points elsewhere.
`ModelCard.push_to_hub()` also invokes that validation. For deployments that must
keep cards within Shardline, validate against Shardline's endpoint explicitly and
publish README.md with `HfApi.upload_file()`. Do not send private card content to
the public service unintentionally. This upstream client limitation is separate
from Shardline's validation endpoint and metadata support.

## What It Is

The Hub API frontend translates HuggingFace Hub REST endpoints into Shardline CAS
operations. Users and CI pipelines that already work with `huggingface-cli` or the Hub
REST API can point at a Shardline instance and use the supported repository operations.
Client behavior outside the tested workflows may differ from Hugging Face's hosted Hub.

The Hub frontend is registered as a server frontend alongside the other protocol
adapters.

## How to Enable

Pass `--frontend hub` when starting the server:

```bash
shardline serve --frontend hub
```

Or set the environment variable:

```bash
SHARDLINE_SERVER_FRONTENDS=hub
```

The Hub frontend can run alongside other frontends:

```bash
shardline serve --frontend xet --frontend hub
```

## Using with `huggingface-cli`

Point the HuggingFace CLI at your Shardline instance by setting `HF_ENDPOINT`:

```bash
export HF_ENDPOINT=http://localhost:8080
hf auth login  # optional — hub api accepts anonymous by default
hf upload my-org/my-model ./model-files
hf download my-org/my-model
```

For `huggingface-cli` to trust a local HTTP endpoint, you may also need:

```bash
export HF_HUB_DISABLE_TELEMETRY=1
export TRANSFORMERS_VERBOSITY=info
```

The checked-in end-to-end test uses the installed `hf` CLI with `hf_xet` enabled. It
covers repository creation, small model and dataset uploads, filtered downloads,
deletion, and 9 MiB model and dataset uploads and downloads with byte-for-byte
verification. For both large files, the test checks that Shardline persisted the
repository-scoped LFS SHA-256 to native Xet file identity mapping and that the CLI
uploaded Xet shards and Xorbs without sending the file body through ordinary Hub LFS.
This proves the Hub commit and readback resolve to the CAS content. The flows pass
with Hub enabled by itself and with Hub, Xet, and S3 enabled together.

Native shard registration verifies an optional SHA-256 identity against the
selected, authenticated Xorb chunks in file order before publishing it. This
also rejects contradictory empty-file identities. Older shards that omit the
optional identity remain accepted. Verification keeps a single source container
and one decoded chunk, rather than buffering a complete file. It reuses the
original source read for the first container; additional or revisited containers
can require further backend reads to establish the claimed SHA-256.

The explicit Linux 1 GiB qualification also checks a cold native download and
plain HTTP readback without whole-file buffering. Use disk-backed scratch
(`TMPDIR=/var/tmp`, for example) with sufficient quota when running it; free
space reported by `df` does not establish available per-user tmpfs quota.
The 1 GiB checks also pass with `huggingface_hub` 1.25.1 and `hf_xet` 1.5.2
on local storage and PostgreSQL/MinIO. Each verifies the cold native download
and two concurrent slow plain HTTP readers. These are integrity and memory
checks on one host, not production throughput or multi-host scaling results.

The same full client flow also passes with PostgreSQL metadata and MinIO object
storage using `huggingface_hub` 1.25.1 with `hf_xet` 1.5.2, 1.6.0, and 1.7.0.
Run that Docker-backed
qualification explicitly with:

```bash
SHARDLINE_REQUIRE_NATIVE_CLIENTS=1 cargo test --manifest-path e2e/Cargo.toml \
  --test native_huggingface_cli_e2e native_huggingface_cli_flows_with_postgres_and_minio \
  -- --ignored --test-threads=1
```

It is mandatory in E2E CI; a missing Docker service or native client fails the
qualification. This does not establish split-role deployment or URL-expiry
recovery coverage.

The plain HTTP fallback for Xet-backed Hub files streams with incremental length
and SHA-256 verification. It holds the final nonempty frame until verification
succeeds, so a client cannot receive the complete advertised length before the
integrity check. A failure interrupts the transfer; bytes already streamed cannot
be recalled. Single-range requests receive `206` and correct `Content-Range`;
unsatisfiable ranges receive `416`. The final selected frame is held until the
whole-file SHA-256 passes, including bytes outside the selected range. Memory
stays bounded, but this fallback still reads the complete file from storage.
Disconnecting stops further container reads and verification at cancellation
checkpoints. An already-issued storage request or bounded decoding step may
finish first; its worker keeps admission until it exits. Successful transfers
still require the same complete integrity checks.

Native plain HTTP downloads wait up to 30 seconds for existing reconstruction
capacity before starting metadata or payload work. This absorbs short bursts
from analytical range readers without increasing active verification work.
If capacity remains exhausted, the Hub LFS bridge preserves HTTP
`503` (or upstream `429`) with `Retry-After: 1`. Clients should retry after that
delay. This keeps temporary overload distinct from an internal failure; it does
not bypass admission or weaken content verification.

The internal Hub-to-Xet transfer has a five-minute total deadline, independent
of the webhook client's ten-second deadline. Analytical E2E tests explicitly
set DuckDB's `http_timeout` to 120 seconds so unoptimized verification on small
CI hosts can finish. These tests establish protocol and integrity behavior;
they do not establish that DuckDB's default 30-second budget is sufficient for
every file or host. Slow storage and full-file verification can still make
small range requests expensive. Measure that cost for your workload before
choosing a client timeout.

Internal reads use a separate connection pool and can resolve the configured
Shardline endpoint to private service addresses. Webhook delivery retains its
public-only DNS policy. Internal reads do not follow redirects or use environment
proxies, so their scoped token stays on the configured route.

The analytical HTTP qualification uses DuckDB 1.5.5 and PyArrow 25.0.0, with
unmodified Hub 2.2.0/hf_xet 1.7.0 uploads. It passes regular and native-Xet
Parquet reads on local storage and PostgreSQL/MinIO, with combined or split
API/transfer listeners on one host. It verifies selected payload bytes,
aggregates, actual `206` responses, ETag-matched ranges, `304`/`412` conditions
and rejection of anonymous reads. Run all four configurations explicitly:

```bash
SHARDLINE_REQUIRE_NATIVE_CLIENTS=1 cargo test --manifest-path e2e/Cargo.toml \
  --test native_huggingface_cli_e2e native_huggingface_http_analytics \
  -- --include-ignored --test-threads=1
```

DuckDB 2.0.0 with PyArrow 26.0.0 and Hub 2.3.0/hf_xet 1.7.0 also passes
these four configurations with the 120-second client budget, including two
simultaneous tests restricted to two CPU cores. This is a shared-host result.

The same current-client stack also passes all four configurations sequentially
in an optimized E2E build restricted to two CPU cores with a 30-second client
HTTP timeout. Native aggregate queries took 2.38–3.07 seconds in that run;
all four workflows completed in 35.38 seconds. This qualifies that fixture,
not general latency, sustained load or a shipped-binary performance comparison.
Content verification and authorization assertions remain unchanged. Reproduce
with the required Python clients and Docker available:

```bash
SHARDLINE_REQUIRE_NATIVE_CLIENTS=1 \
SHARDLINE_ANALYTICAL_HTTP_TIMEOUT_SECONDS=30 \
taskset -c 0,1 cargo test --manifest-path e2e/Cargo.toml --release \
  --test native_huggingface_cli_e2e native_huggingface_http_analytics \
  -- --include-ignored --test-threads=1 --nocapture
```

The test timeout override accepts 1–120 seconds and otherwise defaults to
120 seconds for unoptimized CI. It does not change a server timeout or a
production DuckDB setting. The test emits aggregate durations without URLs,
credentials or query results.

The remote cases require Docker PostgreSQL and disk-backed MinIO. This does not
qualify multi-host deployments, other analytical client versions or sustained
overload. The E2E workflow explicitly selects all four cases with the pinned
Hub 2.2.0 analytical environment; required native-client mode makes missing
prerequisites fail qualification.

The Hub token exchange keeps the legacy `token` field and adds the `casUrl`,
`accessToken`, and `exp` JSON fields expected by current `hf_xet` clients. It also
returns the corresponding Xet response headers. Token exchange verifies that the
repository and revision exist, rejects a revision outside the Hub token's scope,
and scopes the resulting CAS token to the requested revision. `/preupload` selects LFS for files
above 8 MiB (and files explicitly marked LFS by the client), while smaller files
remain inline. For large uploads, the LFS batch endpoint now selects the Xet
transfer when the client offers it and the request has write authorization. The
CLI then uploads Xorbs and a shard to Shardline's CAS routes; the end-to-end test
checks those CAS upload counters increase and that the file body did not go through
the ordinary Hub LFS upload route. Shardline records the file's LFS SHA-256 in its
repository-scoped Xet identity, so the committed pointer remains readable by Hub
downloads. Clients without Xet support continue to use the basic LFS upload path.

Multi-file commits stream one NDJSON instruction at a time rather than buffering
the complete upload. The transport accepts up to 1 GiB per request, with a
16 MiB limit per encoded line and the existing 10 MiB decoded inline-file limit.
The server retains bounded queued file content and publishes the revision only
after the complete request validates. A failed or interrupted request may leave
unpublished content-addressed blobs, but cannot publish a partial file tree.
Existing Git projection and tree limits still apply.

Stored LFS objects and inline `/resolve` files support bounded single-range
GETs: a satisfiable `Range` returns `206` with `Content-Range`, an unsatisfiable
range returns `416`, and a nonmatching `If-Range` causes a full `200` response.
Multi-range requests are currently ignored and return the full object. Xet-backed
plain HTTP downloads, inline resolves and stored LFS downloads support strong `If-Match`, weak comparison for
`If-None-Match`, and strong `If-Range`; a stale or weak `If-Range` requests the
full representation. HEAD returns the full length and quoted SHA-256 ETag without
reconstructing the file. The native-client E2E flow also checks these plain HTTP
operations through the normal Hub resolve redirect with SQLite and with
PostgreSQL/MinIO. A separate HTTP test covers conditional GET/HEAD through inline,
direct LFS and resolve-to-LFS paths and verifies authentication precedes cache
validation. Date validators are not advertised, since these routes do not provide
Last-Modified.

DuckDB 1.5.5 also passes authenticated Parquet queries over the ordinary Hub
resolve URLs for private regular and native-Xet files on the local backend.
The test uses PyArrow 25.0.0 fixtures uploaded with `huggingface_hub` 1.25.1 and
`hf_xet` 1.5.2, observes actual `206` responses through the resolve/LFS path and
rejects anonymous reads. The newer four-configuration qualification above covers
remote storage and split roles for its named client versions; this older-client
case does not extend that matrix or measure backend read amplification.

End-to-end expired-URL recovery passes with Hub 1.25.1 and `hf_xet` 1.5.2,
1.6.0 and 1.7.0, using both local storage and PostgreSQL/MinIO. Tests cover
combined roles and separate API/transfer listeners on one host: an expired
headerless fetch returns 403, the client refreshes reconstruction, and the new
URL downloads the exact file bytes. The Shardline SDK also passes bearer reads
through an expired capability URL with separate roles on both backends.
These results cover the named versions and local deployment fixtures; they do
not establish the complete client-version matrix or multi-host behavior.

Hub and native Xet both use the `/api/{provider}/{owner}/{repo}/tree/{rev}` and
`/revisions` paths. Hub requests keep their existing `application/json` behavior.
Native Xet metadata clients send
`Accept: application/vnd.stexs.xet-metadata+json, application/json`, which selects
the Xet tree and revision-list response shapes on those overlapping paths. The
non-overlapping Xet path registration and single-revision routes remain available
when both frontends are enabled. The `hf` CLI flow is unchanged and continues to use
the Hub response format.

## Using with Git

The Hub API supports Git Smart HTTP protocol for clone, fetch, and push:

```bash
# Clone a repository
git clone http://localhost:8080/models/my-org/my-model

# Push changes
cd my-model
git remote add hub http://localhost:8080/models/my-org/my-model
git push hub main
```

Hub SDK fields (`sha`, `commitOid`, history IDs and reference targets) use the
same Git commit IDs as Git discovery. Previously issued opaque Hub revision IDs
remain valid for reads and conditional writes. The legacy `commit_id` response
field and `/revisions` endpoint retain internal IDs for existing integrations.
Clients comparing saved opaque IDs with new SDK responses should resolve the
saved revision first; the returned SDK ID identifies the same immutable commit.
Content and internal revision identities are unchanged. The ID mapping is retained
as an actual commit object in the existing repository/provider-scoped Git archive,
so it survives restarts and branch deletion without another service or migration.
Older server binaries still read the unchanged stored revisions, but do not
understand newly returned Git aliases for NDJSON commits. During a rolling upgrade,
route Hub SDK traffic to upgraded nodes; rollback integrations can retain the
legacy `commit_id` or `/revisions` IDs. This does not require rewriting uploaded data.
Git commit trees contain the actual inline bytes and LFS pointer metadata.
Tree listings and paths-info expose Git blob and tree IDs, including for LFS
pointers; these differ from payload SHA-256 and internal storage hashes. Listed
paths are relative to the repository root even when listing a nested directory.
Recursive listings include directories as well as files. A missing directory,
or a file requested as a tree, returns HTTP 404 with `X-Error-Code: EntryNotFound`;
an existing directory with an exhausted cursor or zero page limit remains a
successful empty page. SQLite and PostgreSQL derive paged directory entries from
the selected immutable file tree within the existing bounded read transaction.
`expand=true` adds verified last-change commit metadata and returns at most
50 entries per page. The SDK follows the `Link` header automatically; each next
page stays on the same immutable commit even if the branch advances. Links use
the configured public URL. Shardline does not report security scan results that
it has not performed. Existing non-expanded JSON array and explicit cursor
behavior is preserved. History lookup retains metadata and legacy payload-work
bounds; cached tree metadata is not charged as another full payload read.
Hub-created commits retain verified tree metadata in the existing Git archive so
subsequent metadata lookups do not need to read file payloads again. Missing
derived trees can be rebuilt from verified source content; corrupt archived
trees fail validation. These IDs describe committed content, not the current
health or availability of its backing objects. Downloads and FSCK still verify
the stored content.
Git commit identities depend on repository, Hub revision, and ancestry,
so adding a branch or tag does not change an existing commit. Commits received
through Git preserve their original object bytes, modes, and author metadata in
a repository and provider scoped object archive. Upload-pack accepts only commits
reachable within the authorized repository and sends objects matching those IDs.
Existing `.gitattributes` files are preserved; include LFS tracking rules in your
repository when Git LFS checkout is required.

The tested Smart HTTP path supports Git protocol v0/v1 upload-pack and
receive-pack, plus protocol v2 upload-pack (`ls-refs` and `fetch`). Native Git
2.52 tests cover v2 clone and incremental fetch, depth-, date-, and
ref-exclusion-based shallow history, blob omission/size limits, tree-depth
filtering, lazy checkout, and strict repository checks. Filters also support
`object:type` selection for blobs, trees, commits and tags, with explicitly
wanted objects retained; native Git 2.52 checks omitted-tree lazy fetch under
protocol v0, v1 and v2. Combined filters support bounded intersections of
blob-size, tree-depth and object-type selection.
Sparse-checkout filters and other unimplemented filters are rejected.
Protocol v2 is currently used for upload-pack; Git pushes use the v0/v1
receive-pack flow, including when the client is configured for protocol v2.
Receive-pack negotiates and validates the `push-options` packet section. The
server does not expose Git receive hooks, so accepted options currently have
no hook consumer. The frontend supports `side-band-64k`, not the older
`side-band` mode.

Git export is bounded to 10,000 history revisions, 10,000 references, 100,000
unique objects, 128 tree levels, 1,024 bytes per projected path, 1,000,000
cumulative file entries across projected revisions, and 64 MiB of unique
uncompressed object payload. Requests exceeding these bounds fail explicitly.
File-entry quotas are applied in SQL before decoding the next revision. Object
quotas are enforced during construction and archive decompression. Compression
and HTTP framing retain additional bounded buffers. Inline bytes are loaded once
per unique content identity during an export; unchanged trees reuse their
previously constructed objects. Tree construction borrows paths instead of
cloning complete file maps at every directory level. Git projection, reference collection, pack compression and push parsing run on
Tokio blocking threads so those storage waits and CPU operations leave the async
executor runnable. Push metadata and object publication stay in the request
handler to preserve the server maintenance guard during mutation. Canceling a request does not cancel already running
blocking work. Archived Git objects are deduplicated and
excluded from ordinary chunk GC, like Hub inline objects. Deleting a repository
removes its metadata and refs; archived bytes remain retained.

New Git pushes accept regular files and directories. Symlinks, submodules and
other unsupported modes are rejected because Hub file metadata cannot represent
them faithfully. Existing archived Git objects retain their original modes on
export. Unsafe, reserved or duplicate tree names are rejected; push traversal
is bounded to 100,000 expanded entries, 128 levels and 64 MiB of aggregate path
bytes. LFS pointers require one lowercase 64-hex SHA-256 OID and one size field.
Their payload must match both the SHA-256 and size, either in the push or already
uploaded to the authorized repository. Missing or mismatched payloads fail before
the new tree or reference is published.
Hub LFS route errors use `application/vnd.git-lfs+json` and the LFS `message`
field, including errors raised during request decoding or object-store lookup.

Historical short Hub revision IDs that require recovery are not exported. A
recovered revision starts Git history at that recovery boundary. Historical Git
revisions without their original object archive fail explicitly; restore the
original objects rather than substituting a different commit under the same ID.

## Supported Endpoints

| Endpoint | Method | Description |
| --- | --- | --- |
| `/health` | GET | Health check |
| `/api/whoami-v2` | GET | Current user identity |
| `/api/repos/create` | POST | Create or retry creation of the repository named by the write token |
| `/api/{type}/{ns}/{repo}` | POST | Create the repository only when its path matches the write token |
| `/api/{type}/{ns}/{repo}` | GET | Get repository info |
| `/api/{type}/{ns}/{repo}/preupload/{rev}` | POST | Pre-upload check |
| `/api/{type}/{ns}/{repo}/commit/{rev}` | POST | Commit file changes |
| `/api/{type}/{ns}/{repo}/revision/{rev}` | GET | Revision metadata and siblings |
| `/api/{type}/{ns}/{repo}/commits/{rev}` | GET | SDK first-parent history from Hub metadata, bounded to 10,000 repository revisions; unknown authors remain empty |
| `/api/{type}/{ns}/{repo}/refs` | GET | Live branches and tags in Hugging Face SDK format (at most 10,000 references) |
| `/api/{type}/{ns}/{repo}/branch/{name}` | POST, DELETE | SDK branch creation from an existing revision and safe branch deletion |
| `/api/{type}/{ns}/{repo}/tag/{name}` | POST, DELETE | SDK lightweight tag creation (POST uses the source revision in the path) and deletion |
| `/api/{type}/{ns}/{repo}/tree/{rev}` | GET | Browse the repository root |
| `/api/{type}/{ns}/{repo}/tree/{rev}/{path}` | GET | Browse file tree |
| `/api/{type}/{ns}/{repo}/paths-info/{rev}` | POST | Resolve requested file and directory metadata in the format consumed by `huggingface_hub`; accepts form-encoded client requests and JSON |
| `/api/{type}/{ns}/{repo}/xet-read-token/{rev}` | GET | Xet read token exchange |
| `/api/{type}/{ns}/{repo}/xet-write-token/{rev}` | GET | Xet write token exchange |
| `/{type}/{ns}/{repo}/resolve/{rev}/{path}` | GET | Resolve and download a typed file |
| `/{ns}/{repo}/resolve/{rev}/{path}` | GET | Resolve and download a model file |
| `/{type}/{ns}/{repo}/info/refs` | GET | Git Smart HTTP refs discovery |
| `/{type}/{ns}/{repo}/HEAD` | GET | Git HEAD reference |
| `/{type}/{ns}/{repo}/git-upload-pack` | POST | Git clone/fetch (upload-pack) |
| `/{type}/{ns}/{repo}/git-receive-pack` | POST | Git push (receive-pack) |
| `/objects/batch` | POST | LFS batch request |
| `/{ns}/{repo}.git/info/lfs/objects/batch` | POST | Git LFS discovery endpoint used by `hf upload` |
| `/lfs/objects/{oid}` | PUT | Upload an LFS object |
| `/lfs/objects/{oid}` | GET | Download an LFS object |
| `/lfs/objects/{oid}/verify` | POST | Verify an uploaded LFS object by size and SHA-256 |

Git Smart HTTP supports safe branch and tag deletion through the normal Git zero-SHA
receive-pack update.
Deletion is compare-and-delete atomic, so stale pushes cannot remove a ref that has
advanced; the default `main` branch is protected.
Removing a ref does not remove the immutable commit history.

## Architecture

Hub metadata is persisted to the configured index store:

- **SQLite** (local): Hub metadata remains in
  `{root_dir}/hub/metadata.sqlite3`. Native storage records and Xet identities
  remain in `{root_dir}/metadata.sqlite3`; an explicit scoped lookup connects
  the two databases without moving existing data.
- **Postgres** (production): the configured index database. Run the upgraded
  binary's owner-managed migrations before starting upgraded servers; Hub Xet
  identities and LFS locks add durable tables.

Hub metadata storage goes through the same index store contract used by the rest of
the server.
Postgres-backed revision creation and repository deletion lock the repository row in
their transaction. This serializes mutable ref updates and delete-vs-push races across
Shardline replicas; the parent-SHA comparison then permits exactly one of two stale-parent
competitors to advance a ref. Immutable file and commit content may be prepared before
that linearization point, so a losing request can leave unreachable content for GC, but
it cannot overwrite the winning ref or resurrect a deleted repository.
When an auth provider is configured, Hub API routes validate bearer tokens against it,
the same as every other frontend.

The Hub API merges into the main HTTP router at startup, sharing the same bind address
and TLS configuration as all other frontends.

`paths-info` returns the actual Git OIDs of files and directories, LFS object
SHA-256/size/pointer-size metadata where applicable, and a verified Xet hash for
native Xet files. Requested paths that do not exist are omitted. With
`expand=true`, each returned entry includes the first-parent commit that last
changed its content or Git mode. Historical queries resolve immutable public
commit IDs. Security scan results are omitted because Shardline does not run
the upstream Hub's security scanners.

Metadata hashing shares Git work admission. A request permits up to 1,000 paths
and 64 KiB of request data. Inline-content hashing is limited to 64 MiB;
expanded history additionally permits at most 10,000 revisions and one million
file entries, with a cumulative 64 MiB inline-content budget. Native Xet and
LFS payloads are represented by their Git pointer blobs and are not downloaded
to compute metadata. Git-origin paths use verified archived trees directly.

For native Xet files, resolve HEAD and GET responses include `X-Xet-Hash` and
an absolute `Link` with `rel="xet-auth"`, built from the configured public URL.
The normal `hf download` flow uses that discovery data to fetch a read token
and reconstruct through CAS. Clients using plain HTTP continue through the
LFS download URL.

Storage kind, rather than file size, selects the download path. Small native
Xet and LFS files use the same discovery and transfer behavior as larger ones;
ordinary inline files continue streaming from the resolve route.

> **Internals note:** the Hub storage layer is defined by the `HubStore` trait in the
> `shardline-index` crate, accessed type-erased as `BoxedHubStore`, and its routes
> validate tokens through the `HubAuth` wrapper around the shared auth provider. These
> names are implementation details; user documentation refers to the Hub metadata
> store and the shared bearer-token auth.

## Limitations

Shardline implements the repository-storage workflows in the table above, not the entire
Hugging Face SaaS product.
Collections, user profiles, discussions, jobs, inference endpoints, Spaces runtime
management, and advanced Hub administration APIs are outside this frontend's current
contract. Webhooks, model cards, basic repository search, and dataset preview routes are
implemented.
