# HuggingFace Hub API Compatibility

Shardline includes a HuggingFace Hub API compatibility layer for the repository
workflows listed below. It is a compatible subset, not a drop-in replacement for every
Hugging Face Hub API or client transfer mode.

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

Hub owns overlapping `/api/.../tree` and `/api/.../revisions` routes when Hub and Xet
frontends run together. The tested CLI upload, commit, and download flow works with
that combined configuration; the native Xet frontend's separate metadata operations
on those paths are not exposed in that mode.

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
| `/api/repos/create` | POST | Create a repository |
| `/api/{type}/{ns}/{repo}` | POST | Create a repository (typed) |
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

- **SQLite** (local): the existing `{root_dir}/metadata.sqlite3` database, shared
  with the storage index and recovery evidence.
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
