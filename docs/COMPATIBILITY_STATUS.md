# Compatibility Status

Shardline has a CAS-agnostic runtime with explicit protocol frontends.
The current compatibility contract is scoped to the protocol and operator workflows
documented in this repository.
Promotion between maturity tiers follows the checked-in evidence requirements in
[Stability Graduation Policy](STABILITY_GRADUATION.md).

## Current Source-Tree Contract

Shardline 1.13.0 is a content-addressed backend for the validated workflows
covered by this repository. This document describes the 1.13.0 source contract;
it does not claim compatibility with every upstream service API or client version.
See the [release notes](../CHANGELOG.md) and the
[1.12.1 upgrade procedure](ROLLING_UPGRADE.md#protocol-frontend-upgrade-from-1121-to-1130)
for the included changes and mixed-version limits.

Native Xet uploads now retain the verified Xorb container without an additional
decoded chunk copy. Existing Xorbs and standalone chunks remain readable; the
upgrade does not delete old payloads. Independent native chunk reads use verified
member discovery and authenticate the backing Xorb. Shard publication records
the member locations, and index rebuild can recover them from verified content.
Pause native uploads while upgrading every reader, then resume writes once all
replicas support this layout. Unchanged v1.12.1 readers
that require standalone native chunks cannot serve those reads for new uploads;
full rollback to those readers after new writes is not supported. Generic chunk
uploads continue storing their explicitly requested standalone objects.

Clients that require an `hf_` token prefix can use the opt-in
`shardline_protocol::format_hub_token` formatter on an existing signed Shardline
credential. The prefix changes presentation only; signature, expiry and repository
permissions are still verified. Existing bare tokens and default token issuance
remain supported. Use prefixed tokens only after all receiving servers support
this format; older releases expect bare credentials.

For native-Xet files, Hub resolve supports explicit
`Accept: application/vnd.xet-fileinfo+json` discovery with file identity, size,
token-refresh and reconstruction links. Requests without that media type retain
the ordinary download behavior. The unmodified `@huggingface/hub` 2.17.1 client
passes model and dataset native upload, token refresh, delta editing and
byte-for-byte native download on SQLite/local, PostgreSQL/local and
PostgreSQL/MinIO, including split API/transfer roles behind a path-routing front
door. MinIO qualification includes memory-backed protocol fixtures and a
separate disk-backed upload/edit/download case; neither establishes recovery
after a MinIO process crash. Cancellation and SDK background handle
cleanup remain separate qualification items. The one-shot test exits
after all awaited assertions, rather than waiting for SDK background handles.

In the deterministic 9 MiB fixture with a 32 KiB edit, Xorb upload payload falls
from 9,438,424 to 266,697 bytes (97.17%). This measures client-to-CAS Xorb bytes,
excluding shard/control traffic and backend duplication; it is not a general
storage or provider-billing saving.

## Surface Maturity

| Surface | Tier | Evidence |
| --- | --- | --- |
| Xet CAS | **Stable** | Native Xet upload and download flows; checked-in `git` + `git-lfs` + `git-xet` push, clone, fetch, pull, historical checkout, and sparse checkout flows exercise Shardline CAS, with broader scenarios using a mock forge LFS control plane. Separate stock git-xet 0.2.1 tests use the actual Hub Git/LFS control plane for native upload, repeat-upload recognition, cold clone, incremental push/fetch, fast-forward pull and historical checkout on SQLite/local and PostgreSQL/MinIO. They verify both file versions, commit identities and `git fsck --strict`, with downloads through standard Git LFS and no basic-upload fallback. CAS batch reconstruction accepts the published JSON `POST /v1/reconstructions` request while retaining Shardline's older GET form. Authenticated reconstruction URLs carry short-lived repository/file/Xorb/range-bound capabilities and are tested without an Authorization header; stock `hf_xet` expiry/refresh behavior has local and PostgreSQL/MinIO split-role qualification. Download capabilities authorize Shardline endpoints only; clients do not receive direct backend object-store URLs or credentials. With Hub and Xet enabled together, native Xet metadata clients select the Xet tree and revision-list response formats using `application/vnd.stexs.xet-metadata+json`; normal Hub requests keep their existing routes and formats. |
| Git LFS | **Beta** | LFS batch negotiation plus direct object GET/HEAD/PUT routes; pinned Git LFS 3.7.1 and adjacent 3.6.1 push/pull plus separate pull and fetch --all flows; conformance-tested but limited production evidence |
| Bazel HTTP remote cache | **Beta** | `ac` and `cas` object GET and PUT routes; pinned Bazel 8.7.0 and 9.2.0 remote-cache flows with `remote_download_outputs=all` and `toplevel`; conformance-tested but limited production evidence |
| OCI Distribution | **Stable** | Full blob upload/download, manifest PUT/GET/HEAD/DELETE, tag listing with pagination, token-service flow at /v2/token, upload cancellation, scoped upload-session handling; checked-in `skopeo`, Docker, Helm, and Podman client coverage |
| Hugging Face Hub API | **Beta** | Repository create/info/delete, revision and tree lookup, size-aware preupload, NDJSON commit, resolve/download, dataset viewer routes, basic search, webhooks, Git Smart HTTP clone/fetch/push, and LFS transfers. The native `hf` CLI E2E covers small model and dataset files plus 9 MiB `hf_xet` uploads, Hub commits, and byte-for-byte downloads for both repository types. It verifies Xet shard and Xorb transfer, no ordinary LFS body-upload fallback, and repository-scoped SHA-256 to native Xet file mappings. The flows pass with Hub enabled alone and with Hub, Xet, and S3 enabled together. The Xet-token response includes current JSON connection fields and matching headers. Stock git-xet 0.2.1 also passes native upload, cold clone, incremental push/fetch, fast-forward pull and historical checkout through the actual Hub Git/LFS control plane with separate client homes on SQLite/local and PostgreSQL/MinIO; broader Git/Xet scenarios retain their mock forge fixture. Git Smart HTTP supports protocol v2 upload-pack (`ls-refs` and `fetch`) plus the traditional v0/v1 upload-pack and receive-pack flow; receive-pack supports negotiated `push-options` but has no Git receive hooks. Git protocol v2 defines fetch negotiation; push continues through receive-pack. |
| S3 frontend (protocol) | **Stable** | Scoped object, listing and multipart workflows documented in [S3_FRONTEND.md](S3_FRONTEND.md), not the entire AWS S3 service. Existing access-key-as-token and Bearer credentials remain supported. Optional configured signing identities verify SigV4, session credentials, presigned GET/PUT, signed browser PostObject policies, classic signed payload streams, and signed checksum trailers. Trailer coverage is qualified with the documented MinIO client version; broader client-version and remote-failure qualification remains. Extended service APIs remain unimplemented as listed in the S3 guide. Native client coverage includes `mc`, AWS CLI, boto3, `s3cmd`, `rclone`, and pyarrow 25. |
| Ed25519 auth provider | **Beta** | Signing and verification, bounded overlapping key rotation, verification-only mode, environment/TOML configuration, authenticated HTTP flows, and operator CLI mint-to-verify interoperability have targeted tests; broader deployment evidence remains graduation work |
| Local filesystem storage | **Stable** | Checked-in adapter, concurrency, and operator workflow coverage |
| S3-compatible storage | **Stable** | Checked-in object read/write/list and HTTP integration coverage |
| Postgres metadata | **Stable** | Checked-in index, dedupe, concurrency, and operator workflow coverage |
| SQLite metadata | **Stable** | Checked-in local single-node and operator workflow coverage |
| Redis reconstruction cache | **Beta** | TLS/mTLS connectivity, bounded per-operation timeout, corrupt-value repair, partition and kill/restart recovery, plus operation-scoped cross-replica cold-load reservations with cancellation takeover and stale-owner fencing are validated; broader production evidence remains |
| Provider integration | **Beta** | Checked-in token issuance, webhook handling, and repository-scoped authorization coverage for GitHub, GitLab, Gitea, Codeberg, and the generic adapter |

The real-Hub PostgreSQL/MinIO git-xet test uses two Hub instances with independent
local roots and shared durable backends. After the original HTTP server and its
startup workers stop, the peer serves a cold clone, accepts an incremental native
upload and preserves historical checkout. The test confirms the original endpoint
is unreachable. This qualifies shared-backend takeover on one host.
Both real-Hub cases also truncate a successful Git publication reply. The stock
client reports failure while the accepted ref remains visible; retry succeeds
without another Xorb or shard upload. The remote case retries through the peer
after stopping the original Hub, then verifies cold-cloned and historical bytes.

### Tier Definitions

- **Stable**: broad checked-in route, integration, and native-client coverage for the
  advertised workflows.
  This tier does not claim a particular deployment's production, load,
  failure-injection, or upgrade history.
- **Beta**: checked-in route or client coverage exists, but the compatibility surface or
  operational evidence is narrower.
- **Experimental**: implemented with targeted tests, but configuration or
  interoperability may still change.
- **Internal**: architectural component, not a user-facing promise.

### Client Test Execution

The following S3 frontend clients pass the real-HTTP source-tree suite on
SQLite/filesystem storage with the legacy access-key-as-token credential mode.
The suite requires every listed tool and reports missing tools as failures in
mandatory qualification. These results do not qualify every client operation,
verified SigV4, remote storage, split roles or a released binary; those have
separate tests.

| Client | Tested version | Executed workflow |
| --- | --- | --- |
| AWS CLI | 1.46.1 | Upload, download, copy and delete |
| MinIO `mc` | RELEASE.2025-08-13T08-35-41Z | Object round trip, listing/stat, multipart upload and recursive mirror/removal |
| Boto3 | 1.43.95 | Object and multipart SDK round trips |
| `s3cmd` | 2.4.0 | Upload, download, list and delete |
| `rclone` | 1.60.1, 1.75.0 | Copy, list, read and delete with `acl = bucket-owner-full-control` |
| PyArrow | 25.0.0 | Parquet write, read and metadata inspection |
| DuckDB with PyArrow | 1.5.5 / 25.0.0 | Parquet glob, filtering, aggregation and write |

Targeted stock Git LFS 3.7.1 and 3.6.1 checks pass push/pull, fetch-all and
byte-exact downloads with local storage, plus a dedicated MinIO-backed transfer.
Both versions also pass lock creation/listing/verification, competing owners,
forced unlock and server/backend reopen checks across SQLite and PostgreSQL
HTTP server instances. These instances share one test host; process-kill recovery
and the remaining transfer-failure scenarios have separate qualification.
The dedicated MinIO transfer runs explicitly for both versions in E2E CI.

Separate HTTP failure-injection tests cover Hub and standalone LFS uploads with
SQLite/local and PostgreSQL/MinIO storage:
a disconnected partial body remains unavailable, a complete retry succeeds, and
a mismatching retry cannot replace valid bytes. Reopening the backend preserves
the bytes and repository isolation. Streamed basic uploads accept exactly 64 MiB
and reject one byte more, including retries against an existing object, without
requiring a Content-Length header. The E2E gate explicitly includes the disposable
PostgreSQL/MinIO case. A separate Unix subprocess test kills local Hub and
standalone LFS servers with SIGKILL during an unfinished request, verifies a full
retry after restart, then kills the restarted server after acknowledgment and
verifies the bytes after another restart. PostgreSQL/MinIO also passes this
process-kill sequence, retries through a fresh server root, and serves acknowledged
bytes from a separate live replica after the uploader is killed. These processes
share one host and the same durable backends. This covers wire-upload interruption;
specific internal persistence boundaries and split-role cancellation remain separate
qualification items.

The E2E workflow pins these Hub/Xet client combinations. This is the required
test matrix, not a claim that the current branch has passed release CI.

| `huggingface_hub` | `hf_xet` | Required workflows |
| --- | --- | --- |
| 1.25.1 | 1.5.2 | Model/dataset workflows on SQLite/filesystem, PostgreSQL/filesystem and PostgreSQL/MinIO; 1 GiB native upload/download with concurrent slow HTTP readers, combined and separate API/transfer roles; expired download URL and refresh |
| 1.25.1 | 1.6.0 | Model/dataset workflows on SQLite/filesystem, PostgreSQL/filesystem and PostgreSQL/MinIO; 1 GiB transfers with combined and separate API/transfer roles, interrupted uploads, URL refresh and identity checks |
| 1.25.1 | 1.7.0 | Model/dataset upload and download on local and PostgreSQL/MinIO storage, 1 GiB transfers with combined and separate API/transfer roles, URL refresh and interrupted uploads |
| 2.2.0 | 1.7.0 | Model/dataset upload and download on SQLite/filesystem, PostgreSQL/filesystem and PostgreSQL/MinIO storage; 1 GiB split-role PostgreSQL/MinIO transfer with cancellation and concurrent slow readers; URL refresh with combined and split API/transfer roles; endpoint-local stock `ModelCard.push_to_hub()` on local and PostgreSQL/MinIO storage |

Versions outside this matrix require qualification before support is claimed.
The selected workflows do not establish every upstream Hub API for these versions.

The large-file fixture's RSS ceiling applies to the server/test process and
excludes stock-client RSS. Its `/proc/self/io` counters include waited-for
client subprocesses, so upload and cold-download totals do not isolate server
I/O, scratch writes or SSD wear. Use independently measured server and client
processes for operational-cost comparisons.

An additional HTTPS front-door check passes a 9 MiB native upload and
SHA-256-verified download with Hub 1.25.1 / `hf_xet` 1.5.2 and 1.6.0, and Hub
2.2.0 / `hf_xet` 1.7.0, against owned PostgreSQL/MinIO fixtures. Clients trust
an explicitly configured test CA; Hub requests and direct native Xet uploads
reject an untrusted CA and a hostname mismatch. TLS terminates at the Shardline
reverse proxy; the backing MinIO service is not a client download endpoint. These shared-host
checks do not qualify every TLS proxy, split-role deployment or backend TLS
configuration, and do not replace final-head CI.

The committed HTTPS regression test also passes with these three Xet versions
and with Hub 2.3.0 / `hf_xet` 1.7.0.
It uses separate upload/download processes and empty caches, checks rejection
of an untrusted certificate and a trusted-CA certificate with the wrong hostname,
counts successful HTTPS Xorb download requests, and verifies the downloaded
SHA-256. Negative checks exercise both ordinary Hub API calls and unmodified
native Xet uploads. They require certificate-specific rejection evidence,
including hostname-specific evidence for the mismatched certificate, rather
than accepting a generic network failure. Native diagnostics are inspected in
a bounded private fixture log and are not printed. The pinned-client CI
selections include this test.

Additional source-tree qualification with Hub 2.3.0 / `hf_xet` 1.7.0 passes
model/dataset workflows on SQLite/local, PostgreSQL/local and PostgreSQL/MinIO,
plus expired-URL recovery with combined and split API/transfer roles. The full
workflows include competing SDK commits, rejected basic-LFS negotiation without
publication, a healthy native-Xet retry and byte-exact cold download after the
branch advances. These selected shared-host checks do not qualify every Hub API,
multi-host load or the complete failure matrix. They are separate from the
pinned CI requirements above and do not establish final-head release acceptance.

The Hub 2.3.0 combination also passes a 1 GiB native upload and cold download
on PostgreSQL/MinIO with separate API and transfer roles, an abandoned HTTP
response and two concurrent slow full-file HTTP readers. The server/test process
peaked at 213,868 KiB RSS, excluding client memory, below the fixture's 384 MiB
ceiling. This shared-host resource check is not a paired performance measurement;
its process I/O includes reaped clients as described above.

DuckDB 2.0.0 / PyArrow 26.0.0 also pass authenticated Hub HTTP Parquet reads
with Hub 2.3.0 / `hf_xet` 1.7.0. Regular and native-Xet files pass filtering,
aggregates, HEAD/conditional checks and real partial responses on local storage
and PostgreSQL/MinIO, with combined and split roles. Anonymous reads are denied.
These four shared-host workflows qualify the tested analytical reads, not all
DuckDB operations or performance; see [DuckDB integration](DUCKDB.md).

Hub 1.25.1 and 2.3.0 with `hf_xet` 1.7.0 also pass a real PostgreSQL publication
session termination during a 9 MiB native upload. The interrupted transaction
publishes no records; the stock client receives a retryable v2 shard error,
retries through v2 without a v1 fallback, and cold-downloads the exact bytes.
The same owned PostgreSQL/MinIO cut also passes Hub 1.25.1 with `hf_xet`
1.6.0 through v2, and `hf_xet` 1.5.2 through v1: the older client receives
HTTP 503, retries once, and cold-downloads the exact bytes. The regression
checks the expected wire version for each pinned client, one failed attempt,
one successful attempt and no records from the interrupted publication.
This PostgreSQL/MinIO fixture does not qualify every transient failure or
multi-host takeover scenario.

Stock `hf_xet` 1.5.2 and 1.6.0 also pass a 512 MiB PostgreSQL/MinIO
upload-recovery drill at six SIGKILL boundaries: intent creation, storing,
completion of object work, stored state, metadata commit and visible state. Each
interruption is followed by a retry and SHA-256-verified download. These
tests run a server subprocess built from the current library; they do not
replace released-binary upgrade tests or qualify every split-role and
competing-replica failure scenario.

These six-cut tests were repeated after enabling copy-free native storage and
buffered shard validation, for both local storage and PostgreSQL/MinIO, with
`hf_xet` 1.5.2 and 1.6.0 (Hub 1.25.1 environments). All 24 interruption/retry
cases passed with verified downloads. This checks older native clients against
the upgraded server; it does not imply that an unchanged older Shardline server
can read newly written copy-free member locations, or qualify all Hub workflows.

Copy-free storage also passes a 1 GiB Hub transfer through separate API and
transfer roles on local storage with hf_xet 1.5.2, 1.6.0 and 1.7.0. These checks
include cold native downloads, cancellation and two concurrent verified slow
HTTP readers. Server/test peak RSS stayed below 117 MiB; client memory is
excluded. This does not establish upload-speed parity or remote load capacity.

The mixed-client recovery gate separately pairs `hf_xet` 1.7.0 with 1.5.2
and 1.6.0. After each of the same six 512 MiB upload interruptions, both
clients retry through independent replacement replicas sharing PostgreSQL and
MinIO. The gate requires matching reconstructed size and SHA-256, and a visible
intent for the exact interrupted immutable object. It permits different legal
container boundaries between client versions. This is a shared-host recovery
test, not a throughput comparison or a mixed Shardline-version rollout test.
Both pairings were repeated after enabling copy-free native storage and buffered
shard validation. All twelve interruption cases passed, with both clients
retrying and verifying downloaded bytes through their replacement replicas.

Cold 1 GiB uploads with `hf_xet` 1.5.2, 1.6.0 and 1.7.0 also exercise
footerless normalization beyond the former 16 MiB limit. Each run sent
1,073,875,784 Xorb bytes and retained 1,074,681,584 bytes across 17 containers.
The 805,800-byte difference matches the serialized footers for 16,745 members
and 17 containers. All uploads, cold downloads, sixteen concurrent plain reads
and cancellation recovery verified successfully on PostgreSQL/MinIO. This
qualifies that workload, not every malformed-input or low-disk boundary.

Repository-wide read credentials also support plain HTTP full and range
downloads of native-Xet Hub files stored under a branch. The server selects a
verified storage scope inside the authorized repository rather than looking
for a revision-less storage record. Targeted stock-upload tests cover SQLite/
local and PostgreSQL/MinIO, with unrelated repositories and refs still denied.
Branch-limited credentials retain their existing scope checks; no metadata or
object-format migration is required.

Cold-cache deduplication after descriptor-index loss and rebuild also passes with
Hub 1.25.1 / `hf_xet` 1.5.2, 1.6.0 and 1.7.0, and Hub 2.2.0 / `hf_xet` 1.7.0.
Each combination covers local and PostgreSQL/MinIO storage with combined and
split API/transfer roles. The tests verify upload-byte reuse and exact downloads;
they do not replace predecessor-binary or maintenance-failure qualification.

Xet's newer [`GET /v2/file-chunk-hashes/{file_id}`](https://huggingface.co/docs/xet/api#9-get-file-chunk-hashes)
delta-upload API is implemented with repository authorization, chunk-aligned edit
windows and upstream-compatible gap commitments. HTTP tests cover proof
composition, verified legacy descriptor recovery and rejection of tampered
descriptors. A composed-file workflow uploads only the changed Xorb, reuses the
existing unchanged source, publishes the shard and downloads byte-exact content
with the upstream Xet download library. Two PostgreSQL-backed replicas preserve
unresolved-delete blocking across restart with either shared filesystem objects
or MinIO. The MinIO test uses separate replica roots and a fresh root after
restart; both replicas reject unauthorized repositories, return retryable errors
for unresolved deletion, recover after settlement of an attempt that issued no
DELETE, and reject a subsequently missing source. These HTTP fixtures do not
simulate a delayed provider DELETE completing after settlement. The official JavaScript
client workflow above also verifies automatic delta selection and byte-exact
downloads on PostgreSQL/MinIO, including split roles. These are distinct from
the composed-file fixture; neither establishes automatic delta selection by
the Python `hf` CLI or every delayed in-flight deletion scenario.

A separate PostgreSQL/MinIO HTTP regression pauses an issued DELETE before
MinIO applies it, restarts one API replica, and checks that both replicas reject
Xorb retries and shard publication with `503` and `Retry-After: 1`. This includes
retries whose upload intent was already complete. The test observes the actual
DELETE response before settling its marker, then verifies missing-source reads,
successful reupload and recovery. It does not establish cluster-wide exactly-once
execution or authorize clearing markers for requests whose outcome is unknown.

Native Xorb deletion guards use the request's original PostgreSQL session.
A configured test occupies the entire separate metadata pool and still completes
both a new upload and an identical retry. Existing pool limits, storage formats
and owner-loss rejection remain unchanged; this is not a measured throughput gain.

Snapshot-scoped Git credentials currently negotiate the legacy fetch protocol,
including when stock Git requests v2. Repository and branch credentials retain
native v2 support. Native v2 snapshot fetch remains unqualified; a requested-v2
clone that falls back to legacy framing is not evidence that it works.
Git 2.52.0 and 2.55.0 also reproduce recursive filtered fetching against an unmodified
stock shallow Git server; an unfiltered v2 clone of that fixture passes strict
fsck. The 2.55.0 filtered clone returns success after 32 recursive fetch commands
and connection/shallow-root errors; its exit code alone does not establish
healthy interoperability. Shardline's snapshot fallback remains necessary for
the tested clients.

The recursion also reproduces with Git's own source repository at tag v2.55.0,
served by stock Git at depth one. With a 64-process container limit, both
2.52.0 and 2.55.0 start 22 recursive fetch processes for filtered v2 and 31
for filtered v0; the unfiltered v2 control starts none. All six clones reach
the expected commit and pass strict fsck. The process cap bounds this control,
so these counts are not a protocol limit or proof of an inevitable hang.

Upstream Git's later [lazy-fetch recursion limit](https://github.com/git/git/commit/4360734e907c9c92e72217a8ba9bdc1989f3c3a5)
reduces this amplification but does not eliminate it. With that revision built
privately, the same filtered clone makes six fetch requests and emits five
shallow-root warnings; the unfiltered control makes one request without warnings.
Both pass strict fsck. This is a stock-Git comparison, not qualification of
Shardline's native v2 snapshot flow or a requirement to upgrade client Git.

A separate Git 2.55.0 diagnostic identifies pre-pack shallow-root classification
as a cause: it permits a promisor fetch while deciding whether an advertised
root exists locally, before the incoming pack supplies that root. Disabling
that fetch in an isolated client build reduces recursive fetch processes from
22 (filtered v2) and 31 (filtered v0) to zero in the same depth-one Git-source
control. All six baseline/patched clones reach the expected commit and pass
strict fsck. This experimental client patch is not distributed with Shardline
and does not qualify native v2 snapshots for existing stock clients; the
compatibility fallback remains in place.

The Hub API's native Git regression compares incremental-fetch HTTP response
bytes with a fresh clone at the same commit. It requires less transfer for the
incremental fetch, exact current and historical file contents, a successful
fast-forward pull, and strict Git object checks. This small fixture does not
establish large-repository throughput or concurrent resource limits.

Hub commit-history responses contain at most 100 revisions (the default), with
an optional `limit` from 1 to 100. The stock SDK follows `Link` headers to the
next immutable parent, so a branch advancing between pages does not replace the
remaining ancestry. Every page is authorized; snapshot-scoped credentials expose
only their permitted commit. Cold Git identity recovery uses 512-revision windows
and verified ancestor archives rather than collecting the entire uncached history
in one projection. A 10,001-ancestor regression exercises this path. Discovery is
bounded to one million revisions and file work shares one cumulative
one-million-entry budget. Git export and path-history limits remain unchanged;
this does not establish unbounded history or large-repository throughput.

Git request buffering, history projection and pack generation share the existing
two-slot identity work limit. Blocking work retains its slot after request
cancellation. Download responses retain the slot until their body is consumed
or dropped, handing data to HTTP in frames of at most 64 KiB. Existing request,
projection byte and object limits remain in place. This bounds admitted Git work
and pack buffers; it is not a total process RSS or connection-count guarantee.
Git advertises its supported legacy smart-HTTP capabilities, including v1 and
v2 push status reports and receive-side `side-band-64k`. Clients requesting
sideband receive the status stream through channel 1; other clients retain the
plain status format. Native Git checks verify negotiation, ordinary pushes,
branch deletion and rejected default-branch deletion. Ref names and object IDs
are not rewritten through proc-receive hooks, so v2's optional rewrite details
are absent. SQLite and PostgreSQL support negotiated atomic pushes: payloads
and trees are prepared first, then all refs and their reliability evidence
commit in one metadata transaction. A conflict or preparation/storage failure
rejects every requested ref. Native Git checks cover multi-ref creation,
updates, deletion and protected-ref rejection. Separate native HTTP workflows
run through ordinary server startup on SQLite/local, PostgreSQL/local and
PostgreSQL/MinIO storage, then verify a fresh clone, historical file bytes and
strict Git fsck after managed restart. The PostgreSQL workflows terminate the
observed publication session before its second ref write, verify both old heads
and the default branch remain unchanged, then retry. All three backends also
drop the client status after observing a successful server commit, verify both
new heads and retry through the normal endpoint. A PostgreSQL/MinIO deployment
drill also kills the Shardline process while the second ref is paused inside the
atomic transaction, verifies both refs remain at their old heads, then restarts,
retries, clones and runs strict `git fsck`. Broader concurrent deployment
qualification remains separate.
An owned PostgreSQL/MinIO test also gates two stock Git pushes after both have
read the same old refs from separate Shardline nodes. Exactly one atomic push
wins; both nodes expose both winning refs, the losing commit is not fetchable,
and a fresh server clones the winning bytes and passes strict fsck. These nodes
share one host; this does not establish production-scale concurrency limits.
Custom stores advertise atomic support
only when they implement the batch contract. Receive-pack negotiates and
validates push-options framing before publication. Shardline has no Git receive
hooks, so those option values currently have no hook consumer. Unimplemented
response modes remain rejected before publication.
Depth-limited clone, relative deepening, date-based and exclusion-based shallow
fetch, and unshallowing preserve verified commit
bytes. Missing or mistyped parent commits fail before depth boundaries are applied.
A native Git workflow checks history counts, complete-history identity and
strict fsck after these operations, including an earlier date cutoff restoring
full history. Date selection uses committed committer timestamps. Exclusions
resolve repository branch/tag names, reject unknown or ambiguous names and omit
their ancestry. Native Git checks exclusion by baseline tag and complete-history
recovery. Partial clones support `blob:none`, `blob:limit=<bytes>[kmg]` and
`tree:<depth>` using the minimum depth across selected history and explicit roots.
Native Git checks omitted initial blobs, size limits, lazy checkout and historical
content recovery. Explicit object requests must belong to the verified reachable
graph of the authorized repository. Native tree-depth clones verify directory
omission and lazy checkout of nested files. Protocol-v2 upload-pack now supports
`ls-refs` and `fetch`, including shallow depth/date/exclusion and the advertised
`blob:none`, blob-size and tree-depth filters, including bounded combined
blob-size/tree-depth intersections. `object:type` filters select blobs, trees,
commits or tags; explicitly wanted objects remain available, and native Git
verifies lazy fetching of an omitted tree during checkout over protocol v0, v1
and v2. Native Git checks v2 clone, incremental fetch, deepen/unshallow, shallow
date/exclusion, omitted objects, lazy checkout and strict fsck. Sparse-checkout
and other unimplemented filters are rejected explicitly. The same native Git
2.39.5, 2.52.0 and 2.55.0 workflows pass on SQLite/local storage and disposable
PostgreSQL/MinIO. Git 2.39.5 also passes the local and remote snapshot-token
isolation scenarios using the documented legacy negotiation.
The 2.39.5 qualification uses the checksum-verified upstream release archive;
2.55.0 uses an upstream-tag source build. Both builds use curl-backed Smart
HTTP with optional WebDAV push disabled. They do not qualify WebDAV.
For four v2 partial-clone filters, the checks prove a historical blob is absent
before lazy retrieval, fetch its exact bytes and run full strict fsck. Broader
shallow-history merge and failure qualification remains open; these checks do not establish full Git
protocol coverage.
Receive-pack validates the entire command section before publication: malformed
framing, invalid object IDs/ref names, duplicate ref commands and misplaced
capabilities reject the request rather than applying only its valid commands.

The E2E workflow provisions Git LFS, git-xet, Bazel, Hugging Face tooling, S3 clients,
Skopeo, Helm and Podman, and checks the Docker and Podman runtimes. Its native
protocol, git-xet, native Xet and Hugging Face CLI targets use
`SHARDLINE_REQUIRE_NATIVE_CLIENTS=1`: missing
required tools or native Xet's Docker/MinIO backend fail those tests.
Local runs without this flag retain optional-tool
behavior. Other E2E targets still need an individual skip audit. Review actual
execution logs before treating a specific revision as qualified; workflow changes
alone are not a passing CI result.

The same required-mode policy applies to shared disposable Docker fixtures and
deployment-chaos infrastructure, candidate/predecessor binaries and clock-fault
prerequisites. Missing services or failed startup fail qualification even when
an optional caller would otherwise discard the fixture error. Docker availability
probes have a 10-second deadline; deployment commands and cleanup have bounded
execution. The distributed-integrity runner and mixed-version CI step enable
required mode explicitly. Other prerequisite paths still need individual review.

Library defaults leave infrastructure and full-size checks explicitly ignored.
Inspect the selected build's inventory rather than treating a default pass as
their qualification; enabled features can change that inventory:

```bash
cargo nextest list --locked -p shardline-server --lib --all-features --message-format json
```

| Checks | Where they run explicitly |
| --- | --- |
| PostgreSQL OCI subject-index and reclamation contracts | Protocol-conformance gate with a migrated disposable database |
| PostgreSQL copy cancellation, corruption, snapshots and lost acknowledgments | PostgreSQL gate in `scripts/test-postgres.sh` |
| PostgreSQL frontend-schema compatibility and permission contracts; startup-worker shutdown | Protocol-conformance gate with a migrated disposable database |
| Full-size maintenance-history capacity and shared-hash authorization scale | Explicit release-mode capacity campaigns; ordinary CI does not establish their result |

Released-recovery library cases also require the actual predecessor and historical
binaries. Missing binaries fail mandatory native-client or PostgreSQL qualification;
optional local runs report an explicit skip. `scripts/test-postgres.sh` supplies
both releases before running the full server library.

The packed-range deployment drill uses 36 MiB files written by the actual
v1.12.1 and candidate binaries against PostgreSQL/MinIO. It requires multiple
packed containers and checks complete downloads plus 4 KiB reads across the
container transitions recorded in metadata. The checks continue during candidate
GC, after predecessor rollback and after re-upgrade; a small single-container
fixture would not establish this boundary coverage.

A separate PostgreSQL/MinIO drill uploads an uncompressed native Xorb through
the HTTP CAS endpoint, then publishes the same CDC chunks through S3. It checks
that publication reuses the native identity with the retained encoding's offsets,
then verifies full and boundary-range reads after GC, actual v1.12.1 rollback and
candidate restart. This covers matching chunk layouts, not arbitrary cross-protocol
deduplication or removal of legacy native chunk copies.

The E2E job separately selects the ignored native Xet PostgreSQL/MinIO test and
the two Bazel flows. Kubernetes and kind tests still require their dedicated
deployment fixtures. The Kubernetes protocol test uses local Docker services only
when no `SHARDLINE_K8S_E2E_*` configuration is supplied. A partially configured external
cluster fails instead of qualifying a replacement local stack. Explicitly selecting
the ignored kind smoke test also requires non-empty API and transfer endpoints;
missing configuration fails instead of returning success without deployment checks.

An October 27 Linux kind run at `61f11191` built the standard Docker image and
verified split API/transfer traffic with deployed PostgreSQL, MinIO and Redis.
Checks passed before and after replacing one pod per role, including persisted
data and native Xet sparse edits. Build caches were reused; this qualifies that
local rollout/recovery scenario, not cold-install time or multi-host scaling.

The live GitHub/Gitea bridge test is explicitly ignored by default because it
requires external credentials and test repositories. Selecting it explicitly
requires its clients and complete configuration; missing prerequisites
or failed identity lookups fail the test. A default E2E pass does not qualify that live integration.
Likewise, optional developer runs without the required-client flag can omit
tool-dependent flows. Keep these limits separate from executed native-client
and disposable-backend results.

The required protocol-conformance step separately runs native Git LFS locking and
transfers and the official OCI Distribution v1.1.1 suite. It fails if its client
tools are missing. PostgreSQL contracts also exercise lock races across two
servers and referrer discovery after restart and deletion.

## Versioning and Deduplication Semantics

The shared CAS core stores immutable content and reuses identical content whenever a
new upload refers to it. That does not mean every frontend exposes the same version
history. Version visibility and overwrite behavior belong to the frontend contract:

| Frontend | User-visible version semantics |
| --- | --- |
| Xet | Revision-oriented workflows can address and retrieve older versions. |
| Git LFS | Version history comes from Git commits and refs; the LFS object endpoint alone is a blob store. |
| Hugging Face Hub | Repository revisions and commits expose historical states. |
| OCI | Blobs and manifests are immutable by digest; tags are mutable, database-linearized pointers and have no tag-history API here. Digest-guarded deletion cannot remove a concurrently retargeted tag. |
| S3 | A `PUT` replaces the logical object key. `ListObjectVersions` reports the current object as a `null` version and both version listing and `ListObjectsV2` expose the stable repository-scoped owner ID. Bucket/object ACL reads report the fixed `BucketOwnerEnforced` model; ACLs do not grant access. Historical versions and delete markers are not implemented. |
| Bazel HTTP cache | Digest-keyed cache entries have no user-facing version history. |

When an overwrite makes a prior record unreachable, its unique chunks become GC
candidates after the configured quarantine/retention period. Chunks shared by another
reachable record remain protected.

## Validated Route Surface

- Git LFS: batch negotiation plus direct object `GET`, `HEAD`, and `PUT`;
  durable lock create/list/verify/unlock at `/v1/lfs/locks`
- Bazel HTTP remote cache: `ac` and `cas` object `GET` and `PUT`
- OCI Distribution: blob `GET`, `HEAD`, upload, mount, and ranged read paths; manifest
  `PUT`, `GET`, `HEAD`, and digest delete; tag listing with pagination; token-service
  flow at `/v2/token`; upload cancellation and scoped upload-session handling;
  OCI referrers with artifact-type filtering and pagination. SQLite and PostgreSQL
  HTTP tests cover deletion followed by identical republication, duplicate upload
  retries, discovery after restart, missing lookup metadata and corruption rejection.
  A PostgreSQL/MinIO test covers concurrent identical uploads through two live
  servers and replacement with fresh local directories. The servers share one
  test process; this does not simulate a process crash.
  Physical-reclamation tests on SQLite/local storage and PostgreSQL/MinIO verify
  that GC removes a deleted manifest while preserving live manifests and their
  shared subject, remains idempotent, and permits republication after restart.
  The MinIO case kills separate GC workers between manifest and companion
  deletion, then after both acknowledged deletions but before metadata commit;
  it terminates the sleeping database sessions, then
  verifies recovery and restart with a fresh local directory. This covers that
  crash boundaries. A separate MinIO fault drill lets the provider complete a
  delete but drops its response: GC reports failure, retains the tombstone and
  exact-key delete fence, blocks republishing, and does not replay the uncertain
  delete. The S3 API provides no proof that would permit safe in-place recovery
  for this case; follow the isolated-restore procedure in
  [DISASTER_RECOVERY.md](DISASTER_RECOVERY.md#unprovable-remote-deletes).
  An additional crash phase observes a replacement HTTP upload waiting on the
  PostgreSQL GC barrier, kills the owner, and verifies that publication succeeds
  and a later GC preserves the exact replacement bytes.
  A durable subject index selects candidate manifests. Each request reconciles the
  immutable manifest inventory so uploads from older servers remain discoverable;
  warm reconciliation reads metadata instead of all unrelated manifest payloads.
  Returned manifests still undergo content verification and deletion visibility
  checks. Lookup metadata does not publish or restore content. This retains an
  inventory scan; it does not establish constant-time lookup or measured throughput.
  PostgreSQL installs the index through the existing owner-run `db migrate up`
  command, preserves released migration history and mirrors existing OCI tag
  read/write grants. Read-only database roles, read-only database sessions and
  schemas without this optional index use verified manifest scanning instead,
  preserving discovery without metadata writes. PostgreSQL serving performs no DDL.
  Completed manifest reclamation removes its derived subject mapping in the same
  metadata transaction. PostgreSQL skips this optional cleanup when the table or
  existing SELECT/DELETE grants are absent; no additional grant is required for
  GC. Legacy inventory reconciliation still protects discovery if a mapping is
  missing. Updated-runtime backfill checks verified lifecycle evidence transactionally and does
  not recreate a mapping after reclamation; PostgreSQL serializes that check with
  derived-row cleanup. Normal republication can index the manifest again.
  Legacy rows and roles without cleanup grants can still retain obsolete entries;
  this is not a guarantee that every orphaned cache row is immediately removed.
- Hugging Face Hub API: repository create/info/delete, revision and tree lookup,
  preupload, NDJSON commit, resolve/download, dataset viewer routes, basic search,
  webhooks, Git Smart HTTP clone/fetch/push, SDK live-reference listing, and SDK
  branch/lightweight-tag creation and deletion
- S3 frontend: object `PUT`, `GET` (with `Range`/`Content-Range`, stored `Content-Type` and checksum mode), checksum-enabled `HEAD`, `DELETE`, and
  partial `GetObjectAttributes` (`ETag`, `ObjectSize`, multipart total-part count,
  persisted single-PUT checksums, COMPOSITE part checksums, and CRC32/CRC32C/CRC64NVME
  FULL_OBJECT checksums); active `ListParts` returns the negotiated
  checksum algorithm and per-part checksum values, deriving missing values for older
  in-progress sessions from integrity-checked part bytes;
  multipart upload (`CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`,
  `AbortMultipartUpload`, `ListParts`, and `ListMultipartUploads`); `HeadBucket`/
  `GetBucketLocation`/`CreateBucket` stubs;
  `ListObjectsV2` (`prefix`/`delimiter`/`max-keys`/`continuation-token`/`start-after`)
  and `ListObjects` v1 (`marker`/`NextMarker`, the `s3cmd ls` path); standard MD5
  ETags and user metadata (`x-amz-meta-*`); with the SigV4 access-key and Bearer
  auth bridges

## Current Limits

- Hub SDK commit fields use Git IDs; historical opaque IDs remain accepted.
  The legacy `commit_id` field and `/revisions` listing retain internal IDs.
  SDK response IDs can therefore differ from opaque IDs saved before upgrading;
  resolve an old revision to obtain its corresponding Git ID before comparison.
  Released 1.12.1 replicas do not resolve the new public Git IDs. During rollout,
  route those requests to upgraded replicas; rollback historical reads require
  the legacy IDs from `/revisions`. See [Rolling Upgrade](ROLLING_UPGRADE.md).
- The compatibility claim applies to the documented route surfaces and native client
  workflows above. It does not imply implementation of unrelated upstream products or
  optional APIs such as Bazel Remote Execution, Hugging Face
  collections/profile/jobs/inference APIs, or every third-party client-version
  extension.
- Xet and OCI currently have the deepest native-client coverage in this repository.
- Git LFS, Bazel HTTP remote cache, and OCI Distribution claims are scoped to the route
  behavior and client flows covered by the repository tests.
- Patch releases publish the internal crate graph in dependency order before publishing
  the `shardline` CLI crate.
DuckDB analytics compatibility and the real-client validation workflow are
documented in [`DUCKDB.md`](DUCKDB.md). Full SQL runs in external DuckDB over
the authenticated S3 frontend. The Hub also enables bounded native dataset
queries by default, using Arrow/Parquet rather than DuckDB; operators can
disable them with `SHARDLINE_ENABLE_DATASET_QUERY=false`. The Hub API
does not expose arbitrary SQL.
