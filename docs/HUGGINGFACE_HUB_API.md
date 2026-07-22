# HuggingFace Hub API Compatibility

Shardline includes a HuggingFace Hub API compatibility layer that makes it a drop-in
alternative to the HuggingFace Hub for model and dataset storage.

## What It Is

The Hub API frontend translates HuggingFace Hub REST endpoints into Shardline CAS
operations. Users and CI pipelines that already work with `huggingface-cli` or the Hub
REST API can point at a Shardline instance and upload, download, and manage model
repositories without code changes.

The frontend is implemented in the `shardline-hub-api` crate and is registered as a
server frontend alongside the other protocol adapters.

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

Pack files are generated from stored revisions with real Git tree, blob, and commit
objects. LFS pointer blobs are created for files tracked via LFS. The `.gitattributes`
file is auto-generated when LFS files are present.

## Supported Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/api/whoami-v2` | GET | Current user identity |
| `/api/repos/create` | POST | Create a repository |
| `/api/{type}/{ns}/{repo}` | POST | Create a repository (typed) |
| `/api/{type}/{ns}/{repo}` | GET | Get repository info |
| `/api/{type}/{ns}/{repo}/preupload/{rev}` | POST | Pre-upload check |
| `/api/{type}/{ns}/{repo}/commit/{rev}` | POST | Commit file changes |
| `/api/{type}/{ns}/{repo}/revision/{rev}` | GET | Revision metadata and siblings |
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
| `/lfs/objects/{oid}` | PUT | Upload an LFS object |
| `/lfs/objects/{oid}` | GET | Download an LFS object |

Git Smart HTTP supports safe branch and tag deletion through the normal Git
zero-SHA receive-pack update. Deletion is compare-and-delete atomic, so stale
pushes cannot remove a ref that has advanced; the default `main` branch is
protected. Removing a ref does not remove the immutable commit history.

## Architecture

Hub metadata is persisted to the configured index store:

- **SQLite** (local): `{root_dir}/hub/` directory — 4 tables for repos, revisions,
  file entries, and LFS objects
- **Postgres** (production): same connection as the main index — 7th migration in the
  bundled set

The `HubStore` trait in `shardline-index` defines the storage contract. `BoxedHubStore`
provides type-erased access. When an auth provider is configured, Hub API routes use
`HubAuth` (wrapping `Arc<dyn AuthProvider>`) for bearer token validation.

The Hub API merges into the main Axum router at startup, sharing the same bind address
and TLS configuration as all other frontends.

## Limitations

Shardline implements the repository-storage workflows in the table above, not the
entire Hugging Face SaaS product. Collections, user profiles, discussions, jobs,
inference endpoints, Spaces runtime management, and advanced Hub administration APIs
are outside this frontend's current contract. Webhooks, model cards, basic repository
search, and dataset preview routes are implemented.
