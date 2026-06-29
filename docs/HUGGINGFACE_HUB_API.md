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
huggingface-cli login  # optional — hub api accepts anonymous by default
huggingface-cli upload my-org/my-model ./model-files
huggingface-cli download my-org/my-model
```

For `huggingface-cli` to trust a local HTTP endpoint, you may also need:

```bash
export HF_HUB_DISABLE_TELEMETRY=1
export TRANSFORMERS_VERBOSITY=info
```

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
| `/api/{type}/{ns}/{repo}/tree/{rev}/{path}` | GET | Browse file tree |
| `/api/{type}/{ns}/{repo}/xet-read-token/{rev}` | GET | Xet read token exchange |
| `/api/{type}/{ns}/{repo}/xet-write-token/{rev}` | GET | Xet write token exchange |
| `/{type}/{ns}/{repo}/resolve/{rev}/{path}` | GET | Resolve and download a file |
| `/objects/batch` | POST | LFS batch request |
| `/lfs/objects/{oid}` | PUT | Upload an LFS object |
| `/lfs/objects/{oid}` | GET | Download an LFS object |

## Architecture

The Hub API state is in-memory by default. Repository metadata, file trees, and LFS
objects are held in process-local stores initialized at startup:

- `RepoStore` — repository registry and revision tracking
- `TreeStore` — per-commit file trees
- `LfsStore` — LFS object storage (batch, upload, download)

The Hub API merges into the main Axum router at startup, sharing the same bind address
and TLS configuration as all other frontends.

## Limitations

This is an initial implementation. Known limitations:

- **In-memory state only** — repository and file metadata is not persisted to disk or a
  database. A server restart loses all Hub API state.
- **No authentication** — the Hub API currently accepts all requests anonymously. Token
  exchange endpoints return placeholder tokens. Production use requires pairing with an
  external reverse proxy or future auth integration.
- **No Git protocol** — the Hub API provides REST endpoints only. Direct `git push` to
  Hub-style repositories is not supported; use the CLI upload/download workflow.
- **No webhooks or callbacks** — repository event notifications are not implemented.
- **Single-process only** — the in-memory stores are not shared across scaled API
  instances. The Hub frontend is intended for single-node deployments today.
- **No model card or metadata search** — repository README, model card, and search
  endpoints are not yet implemented.
- **No dataset viewer** — dataset-specific preview and streaming endpoints are not yet
  implemented.
