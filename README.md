# Shardline

Large binary files change a little and cost a lot to re-upload. Shardline is the
self-hosted store that remembers: content-addressed chunks stored once, every new
version a thin set of pointers to what already exists.

Model checkpoints, game builds, dataset snapshots, CI caches. The files that make a
team's storage bill spike all deduplicate across versions, live on your infrastructure,
and stream back over range reads or resumable uploads when the network is imperfect.

## Two workloads that read it instantly

**AI models & datasets.** Weights and datasets evolve release over release. Shardline is
a drop-in Hub for internal use: point `huggingface-cli` at it and model versioning works
unchanged, deduplicated across revisions.

```bash
export HF_ENDPOINT=http://127.0.0.1:18080
hf upload my-org/my-model ./weights
```

**Game assets & pipelines.** Textures, meshes, and audio run to gigabytes and change
constantly. Shardline deduplicates asset versions, resumes interrupted uploads instead
of restarting them, and streams range reads so artists open large files fast.

```bash
# a game build pipeline talks S3 or git-lfs straight into Shardline
```

**Build & CI caches** get the same treatment: unchanged compilation outputs are reused,
not rebuilt and not re-uploaded.

## Protocols your tools already speak

git-lfs · OCI/docker · Bazel remote cache · S3 · Hugging Face Hub · Xet · git-xet

## Start

```bash
git clone https://github.com/STEXS-Technologies/shardline && cd shardline
docker compose up --build    # http://127.0.0.1:18080
```

More in [Getting Started](docs/GETTING_STARTED.md), [Deployment](docs/DEPLOYMENT.md),
and [Compatibility Status](docs/COMPATIBILITY_STATUS.md).

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE).
