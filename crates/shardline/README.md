# Shardline CLI

The Shardline command-line interface is the primary entrypoint for operators.
It is invoked as `shardline` and provides server operations and administrative
commands.

## Usage

```bash
shardline <command> [options]
```

## Commands

- `serve` — Start the Shardline server with configured frontends and backends.
- `admin` — Issue and manage authentication tokens.
- `fsck` — Check filesystem consistency across metadata and object storage.
- `gc` — Run garbage collection to reclaim orphaned CAS storage.
- `index rebuild` — Rebuild metadata indexes from stored objects.
- `repair` — Run lifecycle repair operations.
- `backup` — Create and manage backup manifests.
- `db migrate` — Apply pending database migrations.
- `bench` — Run performance benchmarks.
- `config check` — Validate server configuration.
- `health` — Check server health.
- `hold` — Manage GC retention holds.
- `gc schedule` — Manage scheduled GC tasks.
- `storage migrate` — Migrate storage between backends.
- `providerless setup` — Configure providerless mode.
- `completion` — Generate shell completion scripts.
- `manpage` — Generate manpage.

## Configuration

Configuration is read from `shardline.toml` (auto-detected from the current directory,
`~/.config/shardline/`, and `/etc/shardline/`) or from environment
variables. See `shardline config check --help` for details.

## Installation

```bash
cargo install shardline
# or build from source
cargo build --release -p shardline
```

See the [main Shardline README](../../README.md) for the project overview.
