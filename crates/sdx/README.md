# sdx

[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green)](#license)

**`sdx` is a native Xet client library for Shardline's Xet frontend.**

It lets Rust programs read and write content-addressed objects over the Xet CAS
protocol — the same protocol the Shardline server serves — with automatic
chunk-level deduplication, ranged reconstruction, and streaming transfers that
stay bounded in memory. `sdx` is the library behind the `sdx` file-management
CLI, which ships inside the `shardline` binary (see below).

## What it does

- **Content-addressed reads** — resolve a path (or use a file_id directly) and
  download a whole file or a byte range via V1/V2 reconstruction + ranged xorb
  fetch.
- **Streaming, bounded-memory downloads** — pull a large file as a stream of
  `Bytes` chunks without buffering it whole; an on-disk chunk cache speeds up
  repeat reads.
- **Streaming uploads with dedup** — CDC chunking, server-side deduplication,
  and streaming xorb + metadata-shard upload; the file is never buffered whole.
- **Authentication** — provider API key or bearer token → short-lived scoped
  CAS token, transparently refreshed (single-flight) with retry/backoff on
  401/403.

## The `xet://` URL scheme

```text
xet://<host>[:<port>]/<provider>/<owner>/<repo>/<revision>/<path...>
```

For example `xet://127.0.0.1:8080/github/team/assets/main/models/model.pt`
addresses the file `models/model.pt` in the `team/assets` repository at revision
`main` on the `github` provider.

## Usage

Build a client with an endpoint and a credential:

```rust,no_run
use sdx::{Auth, RepositoryId, XetClientBuilder};

let client = XetClientBuilder::new()
    .endpoint("xet://127.0.0.1:8080/github/team/assets/main")
    .auth(
        Auth::new(
            "http://127.0.0.1:8080",
            RepositoryId {
                provider: "github".to_owned(),
                owner: "team".to_owned(),
                repo: "assets".to_owned(),
                revision: "main".to_owned(),
            },
        )?
        .with_api_key("bootstrap".to_owned()),
    )
    .build()?;
```

Upload bytes to a remote path, then download by file_id or resolve the path
back to its file_id:

```rust,no_run
# fn example() -> Result<(), Box<dyn std::error::Error>> {
#     let client = /* build as above */ XetClientBuilder::new().build()?;
// Upload in-memory bytes and register them under a remote path.
let info = client.upload_bytes("remote.bin", b"hello shardline".to_vec())?;
println!("file_id: {}", info.file_id);

// Download the whole file by its content-derived file_id.
let bytes = client.download_bytes(&info.file_id)?;

// Or resolve the path to its file_id, then stream a byte range.
let entry = client.resolve_path("remote.bin")?;
let mut stream = client.download_stream(&entry.file_id, Some(0..1024))?;
while let Some(chunk) = stream.next()? {
    // write chunk to a sink without buffering the file whole
}
#     Ok(())
# }
```

## Portability

File operations addressed by **file_id** (download, range download, streaming,
upload of content) follow the upstream Xet wire protocol and work against any
Xet-compatible frontend. The **path** namespace (`resolve_path`, listing,
registration — `tree.rs`/`revisions.rs`) is a Shardline-specific metadata layer
and is out of scope for cross-frontend portability.

## The `sdx` CLI

`sdx` is not a separate binary: it is a symlink to `shardline`, which detects
`argv[0]` and routes to the `cp`/`sync`/`ls`/`rm`/`cat`/`info`/`branch`
commands. The same surface is reachable as `shardline xet ...`.

```bash
sdx cp ./model.pt xet://host/provider/owner/repo/rev/models/
sdx cat xet://host/provider/owner/repo/rev/models/model.pt
```

## License

Licensed under either of [Apache License, Version 2.0](../../LICENSE-APACHE) or
[MIT license](../../LICENSE-MIT) at your option.

Part of the [Shardline](https://github.com/STEXS-Technologies/shardline)
project. See the [workspace README](../../README.md) for the server and the full
surface.
