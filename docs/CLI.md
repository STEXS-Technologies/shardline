# CLI

Shardline ships one operator-facing binary: `shardline`.

The CLI covers runtime startup, configuration checks, migrations, repair operations,
garbage collection, storage movement, benchmarking, and packaging assets such as shell
completions and manpages.

## Help

The CLI uses generated help from the live command definition, so the help output stays
aligned with the actual command surface.

Examples:

```bash
shardline --help
shardline gc --help
shardline gc schedule install --help
shardline bench --help
```

## Shell Completions

Generate one completion script for a supported shell:

```bash
shardline completion bash
shardline completion zsh
shardline completion fish
shardline completion powershell
shardline completion elvish
```

Write the generated completion to a file:

```bash
shardline completion bash --output /usr/share/bash-completion/completions/shardline
shardline completion zsh --output ./_shardline
```

The generated script always comes from the current binary, so it matches the installed
command set and flags.

## Manpage

Generate the CLI manpage to stdout:

```bash
shardline manpage
```

Write it to a file for packaging or system installation:

```bash
shardline manpage --output ./shardline.1
```

## Common Commands

Start the server:

```bash
shardline serve
shardline serve --role api
shardline serve --role transfer
```

Validate configuration:

```bash
shardline config check
```

Bootstrap a local providerless source-checkout deployment:

```bash
shardline providerless setup
shardline serve
shardline admin token --issuer local --subject operator-1 --scope write --provider generic --owner team --repo assets --revision main --key-file .shardline/token-signing-key
```

For the fastest local path, `shardline serve` auto-creates the same `.shardline/`
providerless state on first run from a source checkout.

For the bundled Docker Compose profile, either mint inside the running container with
the generated volume key:

```bash
docker compose -f docker-compose.yml up --build
docker compose -f docker-compose.yml exec -T shardline \
  shardline admin token --issuer local --subject operator-1 --scope write --provider generic --owner team --repo assets --revision main --key-file /var/lib/shardline/secrets/token-signing-key
```

Or pass a development key through the environment and mint on the host with the same
environment variable:

```bash
SHARDLINE_TOKEN_SIGNING_KEY=dev-signing-key docker compose -f docker-compose.yml up --build
SHARDLINE_TOKEN_SIGNING_KEY=dev-signing-key \
  shardline admin token --issuer local --subject operator-1 --scope write --provider generic --owner team --repo assets --revision main --key-env SHARDLINE_TOKEN_SIGNING_KEY
```

Validate the bootstrapped local profile:

```bash
shardline config check
```

Apply metadata migrations:

```bash
shardline db migrate up
shardline db migrate status
```

Run garbage collection:

```bash
shardline gc
shardline gc --mark
shardline gc --mark --sweep --retention-seconds 86400
```

Generate a validated systemd GC schedule:

```bash
shardline gc schedule install \
  --env-file /etc/shardline/shardline.env \
  --user shardline \
  --group shardline
```

Run benchmarks:

```bash
shardline bench --storage-dir /var/lib/shardline-bench
shardline bench --deployment-target configured --storage-dir /var/lib/shardline-bench
shardline bench --mode ingest --iterations 5 --concurrency 16
```

## Notes

- `shardline bench --mode e2e` requires `--storage-dir`
- `shardline bench --deployment-target isolated-local` benchmarks local SQLite metadata
  plus filesystem object storage under the supplied storage root
- `shardline bench --deployment-target configured` benchmarks the active `SHARDLINE_*`
  runtime adapters, including Postgres metadata and S3 object storage
- `shardline gc schedule install` validates the target binary, env file, selected user
  and group, and referenced secret/config paths before writing units
- `shardline completion` and `shardline manpage` are packaging and operator tools; they
  do not modify runtime state unless `--output` writes to a path you choose
