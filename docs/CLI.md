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

## Global Flags

These flags are available on every command, either before or after the subcommand:

```text
--env-file <PATH>   Load a .env file into the process environment before
                    resolving configuration. Use for secrets and credentials.
-c, --config <FILE> Path to a shardline.toml configuration file. When omitted,
                    shardline.toml is auto-detected from the current directory,
                    ~/.config/shardline/, and /etc/shardline/.
```

## Common Commands

Start the server:

```bash
shardline serve
shardline serve --env-file .env.production
shardline serve --config /etc/shardline/shardline.toml
shardline serve --role api
shardline serve --role transfer
shardline serve --frontend xet --frontend hub
shardline serve --frontend xet,lfs,bazel-http,oci,hub,s3
```

`--frontend` selects the protocol frontends this process serves. Repeat the flag or
pass a comma-separated list. `--role` pins the process to `all`, `api`, or `transfer`
and only splits the selected frontend set across processes.

Validate configuration:

```bash
shardline config check
shardline config check --env-file .env.production --config shardline.toml
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
shardline db migrate up --steps 2
shardline db migrate up --database-url postgres://user:password@db.example.com:5432/shardline
shardline db migrate down --steps 1
shardline db migrate status
```

`up` applies pending migrations, `down` reverts applied migrations (optionally limited
by `--steps`), and `status` reports the applied and pending sets. `--database-url`
overrides the configured Postgres metadata URL.

Verify object-store and metadata integrity:

```bash
shardline fsck
shardline fsck --root /var/lib/shardline
```

Rebuild mutable indexes from immutable version history:

```bash
shardline index rebuild
shardline index rebuild --root /var/lib/shardline
```

Repair lifecycle metadata and webhook delivery state:

```bash
shardline repair
shardline repair lifecycle
shardline repair lifecycle --webhook-retention-seconds 604800
```

Export an adapter-neutral recovery manifest:

```bash
shardline backup manifest --output ./backup-manifest.json
```

Copy immutable objects between storage adapters:

```bash
shardline storage migrate --from local --to s3
shardline storage migrate --from local --from-root /var/lib/shardline --to s3 --prefix my-prefix
shardline storage migrate --from s3 --to local --to-root /mnt/cold-storage --dry-run
```

`storage migrate` inventories immutable payload objects under the source adapter and
copies them into the destination. Use `--dry-run` to inventory without writing
destination payloads. `--from-root` / `--to-root` supply the local state root when the
corresponding adapter is `local`.

Run garbage collection:

```bash
shardline gc
shardline gc --mark
shardline gc --mark --sweep --retention-seconds 86400
shardline gc --mark --retention-report reports/gc-retention.json --orphan-inventory reports/gc-orphans.json
```

Generate a validated systemd GC schedule:

```bash
shardline gc schedule install \
  --env-file /etc/shardline/shardline.env \
  --user shardline \
  --group shardline
```

Remove the generated systemd GC units:

```bash
shardline gc schedule uninstall
shardline gc schedule uninstall --output-dir /etc/systemd/system --unit-prefix shardline-gc
```

Manage retention holds that protect object keys from garbage collection:

```bash
shardline hold set --object-key s3://team-assets/model.pt --reason "restore window" --ttl-seconds 604800
shardline hold list
shardline hold list --active-only
shardline hold release --object-key s3://team-assets/model.pt
```

Probe a running server's health:

```bash
shardline health --server http://127.0.0.1:8080
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
- `shardline gc`, `fsck`, `repair`, and `hold` read lifecycle and record metadata from
  Postgres when `SHARDLINE_INDEX_POSTGRES_URL` is set, and inventory or delete payload
  objects through the configured object-storage adapter
- `shardline repair lifecycle --webhook-retention-seconds` bounds how long processed
  webhook-delivery claims are retained before cleanup
- `shardline storage migrate` inventories the source adapter before copying; use
  `--dry-run` to review the inventory without writing destination payloads
- `shardline completion` and `shardline manpage` are packaging and operator tools; they
  do not modify runtime state unless `--output` writes to a path you choose
