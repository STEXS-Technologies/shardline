# systemd

Shardline can run under `systemd` for single-node and small split-role deployments.

The static templates in `docs/systemd/` target this layout:

- the `shardline` binary is installed at `/usr/local/bin/shardline`
- runtime environment lives in `/etc/shardline/shardline.env`
- runtime secrets referenced by that environment file already exist
- local deployments keep state under `SHARDLINE_ROOT_DIR`

Secret files referenced from the environment file must be regular files, not symlinks.
Use owner/group-readable permissions only, for example `0640` with the service group or
`0600` when only the service user needs access.
Do not point Shardline at world-readable runtime keys, provider bootstrap keys, metrics
tokens, provider catalogs, or S3 credential files.

For generated GC units, prefer the CLI helper instead of copying the template blindly.
`shardline gc schedule install` resolves and validates the runtime inputs before it
writes the unit files:

- if `--binary-path` is left at the default, the helper uses the current `shardline`
  executable when it can resolve one
- it requires the environment file to exist
- it parses `SHARDLINE_ROOT_DIR` from that environment file and uses it as the service
  working directory when `--working-directory` was left at the default
- it validates that referenced secret and provider-config paths exist
- it validates that the selected service user and group exist on the host

The generated GC schedule defaults are:

- timer calendar: `*-*-* 03:17:00`
- retention window: `86400` seconds
- output directory: `/etc/systemd/system`
- unit prefix: `shardline-gc`

Override them with `--calendar`, `--retention-seconds`, `--output-dir`, and
`--unit-prefix`.

Use the single-process service when one host handles both API and transfer traffic.
Use the split-role services when API and transfer should run as separate local services
on the same machine.

## Single Process

1. Install the binary.
2. Copy `docs/systemd/shardline.service` to `/etc/systemd/system/shardline.service`.
3. Write `/etc/shardline/shardline.env`.
4. Apply the metadata schema when Postgres-backed metadata is enabled:

```bash
sudo --preserve-env=SHARDLINE_INDEX_POSTGRES_URL shardline db migrate up
```

5. Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now shardline.service
```

## Split Roles

Use these units together:

- `docs/systemd/shardline-api.service`
- `docs/systemd/shardline-transfer.service`

They share the same environment file and binary but pin the role with
`SHARDLINE_SERVER_ROLE`.

## Scheduled Garbage Collection

Use:

- `docs/systemd/shardline-gc.service`
- `docs/systemd/shardline-gc.timer`

Install and enable them with:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now shardline-gc.timer
```

The timer runs `shardline gc --mark --sweep`. Adjust the retention window or schedule
inside the unit and timer files before enabling them in production.

Instead of copying the GC units manually, you can generate them with:

```bash
shardline gc schedule install \
  --output-dir ./systemd \
  --env-file /etc/shardline/shardline.env \
  --user shardline \
  --group shardline
```

Remove generated units with:

```bash
shardline gc schedule uninstall --output-dir ./systemd
```

The install command prints the resolved binary path, environment file, working
directory, calendar, and retention window so the generated schedule can be reviewed
before `systemctl daemon-reload`.

## Environment File

Example `/etc/shardline/shardline.env`:

```bash
SHARDLINE_BIND_ADDR=127.0.0.1:8080
SHARDLINE_PUBLIC_BASE_URL=https://cas.example.com
SHARDLINE_ROOT_DIR=/var/lib/shardline
SHARDLINE_OBJECT_STORAGE_ADAPTER=s3
SHARDLINE_INDEX_POSTGRES_URL=postgres://shardline:replace-me@postgres:5432/shardline
SHARDLINE_S3_BUCKET=shardline
SHARDLINE_S3_REGION=us-east-1
SHARDLINE_S3_ENDPOINT=https://s3.example.com
SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=redis
SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL=redis://default:replace-me@garnet.example.com:6379
SHARDLINE_TOKEN_SIGNING_KEY_FILE=/etc/shardline/token-signing-key
SHARDLINE_METRICS_TOKEN_FILE=/etc/shardline/metrics-token
SHARDLINE_PROVIDER_CONFIG_FILE=/etc/shardline/providers.json
SHARDLINE_PROVIDER_API_KEY_FILE=/etc/shardline/provider-api-key
```

Run `shardline config check` under the same environment before enabling the service.
