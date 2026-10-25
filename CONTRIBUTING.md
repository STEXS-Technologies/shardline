# Contributing to Shardline

This guide defines how to contribute changes to `shardline` while keeping architecture,
reliability, and release standards intact.

## Scope

This repository is a Rust workspace with the main CLI crate at `crates/shardline` (published
as `shardline`).

Primary references:

- Project overview and run commands: `README.md`
- Docs index: `docs/README.md`
- Architecture and boundaries: `docs/ARCHITECTURE.md`
- Protocol contract: `docs/PROTOCOL_CONFORMANCE.md`
- Security and invariants: `docs/SECURITY_AND_INVARIANTS.md`
- Operations docs: `docs/OPERATIONS.md`

## Prerequisites

- Rust stable toolchain
- `cargo-make` (used by contributor quality gates)
- Docker (for Docker-backed integration tests)
- `kind`, `kubectl`, and Python 3 (for the disposable Kubernetes smoke test)

Quick smoke check:

```bash
cargo check -p shardline
```

## Development workflow

1. Make focused changes in the owning crate or module.
2. Add or update tests with the behavior change.
3. Run contributor quality gates locally.
4. Open a PR with a clear summary, risk notes, and test evidence.

### PostgreSQL qualification

Use a disposable database and export its connection string as `DATABASE_URL`.
Apply the candidate's migrations before running the gate; a database migrated
only by the preceding release is missing the candidate's frontend tables.

```bash
cargo run --locked --all-features -p shardline -- db migrate up --database-url "$DATABASE_URL"
cargo make test-postgres
```

The gate checks both v1.12.1 recovery and v1.11.3 preservation of newer evidence.
It fetches checksum-pinned Linux x86_64 binaries when they are not supplied.
To reuse local binaries or run on another platform, set
`SHARDLINE_N_MINUS_ONE_BINARY` to a native v1.12.1 executable and
`SHARDLINE_LEGACY_RECOVERY_BINARY` to a native v1.11.3 executable. These are
distinct compatibility contracts; the historical binary does not replace the
adjacent release. Remote object-store and stock-client qualification require
their separately provisioned gates as well.

### Disk-pressure qualification

On Linux, run the candidate CLI against a disposable 512 MiB ext4 image with
MinIO. The drill fills only that image, checks that failed uploads remain
unpublished, frees space, retries the identical content, and restarts Shardline
before checking every downloaded byte. It exercises both S3 and the stock
`hf_xet` client, including its normal retry window.

Requires Docker with privileged containers, `mkfs.ext4`, Python 3 with `boto3`,
and a Python environment exposing `hf_xet.XetSession` (validated with 1.7.0).
Pass a frozen candidate executable; do not rebuild or replace it during the
drill. Use a new evidence directory outside the checkout for each run:

```bash
cargo make test-disk-pressure /path/to/shardline /private/evidence/sqlite \
  --metadata sqlite --native-python /path/to/xet-environment/bin/python
cargo make test-disk-pressure /path/to/shardline /private/evidence/postgres \
  --metadata postgresql --native-python /path/to/xet-environment/bin/python
```

The runner creates and removes its own database, MinIO containers and loop
mount. Client logs stay private because upstream errors may contain signed URLs
or credentials. Missing prerequisites and timeouts fail qualification. This is
a bounded 1 MiB S3 / 9 MiB native-Xet recovery drill, not a throughput benchmark
or proof of every client's behavior under disk pressure.

## Git commits and pull requests

### Philosophy

- Commits are brief and atomic: one logical change, easy to scan.
- Pull requests carry the full context: why, scope, testing, and impact.

### Commit message format

Use one of:

- `<type>: <subject>`
- `<type>(<scope>): <subject>` (optional scope, e.g. `server`, `storage`, `protocol`, `ci`, `docs`)

Examples:

- `feat(cli): add config check command`
- `fix(server): reject ambiguous signing key input`
- `chore(ci): run dependency policy checks`
- `docs(deployment): clarify host-native startup`
- `refactor(index): extract lifecycle state reader`

Supported types:

- `docs` documentation
- `feat` new feature
- `fix` bug fix
- `refactor` code restructuring
- `test` test changes
- `chore` build, CI, dependencies, tooling
- `perf` performance changes

Subject rules:

- Imperative mood (`add`, `fix`, `refactor`)
- Lowercase start (except proper nouns)
- No trailing period
- Aim for <= 50 characters
- Be specific, not generic

Key rule:

- Keep commit titles short and scannable.
- Put detailed rationale and impact in the PR description, not in commit titles.

### Pull request title format

Use:

- `<type>: <subject> — <impact or scope>`
- Optional scope is allowed: `<type>(<scope>): <subject> — <impact or scope>`

Examples:

- `feat(cli): add providerless setup validation — reduces bootstrap mistakes`
- `fix(server): reject conflicting signing key sources — hardens startup safety`
- `docs(deployment): simplify host run path — reduces operator confusion`

### Pull request description requirements

PRs must include:

- Description: what changed and why it matters.
- Changes: concrete list of file/module changes.
- Motivation: business and technical reasons; alternatives considered if relevant.
- Scope and impact: affected crates, compatibility, operational impact.
- Testing: what was run and what scenarios were validated.
- Related docs/issues: docs updated, issue references, release or rollout notes when relevant.

For cross-crate, protocol, or deployment-impacting changes:

- Explicitly list affected crates (`shardline-protocol`, `shardline-storage`, `shardline-index`, `shardline-cas`, `shardline-cache`, `shardline-vcs`, `shardline-server`, `shardline`).
- Explain cross-crate interaction changes.
- Note protocol, migration, or rollback implications if any.
- Call out security or operator impact when behavior changes at runtime boundaries.

Reference template:

- `.github/pull_request_template.md`

## Issues

### Issue title format

Use the same title shape as PRs wherever possible:

- `<type>: <subject> — <impact or scope>`
- Optional scope is allowed: `<type>(<scope>): <subject> — <impact or scope>`

Examples:

- `fix(storage): local root symlink race on startup — can escape deployment root`
- `feat(cli): add backup manifest diff mode — simplifies restore validation`
- `refactor(server): split provider bootstrap wiring — reduces app.rs churn`

Supported issue types follow the same repository vocabulary:

- `fix`
- `feat`
- `docs`
- `refactor`
- `test`
- `chore`
- `perf`

### Issue body requirements

Issues should include:

- Description: what is wrong or what should change.
- Current behavior or gap.
- Expected behavior or requested outcome.
- Scope and impact: affected crates, deployments, operators, or clients.
- Reproduction or validation evidence when filing bugs.
- Related docs or prior issues.

Reference templates:

- `.github/ISSUE_TEMPLATE/bug_report.md`
- `.github/ISSUE_TEMPLATE/feature_request.md`
- `.github/ISSUE_TEMPLATE/maintenance_task.md`

## Required local checks before PR

Run:

```bash
cargo make ci
```

This includes:

- formatting check
- clippy across the workspace
- nextest across the workspace
- release binary packaging smoke test
- dependency audit

Run infrastructure coverage when changing Docker-backed storage, service lifecycle, or
Kubernetes deployment behavior:

```bash
cargo make test-docker
cargo make test-kubernetes
```

To run both sequentially (Docker coverage first, then the disposable `kind` cluster):

```bash
cargo make test-infrastructure
```

`test-kubernetes` creates and deletes a local `kind` cluster and image; it preserves
diagnostics only when the smoke test fails.

For dependency-policy checks too:

```bash
cargo make ci-full
```

Coverage uses LLVM source-based instrumentation. Generate a machine-readable summary
and enforce the checked-in non-regression ratchet with:

```bash
cargo make coverage-check
```

Use `cargo make coverage-html` for the local HTML report.

## Architecture and code rules

Follow the current project constraints:

- Keep crate boundaries intentional: `shardline-protocol`, `shardline-storage`, `shardline-index`, `shardline-cache`, `shardline-cas`,
  `shardline-vcs`, `shardline-server`, and `shardline` should keep their current responsibilities clear.
- Keep public contracts explicit and type-driven.
- Prefer validated constructors and domain newtypes for invariants.
- Use typed enum errors (`thiserror`) instead of stringly error categories.
- Keep security-sensitive filesystem, protocol, and token logic fail-closed.
- Prefer declaration and re-export only in `mod.rs`.

Reference docs:

- `docs/ARCHITECTURE.md`
- `docs/PROTOCOL_CONFORMANCE.md`
- `docs/SECURITY_AND_INVARIANTS.md`

## Testing expectations

- For behavior changes, update or add tests that fail before the fix and pass after.
- Every bug fix should include a regression test.
- Keep tests deterministic and fast.
- If you touch protocol parsing, lifecycle repair, storage boundaries, or token flows,
  expand edge-case coverage.

Benchmark workflow for hot-path changes:

- Use `cargo make shardline-bench-protocol` for protocol microbenchmarks.
- Use `cargo make shardline-bench-local-backend` for local-backend hot paths.
- Use `cargo make shardline-bench-e2e` or `cargo make shardline-bench-ingest` when
  changing end-to-end or ingest-critical behavior.
- Treat benchmark deltas as evidence, not proof by themselves: explain workload shape
  and limits in the PR.

Fuzzing workflow:

- Fuzz targets and corpora are in `crates/fuzz/`.
- Run bounded fuzz smoke locally with `cargo make shardline-fuzz-smoke`.
- Replay every checked-in corpus deterministically with
  `cargo make shardline-fuzz-regression`.
- For deeper campaigns, run `bash scripts/shardline/fuzz.sh run <target>` or use the
  underlying `cargo fuzz` workflow from `crates/fuzz/` if needed.
- When fuzz finds a crash, reproduce it, then add:
  - a deterministic regression test in the owning module, and
  - a minimized corpus seed in the owning fuzz corpus.

## Observability and operability requirements

For operationally significant changes:

- Add or update structured logs, metrics, and traces with stable fields.
- Keep telemetry lightweight on hot paths.
- Update docs when behavior, config, rollout shape, or operator procedure changes.

References:

- `docs/OPERATIONS.md`
- `docs/DEPLOYMENT.md`
- `docs/SYSTEMD.md`
- `docs/k8s/README.md`

## CI and release notes

- PRs run CI via `.github/workflows/ci.yml`.
- Release checks and publish are handled by `.github/workflows/release.yml`.
- Release publishes are driven from version tags and publish the internal crate graph in
  dependency order before the `shardline` CLI crate.

## PR checklist

Before requesting review, confirm:

- [ ] `cargo make ci` passes locally
- [ ] tests cover new behavior and regressions
- [ ] crate boundaries and runtime invariants are preserved
- [ ] docs are updated if behavior, config, or operations changed
- [ ] performance impact is documented when relevant
- [ ] security implications are documented when relevant
