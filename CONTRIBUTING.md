# Contributing to Shardline

This guide defines how to contribute changes to `shardline` while keeping architecture,
reliability, and release standards intact.

## Scope

This repository is a Rust workspace with the main CLI crate at `crates/cli` (published
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

Quick smoke check:

```bash
cargo check -p shardline
```

## Development workflow

1. Make focused changes in the owning crate or module.
2. Add or update tests with the behavior change.
3. Run contributor quality gates locally.
4. Open a PR with a clear summary, risk notes, and test evidence.

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

- Explicitly list affected crates (`protocol`, `storage`, `index`, `cas`, `cache`, `vcs`, `server`, `cli`).
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

For dependency-policy checks too:

```bash
cargo make ci-full
```

## Architecture and code rules

Follow the current project constraints:

- Keep crate boundaries intentional: `protocol`, `storage`, `index`, `cache`, `cas`,
  `vcs`, `server`, and `cli` should keep their current responsibilities clear.
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
