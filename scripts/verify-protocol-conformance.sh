#!/usr/bin/env bash
# Qualify the documented Git LFS and OCI endpoints with upstream clients.
set -euo pipefail
cd "$(dirname "$0")/.."
git lfs version
command -v aws >/dev/null
aws --version
export BOTOCORE_PYTHON="${BOTOCORE_PYTHON:-python3}"
"$BOTOCORE_PYTHON" -c 'import boto3, botocore.session'

if [[ -z "${MINIO_SIGNER_BIN:-}" ]]; then
  command -v go >/dev/null
  protocol_signer_workdir="$(mktemp -d "${TMPDIR:-/tmp}/shardline-minio-signer.XXXXXXXX")"
  trap 'rm -rf -- "${protocol_signer_workdir:-}" "${protocol_workdir:-}"' EXIT
  (cd e2e/clients/minio-signed-stream && go build -mod=readonly -o "$protocol_signer_workdir/minio-signed-stream" .)
  export MINIO_SIGNER_BIN="$protocol_signer_workdir/minio-signed-stream"
fi
[[ -x "$MINIO_SIGNER_BIN" ]]

# Fail before compilation if a configured deployment cannot run its checks.
protocol_remote_storage=false
if [[ -n "${TEST_S3_BUCKET:-}${TEST_S3_ENDPOINT:-}${TEST_S3_ACCESS_KEY:-}${TEST_S3_SECRET_KEY:-}" ]]; then
  : "${TEST_S3_BUCKET:?remote qualification requires a disposable S3 bucket}"
  : "${DATABASE_URL:?remote qualification requires a disposable PostgreSQL database}"
  : "${TEST_S3_ENDPOINT:?remote qualification requires an S3 endpoint}"
  : "${TEST_S3_ACCESS_KEY:?remote qualification requires an S3 access key}"
  : "${TEST_S3_SECRET_KEY:?remote qualification requires an S3 secret key}"
  protocol_remote_storage=true
fi

if [[ -z "${OCI_CONFORMANCE_BIN:-}" ]]; then
  command -v go >/dev/null
  protocol_workdir="$(mktemp -d "${TMPDIR:-/tmp}/shardline-oci-conformance.XXXXXXXX")"
  trap 'rm -rf -- "${protocol_signer_workdir:-}" "$protocol_workdir"' EXIT
  # OCI Distribution v1.1.1, pinned to the commit rather than a movable tag.
  protocol_revision=a139cc423184af6078077b9b7ee336eddbd03f8f
  git -C "$protocol_workdir" init --quiet
  git -C "$protocol_workdir" fetch --quiet --depth=1 \
    https://github.com/opencontainers/distribution-spec.git "$protocol_revision"
  git -C "$protocol_workdir" checkout --quiet --detach FETCH_HEAD
  [[ "$(git -C "$protocol_workdir" rev-parse HEAD)" == "$protocol_revision" ]]
  (cd "$protocol_workdir/conformance" && go test -c -o "$protocol_workdir/conformance.test")
  export OCI_CONFORMANCE_BIN="$protocol_workdir/conformance.test"
fi
[[ -x "$OCI_CONFORMANCE_BIN" ]]

# These tests start an isolated HTTP server and use disposable local metadata.
cargo test --locked -p shardline-hub-api --test git_cli --test git_smart_http -- --nocapture
cargo test --locked -p shardline-index --lib atomic_ref_batch -- --skip hub_postgres --nocapture
cargo test --locked -p shardline-server --test credential_header_ambiguity -- --nocapture
cargo test --locked -p shardline-server --lib subject_index -- --nocapture
cargo test --locked -p shardline-server --test protocol_completion \
  native_minio_signed_streaming -- --ignored --skip postgres --nocapture
cargo nextest run --locked -p shardline-server --test protocol_completion \
  --run-ignored all -E '!test(/postgres|minio_|lfs_process_child/)'

# Own disposable backends; unlike optional developer fixtures, missing Docker
# or a failed database/object-store startup must fail this required gate.
cargo nextest run --locked -p shardline-server --features docker \
  --test protocol_completion --run-ignored only \
  -E 'test(official_oci_distribution_on_postgres_and_minio) | test(native_atomic_git_with_owned_postgres_and_minio) | test(native_botocore_checksum_with_owned_postgres_and_minio) | test(native_small_ranges_with_owned_postgres_and_minio)'
cargo test --locked -p shardline-server --features docker --lib \
  authenticated_ranges_on_real_minio -- --ignored --nocapture

if [[ -n "${DATABASE_URL:-}" ]]; then
  cargo test --locked -p shardline-server --lib \
    database_migration::frontend_schema::tests:: -- --ignored --nocapture
  cargo test --locked -p shardline-index --lib hub_postgres::tests::atomic_ref_batch -- --nocapture
  # Includes an owned temporary role to prove runtime indexing needs no DDL.
  cargo test --locked -p shardline-index --lib subject_index -- \
    --include-ignored --nocapture
  cargo test --locked -p shardline-index --lib reclamation_barrier -- \
    --include-ignored --nocapture
  cargo test --locked -p shardline-server --lib \
    corrupt_copy_source_preserves -- --include-ignored --nocapture
  cargo test --locked -p shardline-server --lib \
    hub_server_shutdown_joins_startup_workers_before_runtime_teardown -- \
    --ignored --nocapture
  cargo test --locked -p shardline-server --test protocol_completion postgres -- \
    --ignored --skip on_postgres_s3_replica --skip on_postgres_and_minio --nocapture
else
  echo "PostgreSQL qualification is separate: set DATABASE_URL to a disposable database."
fi

if [[ "$protocol_remote_storage" == true ]]; then
  cargo test --locked -p shardline-server --test protocol_completion \
    native_botocore_full_object_checksum_on_postgres_and_minio_survives_restart -- --ignored --nocapture
  cargo test --locked -p shardline-server --test protocol_completion \
    native_atomic_git_on_postgres_and_minio_survives_server_restart -- --ignored --nocapture
  cargo test --locked -p shardline-server --test protocol_completion \
    native_minio_signed_streaming_on_postgres_and_minio -- --ignored --nocapture
  cargo test --locked -p shardline-server --test protocol_completion \
    minio_oci_physical_reclamation_preserves_live_referrers -- --ignored --nocapture
  cargo test --locked -p shardline-server --test protocol_completion \
    minio_oci_referrers_survive_replica_replacement -- --ignored --nocapture
  cargo test --locked -p shardline-server --test protocol_completion \
    native_aws_cli_resumes_multipart_on_postgres_s3_replica -- --ignored --nocapture
  cargo test --locked -p shardline-server --test protocol_completion \
    minio_completion_abort_race -- --ignored --nocapture
  cargo test --locked -p shardline-server --test protocol_completion \
    minio_large_copy_range_survives_replica_restart -- --ignored --nocapture
else
  echo "Remote-storage qualification is separate: configure DATABASE_URL and TEST_S3_* to run it."
fi
