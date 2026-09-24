#!/usr/bin/env bash
set -euo pipefail

minio_port="${SHARDLINE_CHAOS_MINIO_PORT:-39000}"
compose_project="${SHARDLINE_CHAOS_COMPOSE_PROJECT:-shardline-chaos}"

container_id() {
  docker compose -f docker-compose.chaos.yml -p "${compose_project}" ps --all -q "$1"
}

for service in chaos-postgres chaos-redis; do
  until container="$(container_id "${service}")" && [[ -n "${container}" ]] \
    && [[ "$(docker inspect -f '{{.State.Health.Status}}' "${container}" 2>/dev/null)" == "healthy" ]]; do
    sleep 1
  done
done

until curl --fail --silent "http://127.0.0.1:${minio_port}/minio/health/live" >/dev/null; do
  sleep 1
done

until init_container="$(container_id chaos-minio-init)" && [[ -n "${init_container}" ]] \
  && [[ "$(docker inspect -f '{{.State.Status}}' "${init_container}" 2>/dev/null)" == "exited" ]]; do
  sleep 1
done

[[ "$(docker inspect -f '{{.State.ExitCode}}' "${init_container}")" == "0" ]]
