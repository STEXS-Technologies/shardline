#!/bin/sh
set -eu

psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE shardline_replication WITH REPLICATION LOGIN PASSWORD 'replication-dev-password';
SQL

printf '%s\n' \
  'host replication shardline_replication 0.0.0.0/0 scram-sha-256' \
  'host replication shardline_replication ::/0 scram-sha-256' \
  >> "$PGDATA/pg_hba.conf"
