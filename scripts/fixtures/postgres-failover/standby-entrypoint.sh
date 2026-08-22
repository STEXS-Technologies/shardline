#!/bin/sh
set -eu

until pg_basebackup \
  --host failover-primary \
  --username shardline_replication \
  --pgdata "$PGDATA" \
  --write-recovery-conf \
  --wal-method stream \
  --checkpoint fast
do
  sleep 1
done

chmod 0700 "$PGDATA"
exec docker-entrypoint.sh postgres -c hot_standby=on
