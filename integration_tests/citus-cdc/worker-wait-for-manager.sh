#!/bin/bash
# override wait-for-manager.sh to set wal_level to logical

set -e

until test -f /healthcheck/manager-ready ; do
  >&2 echo "Manager is not ready - sleeping"
  sleep 1
done

>&2 echo "Manager is up - starting worker"

exec gosu postgres "/usr/local/bin/docker-entrypoint.sh" "postgres" "$@"
