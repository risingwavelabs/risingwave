#!/bin/bash
set -e

# Setup SSL key with correct permissions (must be owned by postgres:postgres and 0600)
# We copy it to PGDATA which is writable by the postgres user.
cp /etc/postgresql/server.key.mount /var/lib/postgresql/data/server.key
chmod 600 /var/lib/postgresql/data/server.key

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ssl_user WITH PASSWORD 'ssl_password';
EOSQL

echo "hostssl all ssl_user all md5" >> /var/lib/postgresql/data/pg_hba.conf
