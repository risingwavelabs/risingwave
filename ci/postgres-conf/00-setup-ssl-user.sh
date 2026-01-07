#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ssl_user WITH PASSWORD 'ssl_password';
EOSQL

echo "hostssl all ssl_user all md5" >> /var/lib/postgresql/data/pg_hba.conf
