#!/bin/bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ssl_user WITH PASSWORD 'ssl_password' SUPERUSER;
EOSQL

# Prepend the rule to ensure it takes precedence over default "host all all ..." rules
sed -i '1s/^/hostnossl all ssl_user all reject\n/' /var/lib/postgresql/data/pg_hba.conf
