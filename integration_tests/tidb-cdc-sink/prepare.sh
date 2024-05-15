#!/bin/bash

set -euo pipefail

docker compose exec mysql bash -c "mysql --password= -h tidb --port 4000 -u root test < tidb_create_tables.sql"

docker compose exec mysql bash -c "mysql --password= -h tidb --port 4000 -u root test < tidb_prepare.sql"

psql "port=4566 host=localhost user=postgres dbname=dev sslmode=disable" -f create_source.sql
psql "port=4566 host=localhost user=postgres dbname=dev sslmode=disable" -f create_mv.sql
psql "port=4566 host=localhost user=postgres dbname=dev sslmode=disable" -f create_sink.sql


sleep 15
