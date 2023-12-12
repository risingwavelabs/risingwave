#!/bin/bash

set -euo pipefail

sleep 10

docker compose exec mysql bash -c "mysql --password= -h tidb --port 4000 -u root test < tidb_create_tables.sql"

docker compose exec mysql bash -c "mysql --password= -h tidb --port 4000 -u root test < tidb_prepare.sql"
