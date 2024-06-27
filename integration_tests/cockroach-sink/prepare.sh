#!/bin/bash

set -euo pipefail

# setup cockroach
docker compose exec postgres bash -c "psql postgresql://root@cockroachdb:26257/defaultdb < cockroach_prepare.sql"
