#!/bin/bash

set -euo pipefail

# setup postgres
docker compose exec postgres bash -c "psql postgresql://myuser:123456@postgres:5432/mydb < postgres_prepare.sql"
