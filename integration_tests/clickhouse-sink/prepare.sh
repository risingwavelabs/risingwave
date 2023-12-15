#!/bin/bash

set -euo pipefail

# setup clickhouse
docker compose exec clickhouse-server bash -c "clickhouse-client < /clickhouse_prepare.sql"
