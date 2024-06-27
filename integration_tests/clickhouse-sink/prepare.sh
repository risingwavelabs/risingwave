#!/bin/bash

set -euo pipefail

# setup clickhouse
docker compose exec clickhouse-server bash -c "clickhouse-client --multiquery < /clickhouse_prepare.sql"
