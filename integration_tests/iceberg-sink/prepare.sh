#!/bin/bash

set -euo pipefail

# setup
docker compose exec spark bash /spark-script/run-sql-file.sh create-table
