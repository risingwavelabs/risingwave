#!/bin/bash

set -euo pipefail

# setup doris
docker compose exec doris bash -c "mysql -uroot -P9030 -h127.0.0.1 < doris_prepare.sql"
