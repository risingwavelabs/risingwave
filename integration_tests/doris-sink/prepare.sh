#!/bin/bash

set -euo pipefail

sleep 30

docker compose logs be
# setup doris
docker compose exec mysql bash -c "mysql -uroot -P9030 -hfe < doris_prepare.sql"
