#!/bin/bash

set -euo pipefail

# setup doris
docker compose exec mysql bash -c "mysql -uroot -P9030 -hfe < doris_prepare.sql"
