#!/bin/bash

set -euo pipefail

# wait starrocks start
sleep 60

# setup starrocks
docker compose exec starrocks-fe bash -c "mysql -uroot -P9030 -h127.0.0.1 < /starrocks_prepare.sql"
