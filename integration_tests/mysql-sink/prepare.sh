#!/bin/bash

set -euo pipefail

sleep 10

# setup mysql
docker compose exec mysql bash -c "mysql -p123456 -h mysql mydb < mysql_prepare.sql"
