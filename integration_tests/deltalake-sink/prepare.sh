#!/bin/bash

set -euo pipefail

# build minio dir and create table
docker compose exec minio-0 mkdir /data/deltalake
docker compose exec spark bash /spark-script/run-sql-file.sh create-table