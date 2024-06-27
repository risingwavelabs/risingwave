#!/bin/bash

set -euo pipefail

# setup kafka
docker compose exec message_queue \
    /usr/bin/rpk topic create -p 2 counts

docker compose exec message_queue \
    /usr/bin/rpk topic create -p 1 types

docker compose exec message_queue \
    /usr/bin/rpk topic create -p 1 flinktypes

# setup flink
docker compose run flink-sql-client \
    /opt/flink/bin/sql-client.sh -f /tmp/flink.sql
docker compose run flink-sql-client \
    /opt/flink/bin/sql-client.sh -f /tmp/compatibility-flink.sql

# setup connect
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @jsons/pg-sink.json

# set rw for compatibility test
psql -h localhost -p 4566 -d dev -U root -f compatibility-rw.sql
psql -h localhost -p 4566 -d dev -U root -f compatibility-rw-flink.sql
