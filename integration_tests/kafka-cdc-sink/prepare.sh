#!/bin/bash

set -euo pipefail

# setup kafka
docker compose exec message_queue \
    /usr/bin/rpk topic create -p 2 counts

# setup flink
docker compose run flink-sql-client \
    /opt/flink/bin/sql-client.sh -f /tmp/flink.sql

# setup connect
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @jsons/pg-sink.json
