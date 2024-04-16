#!/bin/bash

set -euo pipefail

# setup kafka
docker compose exec kafka \
kafka-topics --create --topic orders.upsert.log --bootstrap-server localhost:9092

# setup pinot
docker exec -it pinot-controller /opt/pinot/bin/pinot-admin.sh AddTable \
-tableConfigFile /config/orders_table.json \
-schemaFile /config/orders_schema.json -exec
