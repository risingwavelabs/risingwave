#!/bin/bash

set -euo pipefail

wait_for_container_running() {
  local service_name=$1
  local retries=${2:-30}
  local interval=${3:-2}

  for _ in $(seq 1 "$retries"); do
    if docker compose ps --status running "$service_name" | grep -q "$service_name"; then
      return 0
    fi
    sleep "$interval"
  done

  echo "service \"${service_name}\" is not running"
  docker compose ps "$service_name"
  return 1
}

# setup kafka
wait_for_container_running kafka 45 2
docker compose exec kafka \
kafka-topics --create --topic orders.upsert.log --bootstrap-server localhost:9092

# setup pinot
wait_for_container_running pinot-controller 45 2
docker exec -it pinot-controller /opt/pinot/bin/pinot-admin.sh AddTable \
-tableConfigFile /config/orders_table.json \
-schemaFile /config/orders_schema.json -exec
