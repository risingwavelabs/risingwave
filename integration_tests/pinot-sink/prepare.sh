#!/bin/bash

set -euo pipefail
set -x  # echo commands for CI debuggability

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

ensure_service_running() {
  local service_name=$1
  local retries=${2:-30}
  local interval=${3:-2}

  if wait_for_container_running "$service_name" "$retries" "$interval"; then
    return 0
  fi

  echo "restart service \"${service_name}\" and retry"
  docker compose up -d "$service_name"
  wait_for_container_running "$service_name" "$retries" "$interval"
}

# setup kafka
ensure_service_running kafka 45 2
# create topic if not exists; tolerate already-exists error in CI reruns
docker compose exec kafka \
  kafka-topics --create --topic orders.upsert.log --bootstrap-server localhost:9092 || true

# setup pinot
ensure_service_running pinot-controller 45 2
# Avoid -it in CI (no TTY); call pinot-admin directly in the container
docker exec pinot-controller /opt/pinot/bin/pinot-admin.sh AddTable \
  -tableConfigFile /config/orders_table.json \
  -schemaFile /config/orders_schema.json -exec
