#!/bin/bash

# Exits as soon as any line fails.
set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/.." || exit 1

echo "Stopping single node zookeeper and kafka"
docker compose -f "$SCRIPT_PATH"/docker-compose.yml down
