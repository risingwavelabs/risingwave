#!/usr/bin/env bash

set -euo pipefail

echo "--- starting risingwave cluster with connector node"
cargo make ci-start ci-1cn-1fe

echo "--- starting message_queue mongodb connect"
docker compose -f e2e_test/debezium/docker-compose.yml up -d message_queue mongodb connect

sleep 5

echo "--- prepare data"
docker compose -f e2e_test/debezium/docker-compose.yml exec mongodb bash -c '/usr/local/bin/init-inventory.sh'

curl -i -X POST -H "Accept:application/json" \
                -H  "Content-Type:application/json" \
                http://localhost:8083/connectors/ \
                -d @e2e_test/debezium/register-mongodb.json

sleep 5

echo "--- run test"
sqllogictest -p 4566 -d dev 'e2e_test/debezium/mongo_source.slt'

echo "--- Kill cluster"
cargo make ci-kill
docker compose -f e2e_test/debezium/docker-compose.yml down