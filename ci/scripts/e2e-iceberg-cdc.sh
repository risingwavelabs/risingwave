#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# prepare environment
export CONNECTOR_RPC_ENDPOINT="localhost:50051"
export CONNECTOR_LIBS_PATH="./connector-node/libs"

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

download_and_prepare_rw "$profile" source

echo "--- Download connector node package"
buildkite-agent artifact download risingwave-connector.tar.gz ./
mkdir ./connector-node
tar xf ./risingwave-connector.tar.gz -C ./connector-node

echo "--- e2e, ci-1cn-1fe, iceberg cdc"

node_port=50051
node_timeout=10

wait_for_connector_node_start() {
  start_time=$(date +%s)
  while :
  do
      if nc -z localhost $node_port; then
          echo "Port $node_port is listened! Connector Node is up!"
          break
      fi

      current_time=$(date +%s)
      elapsed_time=$((current_time - start_time))
      if [ $elapsed_time -ge $node_timeout ]; then
          echo "Timeout waiting for port $node_port to be listened!"
          exit 1
      fi
      sleep 0.1
  done
  sleep 2
}

echo "--- starting risingwave cluster with connector node"

RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cargo make ci-start ci-1cn-1fe-with-recovery
./connector-node/start-service.sh -p $node_port > .risingwave/log/connector-node.log 2>&1 &
echo "waiting for connector node to start"
wait_for_connector_node_start

# prepare minio iceberg sink
echo "--- preparing iceberg"
.risingwave/bin/mcli -C .risingwave/config/mcli mb hummock-minio/icebergdata

cd e2e_test/iceberg
bash ./start_spark_connect_server.sh

# Don't remove the `--quiet` option since poetry has a bug when printing output, see
# https://github.com/python-poetry/poetry/issues/3412
"$HOME"/.local/bin/poetry update --quiet

# 1. import data to mysql
mysql --host=mysql --port=3306 -u root -p123456 < ./test_case/cdc/mysql_cdc.sql

# 2. create table and sink
"$HOME"/.local/bin/poetry run python main.py -t ./test_case/cdc/no_partition_cdc_init.toml

# 3. insert new data to mysql
mysql --host=mysql --port=3306 -u root -p123456 < ./test_case/cdc/mysql_cdc_insert.sql

sleep 20

# 4. check change
"$HOME"/.local/bin/poetry run python main.py -t ./test_case/cdc/no_partition_cdc.toml