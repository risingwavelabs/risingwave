#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# prepare environment
export CONNECTOR_RPC_ENDPOINT="localhost:50051"

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

download_java_binding "$profile"

echo "--- Download connector node package"
buildkite-agent artifact download risingwave-connector.tar.gz ./
mkdir ./connector-node
tar xf ./risingwave-connector.tar.gz -C ./connector-node

echo "--- Prepare data"
cp src/connector/src/test_data/simple-schema.avsc ./avro-simple-schema.avsc
cp src/connector/src/test_data/complex-schema.avsc ./avro-complex-schema.avsc
cp src/connector/src/test_data/complex-schema ./proto-complex-schema



echo "--- e2e, ci-1cn-1fe, mysql & postgres cdc"

# import data to mysql
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source/cdc/mysql_cdc.sql

# import data to postgres
export PGPASSWORD='postgres';
createdb -h db -U postgres cdc_test
psql -h db -U postgres -d cdc_test < ./e2e_test/source/cdc/postgres_cdc.sql

node_port=50051
node_timeout=10

echo "--- starting risingwave cluster with connector node"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info"
cargo make ci-start ci-1cn-1fe-with-recovery
./connector-node/start-service.sh -p $node_port > .risingwave/log/connector-node.log 2>&1 &

echo "waiting for connector node to start"
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

echo "--- mysql & postgres cdc validate test"
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.validate.mysql.slt'
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.validate.postgres.slt'

echo "--- mysql & postgres load and check"
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.load.slt'
# wait for cdc loading
sleep 10
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.check.slt'

# kill cluster
cargo make kill
# insert new rows
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source/cdc/mysql_cdc_insert.sql
psql -h db -U postgres -d cdc_test < ./e2e_test/source/cdc/postgres_cdc_insert.sql

# start cluster w/o clean-data
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info"
cargo make dev ci-1cn-1fe-with-recovery
echo "wait for recovery finish"
sleep 20
echo "check mviews after cluster recovery"
# check results
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.check_new_rows.slt'

echo "--- Kill cluster"
cargo make ci-kill
pkill -f connector-node

echo "--- e2e, ci-kafka-plus-pubsub, kafka and pubsub source"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info"
cargo make ci-start ci-kafka-plus-pubsub
./scripts/source/prepare_ci_kafka.sh
cargo run --bin prepare_ci_pubsub
sqllogictest -p 4566 -d dev './e2e_test/source/basic/*.slt'

echo "--- Run CH-benCHmark"
./risedev slt -p 4566 -d dev './e2e_test/ch_benchmark/batch/ch_benchmark.slt'
./risedev slt -p 4566 -d dev './e2e_test/ch_benchmark/streaming/*.slt'
