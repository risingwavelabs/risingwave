#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# prepare environment
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

echo "--- starting risingwave cluster"

RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cargo make ci-start ci-1cn-1fe-with-recovery

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