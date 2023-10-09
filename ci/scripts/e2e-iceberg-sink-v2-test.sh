#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

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

echo "--- starting risingwave cluster"
mkdir -p .risingwave/log
cargo make ci-start ci-iceberg-test
sleep 1

# prepare minio iceberg sink
echo "--- preparing iceberg"
.risingwave/bin/mcli -C .risingwave/config/mcli mb hummock-minio/icebergdata

cd e2e_test/iceberg
bash ./start_spark_connect_server.sh

# Don't remove the `--quiet` option since poetry has a bug when printing output, see
# https://github.com/python-poetry/poetry/issues/3412
"$HOME"/.local/bin/poetry update --quiet
"$HOME"/.local/bin/poetry run python main.py -t ./test_case/no_partition_append_only.toml
"$HOME"/.local/bin/poetry run python main.py -t ./test_case/no_partition_upsert.toml
"$HOME"/.local/bin/poetry run python main.py -t ./test_case/partition_append_only.toml
"$HOME"/.local/bin/poetry run python main.py -t ./test_case/partition_upsert.toml


echo "--- Kill cluster"
cd ../../
cargo make ci-kill
