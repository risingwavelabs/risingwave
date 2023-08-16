#!/usr/bin/env bash

# Exits as soon as any line fails.
set -exuo pipefail

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

"$HOME"/.local/bin/poetry update --quiet
"$HOME"/.local/bin/poetry run python main.py


echo "--- Kill cluster"
cd ../../
cargo make ci-kill
