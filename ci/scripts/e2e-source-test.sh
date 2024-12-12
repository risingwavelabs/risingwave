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

echo "--- Install dependencies"
python3 -m pip install --break-system-packages requests protobuf fastavro confluent_kafka jsonschema nats-py requests psycopg2-binary
apt-get -y install jq

echo "--- e2e, inline test"
RUST_LOG="debug,risingwave_stream=debug,risingwave_batch=info,risingwave_storage=info,risingwave_meta=info,events::stream::message::chunk=trace" \
risedev ci-start ci-inline-source-test
risedev slt './e2e_test/source_inline/**/*.slt' -j16
risedev slt './e2e_test/source_inline/**/*.slt.serial'
echo "--- Kill cluster"
risedev ci-kill
