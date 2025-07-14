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
python3 -m pip install --break-system-packages -r ./e2e_test/requirements.txt

echo "--- Setup Kafka Admin ---"
# two containers: message_queue_sasl_1:9092 and message_queue_sasl_2:9093, set with the same user and admin

echo "--- Setup Kafka 1(message_queue_sasl_1:9092) ---"
RPK_BROKERS="message_queue_sasl_1:9092" ./ci/scripts/rpk-sasl-setup-auth.sh
echo "--- Setup Kafka 2(message_queue_sasl_2:9093) ---"
RPK_BROKERS="message_queue_sasl_2:9093" ./ci/scripts/rpk-sasl-setup-auth.sh

echo "--- test begins ---"
