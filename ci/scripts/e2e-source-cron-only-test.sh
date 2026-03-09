#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# prepare environment
export CONNECTOR_LIBS_PATH="./connector-node/libs"

wait_for_service() {
    local name="$1"
    local cmd="$2"
    local retries="${3:-60}"
    local interval="${4:-2}"

    for ((i = 1; i <= retries; i++)); do
        if eval "$cmd" >/dev/null 2>&1; then
            echo "--- ${name} is ready"
            return 0
        fi
        echo "--- waiting for ${name} (${i}/${retries})"
        sleep "$interval"
    done

    echo "--- ${name} is not ready after ${retries} retries"
    return 1
}

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

echo "--- Wait for docker compose dependencies"
wait_for_service "Postgres(db)" "pg_isready -h db -p 5432 -U postgres"
wait_for_service "Kafka(message_queue)" "rpk cluster info -X brokers=message_queue:29092"

echo "--- e2e, cron_only inline source tests"
RUST_LOG="debug,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_meta=info" \
risedev ci-start ci-inline-source-cron-only-test

echo "--- Run cron_only source inline tests"
risedev slt './e2e_test/source_inline/**/cron_only/**/*.slt' -j4
risedev slt './e2e_test/source_inline/**/cron_only/**/*.slt.serial'

echo "--- Kill cluster"
risedev ci-kill
