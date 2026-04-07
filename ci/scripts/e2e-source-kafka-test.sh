#!/usr/bin/env bash

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

source_test_env_setup "$profile" ci-source-kafka-test true true

echo "--- Setup HashiCorp Vault for Kafka source testing"
export VAULT_ADDR="http://vault-server:8200"
export VAULT_TOKEN="root-token"
./ci/scripts/setup-vault.sh

echo "--- Run Kafka source tests"
risedev slt './e2e_test/kafka-sasl/**/*.slt' -j4
risedev slt './e2e_test/source_inline/connection/*.slt'
risedev slt './e2e_test/source_inline/vault/vault_secret_source.slt'
risedev slt './e2e_test/source_inline/kafka/**/*.slt' --skip 'cron_only' -j8
risedev slt './e2e_test/source_inline/kafka/**/*.slt.serial' --skip 'cron_only'

if [ "$profile" == "ci-release" ]; then
    echo "--- Run release mode only tests"
    risedev slt './e2e_test/backfill/backfill_progress/create_materialized_view_mix_source_and_normal.slt'
fi

echo "--- Kill cluster"
risedev ci-kill

echo "--- Prepare Kafka source fixtures"
cp src/connector/src/test_data/simple-schema.avsc ./avro-simple-schema.avsc
cp src/connector/src/test_data/complex-schema.avsc ./avro-complex-schema.avsc
cp src/connector/src/test_data/complex-schema.json ./json-complex-schema

echo "--- Run legacy Kafka source tests"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
risedev ci-start ci-kafka
./e2e_test/source_legacy/basic/scripts/prepare_ci_kafka.sh
risedev slt './e2e_test/source_legacy/basic/*.slt'
risedev slt './e2e_test/source_legacy/basic/old_row_format_syntax/*.slt'

echo "--- Run CH-benCHmark"
risedev slt './e2e_test/ch_benchmark/batch/ch_benchmark.slt'
risedev slt './e2e_test/ch_benchmark/streaming/*.slt'

echo "--- Kill cluster"
risedev ci-kill
