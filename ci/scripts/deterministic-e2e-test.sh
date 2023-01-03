#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Download artifacts"
buildkite-agent artifact download risingwave_simulation .
chmod +x ./risingwave_simulation

echo "--- Extract data for Kafka"
cd ./scripts/source/
mkdir -p ./test_data
unzip -o test_data.zip -d .
cd ../../

export RUST_LOG=info
export LOGDIR=.risingwave/log

mkdir -p $LOGDIR

echo "--- deterministic simulation e2e, ci-3cn-1fe, ddl"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation ./e2e_test/ddl/\*\*/\*.slt > $LOGDIR/ddl-{}.log && rm $LOGDIR/ddl-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-1fe, streaming"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation ./e2e_test/streaming/\*\*/\*.slt > $LOGDIR/streaming-{}.log && rm $LOGDIR/streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-1fe, batch"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation ./e2e_test/batch/\*\*/\*.slt > $LOGDIR/batch-{}.log && rm $LOGDIR/batch-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-1fe, kafka source"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation --kafka-datadir=./scripts/source/test_data ./e2e_test/source/kafka.slt > $LOGDIR/source-{}.log && rm $LOGDIR/source-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, streaming"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation -j 16 ./e2e_test/streaming/\*\*/\*.slt > $LOGDIR/parallel-streaming-{}.log && rm $LOGDIR/parallel-streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, batch"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation -j 16 ./e2e_test/batch/\*\*/\*.slt > $LOGDIR/parallel-batch-{}.log && rm $LOGDIR/parallel-batch-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-1fe, fuzzing"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata > $LOGDIR/fuzzing-{}.log && rm $LOGDIR/fuzzing-{}.log'
