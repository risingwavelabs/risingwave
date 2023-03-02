#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh
source ci/scripts/pr.env.sh

echo "--- Download artifacts"
buildkite-agent artifact download risingwave_simulation .
chmod +x ./risingwave_simulation

echo "--- Extract data for Kafka"
cd ./scripts/source/
mkdir -p ./test_data
unzip -o test_data.zip -d .
cd ../../

echo "--- Extract data for SqlSmith"
cd ./src/tests/sqlsmith/tests
git clone https://"$GITHUB_TOKEN"@github.com/risingwavelabs/sqlsmith-query-snapshots.git

export RUST_LOG=info
export LOGDIR=.risingwave/log

mkdir -p $LOGDIR

echo "--- deterministic simulation e2e, ci-3cn-2fe, ddl"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation ./e2e_test/ddl/\*\*/\*.slt 2> $LOGDIR/ddl-{}.log && rm $LOGDIR/ddl-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, streaming"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation ./e2e_test/streaming/\*\*/\*.slt 2> $LOGDIR/streaming-{}.log && rm $LOGDIR/streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, batch"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation ./e2e_test/batch/\*\*/\*.slt 2> $LOGDIR/batch-{}.log && rm $LOGDIR/batch-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, kafka source"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation --kafka-datadir=./scripts/source/test_data ./e2e_test/source/basic/kafka\*.slt 2> $LOGDIR/source-{}.log && rm $LOGDIR/source-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, streaming"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation -j 16 ./e2e_test/streaming/\*\*/\*.slt 2> $LOGDIR/parallel-streaming-{}.log && rm $LOGDIR/parallel-streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, batch"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation -j 16 ./e2e_test/batch/\*\*/\*.slt 2> $LOGDIR/parallel-batch-{}.log && rm $LOGDIR/parallel-batch-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, fuzzing (pre-generated-queries)"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation  --run-sqlsmith-queries ./src/tests/sqlsmith/tests/sqlsmith-query-snapshots/{} 2> $LOGDIR/fuzzing-{}.log && rm $LOGDIR/fuzzing-{}.log'
