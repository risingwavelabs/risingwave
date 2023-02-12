#!/usr/bin/env bash

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

(wget -O - pi.dk/3 || lynx -source pi.dk/3 ||
	curl pi.dk/3/ || fetch -o - http://pi.dk/3) > install.sh

bash install.sh

echo "--- deterministic simulation e2e, ci-3cn-2fe, ddl"
seq $TEST_NUM | /var/lib/buildkite-agent/bin/parallel-20230122 MADSIM_TEST_SEED={} './risingwave_simulation ./e2e_test/ddl/\*\*/\*.slt 2> $LOGDIR/ddl-{}.log && rm $LOGDIR/ddl-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, streaming"
seq $TEST_NUM | /var/lib/buildkite-agent/bin/parallel-20230122 MADSIM_TEST_SEED={} './risingwave_simulation ./e2e_test/streaming/\*\*/\*.slt 2> $LOGDIR/streaming-{}.log && rm $LOGDIR/streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, batch"
seq $TEST_NUM | /var/lib/buildkite-agent/bin/parallel-20230122 MADSIM_TEST_SEED={} './risingwave_simulation ./e2e_test/batch/\*\*/\*.slt 2> $LOGDIR/batch-{}.log && rm $LOGDIR/batch-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, kafka source"
seq $TEST_NUM | /var/lib/buildkite-agent/bin/parallel-20230122 MADSIM_TEST_SEED={} './risingwave_simulation --kafka-datadir=./scripts/source/test_data ./e2e_test/source/basic/kafka\*.slt 2> $LOGDIR/source-{}.log && rm $LOGDIR/source-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, /var/lib/buildkite-agent/bin/parallel-20230122, streaming"
seq $TEST_NUM | /var/lib/buildkite-agent/bin/parallel-20230122 MADSIM_TEST_SEED={} './risingwave_simulation -j 16 ./e2e_test/streaming/\*\*/\*.slt 2> $LOGDIR//var/lib/buildkite-agent/bin/parallel-20230122-streaming-{}.log && rm $LOGDIR//var/lib/buildkite-agent/bin/parallel-20230122-streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, /var/lib/buildkite-agent/bin/parallel-20230122, batch"
seq $TEST_NUM | /var/lib/buildkite-agent/bin/parallel-20230122 MADSIM_TEST_SEED={} './risingwave_simulation -j 16 ./e2e_test/batch/\*\*/\*.slt 2> $LOGDIR//var/lib/buildkite-agent/bin/parallel-20230122-batch-{}.log && rm $LOGDIR//var/lib/buildkite-agent/bin/parallel-20230122-batch-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, fuzzing"
seq $TEST_NUM | /var/lib/buildkite-agent/bin/parallel-20230122 MADSIM_TEST_SEED={} './risingwave_simulation --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata 2> $LOGDIR/fuzzing-{}.log && rm $LOGDIR/fuzzing-{}.log'
