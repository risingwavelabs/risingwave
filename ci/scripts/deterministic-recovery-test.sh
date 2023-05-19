#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Download artifacts"
download-and-decompress-artifact risingwave_simulation .
chmod +x ./risingwave_simulation

export RUST_LOG=info
export LOGDIR=.risingwave/log

mkdir -p $LOGDIR

echo "--- deterministic simulation e2e, ci-3cn-2fe-3meta, recovery, ddl"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation --kill --kill-rate=${KILL_RATE} ./e2e_test/ddl/\*\*/\*.slt 2> $LOGDIR/recovery-ddl-{}.log && rm $LOGDIR/recovery-ddl-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe-3meta, recovery, streaming"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation --kill --kill-rate=${KILL_RATE} ./e2e_test/streaming/\*\*/\*.slt 2> $LOGDIR/recovery-streaming-{}.log && rm $LOGDIR/recovery-streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe-3meta, recovery, batch"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation --kill --kill-rate=${KILL_RATE} ./e2e_test/batch/\*\*/\*.slt 2> $LOGDIR/recovery-batch-{}.log && rm $LOGDIR/recovery-batch-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe-3meta, recovery, kafka source,sink"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation --kill --kill-rate=${KILL_RATE} --kafka-datadir=./scripts/source/test_data ./e2e_test/source/basic/kafka\*.slt 2> $LOGDIR/recovery-source-{}.log && rm $LOGDIR/recovery-source-{}.log'
