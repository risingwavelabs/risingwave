#!/bin/bash

source ci/scripts/common.env.sh

echo "--- Download artifacts"
buildkite-agent artifact download risingwave_simulation .
chmod +x ./risingwave_simulation

export RUST_LOG=info
export LOGDIR=.risingwave/log

mkdir -p $LOGDIR

echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, streaming"
seq 1 | parallel MADSIM_TEST_SEED={} './risingwave_simulation --kill-meta --kill-frontend --kill-compute --kill-compactor --kill-rate=1.0 ./e2e_test/streaming/\*\*/\*.slt > $LOGDIR/recovery-streaming-{}.log && rm $LOGDIR/recovery-streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, batch"
seq 1 | parallel MADSIM_TEST_SEED={} './risingwave_simulation --kill-meta --kill-frontend --kill-compute --kill-compactor --kill-rate=1.0 ./e2e_test/batch/\*\*/\*.slt > $LOGDIR/recovery-batch-{}.log && rm $LOGDIR/recovery-batch-{}.log'
