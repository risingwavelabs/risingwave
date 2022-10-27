#!/bin/bash

source ci/scripts/common.env.sh

echo "--- Download artifacts"
buildkite-agent artifact download risingwave_simulation .
chmod +x ./risingwave_simulation

export RUST_LOG=info
export LOGDIR=.risingwave/log

# bugs here! Tracking issue https://github.com/risingwavelabs/risingwave/issues/4527
echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, streaming"
mkdir -p $LOGDIR
seq 1 | parallel MADSIM_TEST_SEED={} './risingwave_simulation --kill-meta --kill-frontend --kill-compute --kill-compactor ./e2e_test/streaming/\*\*/\*.slt > $LOGDIR/{}.log'
rm -rf $LOGDIR

# bugs here! Tracking issue https://github.com/risingwavelabs/risingwave/issues/4527
echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, batch"
mkdir -p $LOGDIR
seq 1 | parallel MADSIM_TEST_SEED={} './risingwave_simulation --kill-meta --kill-frontend --kill-compute --kill-compactor ./e2e_test/batch/\*\*/\*.slt > $LOGDIR/{}.log'
rm -rf $LOGDIR

# bugs here! Tracking issue https://github.com/risingwavelabs/risingwave/issues/5103
echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, streaming"
mkdir -p $LOGDIR
seq 1 | parallel MADSIM_TEST_SEED={} './risingwave_simulation --etcd-timeout-rate=0.01 ./e2e_test/streaming/\*\*/\*.slt > $LOGDIR/{}.log'
rm -rf $LOGDIR
