#!/bin/bash

source ci/scripts/common.env.sh

echo "--- Download artifacts"
buildkite-agent artifact download risingwave_simulation .
chmod +x ./risingwave_simulation

export RUNNER=./risingwave_simulation
export RUST_LOG=off

# bugs here! Tracking issue https://github.com/risingwavelabs/risingwave/issues/4527
echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, streaming"
seq 1 | parallel MADSIM_TEST_SEED={} $RUNNER --kill-meta --kill-frontend --kill-compute './e2e_test/streaming/\*\*/\*.slt'

# bugs here! Tracking issue https://github.com/risingwavelabs/risingwave/issues/4527
echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, batch"
seq 1 | parallel MADSIM_TEST_SEED={} $RUNNER --kill-meta --kill-frontend --kill-compute './e2e_test/batch/\*\*/\*.slt'

# bugs here! Tracking issue https://github.com/risingwavelabs/risingwave/issues/5103
echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, streaming"
seq 1 | parallel MADSIM_TEST_SEED={} $RUNNER --etcd-timeout-rate=0.01 './e2e_test/streaming/\*\*/\*.slt'
