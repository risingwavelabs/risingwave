#!/bin/bash

source ci/scripts/common.env.sh

echo "--- Download artifacts"
buildkite-agent artifact download risingwave_simulation .
chmod +x ./risingwave_simulation

export RUST_LOG=off
export RUN="seq 1 | parallel --res .risingwave/log MADSIM_TEST_SEED={} ./risingwave_simulation"

# bugs here! Tracking issue https://github.com/risingwavelabs/risingwave/issues/4527
echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, streaming"
$RUN --kill-meta --kill-frontend --kill-compute './e2e_test/streaming/\*\*/\*.slt'
rm -rf .risingwave

# bugs here! Tracking issue https://github.com/risingwavelabs/risingwave/issues/4527
echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, batch"
$RUN --kill-meta --kill-frontend --kill-compute './e2e_test/batch/\*\*/\*.slt'
rm -rf .risingwave

# bugs here! Tracking issue https://github.com/risingwavelabs/risingwave/issues/5103
echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, streaming"
$RUN --etcd-timeout-rate=0.01 './e2e_test/streaming/\*\*/\*.slt'
rm -rf .risingwave
