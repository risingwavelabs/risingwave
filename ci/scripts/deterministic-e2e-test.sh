#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Build deterministic simulation e2e test runner"
timeout 10m cargo make sslt --profile ci-release -- --help

export RUNNER=./target/sim/ci-release/risingwave_simulation

echo "--- deterministic simulation e2e, ci-3cn-1fe, ddl"
seq 10 | timeout 10s parallel MADSIM_TEST_SEED={} $RUNNER './e2e_test/ddl/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, streaming"
seq 10 | timeout 1m parallel MADSIM_TEST_SEED={} $RUNNER './e2e_test/streaming/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, batch"
seq 10 | timeout 30s parallel MADSIM_TEST_SEED={} $RUNNER './e2e_test/batch/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, streaming"
seq 10 | timeout 90s parallel MADSIM_TEST_SEED={} $RUNNER -j 16 './e2e_test/streaming/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, batch"
seq 10 | timeout 30s parallel MADSIM_TEST_SEED={} $RUNNER -j 16 './e2e_test/batch/**/*.slt'

# bugs here!
echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, streaming"
seq 10 | timeout 2m parallel MADSIM_TEST_SEED={} RUST_LOG=off $RUNNER --kill-node './e2e_test/streaming/**/*.slt' || true

echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, batch"
seq 10 | timeout 2m parallel MADSIM_TEST_SEED={} RUST_LOG=off $RUNNER --kill-node './e2e_test/batch/**/*.slt'
