#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Build deterministic simulation e2e test runner"
cargo make sslt --profile ci-release -- --help

export RUNNER=./target/sim/ci-release/risingwave_simulation
export RUST_LOG=off

echo "--- deterministic simulation e2e, ci-3cn-1fe, ddl"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER './e2e_test/ddl/\*\*/\*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, streaming"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER './e2e_test/streaming/\*\*/\*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, batch"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER './e2e_test/batch/\*\*/\*.slt'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, streaming"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER -j 16 './e2e_test/streaming/\*\*/\*.slt'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, batch"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER -j 16 './e2e_test/batch/\*\*/\*.slt'

# bugs here! Tracking issue https://github.com/risingwavelabs/risingwave/issues/4527
# echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, streaming"
# seq 1 | parallel MADSIM_TEST_SEED={} $RUNNER --kill-compute './e2e_test/streaming/\*\*/\*.slt' || true

# bugs here! Tracking issue https://github.com/risingwavelabs/risingwave/issues/4527
# echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, batch"
# seq 1 | parallel MADSIM_TEST_SEED={} $RUNNER --kill-compute './e2e_test/batch/\*\*/\*.slt' || true

echo "--- deterministic simulation e2e, ci-3cn-1fe, fuzzing"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata
