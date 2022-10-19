#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

pwd
ls

echo "--- Download artifacts"
buildkite-agent artifact download risingwave_simulation .
chmod +x ./risingwave_simulation

export RUNNER=./risingwave_simulation
export RUST_LOG=off

echo "--- deterministic simulation e2e, ci-3cn-1fe, ddl"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER './e2e_test/ddl/\*\*/\*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, streaming"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER './e2e_test/streaming/\*\*/\*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, batch"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER './e2e_test/batch/\*\*/\*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, kafka source"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER './e2e_test/source/kafka.slt'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, streaming"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER -j 16 './e2e_test/streaming/\*\*/\*.slt'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, batch"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER -j 16 './e2e_test/batch/\*\*/\*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, fuzzing"
seq 16 | parallel MADSIM_TEST_SEED={} $RUNNER --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata
