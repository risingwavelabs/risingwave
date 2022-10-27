#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Download artifacts"
buildkite-agent artifact download risingwave_simulation .
chmod +x ./risingwave_simulation

export RUST_LOG=info
export RUN="seq 16 | parallel --res .risingwave/log MADSIM_TEST_SEED={} ./risingwave_simulation"

echo "--- deterministic simulation e2e, ci-3cn-1fe, ddl"
$RUN './e2e_test/ddl/\*\*/\*.slt'
rm -rf .risingwave

echo "--- deterministic simulation e2e, ci-3cn-1fe, streaming"
$RUN './e2e_test/streaming/\*\*/\*.slt'
rm -rf .risingwave

echo "--- deterministic simulation e2e, ci-3cn-1fe, batch"
$RUN './e2e_test/batch/\*\*/\*.slt'
rm -rf .risingwave

echo "--- deterministic simulation e2e, ci-3cn-1fe, kafka source"
$RUN './e2e_test/source/kafka.slt'
rm -rf .risingwave

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, streaming"
$RUN -j 16 './e2e_test/streaming/\*\*/\*.slt'
rm -rf .risingwave

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, batch"
$RUN -j 16 './e2e_test/batch/\*\*/\*.slt'
rm -rf .risingwave

echo "--- deterministic simulation e2e, ci-3cn-1fe, fuzzing"
$RUN --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata
rm -rf .risingwave
