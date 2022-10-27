#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Download artifacts"
buildkite-agent artifact download risingwave_simulation .
chmod +x ./risingwave_simulation

export RUST_LOG=info

echo "--- deterministic simulation e2e, ci-3cn-1fe, ddl"
seq 16 | parallel --res .risingwave/log MADSIM_TEST_SEED={} ./risingwave_simulation './e2e_test/ddl/\*\*/\*.slt'
rm -rf .risingwave

echo "--- deterministic simulation e2e, ci-3cn-1fe, streaming"
seq 16 | parallel --res .risingwave/log MADSIM_TEST_SEED={} ./risingwave_simulation './e2e_test/streaming/\*\*/\*.slt'
rm -rf .risingwave

echo "--- deterministic simulation e2e, ci-3cn-1fe, batch"
seq 16 | parallel --res .risingwave/log MADSIM_TEST_SEED={} ./risingwave_simulation './e2e_test/batch/\*\*/\*.slt'
rm -rf .risingwave

echo "--- deterministic simulation e2e, ci-3cn-1fe, kafka source"
seq 16 | parallel --res .risingwave/log MADSIM_TEST_SEED={} ./risingwave_simulation './e2e_test/source/kafka.slt'
rm -rf .risingwave

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, streaming"
seq 16 | parallel --res .risingwave/log MADSIM_TEST_SEED={} ./risingwave_simulation -j 16 './e2e_test/streaming/\*\*/\*.slt'
rm -rf .risingwave

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, batch"
seq 16 | parallel --res .risingwave/log MADSIM_TEST_SEED={} ./risingwave_simulation -j 16 './e2e_test/batch/\*\*/\*.slt'
rm -rf .risingwave

echo "--- deterministic simulation e2e, ci-3cn-1fe, fuzzing"
seq 16 | parallel --res .risingwave/log MADSIM_TEST_SEED={} ./risingwave_simulation --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata
rm -rf .risingwave
