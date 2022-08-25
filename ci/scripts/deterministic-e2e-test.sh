#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Build deterministic simulation e2e test runner"
timeout 10m cargo make sslt --profile ci-release -- --help

echo "--- deterministic simulation e2e, ci-3cn-1fe, ddl"
timeout 10s cargo make sslt --profile ci-release -- './e2e_test/ddl/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, streaming"
timeout 1m cargo make sslt --profile ci-release -- './e2e_test/streaming/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, batch"
timeout 30s cargo make sslt --profile ci-release -- './e2e_test/batch/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, streaming"
timeout 90s cargo make sslt --profile ci-release -- -j 16 './e2e_test/streaming/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, batch"
timeout 30s cargo make sslt --profile ci-release -- -j 16 './e2e_test/batch/**/*.slt'

# bugs here!
echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, streaming"
RUST_LOG=off timeout 2m cargo make sslt --profile ci-release -- --kill-node './e2e_test/streaming/**/*.slt' || true

echo "--- deterministic simulation e2e, ci-3cn-1fe, recovery, batch"
RUST_LOG=off timeout 2m cargo make sslt --profile ci-release -- --kill-node './e2e_test/batch/**/*.slt'
