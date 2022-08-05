#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Build deterministic simulation e2e test runner"
cargo make sslt --release -- --help

echo "--- deterministic simulation e2e, ci-3cn-1fe, ddl"
cargo make sslt --release -- './e2e_test/ddl/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, streaming"
cargo make sslt --release -- './e2e_test/streaming/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, streaming_delta_join"
cargo make sslt --release -- './e2e_test/streaming_delta_join/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, batch"
cargo make sslt --release -- './e2e_test/batch/**/*.slt'
