#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Build deterministic simulation e2e test runner"
cargo make sslt-build-all --profile ci-sim

echo "--- Build and archive deterministic scaling simulation tests"
NEXTEST_PROFILE=ci-scaling cargo make sarchive-scale-test --cargo-profile ci-sim

echo "--- Upload artifacts"
mv target/sim/ci-sim/risingwave_simulation ./risingwave_simulation

artifacts=(risingwave_simulation scale-test.tar.zst)
echo -n "${artifacts[*]}" | parallel -d ' ' "buildkite-agent artifact upload ./{}"

echo "--- Show sccache stats"
sccache --show-stats
