#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Build deterministic simulation e2e test runner"
cargo make sslt-build-all --profile ci-sim --timings

echo "--- Build and archive deterministic simulation integration tests"
NEXTEST_PROFILE=ci-sim cargo make sarchive-it-test --cargo-profile ci-sim

echo "--- Upload artifacts"
mv target/sim/ci-sim/risingwave_simulation ./risingwave_simulation

artifacts=(risingwave_simulation simulation-it-test.tar.zst)
echo -n "${artifacts[*]}" | parallel -d ' ' "buildkite-agent artifact upload ./{}"

buildkite-agent artifact upload target/cargo-timings/cargo-timing.html

echo "--- Show sccache stats"
sccache --show-stats
