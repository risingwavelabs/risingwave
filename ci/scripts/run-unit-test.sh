#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

REPO_ROOT=${PWD}

source ci/scripts/common.sh
# Set the LLVM_PROFILE_FILE to avoid generate too many coverage data by not interpolating process id.
export LLVM_PROFILE_FILE='/risingwave/target/risingwave-unit-test-%8m.profraw'

echo "+++ Run unit tests"
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo nextest run --features failpoints,sync_point --workspace --exclude risingwave_simulation

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats
