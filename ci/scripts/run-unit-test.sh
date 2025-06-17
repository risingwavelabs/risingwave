#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

REPO_ROOT=${PWD}

source ci/scripts/common.sh
unset RW_BUILD_INSTRUMENT_COVERAGE # TODO: should use llvm-cov's nextest wrapper

echo "+++ Run unit tests"
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo nextest run --features failpoints,sync_point --workspace --exclude risingwave_simulation

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats
