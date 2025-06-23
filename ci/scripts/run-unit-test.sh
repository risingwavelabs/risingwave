#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

REPO_ROOT=${PWD}

source ci/scripts/common.sh

# Enable coverage instrumentation.
# `cargo-nextest` will spawn a process for each test. Avoid including `%p` in the file name pattern
# to avoid creating too many profraw files.
# https://github.com/taiki-e/cargo-llvm-cov/issues/335#issuecomment-1890349373
export RW_BUILD_INSTRUMENT_COVERAGE=1
export LLVM_PROFILE_FILE='/risingwave/target/risingwave-unit-test-%2m.profraw'

echo "+++ Run unit tests"
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo nextest run --features failpoints,sync_point --workspace --exclude risingwave_simulation

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats
