#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

REPO_ROOT=${PWD}

source ci/scripts/common.sh

# Enable coverage instrumentation only for ci-dev (PR workflow),
# as ci-release (main-cron workflow) has heavier workload.
if [[ "$profile" == "ci-dev" ]]; then
    export RW_BUILD_INSTRUMENT_COVERAGE=1
fi

# `cargo-nextest` will spawn a process for each test. Avoid including `%p` in the file name pattern
# to avoid creating too many profraw files. Instead, use `%m` to reuse the profraw files for the
# same test binary. Here `%4m` means that there's a pool of 4 profraw files for each test binary.
# This achieves a good balance between the performance of running tests and generating reports.
# https://github.com/taiki-e/cargo-llvm-cov/issues/335#issuecomment-1890349373
export LLVM_PROFILE_FILE='/risingwave/target/risingwave-unit-test-%4m.profraw'

echo "+++ Run unit tests"
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo nextest run --features failpoints,sync_point --workspace --exclude risingwave_simulation

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats
