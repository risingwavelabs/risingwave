#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

export RUST_LOG="info,risingwave_simulation=debug"
export LOGDIR=.risingwave/log
mkdir -p $LOGDIR

echo "--- Download artifacts"
buildkite-agent artifact download simulation-it-test.tar.zst .

# Manually extract instead of using `cargo nextest run --archive-file` to ensure `cargo llvm-cov`
# can find the object files later.
echo "--- Extract artifacts"
tar -xvf simulation-it-test.tar.zst

if [[ -z "${1:-}" ]]; then
  TEST_PATTERN=""
elif [[ "$1" == "pull-request" ]]; then
  # exclude logstore tests
  TEST_PATTERN="-E 'not test(/^log_store/)'"
else
  TEST_PATTERN="-E 'test(/^$1/)'"
fi

# NOTE(kwannoel): for TEST_PATTERN, please consult: https://nexte.st/docs/filtersets/reference/.
# Using substring matching may run unexpected tests.
# For example, `scale::` will run all tests with a `scale` module prefix.
# This includes sink::scale::*, scale::*.
# We just want to run `scale::*`.

# `cargo-nextest` will spawn a process for each test. Avoid including `%p` in the file name pattern
# to avoid creating too many profraw files. Instead, use `%m` to reuse the profraw files for the
# same test binary. Here `%4m` means that there's a pool of 4 profraw files for each test binary.
# This achieves a good balance between the performance of running tests and generating reports.
# https://github.com/taiki-e/cargo-llvm-cov/issues/335#issuecomment-1890349373
export LLVM_PROFILE_FILE='/risingwave/target/risingwave-madsim-it-test-%4m.profraw'
export RW_COVERAGE_CARGO_PROFILE="ci-sim"

echo "--- Run integration tests in deterministic simulation mode"
seq "$TEST_NUM" | parallel -j 8 --line-buffer "MADSIM_TEST_SEED={} NEXTEST_PROFILE=ci-sim \
 cargo nextest run \
 $NEXTEST_PARTITION_ARG \
 --no-fail-fast \
 --cargo-metadata target/nextest/cargo-metadata.json \
 --binaries-metadata target/nextest/binaries-metadata.json \
 $TEST_PATTERN \
 2> $LOGDIR/it-test-{}.log && rm $LOGDIR/it-test-{}.log"
