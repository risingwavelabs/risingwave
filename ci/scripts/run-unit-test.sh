#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

REPO_ROOT=${PWD}

echo "+++ Run unit tests"
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo nextest run --features failpoints,sync_point --workspace --exclude risingwave_simulation

echo "+++ Run hummock memory limiter loom test"
LOOM_MAX_BRANCHES=100000 RUSTFLAGS="--cfg loom" cargo test -p risingwave_hummock_memory_limiter
