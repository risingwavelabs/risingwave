#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Run dylint check (dev, all features)"
cargo dylint --all -- --all-targets --all-features --locked

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats
