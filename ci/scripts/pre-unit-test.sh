#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Run clippy check"
cargo clippy --locked -- -D warnings

echo "--- Run clippy check w/ all targets and features"
cargo clippy --all-targets --features failpoints,sync_point --locked -- -D warnings

echo "--- Build documentation"
cargo doc --document-private-items --no-deps

echo "--- Run doctest"
cargo test --doc
