#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Run clippy check"
cargo clippy --release --features "static-link static-log-level" --all-targets --locked -- -D warnings

echo "--- Build documentation"
cargo doc --document-private-items --no-deps

echo "--- Run doctest"
cargo test --doc