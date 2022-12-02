#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Check release mod"
cargo check --release --features "static-link static-log-level"

echo "--- Run clippy check"
cargo clippy --all-targets --all-features --locked -- -D warnings

echo "--- Build documentation"
cargo doc --document-private-items --no-deps

echo "--- Run doctest"
