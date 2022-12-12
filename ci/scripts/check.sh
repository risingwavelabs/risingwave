#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Run clippy check (dev, all features)"
cargo clippy --all-targets --all-features --locked -- -D warnings

echo "--- Run clippy check (release)"
cargo clippy --release  --all-targets --features "static-link static-log-level" --locked -- -D warnings

echo "--- Build documentation"
cargo doc --document-private-items --no-deps

echo "--- Run doctest"
cargo test --doc
