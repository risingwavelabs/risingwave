#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Install required tools"
rustup default "$(cat ./rust-toolchain)" && rustup component add llvm-tools-preview clippy
cargo install cargo-llvm-cov

echo "--- Run clippy check"
cargo clippy --all-targets --features failpoints --locked -- -D warnings

echo "--- Build documentation"
cargo doc --document-private-items --no-deps

echo "--- Run unit tests with coverage"
cargo llvm-cov nextest --lcov --output-path lcov.info --features failpoints -- --no-fail-fast

echo "--- Run doctest"
cargo test --doc

echo "--- Codecov upload coverage reports"
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov
./codecov -t "$CODECOV_TOKEN" -s . -F rust