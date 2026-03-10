#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

# Additional clippy and cargo check passes with release profile and alternative feature sets.
# These are run in main-cron only to keep PR CI fast.
# The primary clippy check (dev, all features) is in check.sh.

source ci/scripts/common.sh

echo "--- Set openssl static link env vars"
configure_static_openssl

echo "--- Run clippy check (dev, no connector)"
cargo clippy --all-targets --features rw-static-link --no-default-features --locked -- -D warnings

echo "--- Run clippy check (release)"
cargo clippy --release --all-targets --features "rw-static-link" --locked -- -D warnings

echo "--- Run cargo check on building the release binary (release)"
cargo check -p risingwave_cmd_all --features "rw-static-link" --profile release
cargo check -p risingwave_cmd --bin risectl --features "rw-static-link" --profile release

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats
