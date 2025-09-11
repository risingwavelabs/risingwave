#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Set openssl static link env vars"
configure_static_openssl

echo "--- Build documentation"
RUSTDOCFLAGS="-Dwarnings" cargo doc --document-private-items --no-deps

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats

echo "--- Run doctest"
RUSTDOCFLAGS="-Clink-arg=-fuse-ld=lld" cargo test --doc

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats
