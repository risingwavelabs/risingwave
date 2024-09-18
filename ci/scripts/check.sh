#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

# Check ci bash scripts contains `set -euo pipefail`.
for script in ci/**/*.sh; do
    # skip .env.sh and common.sh
    if [[ "$script" == *"common.sh" ]] || [[ "$script" == *".env.sh" ]]; then
        continue
    fi
    if ! grep -Fq 'set -euo pipefail' "$script"; then
        echo "ERROR: $script does not contain 'set -euo pipefail'"
        exit 1
    fi
done

source ci/scripts/common.sh

echo "--- Set openssl static link env vars"
export OPENSSL_STATIC=1
export OPENSSL_LIB_DIR="$(dpkg -L libssl-dev | grep libssl.a | xargs dirname)"
export OPENSSL_INCLUDE_DIR="$(dpkg -L libssl-dev | grep openssl/ssl.h | xargs dirname)"
echo "OPENSSL_STATIC: $OPENSSL_STATIC"
echo "OPENSSL_LIB_DIR: $OPENSSL_LIB_DIR"
echo "OPENSSL_INCLUDE_DIR: $OPENSSL_INCLUDE_DIR"

echo "--- Run trailing spaces check"
scripts/check/check-trailing-spaces.sh

echo "--- Run clippy check (dev, all features)"
cargo clippy --all-targets --all-features --locked -- -D warnings

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats

echo "--- Run clippy check (release)"
cargo clippy --release --all-targets --features "rw-static-link" --locked -- -D warnings

echo "--- Run cargo check on building the release binary (release)"
cargo check -p risingwave_cmd_all --features "rw-static-link" --profile release
cargo check -p risingwave_cmd --bin risectl --features "rw-static-link" --profile release

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats

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
