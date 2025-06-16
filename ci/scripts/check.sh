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
unset RW_INSTRUMENT_COVERAGE

echo "--- Set openssl static link env vars"
configure_static_openssl

echo "--- Run trailing spaces check"
scripts/check/check-trailing-spaces.sh

echo "--- Check protobuf code format && Lint protobuf"
cd proto
buf format -d --exit-code
buf lint
cd ..

echo "--- Rust cargo-sort check"
cargo sort --check --workspace --grouped

# Disable hakari until we make sure it's useful
# echo "--- Rust cargo-hakari check"
# cargo hakari generate --diff
# cargo hakari verify

echo "--- Rust format check"
cargo fmt --all -- --check

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

echo "--- Check unused dependencies"
cargo machete
