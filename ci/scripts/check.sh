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

echo "--- Run trailing spaces check"
scripts/check/check-trailing-spaces.sh

echo "--- Run clippy check (dev, all features)"
cargo clippy --all-targets --all-features --locked -- -D warnings

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats

echo "--- Run clippy check (release)"
cargo clippy --release --all-targets --features "rw-static-link" --locked -- -D warnings

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

echo "--- Run audit check"
cargo audit \
  --ignore RUSTSEC-2023-0052 # https://github.com/risingwavelabs/risingwave/issues/11842
