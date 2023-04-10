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

echo "--- Run clippy check (dev, all features)"
cargo clippy --all-targets --all-features --locked -- -D warnings

echo "--- Run clippy check (release)"
cargo clippy --release  --all-targets --features "static-link static-log-level" --locked -- -D warnings

echo "--- Build documentation"
cargo doc --document-private-items --no-deps

echo "--- Run doctest"
cargo test --doc

echo "--- Run audit check"
cargo audit
