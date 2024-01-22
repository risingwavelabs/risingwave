#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
unset RUSTC_WRAPPER

echo "--- Run dylint check (dev, all features)"
cargo dylint --all -- --all-targets --all-features --locked
