#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
unset RUSTC_WRAPPER # disable sccache, see https://github.com/mozilla/sccache/issues/861

echo "--- Run dylint check (dev, all features)"
# Instead of `-D warnings`, we only deny warnings from our own lints. This is because...
# - Warnings from `check` or `clippy` are already checked in `check.sh`.
# - The toolchain used for linting could be slightly different from the one used to
#   compile RisingWave. Warnings from `rustc` itself may produce false positives.
DYLINT_RUSTFLAGS="-A warnings -D rw_warnings" cargo dylint --all -- --all-targets --all-features --locked
