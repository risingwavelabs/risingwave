#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

# RUSTC_WORKSPACE_WRAPPER script that adds coverage-related rustflags
# for workspace members only when RW_BUILD_INSTRUMENT_COVERAGE is set.
# External dependencies won't get coverage flags because
# RUSTC_WORKSPACE_WRAPPER only applies to workspace members.
#
# Reference: https://github.com/rust-lang/cargo/issues/13040

if [[ "$1" == *rustc ]]; then
    # The first argument is the rustc executable path, respect it
    ACTUAL_RUSTC="$1"
    shift  # Remove the first argument (rustc path) from $@
else
    # Workaround for `sccache` does not work together with `RUSTC_WORKSPACE_WRAPPER`
    ACTUAL_RUSTC="rustc"
fi

# Only add coverage flags if RW_BUILD_INSTRUMENT_COVERAGE is set
if [[ "${RW_BUILD_INSTRUMENT_COVERAGE:-}" == "1" ]]; then
    exec "$ACTUAL_RUSTC" "$@" -C instrument-coverage --cfg coverage
else
    exec "$ACTUAL_RUSTC" "$@"
fi
