#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

echo "--- Install required tools"
rustup default "$(cat ./rust-toolchain)"

echo "--- Generate RiseDev CI config"
cp risedev-components.ci.env risedev-components.user.env

echo "--- Run unit tests in deterministic simulation mode"
~/cargo-make/makers stest \
    --no-fail-fast \
    -p risingwave_batch \
    -p risingwave_common \
    -p risingwave_compute \
    -p risingwave_connector \
    -p risingwave_ctl \
    -p risingwave_expr \
    -p risingwave_meta \
    -p risingwave_source \
    -p risingwave_storage \
    -p risingwave_stream
