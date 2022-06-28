#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "+++ Running deterministic simulation test"
echo "$(tput setaf 3)This test won't compile because madsim doesn't support tokio::net yet. Tracking issue: https://github.com/singularity-data/risingwave/issues/3467$(tput sgr0)"

echo "--- Generate RiseDev CI config"
cp risedev-components.ci.env risedev-components.user.env

echo "--- Run unit tests in deterministic simulation mode"
cargo make stest \
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
