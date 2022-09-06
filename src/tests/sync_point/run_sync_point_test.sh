#!/bin/bash
# On macOS: brew install coreutils

set -e

SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
RW_WORKSPACE=$(realpath "${SCRIPT_PATH}"/../../../)
export RW_WORKSPACE=${RW_WORKSPACE}
RUSTFLAGS='--cfg tokio_unstable --cfg sync_point_test'
export RUSTFLAGS=${RUSTFLAGS}

cp -R -n "${RW_WORKSPACE}"/e2e_test "${SCRIPT_PATH}"/slt/ || :

cargo test -p risingwave_sync_point_test
