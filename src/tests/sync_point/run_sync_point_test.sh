#!/bin/bash

set -e

SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
RW_WORKSPACE=$(realpath "${SCRIPT_PATH}"/../../../)
export RW_WORKSPACE=${RW_WORKSPACE}
RUSTFLAGS='--cfg tokio_unstable --cfg sync_point_test'
export RUSTFLAGS=${RUSTFLAGS}

cp -R -n "${RW_WORKSPACE}"/e2e_test "${SCRIPT_PATH}"/slt/ || :

# cargo test -p risingwave_sync_point_test
# A workaround before we can wipe object store before each test. Because all tests are sharing the same shared in-mem object store.
cargo test -p risingwave_sync_point_test -- --list --format=terse| awk '/: test/{print substr($1, 1, length($1)-1)}' |xargs -I{} cargo test -p risingwave_sync_point_test {}
