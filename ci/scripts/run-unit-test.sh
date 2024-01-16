#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

REPO_ROOT=${PWD}

echo "+++ Run python UDF SDK unit tests"
cd ${REPO_ROOT}/src/expr/udf/python
python3 -m pytest
cd ${REPO_ROOT}

echo "+++ Run unit tests"
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo nextest run --features failpoints,sync_point --workspace --exclude risingwave_simulation
