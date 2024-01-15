#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

REPO_ROOT=${PWD}

echo "+++ Run python UDF SDK unit tests"
cd ${REPO_ROOT}/src/expr/udf/python
python3 -m pytest
cd ${REPO_ROOT}

echo "+++ Run unit tests with coverage"
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo nextest run --features failpoints,sync_point --workspace --exclude risingwave_simulation

# echo "--- Codecov upload coverage reports"
# curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov
# ./codecov -t "$CODECOV_TOKEN" -s . -F rust
