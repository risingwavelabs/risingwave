#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

REPO_ROOT=${PWD}

echo "+++ Run python UDF SDK unit tests"
cd ${REPO_ROOT}/src/udf/python
pip3 install pytest # pyarrow
python3 -m pytest
cd ${REPO_ROOT}

echo "+++ Run unit tests with coverage"
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo llvm-cov nextest --lcov --output-path lcov.info --features failpoints,sync_point 2> >(tee);

echo "--- Codecov upload coverage reports"
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov
./codecov -t "$CODECOV_TOKEN" -s . -F rust