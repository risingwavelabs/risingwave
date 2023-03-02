#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh
export RUN_SQLSMITH=1
export SQLSMITH_COUNT=1000
export TEST_NUM=100
source ci/scripts/run-fuzz-test.sh
