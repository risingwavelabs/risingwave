#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# NOTE(kwannoel): Disabled because there's some breakage after #12485,
# see https://github.com/risingwavelabs/risingwave/issues/12577.
# Frontend is relatively stable, e2e fuzz test will cover the same cases also,
# so we can just disable it.
export RUN_SQLSMITH_FRONTEND=0
export RUN_SQLSMITH=1
export SQLSMITH_COUNT=1000
export TEST_NUM=100
source ci/scripts/run-fuzz-test.sh
