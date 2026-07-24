#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# PR fuzz test: reduced scope (100 queries, no extended iterations)
export RUN_SQLSMITH_FRONTEND=0
export RUN_SQLSMITH=1
export SQLSMITH_COUNT=100

source ci/scripts/run-fuzz-test.sh
