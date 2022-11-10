#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh
export RUN_SQLSMITH=1; # fuzz tests
export SQLSMITH_COUNT=1000; # fuzz with 1000 queries
export RUN_COMPACTION=1;
source ci/scripts/run-e2e-test.sh
