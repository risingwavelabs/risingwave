#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
export RUN_COMPACTION=0;
source ci/scripts/run-e2e-test.sh
