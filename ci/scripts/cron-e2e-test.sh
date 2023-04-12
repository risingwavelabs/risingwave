#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
export RUN_COMPACTION=1;
export RUN_META_BACKUP=1;
export RUN_DELETE_RANGE=1;
source ci/scripts/run-e2e-test.sh
