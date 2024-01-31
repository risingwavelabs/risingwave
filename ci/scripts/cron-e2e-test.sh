#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
export RUN_COMPACTION=0;
export RUN_META_BACKUP=1;
# NOTE(wenym): Delete range is disabled so its tests will fail.
# So we just disable the delete range test for now.
export RUN_DELETE_RANGE=0;
source ci/scripts/run-e2e-test.sh
