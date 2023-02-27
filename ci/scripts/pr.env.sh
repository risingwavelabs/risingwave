#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

# Don't run e2e compaction test in PR build
export RUN_COMPACTION=0;
# Don't run meta store backup/recovery test
export RUN_META_BACKUP=0;
# Don't run delete-range random test
export RUN_DELETE_RANGE=0;