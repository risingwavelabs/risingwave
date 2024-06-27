#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

# Don't run e2e compaction test in PR build
export RUN_COMPACTION=0;