#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
source ci/scripts/run-unit-test.sh

echo "--- Show sccache stats"
sccache --show-stats
