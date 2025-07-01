#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
# TODO: use llvm-cov's nextest wrapper to provide coverage information

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Run unit tests in deterministic simulation mode"
risedev stest --no-fail-fast
