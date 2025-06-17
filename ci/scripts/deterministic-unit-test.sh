#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
unset RW_BUILD_INSTRUMENT_COVERAGE # TODO: should use llvm-cov's nextest wrapper

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Run unit tests in deterministic simulation mode"
risedev stest --no-fail-fast
