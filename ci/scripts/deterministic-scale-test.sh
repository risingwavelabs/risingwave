#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Run scaling tests in deterministic simulation mode"
MADSIM_TEST_NUM=5 cargo make stest-scale --no-fail-fast
