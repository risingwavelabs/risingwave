#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
source ci/scripts/pr.env.sh

# Set RUST_MIN_STACK to 8MB to avoid stack overflow in planner test.
# This is a Unit Test specific setting.
export RUST_MIN_STACK=8388608
./ci/scripts/run-unit-test.sh
