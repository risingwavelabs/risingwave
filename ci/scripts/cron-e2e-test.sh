#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh
export RUN_SQLSMITH=1; # fuzz tests
source ci/scripts/run-e2e-test.sh
