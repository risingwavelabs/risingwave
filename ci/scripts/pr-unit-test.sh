#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
source ci/scripts/pr.env.sh
./ci/scripts/run-unit-test.sh
