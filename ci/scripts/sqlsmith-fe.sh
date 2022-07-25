#!/bin/bash
set -euo pipefail

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "+++ Run sqlsmith frontend tests"
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo nextest run --package risingwave_sqlsmith 2> >(tee)
