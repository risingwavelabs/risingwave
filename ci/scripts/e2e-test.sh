#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
source ci/scripts/pr.env.sh
source ci/scripts/run-e2e-test.sh
