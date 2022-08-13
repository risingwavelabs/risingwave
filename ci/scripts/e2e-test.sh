#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh
source ci/scripts/pr.env.sh
source ci/scripts/run-e2e-test.sh
