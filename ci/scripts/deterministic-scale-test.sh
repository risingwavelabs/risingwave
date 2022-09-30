#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Download artifacts"
buildkite-agent artifact download scale-test.tar.zst .

echo "--- Run scaling tests in deterministic simulation mode"
MADSIM_TEST_NUM=5 cargo nextest run --archive-file scale-test.tar.zst --no-fail-fast
