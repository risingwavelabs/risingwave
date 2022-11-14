#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Extract DocSlt end-to-end tests"
cargo run --bin risedev-docslt

echo "--- Upload generated end-to-end tests"
buildkite-agent artifact upload "e2e_test/generated/**/*"
