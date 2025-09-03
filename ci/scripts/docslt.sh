#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Extract DocSlt end-to-end tests"
cargo run --bin risedev-docslt

echo "--- Upload generated end-to-end tests"
echo "PATH is: $PATH"
command -v buildkite-agent || echo "buildkite-agent not in PATH"

# Check artifacts exist
ls -lh target/ci-dev || true

tar --zstd -cvf e2e_test_generated.tar.zst e2e_test/generated
buildkite-agent artifact upload e2e_test_generated.tar.zst
