#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Extract DocSlt end-to-end tests"
cargo run --bin risedev-docslt

echo "--- Upload generated end-to-end tests"
tar --zstd -cvf e2e_test_generated.tar.zst e2e_test/generated
buildkite-agent artifact upload e2e_test_generated.tar.zst
