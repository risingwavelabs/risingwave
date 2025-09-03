#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Extract DocSlt end-to-end tests"
cargo run --bin risedev-docslt

echo "--- Upload generated end-to-end tests"

echo "---- Debug: environment ----"
echo "USER: $(whoami)"
echo "PWD: $(pwd)"
echo "PATH: $PATH"
echo "Which buildkite-agent (command -v): $(command -v buildkite-agent || true)"
echo "buildkite-agent --version output:" && (buildkite-agent --version || true)
echo "ls -l $(pwd)"


tar --zstd -cvf e2e_test_generated.tar.zst e2e_test/generated
buildkite-agent artifact upload e2e_test_generated.tar.zst
