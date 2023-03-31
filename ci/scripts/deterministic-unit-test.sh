#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Run unit tests in deterministic simulation mode"
cargo make stest --no-fail-fast
