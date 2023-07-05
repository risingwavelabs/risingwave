#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Check protobuf code format && Lint protobuf"
cd proto
buf format -d --exit-code
buf lint
