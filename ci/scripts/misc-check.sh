#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Check protobuf code format && Lint protobuf"
cd proto
buf format -d --exit-code
buf lint
