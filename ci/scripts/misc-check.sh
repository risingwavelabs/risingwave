#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Check protobuf code format && Lint protobuf"
pushd proto >/dev/null
buf format -d --exit-code
buf lint
popd >/dev/null

echo "--- Check trailing spaces"
ci/scripts/check-trailing-spaces.sh
