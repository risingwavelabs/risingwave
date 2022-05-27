#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

echo "--- Check protobuf code format && Lint protobuf"
cd proto
buf format -d --exit-code
buf lint

