#!/usr/bin/env bash

set -euo pipefail

# Requirements:
# - s3 certs,
# - qa binary

# Example
# remove-micro-bench-results.sh
./qa ctl -I qa-infra.risingwave-cloud.xyz:8081 execution delete -i "$1"