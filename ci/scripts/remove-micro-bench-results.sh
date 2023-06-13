#!/usr/bin/env bash

set -euo pipefail

# Requirements:
# - s3 certs,
# - qa binary

# Example
# remove-micro-bench-results.sh

./qa ctl -I 52.207.243.214:8081 execution delete -i "$1"