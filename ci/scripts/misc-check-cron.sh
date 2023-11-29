#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# RUSTSEC-2023-0052: https://github.com/risingwavelabs/risingwave/issues/11842
# RUSTSEC-2023-0071 https://github.com/risingwavelabs/risingwave/issues/13703
echo "--- Run audit check"
cargo audit \
  --ignore RUSTSEC-2023-0052 \
  --ignore RUSTSEC-2023-0071
