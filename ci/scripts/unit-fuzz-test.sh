#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh
export RUSTFLAGS="${RUSTFLAGS} --cfg enable_sqlsmith_unit_test";
source ci/scripts/run-unit-test.sh
