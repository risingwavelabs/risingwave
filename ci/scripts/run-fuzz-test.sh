#!/bin/bash
set -euo pipefail

if [[ "$RUN_SQLSMITH" -eq "1" ]]; then
    echo "+++ Run sqlsmith tests"
    NEXTEST_PROFILE=ci cargo nextest run run_sqlsmith_on_frontend --features "failpoints sync_point enable_sqlsmith_unit_test" 2> >(tee);
fi
