#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

set +e
# Set features, depending on our workflow
# If sqlsmith files are modified, we run tests with sqlsmith enabled.
MATCHES="ci/scripts/pr.env.sh\
\|ci/scripts/cron-fuzz-test.sh\
\|ci/scripts/pr-fuzz-test.sh\
\|ci/scripts/run-fuzz-test.sh\
\|src/tests/sqlsmith"
CHANGED=$(git diff --name-only origin/main | grep "$MATCHES")
set -e

if [[ -n "$CHANGED" ]]; then
    echo "origin/main SHA: $(git rev-parse origin/main)"
    echo "Changes to Sqlsmith source files detected:"
    echo "$CHANGED"

    export RUN_SQLSMITH=1
    export SQLSMITH_COUNT=100
    export TEST_NUM=32
    echo "Enabled Sqlsmith tests."
else
# Otherwise we use default.
    export RUN_SQLSMITH=0
fi

source ci/scripts/run-fuzz-test.sh
