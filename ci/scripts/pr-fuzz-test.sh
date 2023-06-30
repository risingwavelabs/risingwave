#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

set +e
# Set features, depending on our workflow
# If sqlsmith files are modified, we run tests with sqlsmith enabled.
MATCHES="ci/scripts/cron-fuzz-test.sh\
\|ci/scripts/pr-fuzz-test.sh\
\|ci/scripts/run-fuzz-test.sh\
\|src/tests/sqlsmith"
NOT_MATCHES="\.md"
CHANGED=$(git diff --name-only origin/main | grep -v "$NOT_MATCHES" | grep "$MATCHES")
set -e

# Always run sqlsmith frontend tests
export RUN_SQLSMITH_FRONTEND=1
export RUN_SQLSMITH=1
export SQLSMITH_COUNT=100

# Run e2e tests if changes to sqlsmith source files detected.
# NOTE(kwannoel): Keep this here in-case we ever want to revert.
#if [[ -n "$CHANGED" ]]; then
#    echo "--- Checking whether to run all sqlsmith tests"
#    echo "origin/main SHA: $(git rev-parse origin/main)"
#    echo "Changes to Sqlsmith source files detected:"
#    echo "$CHANGED"
#    export RUN_SQLSMITH=1
#    export SQLSMITH_COUNT=100
#    export TEST_NUM=32
#    echo "Enabled Sqlsmith tests."
#else
#    export RUN_SQLSMITH=0
#fi

source ci/scripts/run-fuzz-test.sh
