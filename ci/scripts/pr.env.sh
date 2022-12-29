#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

set +e
# Set features, depending on our workflow
# If sqlsmith files are modified, we run tests with sqlsmith enabled.
MATCHES="ci/scripts/pr.env.sh\
\|ci/scripts/pr-fuzz-test.sh\
\|ci/scripts/run-e2e-test.sh\
\|ci/scripts/run-fuzz-test.sh\
\|src/tests/sqlsmith"
CHANGED=$(git diff --name-only origin/main | grep "$MATCHES")
set -e

# Don't run e2e compaction test in PR build
export RUN_COMPACTION=0;
# Don't run meta store backup/recovery test
export RUN_META_BACKUP=0;
# Don't run delete-range random test
export RUN_DELETE_RANGE=0;

if [[ -n "$CHANGED" ]]; then
    echo "origin/main SHA: $(git rev-parse origin/main)";
    echo "Changes to Sqlsmith source files detected:";
    echo "$CHANGED";

    export RUN_SQLSMITH=1;
    export SQLSMITH_COUNT=100;
    echo "Enabled Sqlsmith tests.";
else
# Otherwise we use default.
    export RUN_SQLSMITH=0;
fi
