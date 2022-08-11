#!/bin/sh
set -euo pipefail

set +e
# Set features, depending on our workflow
# If sqlsmith files are modified, we run tests with sqlsmith enabled.
MATCHES="ci/scripts/pr.env.sh\
\|ci/scripts/run-e2e-test.sh\
\|ci/scripts/run-unit-test.sh\
\|src/tests/sqlsmith"
CHANGED=$(git diff --name-only origin/main | grep "$MATCHES")
set -e

if [[ -n "$CHANGED" ]]; then
    echo "Changes to Sqlsmith source files detected.";
    export RUN_SQLSMITH=1;
    echo "Enabled Sqlsmith tests.";
else
# Otherwise we use default.
    export RUN_SQLSMITH=0;
fi
