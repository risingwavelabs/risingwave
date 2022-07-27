#!/bin/sh
set -euo pipefail

set +e
# Set features, depending on our workflow
# If sqlsmith files are modified, we run unit tests with sqlsmith enabled
# by overriding RUSTFLAGS to enable sqlsmith feature.
CHANGED=$(git diff --name-only origin/main | grep "ci/scripts/pr.env.sh\|src/tests/sqlsmith")
set -e

if [[ -n "$CHANGED" ]]; then
    echo "Changes to Sqlsmith source files detected.";
    export RUN_SQLSMITH=1;
    echo "Enabled Sqlsmith tests.";
else
# Otherwise we use default.
    export RUN_SQLSMITH=0;
fi
