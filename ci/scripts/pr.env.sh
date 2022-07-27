#!/bin/sh
set -euo pipefail

# Set features, depending on our workflow
# If sqlsmith files are modified, we run unit tests with sqlsmith enabled
# by overriding RUSTFLAGS to enable sqlsmith feature.
CHANGED=$(git diff --name-only origin/main | grep "ci/scripts/pr.env.sh\|src/tests/sqlsmith")
if [[ -n "$CHANGED" ]]; then
    echo "Configuring RUSTFLAGS for sqlsmith"
    export RUSTFLAGS="${RUSTFLAGS} --cfg enable_sqlsmith_unit_test";
    echo "Configured RUSTFLAGS for sqlsmith:"
    echo "RUSTFLAGS=${RUSTFLAGS}"
else
# Otherwise we use default.
    echo 'Running normal PR workflow';
fi
