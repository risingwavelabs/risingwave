#!/usr/bin/env bash
set -euo pipefail

echo "+++ Setup env"
source ci/scripts/common.env.sh

echo "+++ Run sqlsmith frontend tests"
NEXTEST_PROFILE=ci cargo nextest run run_sqlsmith_on_frontend --features "failpoints enable_sqlsmith_unit_test" 2> >(tee);

echo "+++ e2e, ci-3cn-1fe, fuzzing"
buildkite-agent artifact download sqlsmith-"$profile" target/debug/
mv target/debug/sqlsmith-"$profile" target/debug/sqlsmith
chmod +x ./target/debug/sqlsmith

cargo make ci-start ci-3cn-1fe
timeout 20m ./target/debug/sqlsmith test --testdata ./src/tests/sqlsmith/tests/testdata

echo "+++ Kill cluster"
# Using `kill` instead of `ci-kill` avoids storing excess logs.
# If there's errors, the failing query will be printed to stderr.
# We can use that to reproduce logs on local machine.
cargo make kill
