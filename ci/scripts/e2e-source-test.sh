#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

echo "--- Download artifacts"
mkdir -p target/debug
buildkite-agent artifact download risingwave-dev target/debug/
buildkite-agent artifact download risedev-playground-dev target/debug/
buildkite-agent artifact download risingwave_regress_test-dev target/debug/
mv target/debug/risingwave-dev target/debug/risingwave
mv target/debug/risedev-playground-dev target/debug/risedev-playground
mv target/debug/risingwave_regress_test-dev target/debug/risingwave_regress_test

echo "--- Adjust permission"
chmod +x ./target/debug/risingwave
chmod +x ./target/debug/risedev-playground
chmod +x ./target/debug/risingwave_regress_test

echo "--- Generate RiseDev CI config"
cp risedev-components.ci.env risedev-components.user.env

echo "--- Prepare RiseDev playground"
~/cargo-make/makers pre-start-playground
~/cargo-make/makers link-all-in-one-binaries

echo "--- e2e test w/ Rust frontend - source with kafka"
~/cargo-make/makers clean-data
~/cargo-make/makers ci-start ci-kafka
./scripts/source/prepare_ci_kafka.sh
timeout 2m sqllogictest -p 4566 -d dev './e2e_test/source/**/*.slt'