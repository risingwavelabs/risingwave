#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

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
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Prepare RiseDev playground"
cargo make pre-start-playground
cargo make link-all-in-one-binaries

echo "--- Postgres regress test"
apt-get update -yy
apt-get -y install locales
locale-gen C
export LANGUAGE=C
export LANG=C
export LC_ALL=C
export LC_COLLATE=C
locale-gen C
dpkg-reconfigure --frontend=noninteractive locales
# All the above is required because otherwise psql would throw some warning
# that goes into the output file and thus diverges from the expected output file.
export PGPASSWORD='postgres';
RUST_BACKTRACE=1 target/debug/risingwave_regress_test -h db \
  -p 5432 \
  -u postgres \
  --database postgres \
  --input `pwd`/src/tests/regress/data \
  --output `pwd`/src/tests/regress/output \
  --schedule `pwd`/src/tests/regress/data/schedule \
  --mode postgres

echo "--- ci-3cn-1fe, RisingWave regress test"
rm -rf `pwd`/src/tests/regress/output
cargo make ci-start ci-3cn-1fe
RUST_BACKTRACE=1 target/debug/risingwave_regress_test -h 127.0.0.1 \
  -p 4566 \
  -u root \
  --input `pwd`/src/tests/regress/data \
  --output `pwd`/src/tests/regress/output \
  --schedule `pwd`/src/tests/regress/data/schedule \
  --mode risingwave

echo "--- Kill cluster"
cargo make ci-kill
