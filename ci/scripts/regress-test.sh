#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

echo "--- Download artifacts"
mkdir -p target/debug
buildkite-agent artifact download risingwave-"$profile" target/debug/
buildkite-agent artifact download risedev-dev-"$profile" target/debug/
buildkite-agent artifact download risingwave_regress_test-"$profile" target/debug/
mv target/debug/risingwave-"$profile" target/debug/risingwave
mv target/debug/risedev-dev-"$profile" target/debug/risedev-dev
mv target/debug/risingwave_regress_test-"$profile" target/debug/risingwave_regress_test

echo "--- Adjust permission"
chmod +x ./target/debug/risingwave
chmod +x ./target/debug/risedev-dev
chmod +x ./target/debug/risingwave_regress_test

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Prepare RiseDev dev cluster"
cargo make pre-start-dev
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
