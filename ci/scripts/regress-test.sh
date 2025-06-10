#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

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

download_and_prepare_rw "$profile" common

echo "--- Download artifacts"
download-and-decompress-artifact risingwave_regress_test-"$profile" target/debug/
mv target/debug/risingwave_regress_test-"$profile" target/debug/risingwave_regress_test

chmod +x ./target/debug/risingwave_regress_test

echo "--- Postgres regress test"
locale-gen C
export LANGUAGE=C
export LANG=C
export LC_ALL=C
export LC_COLLATE=C
locale-gen C
dpkg-reconfigure --frontend=noninteractive locales
# All the above is required because otherwise psql would throw some warning
# that goes into the output file and thus diverges from the expected output file.
export PGPASSWORD='post\tgres';

# Load extensions. This shall only be done once per database, so not part of test runner.
psql -h db -p 5432 -d postgres -U postgres \
  -c 'create extension pgcrypto;' \
  -c 'create extension hstore;' \
  -c 'create extension tablefunc;'

RUST_BACKTRACE=1 target/debug/risingwave_regress_test --host db \
  -p 5432 \
  -u postgres \
  --database postgres \
  --input $(pwd)/src/tests/regress/data \
  --output $(pwd)/src/tests/regress/output \
  --schedule $(pwd)/src/tests/regress/data/schedule \
  --mode postgres

echo "--- ci-3cn-1fe, RisingWave regress test"
rm -rf $(pwd)/src/tests/regress/output
risedev ci-start ci-3cn-1fe
RUST_BACKTRACE=1 target/debug/risingwave_regress_test --host 127.0.0.1 \
  -p 4566 \
  -u root \
  --input $(pwd)/src/tests/regress/data \
  --output $(pwd)/src/tests/regress/output \
  --schedule $(pwd)/src/tests/regress/data/schedule \
  --mode risingwave

echo "--- Kill cluster"
risedev ci-kill
